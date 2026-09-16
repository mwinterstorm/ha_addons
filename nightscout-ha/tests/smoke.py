#!/usr/bin/env python3
"""Disposable Docker integration test. Never connects to a real glucose database."""
import hashlib
import json
import pathlib
import secrets
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

root = pathlib.Path(__file__).resolve().parents[1]
prefix = 'ns-ha-test-' + secrets.token_hex(4)
network, mongo, nightscout = prefix + '-net', prefix + '-db', prefix + '-web'
secret, password = secrets.token_hex(20), secrets.token_hex(20)

def docker(*args):
    return subprocess.check_output(['docker', *args], text=True).strip()

def request(port, path, data=None, authenticated=True):
    headers = {'Content-Type': 'application/json'}
    if authenticated:
        headers['api-secret'] = hashlib.sha1(secret.encode()).hexdigest()
    req = urllib.request.Request(f'http://127.0.0.1:{port}{path}',
        data=json.dumps(data).encode() if data is not None else None, headers=headers)
    with urllib.request.urlopen(req, timeout=5) as response:
        return json.load(response)

def wait_ready(port):
    for _ in range(120):
        try:
            request(port, '/api/v1/entries.json')
            return
        except (OSError, ValueError):
            time.sleep(2)
    raise RuntimeError('Nightscout did not become ready')

try:
    docker('build', '-t', prefix + '-mongo-image', str(root / 'mongodb'))
    docker('build', '-t', prefix + '-ns-image', str(root / 'nightscout'))
    docker('network', 'create', network)
    docker('volume', 'create', prefix + '-data')
    with tempfile.TemporaryDirectory() as tmp:
        tmp = pathlib.Path(tmp)
        db_options = tmp / 'db.json'
        ns_options = tmp / 'ns.json'
        db_options.write_text(json.dumps({'password': password, 'cache_size_gb': 0.5}))
        options = {'api_secret': secret, 'mongo_host': mongo, 'mongo_password': password,
            'display_units': 'mmol/L', 'auth_default_roles': 'status-only', 'enable': 'careportal', 'extra_env': []}
        ns_options.write_text(json.dumps(options))
        docker('run', '-d', '--init', '--name', mongo, '--network', network,
            '-v', prefix + '-data:/data', '-v', f'{db_options}:/data/options.json:ro', prefix + '-mongo-image')
        docker('run', '-d', '--init', '--name', nightscout, '--network', network,
            '-p', '127.0.0.1::1337', '-v', f'{ns_options}:/data/options.json:ro', prefix + '-ns-image')
        port = docker('port', nightscout, '1337/tcp').split(':')[-1]
        wait_ready(port)
        try:
            request(port, '/api/v1/entries.json', authenticated=False)
            raise AssertionError('Unauthenticated glucose read succeeded')
        except urllib.error.HTTPError as e:
            assert e.code in (401, 403), e.code
        entry = {'type': 'sgv', 'sgv': 123, 'date': int(time.time() * 1000),
                 'dateString': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                 'direction': 'Flat', 'device': prefix}
        request(port, '/api/v1/entries.json', [entry])
        assert any(e.get('device') == prefix for e in request(port, '/api/v1/entries.json'))
        docker('stop', '-t', '60', nightscout)
        docker('stop', '-t', '120', mongo)
        docker('rm', '-v', mongo)
        docker('run', '-d', '--init', '--name', mongo, '--network', network,
            '-v', prefix + '-data:/data', '-v', f'{db_options}:/data/options.json:ro', prefix + '-mongo-image')
        docker('start', nightscout)
        wait_ready(port)
        assert any(e.get('device') == prefix for e in request(port, '/api/v1/entries.json'))
        # Invalid password must not silently rotate or discard an existing database.
        docker('stop', '-t', '60', nightscout)
        docker('stop', '-t', '120', mongo)
        db_options.write_text(json.dumps({'password': secrets.token_hex(20), 'cache_size_gb': 0.5}))
        docker('start', mongo)
        for _ in range(30):
            if docker('inspect', '-f', '{{.State.Running}}', mongo) == 'false':
                break
            time.sleep(1)
        assert docker('inspect', '-f', '{{.State.ExitCode}}', mongo) == '1'
        assert 'Password differs' in docker('logs', mongo)
        print('PASS: authenticated upload/read, denied anonymous read, container-replacement persistence, password-change refusal')
finally:
    for name in [nightscout, mongo]:
        subprocess.run(['docker', 'rm', '-f', '-v', name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for args in [('volume', 'rm', prefix + '-data'), ('network', 'rm', network),
                 ('image', 'rm', prefix + '-mongo-image', prefix + '-ns-image')]:
        subprocess.run(['docker', *args], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
