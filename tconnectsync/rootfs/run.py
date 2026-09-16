"""Translate Supervisor options, drop privileges, then exec unmodified upstream."""
import json
import os
from pathlib import Path
import re
import sys
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

FEATURES = {'BASAL', 'BOLUS', 'PUMP_EVENTS', 'PROFILES', 'CGM', 'IOB',
            'PUMP_EVENTS_BASAL_SUSPENSION', 'CGM_ALERTS', 'DEVICE_STATUS'}


def configuration(options):
    def text(key, minimum=1):
        value = options.get(key)
        if not isinstance(value, str) or len(value) < minimum or any(c in value for c in '\r\n\0'):
            raise ValueError(f'Invalid {key}')
        return value

    email = text('tconnect_email')
    if '@' not in email or email.strip() != email:
        raise ValueError('Invalid tconnect_email')
    password = text('tconnect_password')
    secret = text('nightscout_api_secret', 12)
    if secret.strip() != secret:
        raise ValueError('Invalid nightscout_api_secret')
    region = options.get('tconnect_region')
    if region not in ('US', 'EU'):
        raise ValueError('tconnect_region must be US or EU')
    url = text('nightscout_url')
    try:
        parsed = urlsplit(url)
        port = parsed.port
        if (parsed.scheme not in ('http', 'https') or not parsed.hostname
                or parsed.username is not None or parsed.password is not None
                or parsed.query or parsed.fragment or any(c.isspace() for c in url)
                or port == 0):
            raise ValueError()
    except ValueError:
        raise ValueError('Invalid nightscout_url') from None
    timezone = text('timezone')
    try:
        ZoneInfo(timezone)
    except (ZoneInfoNotFoundError, ValueError):
        raise ValueError('Invalid timezone; use a name such as Pacific/Auckland') from None
    serial = options.get('pump_serial_number', '')
    if not isinstance(serial, str) or not re.fullmatch(r'[0-9]*', serial):
        raise ValueError('pump_serial_number must contain digits only, or be empty')
    features = options.get('features')
    if (not isinstance(features, list) or not features
            or any(not isinstance(f, str) or f not in FEATURES for f in features)
            or len(features) != len(set(features))):
        raise ValueError('Select one or more supported, non-duplicate features')
    interval = options.get('poll_interval_seconds')
    if type(interval) is not int or not 300 <= interval <= 3600:
        raise ValueError('poll_interval_seconds must be between 300 and 3600')
    mode = options.get('mode')
    if mode not in ('sync', 'preview', 'check_login'):
        raise ValueError('Invalid mode')
    env = {
        'PATH': '/usr/local/bin:/usr/bin:/bin', 'HOME': '/data/home',
        'LANG': 'C.UTF-8', 'PYTHONUNBUFFERED': '1', 'PYTHONDONTWRITEBYTECODE': '1',
        'TCONNECT_EMAIL': email, 'TCONNECT_PASSWORD': password, 'TCONNECT_REGION': region,
        'NS_URL': url.rstrip('/'), 'NS_SECRET': secret, 'TIMEZONE_NAME': timezone, 'TZ': timezone,
        'AUTOUPDATE_DEFAULT_SLEEP_SECONDS': str(interval),
        'AUTOUPDATE_USE_FIXED_SLEEP': 'true',
    }
    # Do not set CACHE_CREDENTIALS: v3.0.0 erroneously uses that same variable
    # for both the boolean and cache path. HOME supplies the correct default path.
    if serial:
        env['PUMP_SERIAL_NUMBER'] = serial
    args = ['--features', *features]
    if mode == 'check_login':
        args += ['--check-login']
    else:
        args += ['--auto-update']
        if mode == 'preview':
            args += ['--pretend']
    return env, args


def main():
    try:
        options = json.loads(Path('/data/options.json').read_text())
    except (OSError, ValueError):
        raise ValueError('Cannot read /data/options.json') from None
    if not isinstance(options, dict):
        raise ValueError('Options must be a JSON object')
    env, args = configuration(options)
    os.umask(0o077)
    # Only the state directory is writable by the application; options stay root-owned.
    os.chown('/data', 0, 1000)
    os.chmod('/data', 0o710)
    home = Path('/data/home')
    home.mkdir(mode=0o700, exist_ok=True)
    os.chown(home, 1000, 1000)
    os.chmod(home, 0o700)
    os.setgroups([])
    os.setgid(1000)
    os.setuid(1000)
    os.chdir(home)
    print('Starting tconnectsync in ' + options['mode'] + ' mode; credentials withheld.', flush=True)
    os.execve(sys.executable, [sys.executable, '-u', '/opt/upstream/main.py', *args], env)


if __name__ == '__main__':
    try:
        main()
    except ValueError as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
