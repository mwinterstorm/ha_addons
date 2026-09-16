import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('launcher', Path(__file__).parents[1] / 'rootfs/run.py')
launcher = importlib.util.module_from_spec(spec)
spec.loader.exec_module(launcher)


def options():
    return dict(tconnect_email='test@example.com', tconnect_password='literal $password',
                tconnect_region='EU', nightscout_url='http://repo-nightscout:1337',
                nightscout_api_secret='test-secret-123456789', timezone='Pacific/Auckland',
                pump_serial_number='', features=['BASAL', 'BOLUS', 'PUMP_EVENTS'],
                poll_interval_seconds=300, mode='sync')


class ConfigTests(unittest.TestCase):
    def test_normal_sync_and_no_profile_or_cgm(self):
        env, args = launcher.configuration(options())
        self.assertIn('--auto-update', args)
        self.assertNotIn('CGM', args)
        self.assertNotIn('PROFILES', args)
        self.assertEqual(env['TCONNECT_PASSWORD'], 'literal $password')
        self.assertNotIn('PUMP_SERIAL_NUMBER', env)
        self.assertNotIn('CACHE_CREDENTIALS', env)
        self.assertEqual(env['HOME'], '/data/home')

    def test_preview_and_login_modes(self):
        _, args = launcher.configuration({**options(), 'mode': 'preview'})
        self.assertIn('--pretend', args)
        self.assertIn('--auto-update', args)
        _, args = launcher.configuration({**options(), 'mode': 'check_login'})
        self.assertIn('--check-login', args)
        self.assertNotIn('--auto-update', args)

    def test_explicit_features_and_serial(self):
        env, args = launcher.configuration({**options(), 'pump_serial_number': '1234567', 'features': ['PROFILES']})
        self.assertEqual(env['PUMP_SERIAL_NUMBER'], '1234567')
        self.assertIn('PROFILES', args)

    def test_bad_configuration_and_secret_safe_errors(self):
        for key, value in [('tconnect_region', 'NZ'), ('timezone', 'Not/AZone'),
                           ('pump_serial_number', '1;2'), ('features', []),
                           ('features', ['BASAL', 'BASAL']), ('features', ['UNKNOWN']),
                           ('poll_interval_seconds', 1), ('poll_interval_seconds', True),
                           ('nightscout_url', 'http://user:secret@example.com'),
                           ('nightscout_url', 'http://host:bad'), ('mode', 'arbitrary'),
                           ('nightscout_api_secret', 'short')]:
            with self.subTest(key=key, value=value), self.assertRaises(ValueError) as raised:
                launcher.configuration({**options(), key: value})
            self.assertNotIn('literal $password', str(raised.exception))
            self.assertNotIn('test-secret-123456789', str(raised.exception))

    def test_process_replacement_and_privilege_drop(self):
        import json
        calls = []
        with patch.object(launcher.Path, 'read_text', return_value=json.dumps(options())), \
             patch.object(launcher.Path, 'mkdir'), \
             patch.object(launcher.os, 'umask'), \
             patch.object(launcher.os, 'chown'), \
             patch.object(launcher.os, 'chmod'), \
             patch.object(launcher.os, 'setgroups', side_effect=lambda x: calls.append('groups')), \
             patch.object(launcher.os, 'setgid', side_effect=lambda x: calls.append(('gid', x))), \
             patch.object(launcher.os, 'setuid', side_effect=lambda x: calls.append(('uid', x))), \
             patch.object(launcher.os, 'chdir'), \
             patch.object(launcher.os, 'execve', side_effect=lambda *x: calls.append(('exec', x))):
            launcher.main()
        self.assertEqual(calls[:3], ['groups', ('gid', 1000), ('uid', 1000)])
        env = calls[-1][1][2]
        self.assertNotIn('SUPERVISOR_TOKEN', env)
        self.assertNotIn('PYTHONPATH', env)
        self.assertIn('/opt/upstream/main.py', calls[-1][1][1])


if __name__ == '__main__':
    unittest.main()
