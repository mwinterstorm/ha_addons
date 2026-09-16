"""Set TCONNECT_UPSTREAM to an extracted, unmodified v3.0.0 source tree."""
import importlib.util
import os
from pathlib import Path
import sys
import tempfile
import unittest
from urllib.parse import parse_qs, urlsplit

UPSTREAM = os.environ.get('TCONNECT_UPSTREAM')


@unittest.skipUnless(UPSTREAM, 'Set TCONNECT_UPSTREAM to run patched upstream HTTP tests')
class QueryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        sys.path.insert(0, UPSTREAM)
        import requests_mock
        cls.requests_mock = requests_mock
        root = Path(__file__).parents[1]
        spec = importlib.util.spec_from_file_location('apply_patch', root / 'patches/apply.py')
        patcher = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(patcher)
        cls.tmp = tempfile.TemporaryDirectory()
        target = Path(cls.tmp.name) / 'nightscout.py'
        target.write_bytes((Path(UPSTREAM) / 'tconnectsync/nightscout.py').read_bytes())
        patcher.apply(target)
        spec = importlib.util.spec_from_file_location('tconnectsync.patched_nightscout', target)
        cls.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.module)
        cls.patcher = patcher

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def calls(self, api):
        return [
            lambda **kw: api.last_uploaded_entry('Temp Basal', **kw),
            lambda **kw: api.last_uploaded_devicestatus(**kw),
            lambda **kw: api.last_uploaded_bg_entry(**kw),
            lambda **kw: api.last_uploaded_activity('Exercise', **kw),
        ]

    def test_empty_history_is_one_valid_request_for_all_endpoints(self):
        api = self.module.NightscoutApi('http://nightscout:1337/', 'test-secret-12345')
        for offset in ['+12:00', '+13:00', '+05:30', '-04:00', '+00:00']:
            start, end = '2026-09-15T00:00:00' + offset, '2026-09-16T19:47:13' + offset
            for call in self.calls(api):
                with self.subTest(offset=offset), self.requests_mock.Mocker() as mock:
                    mock.get(self.requests_mock.ANY, json=[])
                    self.assertIsNone(call(time_start=start, time_end=end))
                    self.assertEqual(mock.call_count, 1)
                    q = parse_qs(urlsplit(mock.last_request.url).query)
                    dates = [v[0] for k, v in q.items() if '$gte' in k or '$lte' in k]
                    self.assertEqual(dates, [start, end])
                    self.assertTrue(all(' ' not in date for date in dates))

    def test_existing_record_is_preserved(self):
        api = self.module.NightscoutApi('http://nightscout:1337/', 'test-secret-12345')
        for call in self.calls(api):
            with self.requests_mock.Mocker() as mock:
                mock.get(self.requests_mock.ANY, json=[{'_id': 'existing', 'created_at': '2026-09-16T00:00:00Z'}])
                self.assertEqual(call()['_id'], 'existing')

    def test_errors_do_not_become_empty_history(self):
        api = self.module.NightscoutApi('http://nightscout:1337/', 'test-secret-12345')
        for status in (401, 403, 500):
            for call in self.calls(api):
                with self.requests_mock.Mocker() as mock:
                    mock.get(self.requests_mock.ANY, status_code=status, text='[]')
                    with self.assertRaises(self.module.ApiException):
                        call(time_start='2026-09-15T00:00:00+12:00')
                    self.assertEqual(mock.call_count, 1)

    def test_malformed_response_fails_closed(self):
        api = self.module.NightscoutApi('http://nightscout:1337/', 'test-secret-12345')
        with self.requests_mock.Mocker() as mock:
            mock.get(self.requests_mock.ANY, json={'error': 'unexpected'})
            with self.assertRaises(ValueError):
                api.last_uploaded_devicestatus()

    def test_changed_source_refuses_patch(self):
        target = Path(self.tmp.name) / 'changed.py'
        target.write_text('# unexpected upstream revision')
        with self.assertRaises(RuntimeError):
            self.patcher.apply(target)
