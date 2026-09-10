import copy
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from streaming.rules import load_rules, validate
from streaming.store import bind_identity, commit_batch

RULES = Path(__file__).resolve().parents[1] / 'config/filter-rules.yaml'


def sample():
    rules = load_rules(RULES)
    return {'metadata': {'schema_version': '1.0', 'event_id': 'same-event',
                         'replay_run_id': 'test-run', 'replay_time': '2026-09-09T00:00:00Z',
                         'source_file': 'synthetic.csv', 'source_sha256': 'test-hash', 'source_row': 1},
            'payload': {field: '1780808385411' if field == 'event_timestamp' else ''
                        for field in rules['input_fields']}}


def encoded(event):
    return json.dumps(event, ensure_ascii=False).encode()


class RulesTests(unittest.TestCase):
    def setUp(self):
        self.rules = load_rules(RULES)
    def test_exact_user_allowlist(self):
        expected = 'application_package_name application_title application_version_name arrival_timestamp attributes_channel attributes_current_screen attributes_device_id attributes_event_type attributes_extra attributes_store_id attributes_to_screen device_model device_platform_name device_platform_version event_timestamp event_type event_version metrics_duration session_session_id session_start_timestamp session_stop_timestamp eventreceivedyear eventreceivedmonth eventreceivedday'.split()
        self.assertEqual(self.rules['allowlist'], expected)
        self.assertEqual(len(self.rules['input_fields']), 53)
    def test_valid_event_and_utc_milliseconds(self):
        event = sample()
        event['payload']['attributes_extra'] = 'comma,quote"\n한글'
        result = validate(encoded(event), self.rules)
        self.assertEqual(result['reasons'], [])
        self.assertEqual(result['payload'], event['payload'])
        self.assertEqual(result['event_time'].isoformat(), '2026-06-07T04:59:45.411000+00:00')
    def test_missing_invalid_timestamp_stays_valid_and_null(self):
        for value in ('', None, 'broken', '9'*100, 1234):
            with self.subTest(value=value):
                event = sample()
                event['payload']['event_timestamp'] = value
                result = validate(encoded(event), self.rules)
                self.assertEqual(result['reasons'], [])
                self.assertIsNone(result['event_time'])
                self.assertIsNone(result['payload']['event_timestamp'])
                self.assertTrue(result['quality_flags'])
    def test_json_errors(self):
        for value in (b'{bad', b'\xff', b'{"a":1,"a":2}', b'{"x":NaN}'):
            self.assertIn('MALFORMED_JSON', validate(value, self.rules)['reasons'])
        self.assertIn('INVALID_DATA_TYPE', validate(b'[]', self.rules)['reasons'])
    def test_required_metadata_and_schema_version(self):
        event = sample()
        del event['metadata']['event_id']
        event['metadata']['schema_version'] = '99'
        reasons = validate(encoded(event), self.rules)['reasons']
        self.assertIn('MISSING_REQUIRED_FIELD', reasons)
        self.assertIn('UNSUPPORTED_SCHEMA_VERSION', reasons)
    def test_wrong_type_is_not_silently_coerced(self):
        event = sample()
        event['metadata']['source_row'] = True
        event['payload']['application_title'] = {'not': 'a string'}
        self.assertIn('INVALID_DATA_TYPE', validate(encoded(event), self.rules)['reasons'])
    def test_bad_allowlist_refused(self):
        import yaml
        rules = copy.deepcopy(self.rules)
        rules['allowlist'].append('not_in_input')
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'rules.yaml'
            path.write_text(yaml.safe_dump(rules))
            with self.assertRaises(ValueError):
                load_rules(path)


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'events.sqlite'
        self.rules = load_rules(RULES)
        bind_identity(self.path, {'test': 'fixed'})
    def row(self, offset=0, raw=None):
        raw = raw if raw is not None else encoded(sample())
        result = validate(raw, self.rules)
        payload = result['payload'] or {}
        result['payload'] = {key: payload.get(key) for key in self.rules['allowlist']}
        return dict(result, topic='test', partition=0, offset=offset, value=raw, timestamp=None)
    def count(self, table):
        with sqlite3.connect(self.path) as db:
            return db.execute(f'SELECT count(*) FROM {table}').fetchone()[0]
    def test_duplicates_preserved_but_same_offset_not_written_twice(self):
        commit_batch(self.path, 0, [self.row(0), self.row(1)])
        commit_batch(self.path, 0, [self.row(0), self.row(1)])
        commit_batch(self.path, 1, [self.row(1), self.row(2)])
        self.assertEqual(self.count('silver_events'), 3)
        self.assertEqual(self.count('raw_events'), 3)
    def test_atomic_rollback_and_recovery(self):
        rows = [self.row(0), self.row(1, b'{bad')]
        with self.assertRaises(RuntimeError):
            commit_batch(self.path, 0, rows, fail_after=1)
        self.assertEqual(self.count('raw_events'), 0)
        self.assertEqual(self.count('batches'), 0)
        commit_batch(self.path, 0, rows)
        self.assertEqual(self.count('raw_events'), 2)
        self.assertEqual(self.count('silver_events'), 1)
        self.assertEqual(self.count('rejected_events'), 1)
    def test_changed_identity_refused(self):
        with self.assertRaises(ValueError):
            bind_identity(self.path, {'test': 'changed'})
    def test_raw_bytes_retained_and_silver_has_only_allowlist(self):
        row = self.row()
        commit_batch(self.path, 0, [row])
        with sqlite3.connect(self.path) as db:
            raw = db.execute('SELECT value FROM raw_events').fetchone()[0]
            payload = json.loads(db.execute('SELECT payload FROM silver_events').fetchone()[0])
        self.assertEqual(raw, row['value'])
        self.assertEqual(set(payload), set(self.rules['allowlist']))
        self.assertNotIn('client_client_id', payload)


if __name__ == '__main__':
    unittest.main()
