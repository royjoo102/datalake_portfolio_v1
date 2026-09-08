import csv
import json
from pathlib import Path
import tempfile
import os
import signal
import unittest
from unittest.mock import patch

from replay.cli import parser, records, replay


class Producer:
    def __init__(self, fail_at=None):
        self.messages = []
        self.fail_at = fail_at
    def send(self, topic, **kwargs):
        event = json.loads(kwargs['value'])
        if event['metadata']['source_row'] == self.fail_at:
            raise RuntimeError('broker unavailable')
        self.messages.append(event)
        return self
    def get(self, timeout):
        return self
    def close(self, timeout):
        pass


class ReplayTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.source = self.root / 'source.csv'
        with self.source.open('w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerows([['event_type', 'extra', 'empty'],
                              ['tap', 'comma,quote"\n한글', ''],
                              ['tap', 'comma,quote"\n한글', ''],
                              ['screen', '001', '']])
        self.args = parser().parse_args(['--source', str(self.source), '--run-id', 'test',
            '--state-dir', str(self.root / 'state'), '--rate', '100000'])
    def test_all_values_and_duplicate_rows_preserved(self):
        producer = Producer()
        self.assertEqual(replay(self.args, lambda **kw: producer), 0)
        self.assertEqual([m['payload'] for m in producer.messages], [r for _, r in records(self.source)])
        self.assertEqual(len(producer.messages), 3)
        self.assertNotEqual(producer.messages[0]['metadata']['event_id'], producer.messages[1]['metadata']['event_id'])
    def test_limit_resume_and_stable_identity(self):
        self.args.limit = 1
        first = Producer()
        replay(self.args, lambda **kw: first)
        self.args.resume = True
        self.args.limit = None
        rest = Producer()
        replay(self.args, lambda **kw: rest)
        self.assertEqual([m['metadata']['source_row'] for m in rest.messages], [2, 3])
        self.args.resume = False
        self.args.run_id = 'new'
        again = Producer()
        replay(self.args, lambda **kw: again)
        self.assertEqual(first.messages[0]['metadata']['event_id'], again.messages[0]['metadata']['event_id'])
    def test_failure_does_not_skip_row(self):
        self.assertEqual(replay(self.args, lambda **kw: Producer(fail_at=2)), 1)
        state = json.loads((self.root / 'state/test.json').read_text())
        self.assertEqual(state['last_acked_row'], 1)
        self.args.resume = True
        producer = Producer()
        self.assertEqual(replay(self.args, lambda **kw: producer), 0)
        self.assertEqual([m['metadata']['source_row'] for m in producer.messages], [2, 3])
    def test_reject_changed_source_and_existing_run(self):
        replay(self.args, lambda **kw: Producer())
        with self.assertRaises(ValueError):
            replay(self.args, lambda **kw: Producer())
        self.args.resume = True
        self.source.write_text('a\nb\n')
        with self.assertRaises(ValueError):
            replay(self.args, lambda **kw: Producer())
    def test_malformed_csv_stops_without_discarding(self):
        self.source.write_text('a,b\n1,2\n3\n')
        self.assertEqual(replay(self.args, lambda **kw: Producer()), 1)
        state = json.loads((self.root / 'state/test.json').read_text())
        self.assertEqual(state['last_acked_row'], 1)
    def test_duplicate_header_rejected(self):
        self.source.write_text('a,a\n1,2\n')
        with self.assertRaises(ValueError):
            list(records(self.source))
    def test_graceful_signal_saves_acknowledged_row(self):
        class InterruptedProducer(Producer):
            def get(self, timeout):
                os.kill(os.getpid(), signal.SIGINT)
                return self
        self.assertEqual(replay(self.args, lambda **kw: InterruptedProducer()), 130)
        state = json.loads((self.root / 'state/test.json').read_text())
        self.assertEqual(state['last_acked_row'], 1)
        self.assertFalse(state['complete'])
        self.args.resume = True
        producer = Producer()
        self.assertEqual(replay(self.args, lambda **kw: producer), 0)
        self.assertEqual([m['metadata']['source_row'] for m in producer.messages], [2, 3])
    def test_acknowledgement_failure_does_not_advance_checkpoint(self):
        class UncertainProducer(Producer):
            def get(self, timeout):
                raise TimeoutError('acknowledgement not received')
        self.assertEqual(replay(self.args, lambda **kw: UncertainProducer()), 1)
        state = json.loads((self.root / 'state/test.json').read_text())
        self.assertEqual(state['last_acked_row'], 0)

    def test_rate_spacing(self):
        self.args.rate = 10
        now = [0.0]
        def sleep(seconds):
            now[0] += seconds
        times = []
        class TimedProducer(Producer):
            def send(self, *args, **kwargs):
                times.append(now[0])
                return super().send(*args, **kwargs)
        with patch('replay.cli.time.monotonic', side_effect=lambda: now[0]), patch('replay.cli.time.sleep', side_effect=sleep):
            replay(self.args, lambda **kw: TimedProducer())
        self.assertEqual(len(times), 3)
        self.assertGreaterEqual(times[1] - times[0], 0.1)
        self.assertGreaterEqual(times[2] - times[1], 0.1)


if __name__ == '__main__':
    unittest.main()
