#!/usr/bin/env python3
"""Read a bounded Kafka offset snapshot and compare one run against its CSV."""
import argparse
import json
import sys
import time
from itertools import islice
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dotenv import load_dotenv
from kafka import KafkaConsumer, TopicPartition
from replay.cli import envelope, fingerprint
from replay.cli import parser as replay_parser
from replay.cli import records


def main():
    load_dotenv(Path('.env'))
    parser = argparse.ArgumentParser(parents=[replay_parser()], add_help=False)
    parser.add_argument('--expected', type=int, required=True)
    args = parser.parse_args()
    digest = fingerprint(args.source)
    expected = iter(islice(records(args.source), args.expected))
    consumer = KafkaConsumer(bootstrap_servers=args.bootstrap_servers.split(','),
                             enable_auto_commit=False, group_id=None)
    tp = TopicPartition(args.topic, args.partition)
    consumer.assign([tp])
    consumer.seek(tp, consumer.beginning_offsets([tp])[tp])
    end = consumer.end_offsets([tp])[tp]
    print(f'Verifying partition {args.partition} through offset {end}', flush=True)
    count = 0
    deadline = time.monotonic() + 120
    try:
        while consumer.position(tp) < end:
            if time.monotonic() > deadline:
                raise RuntimeError('Kafka verification timed out')
            for batch in consumer.poll(timeout_ms=1000).values():
                for message in batch:
                    if message.offset >= end:
                        continue
                    try:
                        event = json.loads(message.value)
                    except (ValueError, UnicodeDecodeError):
                        continue  # Existing M1 smoke messages may not be valid JSON.
                    if not isinstance(event, dict) or event.get('metadata', {}).get('replay_run_id') != args.run_id:
                        continue
                    row, payload = next(expected, (None, None))
                    assert row is not None, 'Extra or duplicate run record'
                    target = envelope(payload, digest, row, args.run_id, args.source.name)
                    assert event['payload'] == payload, f'Payload differs at row {row}'
                    for key, value in target['metadata'].items():
                        if key != 'replay_time':
                            assert event['metadata'][key] == value, f'Metadata differs at row {row}: {key}'
                    count += 1
                    if count % 1000 == 0:
                        print(f'Compared {count} records', flush=True)
        assert count == args.expected, f'Expected {args.expected}, received {count}'
        print(f'PASS: {count} records; all source fields, row order and IDs match; no missing or duplicate rows')
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
