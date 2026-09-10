#!/usr/bin/env python3
"""Synthetic Kafka/Spark crash-recovery acceptance test; requires built M3 image."""
import copy
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from streaming.rules import load_rules


def main():
    import os
    load_dotenv(Path('.env'))
    token = uuid.uuid4().hex[:10]
    topic = f'm3-test-{token}'
    pipeline = f'm3-test-{token}'
    bootstrap = os.getenv('KAFKA_LOCAL_BOOTSTRAP_SERVERS', 'localhost:9092')
    rules = load_rules('config/filter-rules.yaml')
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    admin.close()
    event = dict(metadata=dict(schema_version='1.0', event_id='same-event',
                 replay_run_id='synthetic', replay_time='2026-09-09T00:00:00Z',
                 source_file='synthetic.csv', source_sha256='synthetic', source_row=1),
                 payload={key: '1780808385411' if key == 'event_timestamp' else ''
                          for key in rules['input_fields']})
    encode = lambda x: json.dumps(x, ensure_ascii=False).encode()
    events = [encode(event), encode(event)]  # Both must survive: business dedup disabled.
    for timestamp in ('', 'bad', None):
        item = copy.deepcopy(event)
        item['payload']['event_timestamp'] = timestamp
        events.append(encode(item))
    events += [b'{bad', b'\xff', None]
    wrong = copy.deepcopy(event)
    wrong['metadata']['schema_version'] = '99'
    events.append(encode(wrong))
    wrong = copy.deepcopy(event)
    del wrong['metadata']['event_id']
    events.append(encode(wrong))
    wrong = copy.deepcopy(event)
    wrong['payload']['application_title'] = 42
    events.append(encode(wrong))
    # Valid old event must NOT be discarded as late; no watermark is configured.
    old = copy.deepcopy(event)
    old['payload']['event_timestamp'] = '946684800000'
    events.append(encode(old))
    producer = KafkaProducer(bootstrap_servers=bootstrap, acks='all', enable_idempotence=True)
    for value in events:
        producer.send(topic, partition=0, value=value, key=b'synthetic').get(timeout=30)
    producer.close()
    logs = Path('logs/m3') / pipeline
    logs.mkdir(parents=True, exist_ok=True)
    command = ['./scripts/run-streaming.sh', '--pipeline-id', pipeline, '--topic', topic, '--max-offsets', '4']
    with (logs / 'crash.log').open('w') as stream:
        failed = subprocess.run(command + ['--test-crash-after-batch', '0'], stdout=stream, stderr=subprocess.STDOUT)
    if failed.returncode == 0:
        raise AssertionError('Expected injected process crash')
    database = Path('data/m3') / pipeline / 'events.sqlite'
    with sqlite3.connect(database) as db:
        assert db.execute('SELECT count(*) FROM raw_events').fetchone()[0] == 4
        assert db.execute('SELECT count(*) FROM batches').fetchone()[0] == 1
    checkpoint = Path('checkpoints/spark') / pipeline
    assert (checkpoint / 'offsets/0').exists()
    assert not (checkpoint / 'commits/0').exists(), 'Crash should precede Spark commit'
    with (logs / 'resume.log').open('w') as stream:
        subprocess.run(command, stdout=stream, stderr=subprocess.STDOUT, check=True)
    with sqlite3.connect(database) as db:
        counts = [db.execute(f'SELECT count(*) FROM {name}').fetchone()[0]
                  for name in ('raw_events', 'silver_events', 'rejected_events')]
        assert counts == [12, 6, 6], counts
        assert db.execute('SELECT count(*) FROM silver_events WHERE event_time IS NULL').fetchone()[0] == 3
        payloads = [json.loads(row[0]) for row in db.execute('SELECT payload FROM silver_events')]
        assert all(set(p) == set(rules['allowlist']) for p in payloads)
        assert sum(p == payloads[0] for p in payloads) == 2
    with (logs / 'restart.log').open('w') as stream:
        subprocess.run(command, stdout=stream, stderr=subprocess.STDOUT, check=True)
    with sqlite3.connect(database) as db:
        assert db.execute('SELECT count(*) FROM raw_events').fetchone()[0] == 12
    result = dict(status='PASS', topic=topic, pipeline=pipeline, raw=12, silver=6, rejected=6,
                  null_event_time=3, duplicate_events_preserved=True,
                  crash_after_sink_commit_recovered=True, clean_restart_no_extra_rows=True)
    (logs / 'acceptance.json').write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))
    print('Synthetic topic and local artifacts retained for inspection.')


if __name__ == '__main__':
    main()
