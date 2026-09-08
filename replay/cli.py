"""CSV replay with acknowledgement-based checkpoints (at-least-once recovery)."""
import argparse
import csv
import fcntl
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import signal
import time
from datetime import datetime, timezone
from itertools import islice

LOG = logging.getLogger('replay')


def fingerprint(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def records(path):
    csv.field_size_limit(16 * 1024 * 1024)
    with path.open(encoding='utf-8-sig', newline='') as stream:
        reader = csv.reader(stream, strict=True)
        header = next(reader, None)
        if not header or len(set(header)) != len(header) or '' in header:
            raise ValueError('CSV requires nonempty, unique column names')
        for number, values in enumerate(reader, 1):
            if len(values) != len(header):
                raise ValueError(f'CSV row {number}: expected {len(header)} fields, got {len(values)}')
            yield number, dict(zip(header, values))


def envelope(payload, digest, row, run_id, source_file):
    return {
        'metadata': {
            'schema_version': '1.0',
            'event_id': hashlib.sha256(f'{digest}:{row}'.encode()).hexdigest(),
            'replay_run_id': run_id,
            'replay_time': datetime.now(timezone.utc).isoformat(),
            'source_file': source_file,
            'source_sha256': digest,
            'source_row': row,
        },
        'payload': payload,
    }


def save_checkpoint(path, state):
    temp = path.with_suffix('.tmp')
    with temp.open('w') as stream:
        json.dump(state, stream, ensure_ascii=False)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temp, path)


def replay(args, producer_factory=None):
    source = args.source.resolve(strict=True)
    digest = fingerprint(source)
    args.state_dir.mkdir(parents=True, exist_ok=True)
    checkpoint = args.state_dir / f'{args.run_id}.json'
    identity = dict(source_sha256=digest, source_file=source.name,
                    replay_run_id=args.run_id, topic=args.topic,
                    bootstrap_servers=args.bootstrap_servers, partition=args.partition,
                    schema_version='1.0')
    # A held OS lock also prevents two processes from advancing the same run.
    with (args.state_dir / f'{args.run_id}.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ValueError('This run is already active') from exc
        if args.resume:
            state = json.loads(checkpoint.read_text())
            if any(state.get(key) != value for key, value in identity.items()):
                raise ValueError('Resume source or Kafka destination differs from checkpoint')
        else:
            if checkpoint.exists():
                raise ValueError('Run exists: use --resume or a new --run-id')
            state = dict(identity, last_acked_row=0, complete=False)
            save_checkpoint(checkpoint, state)
        if state['complete']:
            LOG.info('Run already complete: acknowledged=%s', state['last_acked_row'])
            return 0
        if producer_factory is None:
            from kafka import KafkaProducer
            producer_factory = KafkaProducer
        producer = None
        stopped = False
        def stop(signum, frame):
            nonlocal stopped
            stopped = True
        old_handlers = {s: signal.signal(s, stop) for s in (signal.SIGINT, signal.SIGTERM)}
        sent = 0
        row = state['last_acked_row'] + 1
        started = time.monotonic()
        last_log = started
        next_send = started
        try:
            producer = producer_factory(
                bootstrap_servers=args.bootstrap_servers.split(','),
                acks='all', enable_idempotence=True, retries=5,
                max_in_flight_requests_per_connection=1,
                request_timeout_ms=10000, delivery_timeout_ms=30000,
                max_block_ms=30000,
            )
            # Resume by CSV record number, not physical line (quoted newlines are valid).
            iterator = islice(records(source), state['last_acked_row'], None)
            while not stopped and (args.limit is None or sent < args.limit):
                row = state['last_acked_row'] + 1
                try:
                    row, payload = next(iterator)
                except StopIteration:
                    state['complete'] = True
                    break
                while not stopped and time.monotonic() < next_send:
                    time.sleep(max(0, min(0.1, next_send - time.monotonic())))
                if stopped:
                    break
                next_send = max(next_send + 1 / args.rate, time.monotonic())
                event = envelope(payload, digest, row, args.run_id, source.name)
                producer.send(
                    args.topic, partition=args.partition,
                    key=event['metadata']['event_id'].encode(),
                    value=json.dumps(event, ensure_ascii=False).encode('utf-8'),
                ).get(timeout=40)
                state['last_acked_row'] = row
                sent += 1
                if sent % 100 == 0:
                    save_checkpoint(checkpoint, state)
                now = time.monotonic()
                if now - last_log >= 5:
                    LOG.info('acknowledged=%s session_sent=%s rate=%.1f events/sec',
                             row, sent, sent / (now - started))
                    last_log = now
            save_checkpoint(checkpoint, state)
            LOG.info('status=%s acknowledged=%s session_sent=%s rate=%.1f events/sec',
                     'complete' if state['complete'] else 'paused', state['last_acked_row'],
                     sent, sent / max(time.monotonic() - started, 0.001))
            return 130 if stopped else 0
        except Exception as exc:
            save_checkpoint(checkpoint, state)
            with (args.state_dir / f'{args.run_id}.failures.jsonl').open('a') as stream:
                stream.write(json.dumps(dict(identity, source_row=row,
                    last_acked_row=state['last_acked_row'], error_type=type(exc).__name__,
                    time=datetime.now(timezone.utc).isoformat())) + '\n')
            LOG.error('Stopped at row %s (%s); no row skipped. Resume after resolving failure.',
                      row, type(exc).__name__)
            return 1
        finally:
            try:
                if producer is not None:
                    producer.close(timeout=5)
            finally:
                for sig, handler in old_handlers.items():
                    signal.signal(sig, handler)


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source', type=Path, default=Path('GCE_EVO_Data/GCE_EVO_202606.csv'))
    p.add_argument('--topic', default=os.getenv('KAFKA_RAW_TOPIC', 'raw-events'))
    p.add_argument('--bootstrap-servers', default=os.getenv('KAFKA_LOCAL_BOOTSTRAP_SERVERS', 'localhost:9092'))
    p.add_argument('--partition', type=int, default=0, help='One partition preserves CSV order')
    p.add_argument('--rate', type=float, default=100, help='Maximum events/sec; default 100')
    p.add_argument('--run-id', required=True)
    p.add_argument('--limit', type=int, help='Maximum records in this invocation; omit for all remaining')
    p.add_argument('--resume', action='store_true')
    p.add_argument('--state-dir', type=Path, default=Path('checkpoints/replay'))
    return p


def main():
    from dotenv import load_dotenv
    load_dotenv(Path('.env'))
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    logging.getLogger('kafka').setLevel(logging.WARNING)
    p = parser()
    args = p.parse_args()
    import re
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,100}', args.run_id):
        p.error('--run-id must be a simple identifier (letters, digits, _, -, .)')
    if not math.isfinite(args.rate) or args.rate <= 0 or args.partition < 0 or (args.limit is not None and args.limit <= 0):
        p.error('rate and limit must be positive; partition must be nonnegative')
    try:
        return replay(args)
    except (OSError, ValueError) as exc:
        LOG.error('%s', exc)
        return 1
