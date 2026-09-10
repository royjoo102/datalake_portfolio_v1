"""Atomic local sink for M3; full Parquet/MinIO publication belongs to M4."""
import json
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

DDL = '''
CREATE TABLE IF NOT EXISTS identity (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS raw_events (
 source_id TEXT PRIMARY KEY, topic TEXT, partition_id INTEGER, offset_id INTEGER,
 kafka_timestamp TEXT, value BLOB);
CREATE TABLE IF NOT EXISTS silver_events (
 source_id TEXT PRIMARY KEY REFERENCES raw_events(source_id), event_id TEXT,
 replay_run_id TEXT, event_time TEXT, metadata TEXT NOT NULL, payload TEXT NOT NULL, quality_flags TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS rejected_events (
 source_id TEXT PRIMARY KEY REFERENCES raw_events(source_id), reason TEXT NOT NULL,
 reasons TEXT NOT NULL, details TEXT NOT NULL, duplicate_of TEXT);
CREATE TABLE IF NOT EXISTS batches (
 batch_id INTEGER PRIMARY KEY, input_count INTEGER, inserted_count INTEGER,
 silver_count INTEGER, rejected_count INTEGER, duration_ms REAL, committed_at TEXT);
'''


def connect(path):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=30)
    db.execute('PRAGMA foreign_keys=ON')
    db.execute('PRAGMA journal_mode=WAL')
    db.execute('PRAGMA synchronous=FULL')
    db.executescript(DDL)
    return db


def bind_identity(path, identity):
    with connect(path) as db:
        existing = dict(db.execute('SELECT key,value FROM identity'))
        encoded = {k: json.dumps(v, sort_keys=True) for k, v in identity.items()}
        if existing and existing != encoded:
            raise ValueError('Source, rules, checkpoint or sink identity changed; use a new pipeline directory')
        db.executemany('INSERT OR IGNORE INTO identity VALUES (?,?)', encoded.items())
    db.close()


def utc_text(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc).isoformat() if value.tzinfo is None else value.astimezone(timezone.utc).isoformat()
    return str(value)


def commit_batch(path, batch_id, rows, fail_after=None):
    """Commit raw, silver/rejected and batch marker in one transaction.

    fail_after is a test hook. Kafka source coordinates also make replay after an
    uncertain Spark commit idempotent. Repeated events at NEW coordinates are preserved without deduplication.
    """
    db = connect(path)
    started = time.monotonic()
    try:
        db.execute('BEGIN IMMEDIATE')
        if db.execute('SELECT 1 FROM batches WHERE batch_id=?', (batch_id,)).fetchone():
            db.rollback()
            return dict(batch_id=batch_id, already_committed=True)
        count = inserted = silver = rejected = 0
        for row in rows:
            count += 1
            source_id = json.dumps([row['topic'], row['partition'], row['offset']], separators=(',', ':'))
            if db.execute('SELECT 1 FROM raw_events WHERE source_id=?', (source_id,)).fetchone():
                continue
            db.execute('INSERT INTO raw_events VALUES (?,?,?,?,?,?)',
                       (source_id, row['topic'], row['partition'], row['offset'],
                        utc_text(row.get('timestamp')), bytes(row['value']) if row['value'] is not None else None))
            inserted += 1
            reasons, details = list(row['reasons']), list(row['details'])
            duplicate_of = None
            if reasons:
                db.execute('INSERT INTO rejected_events VALUES (?,?,?,?,?)',
                           (source_id, reasons[0], json.dumps(reasons), json.dumps(details), duplicate_of))
                rejected += 1
            else:
                meta = row['metadata']
                db.execute('INSERT INTO silver_events VALUES (?,?,?,?,?,?,?)',
                           (source_id, meta.get('event_id'), meta.get('replay_run_id'),
                            utc_text(row['event_time']), json.dumps(meta, ensure_ascii=False),
                            json.dumps(row['payload'], ensure_ascii=False), json.dumps(row['quality_flags'])))
                silver += 1
            if fail_after is not None and inserted >= fail_after:
                raise RuntimeError('Injected sink failure before commit')
        elapsed = (time.monotonic() - started) * 1000
        db.execute('INSERT INTO batches VALUES (?,?,?,?,?,?,?)',
                   (batch_id, count, inserted, silver, rejected, elapsed,
                    datetime.now(timezone.utc).isoformat()))
        db.commit()
        return dict(batch_id=batch_id, input=count, inserted=inserted, silver=silver,
                    rejected=rejected, sink_duration_ms=round(elapsed, 1))
    except BaseException:
        db.rollback()
        raise
    finally:
        db.close()
