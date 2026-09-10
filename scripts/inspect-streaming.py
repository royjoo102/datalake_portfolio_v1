#!/usr/bin/env python3
"""Inspect and validate M3 output without starting Spark or mutating the sink."""
import argparse
import json
from pathlib import Path
import sqlite3
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from streaming.rules import load_rules


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--pipeline-id', default='m3-gce-june')
    p.add_argument('--rules', type=Path, default=Path('config/filter-rules.yaml'))
    p.add_argument('--expected', type=int)
    p.add_argument('--sample', type=int, default=0, help='Print this many records')
    p.add_argument('--zone', choices=['silver', 'rejected', 'raw'], default='silver')
    args = p.parse_args()
    rules = load_rules(args.rules)
    path = Path('data/m3') / args.pipeline_id / 'events.sqlite'
    db = sqlite3.connect(path.resolve().as_uri()+'?mode=rw', uri=True)
    db.execute('PRAGMA query_only=ON')
    db.execute('BEGIN')
    try:
        counts = {name: db.execute(f'SELECT count(*) FROM {name}').fetchone()[0]
                  for name in ('raw_events', 'silver_events', 'rejected_events', 'batches')}
        if counts['raw_events'] != counts['silver_events'] + counts['rejected_events']:
            raise AssertionError('raw != silver + rejected')
        if args.expected is not None and counts['raw_events'] != args.expected:
            raise AssertionError(f'Expected {args.expected}, found {counts["raw_events"]}')
        for payload, in db.execute('SELECT payload FROM silver_events'):
            if set(json.loads(payload)) != set(rules['allowlist']):
                raise AssertionError('Silver payload does not match allowlist')
        rejected = dict(db.execute('SELECT reason,count(*) FROM rejected_events GROUP BY reason'))
        nulls = db.execute('SELECT count(*) FROM silver_events WHERE event_time IS NULL').fetchone()[0]
        offsets = [dict(topic=t, partition=p, first=f, last=l, count=n) for t,p,f,l,n in
                   db.execute('SELECT topic,partition_id,min(offset_id),max(offset_id),count(*) FROM raw_events GROUP BY topic,partition_id')]
        print(json.dumps(dict(status='PASS', counts=counts, rejected_reasons=rejected,
                              null_event_time=nulls, offsets=offsets), ensure_ascii=False, indent=2))
        if args.zone == 'silver':
            for event_time, payload, flags in db.execute(
                    'SELECT event_time,payload,quality_flags FROM silver_events LIMIT ?', (args.sample,)):
                print(json.dumps(dict(event_time=event_time, payload=json.loads(payload),
                                      quality_flags=json.loads(flags)), ensure_ascii=False))
        else:
            sql = ('SELECT source_id,value FROM raw_events LIMIT ?' if args.zone == 'raw' else
                   'SELECT r.source_id,r.value,j.reason,j.details FROM raw_events r '
                   'JOIN rejected_events j USING(source_id) LIMIT ?')
            for record in db.execute(sql, (args.sample,)):
                result = dict(source_id=record[0], raw_value=None if record[1] is None else
                              record[1].decode('utf-8', errors='replace'))
                if args.zone == 'rejected':
                    result.update(reason=record[2], details=json.loads(record[3]))
                print(json.dumps(result, ensure_ascii=False))
    finally:
        db.close()


if __name__ == '__main__':
    main()
