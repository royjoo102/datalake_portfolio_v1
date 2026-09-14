"""Publish raw/silver/rejected files as one logical batch via a final manifest."""
from collections import defaultdict
import hashlib
import json
import re
from datetime import datetime, timezone

from lake.schema import TIMEZONE, schemas, split_rows, parquet_bytes
from streaming.rules import rules_hash


def valid_id(value):
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,100}', value):
        raise ValueError('Use a simple lake ID (letters, digits, _, -, .)')
    return value


class Lake:
    def __init__(self, objects, lake_id, rules):
        self.objects = objects
        self.id = valid_id(lake_id)
        self.control = f'_control/{self.id}'
        self.rules = rules
        self.schemas = schemas(rules)
    def identity(self, topic, cluster_id):
        value = dict(version=1, rules=rules_hash(self.rules), topic=topic,
                     cluster_id=cluster_id, timezone=TIMEZONE, silver_fields=self.rules['allowlist'])
        self.objects.put_json(f'{self.control}/identity.json', value, immutable=True)
        return value
    def ready(self):
        value = self.objects.json(f'{self.control}/import.json')
        if value is None:
            raise ValueError('Import not complete. Run the SQLite migration first.')
        identity = self.objects.json(f'{self.control}/identity.json')
        if identity is None or identity['rules'] != rules_hash(self.rules):
            raise ValueError('Lake rules differ from this reader; use the original rules')
        return value
    def commits(self):
        self.ready()
        return [self.objects.json(key) for key in self.objects.list(f'{self.control}/commits/') if key.endswith('.json')]
    def publish(self, commit_id, rows, fail_after_zone=None):
        valid_id(commit_id)
        key = f'{self.control}/commits/{commit_id}.json'
        grouped = split_rows(rows, self.rules)
        fingerprint = hashlib.sha256()
        for zone in ('raw', 'silver', 'rejected'):
            for row in grouped[zone]:
                fingerprint.update(json.dumps(row, sort_keys=True, default=lambda x: x.hex() if isinstance(x, bytes) else x.isoformat(), ensure_ascii=False).encode())
                fingerprint.update(b'\n')
        digest = fingerprint.hexdigest()
        existing = self.objects.json(key)
        if existing is not None:
            if existing['input_sha256'] != digest:
                raise ValueError('Batch ID reused for different input; checkpoint/source mismatch')
            return dict(existing, retried=True)
        files = []
        for zone in ('raw', 'silver', 'rejected'):
            partitions = defaultdict(list)
            for row in grouped[zone]:
                partitions[row['event_date']].append(row)
            for date, values in sorted(partitions.items()):
                name = f'{zone}/events/{self.id}/event_date={date}/{commit_id}.parquet'
                data = parquet_bytes(values, self.schemas[zone])
                # Retry reuses immutable names and identical bytes; no duplicate files.
                files.append(dict(self.objects.put(name, data, immutable=True), zone=zone, rows=len(values), event_date=date))
            if fail_after_zone == zone:
                raise RuntimeError(f'Injected interruption after {zone} upload, before manifest')
        ranges = {}
        for row in grouped['raw']:
            part = str(row['partition'])
            if part not in ranges:
                ranges[part] = dict(start=row['offset'], end=row['offset']+1, count=0)
            r = ranges[part]
            r.update(start=min(r['start'], row['offset']), end=max(r['end'], row['offset']+1), count=r['count']+1)
        manifest = dict(commit_id=commit_id, input_sha256=digest, files=files,
                        counts={zone:len(values) for zone,values in grouped.items()}, ranges=ranges,
                        committed_at=datetime.now(timezone.utc).isoformat())
        self.objects.put_json(key, manifest, immutable=True)
        return manifest
