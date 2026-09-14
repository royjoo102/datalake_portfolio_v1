"""Stable Parquet schemas and America/Los_Angeles date partitioning."""
import json
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
from streaming.rules import METADATA_FIELDS

TIMEZONE = 'America/Los_Angeles'
META = pa.struct([(f, pa.int64() if f == 'source_row' else pa.string()) for f in METADATA_FIELDS])
TRACE = [('source_id', pa.string()), ('topic', pa.string()), ('partition', pa.int32()),
         ('offset', pa.int64()), ('metadata', META), ('event_time', pa.timestamp('us', tz='UTC')),
         ('event_date', pa.string())]


def schemas(rules):
    return {
        'raw': pa.schema(TRACE + [('kafka_timestamp', pa.timestamp('us', tz='UTC')), ('raw_value', pa.binary())]),
        'silver': pa.schema([(f, pa.string()) for f in rules['allowlist']] + TRACE + [('quality_flags', pa.list_(pa.string()))]),
        'rejected': pa.schema(TRACE + [('reason', pa.string()), ('reasons', pa.list_(pa.string())),
                                      ('details', pa.list_(pa.string())), ('raw_value', pa.binary())]),
    }


def utc(value):
    if value is None:
        return None
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace('Z', '+00:00'))
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def partition_date(value):
    value = utc(value)
    return value.astimezone(ZoneInfo(TIMEZONE)).date().isoformat() if value else 'unknown'


def split_rows(rows, rules):
    output = {'raw': [], 'silver': [], 'rejected': []}
    for row in rows:
        event_time = utc(row['event_time'])
        trace = dict(source_id=json.dumps([row['topic'], row['partition'], row['offset']], separators=(',', ':')),
                     topic=row['topic'], partition=row['partition'], offset=row['offset'],
                     metadata=row.get('metadata'), event_time=event_time, event_date=partition_date(event_time))
        raw_value = bytes(row['value']) if row['value'] is not None else None
        output['raw'].append(dict(trace, kafka_timestamp=utc(row.get('timestamp')), raw_value=raw_value))
        if row['reasons']:
            output['rejected'].append(dict(trace, reason=row['reasons'][0], reasons=row['reasons'],
                                          details=row['details'], raw_value=raw_value))
        else:
            output['silver'].append(dict({field: row['payload'].get(field) for field in rules['allowlist']},
                                          **trace, quality_flags=row['quality_flags']))
    return output


def parquet_bytes(rows, schema):
    table = rows if isinstance(rows, pa.Table) else pa.Table.from_pylist(rows, schema=schema)
    buffer = pa.BufferOutputStream()
    pq.write_table(table, buffer, compression='snappy', row_group_size=10000,
                   version='2.6', use_dictionary=True)
    return buffer.getvalue().to_pybytes()
