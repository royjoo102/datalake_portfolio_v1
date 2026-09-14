#!/usr/bin/env python3
"""Compare the M3 snapshot with committed MinIO data and report file sizes."""
import argparse
import hashlib
import json
from pathlib import Path
import sqlite3
import statistics
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import pyarrow as pa
from dotenv import load_dotenv
from lake.objects import Objects
from lake.writer import Lake
from lake.query import connection
from lake.schema import parquet_bytes, partition_date
from streaming.rules import load_rules


def verify(lake, source):
    commits=lake.commits()
    con=connection(lake,commits)
    db=sqlite3.connect(source.resolve().as_uri()+'?mode=rw',uri=True)
    db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
    try:
        actual={row['source_id']:row for row in con.execute('SELECT * FROM raw').fetch_arrow_table().to_pylist()}
        silver={row['source_id']:row for row in con.execute('SELECT * FROM silver').fetch_arrow_table().to_pylist()}
        rejected={row['source_id']:row for row in con.execute('SELECT * FROM rejected').fetch_arrow_table().to_pylist()}
        compared=0
        full=[];selected=[]
        for sid,value in db.execute('SELECT source_id,value FROM raw_events'):
            assert actual[sid]['raw_value']==value, sid
            compared+=1
        for sid,meta,payload,et,flags in db.execute('SELECT source_id,metadata,payload,event_time,quality_flags FROM silver_events'):
            row=silver[sid];payload=json.loads(payload)
            assert {k:row[k] for k in lake.rules['allowlist']}==payload, sid
            assert row['metadata']==json.loads(meta), sid
            assert row['quality_flags']==json.loads(flags), sid
            assert row['event_date']==partition_date(et), sid
            full.append(json.loads(actual[sid]['raw_value'])['payload']);selected.append(payload)
        for sid,reasons,details in db.execute('SELECT source_id,reasons,details FROM rejected_events'):
            assert rejected[sid]['reasons']==json.loads(reasons),sid
            assert rejected[sid]['details']==json.loads(details),sid
        counts={z:con.execute(f'SELECT count(*) FROM {z}').fetchone()[0] for z in lake.schemas}
        unique=con.execute('SELECT count(DISTINCT source_id) FROM raw').fetchone()[0]
        assert counts['raw']==unique==counts['silver']+counts['rejected']
        file_stats={}
        for zone in lake.schemas:
            sizes=[f['bytes'] for c in commits for f in c['files'] if f['zone']==zone]
            file_stats[zone]=dict(files=len(sizes),bytes=sum(sizes),min=min(sizes,default=0),median=statistics.median(sizes) if sizes else 0,max=max(sizes,default=0),under_1_mib=sum(n<1024**2 for n in sizes))
        full_schema=pa.schema([(k,pa.string()) for k in lake.rules['input_fields']])
        selected_schema=pa.schema([(k,pa.string()) for k in lake.rules['allowlist']])
        full_size=len(parquet_bytes(full,full_schema));selected_size=len(parquet_bytes(selected,selected_schema))
        # Verify bytes against every committed object digest, not only row counts.
        checked=0
        for commit in commits:
            for f in commit['files']:
                data=lake.objects.get(f['key'])
                assert len(data)==f['bytes'] and hashlib.sha256(data).hexdigest()==f['sha256']
                checked+=1
        latest=lake.objects.json(f'{lake.control}/gold/latest.json')
        assert latest is not None and set(latest['commits'])=={c['commit_id'] for c in commits},'Gold snapshot is stale'
        return dict(lake_id=lake.id,counts=counts,source_rows_compared=compared,unique_source_offsets=unique,
                    committed_objects_verified=checked,files=file_stats,gold_snapshot=latest['snapshot_id'],
                    fair_parquet_comparison=dict(rows=len(full),compression='snappy',row_group_size=10000,
                        full_columns=53,selected_columns=24,full_bytes=full_size,selected_bytes=selected_size,
                        reduction_percent=round((1-selected_size/full_size)*100,2)))
    finally:
        con.close();db.close()


def main():
    load_dotenv(Path('.env'))
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--lake-id',default='m4-gce-june')
    p.add_argument('--source',type=Path,default=Path('data/m3/m3-gce-june/events.sqlite'))
    p.add_argument('--output',type=Path,default=Path('data/m4/m4-gce-june/verification.json'))
    args=p.parse_args()
    result=verify(Lake(Objects(),args.lake_id,load_rules('config/filter-rules.yaml')),args.source)
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))

if __name__=='__main__':
    main()
