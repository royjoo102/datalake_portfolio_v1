#!/usr/bin/env python3
"""Exercise migration, partial uploads, Spark retries, and late events on real services."""
import copy
import fcntl
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import uuid
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from lake.objects import Objects
from lake.writer import Lake
from lake.migrate import migrate
from lake.query import connection
from streaming.rules import load_rules, rules_hash, validate
from streaming.store import bind_identity, commit_batch
from tests.test_streaming import sample, encoded


def main():
    load_dotenv(Path('.env'))
    name='m4-test-'+uuid.uuid4().hex[:10]
    bootstrap=os.getenv('KAFKA_LOCAL_BOOTSTRAP_SERVERS','localhost:9092')
    rules=load_rules('config/filter-rules.yaml')
    admin=KafkaAdminClient(bootstrap_servers=bootstrap)
    admin.create_topics([NewTopic(name,num_partitions=3,replication_factor=1)]);admin.close()
    event=sample()
    events=[encoded(event),encoded(event)]
    for timestamp in ('','bad',None):
        row=copy.deepcopy(event);row['payload']['event_timestamp']=timestamp
        events.append(encoded(row))
    events += [b'{bad',b'\xff',None]
    row=copy.deepcopy(event);row['metadata']['schema_version']='99';events.append(encoded(row))
    row=copy.deepcopy(event);del row['metadata']['event_id'];events.append(encoded(row))
    row=copy.deepcopy(event);row['payload']['application_title']=42;events.append(encoded(row))
    old=copy.deepcopy(event);old['payload']['event_timestamp']='946684800000';events.append(encoded(old))
    producer=KafkaProducer(bootstrap_servers=bootstrap,acks='all',enable_idempotence=True)
    coords=[]
    for i,value in enumerate(events):
        result=producer.send(name,partition=0 if i<2 else i%3,value=value,key=b'test').get(timeout=30)
        coords.append((result.partition,result.offset))
    source=Path('data/m4')/name/'events.sqlite'
    source.parent.mkdir(parents=True)
    bind_identity(source,dict(rules=rules_hash(rules),topic=name))
    rows=[dict(validate(value,rules),topic=name,partition=p,offset=o,value=value,timestamp=None)
          for value,(p,o) in zip(events[:2],coords[:2])]
    # Match the M3 sink projection.
    for row in rows:
        row['payload']={k:row['payload'].get(k) for k in rules['allowlist']}
    commit_batch(source,0,rows)
    lake=Lake(Objects(),name,rules)
    root=Path('checkpoints/lake')/name;root.mkdir(parents=True)
    with (root/'writer.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        migrate(lake,source,bootstrap,name)
    logs=Path('logs/m4')/name;logs.mkdir(parents=True)
    command=['./scripts/run-lake.sh','--lake-id',name,'--topic',name,'--max-offsets','4']
    def run(label,extra=(),success=True):
        with (logs/f'{label}.log').open('w') as output:
            result=subprocess.run(command+list(extra),stdout=output,stderr=subprocess.STDOUT)
        assert (result.returncode==0)==success, f'{label}: see {logs}/{label}.log'
    run('partial',['--test-fail-after-zone','raw'],False)
    assert not any(c['commit_id'].startswith('stream-') for c in lake.commits())
    con=connection(lake)
    assert con.execute('SELECT count(*) FROM raw').fetchone()[0]==2
    con.close()
    run('crash',['--test-crash-after-batch','0'],False)
    assert any(c['commit_id']=='stream-000000000000' for c in lake.commits())
    assert (root/'spark/offsets/0').exists() and not (root/'spark/commits/0').exists()
    run('resume')
    con=connection(lake)
    assert [con.execute(f'SELECT count(*) FROM {z}').fetchone()[0] for z in ('raw','silver','rejected')]==[12,6,6]
    assert con.execute('SELECT count(*) FROM silver WHERE event_time IS NULL').fetchone()[0]==3
    assert con.execute("SELECT count(*) FROM silver WHERE event_date='2026-06-06'").fetchone()[0]==2
    con.close()
    before=lake.objects.json(f'{lake.control}/gold/latest.json')
    producer.send(name,partition=2,value=encoded(old),key=b'late').get(timeout=30);producer.close()
    run('late')
    after=lake.objects.json(f'{lake.control}/gold/latest.json')
    assert before['snapshot_id']!=after['snapshot_id']
    import pyarrow.parquet as pq
    import pyarrow as pa
    f=next(f for f in after['files'] if f['metric']=='event-volume')
    gold=pq.read_table(pa.BufferReader(lake.objects.get(f['key']))).to_pylist()
    assert next(r['event_count'] for r in gold if r['event_date']=='1999-12-31')==2
    run('restart')
    con=connection(lake)
    counts={z:con.execute(f'SELECT count(*) FROM {z}').fetchone()[0] for z in ('raw','silver','rejected')}
    assert counts==dict(raw=13,silver=7,rejected=6),counts
    assert con.execute('SELECT count(DISTINCT source_id) FROM raw').fetchone()[0]==13
    actual={(r['partition'],r['offset']):r['raw_value'] for r in con.execute('SELECT * FROM raw').fetch_arrow_table().to_pylist()}
    assert all(actual[c]==v for c,v in zip(coords,events))
    con.close()
    with sqlite3.connect(source) as db:
        assert db.execute('SELECT count(*) FROM raw_events').fetchone()[0]==2
    result=dict(status='PASS',lake_id=name,topic=name,counts=counts,migrated=2,null_timestamp=3,
                partial_upload_invisible=True,crash_after_manifest_recovered=True,
                late_event_gold_updated=True,business_duplicates_preserved=True,
                restart_no_extra_rows=True,sqlite_unchanged=True,three_partitions=True)
    (logs/'acceptance.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))
    print('Synthetic topic, MinIO prefix, and local artifacts retained for inspection.')

if __name__=='__main__':
    main()
