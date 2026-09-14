"""Migrate a frozen M3 SQLite snapshot, preserving Kafka source coordinates."""
import argparse
import fcntl
import hashlib
import json
from pathlib import Path
import sqlite3

from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient
from lake.objects import Objects
from lake.writer import Lake
from streaming.rules import load_rules, rules_hash, validate


def snapshot_source(db, rules, topic):
    identity = {k:json.loads(v) for k,v in db.execute('SELECT key,value FROM identity')}
    if identity.get('rules') != rules_hash(rules) or identity.get('topic') != topic:
        raise ValueError('M3 source/rules differ from selected M4 rules/topic')
    counts = {table:db.execute(f'SELECT count(*) FROM {table}').fetchone()[0]
              for table in ('raw_events', 'silver_events', 'rejected_events')}
    if counts['raw_events'] != counts['silver_events']+counts['rejected_events']:
        raise ValueError('M3 accounting mismatch')
    partitions = {}
    for t,p,lo,hi,n in db.execute('SELECT topic,partition_id,min(offset_id),max(offset_id),count(*) FROM raw_events GROUP BY topic,partition_id'):
        if t != topic or n != hi-lo+1:
            raise ValueError('Cannot cut over a non-contiguous or mixed-topic SQLite source')
        partitions[str(p)] = dict(first=lo, end=hi+1, count=n)
    return partitions, counts


def migration_rows(db, rules):
    query = '''SELECT r.topic,r.partition_id,r.offset_id,r.value,r.kafka_timestamp,
               s.metadata,s.payload,s.event_time,s.quality_flags,j.reasons,j.details
               FROM raw_events r LEFT JOIN silver_events s USING(source_id)
               LEFT JOIN rejected_events j USING(source_id)
               ORDER BY r.topic,r.partition_id,r.offset_id'''
    for t,p,o,value,kt,meta,payload,et,flags,reasons,details in db.execute(query):
        if meta is not None and reasons is not None:
            raise ValueError('Same source row appears in Silver and Rejected')
        if meta is None and reasons is None:
            raise ValueError('Unclassified source row')
        # Preserve stored M3 classification/values, do not silently reclassify during export.
        parsed = validate(value, rules) if meta is None else {}
        yield dict(topic=t, partition=p, offset=o, value=value, timestamp=kt,
                   metadata=json.loads(meta) if meta else parsed.get('metadata'),
                   payload=json.loads(payload) if payload else {}, event_time=et,
                   quality_flags=json.loads(flags) if flags else [],
                   reasons=json.loads(reasons) if reasons else [], details=json.loads(details) if details else [])


def migrate(lake, source, bootstrap, topic, chunk=5000):
    with (source.parent/'pipeline.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX|fcntl.LOCK_NB)
        db=sqlite3.connect(source.resolve().as_uri()+'?mode=rw', uri=True)
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        try:
            partitions, counts = snapshot_source(db, lake.rules, topic)
            digest = hashlib.sha256()
            for row in migration_rows(db, lake.rules):
                digest.update(json.dumps(row,sort_keys=True,default=lambda x:bytes(x).hex()).encode()+b'\n')
            source_hash = digest.hexdigest()
            admin=KafkaAdminClient(bootstrap_servers=bootstrap)
            cluster=admin.describe_cluster()['cluster_id']
            admin.close()
            lake.identity(topic,cluster)
            consumer=KafkaConsumer(bootstrap_servers=bootstrap,enable_auto_commit=False,group_id=None)
            from kafka import TopicPartition
            parts=consumer.partitions_for_topic(topic)
            if parts is None:
                raise ValueError('Kafka topic does not exist')
            if set(partitions)-{str(p) for p in parts}:
                raise ValueError('An imported Kafka partition no longer exists')
            tps=[TopicPartition(topic,p) for p in parts]
            beginnings=consumer.beginning_offsets(tps)
            ends=consumer.end_offsets(tps)
            consumer.close()
            starts={}
            for tp in tps:
                position=partitions.get(str(tp.partition),{}).get('end',beginnings[tp])
                if not beginnings[tp] <= position <= ends[tp]:
                    raise ValueError('Kafka cannot continue from SQLite boundary: retention/reset gap')
                starts[str(tp.partition)]=position
            summary=dict(source_sha256=source_hash,counts=counts,partitions=partitions,
                         starting_offsets={topic:starts},cluster_id=cluster,topic=topic)
            existing=lake.objects.json(f'{lake.control}/import.json')
            if existing is not None:
                if existing['source_sha256'] != source_hash:
                    raise ValueError('SQLite changed after import; use a new lake ID')
                return existing
            buffer=[]
            index=0
            for row in migration_rows(db,lake.rules):
                buffer.append(row)
                if len(buffer)==chunk:
                    lake.publish(f'import-{index:06d}',buffer)
                    index+=1;buffer=[]
            if buffer:
                lake.publish(f'import-{index:06d}',buffer)
            lake.objects.put_json(f'{lake.control}/import.json',summary,immutable=True)
            return summary
        finally:
            db.close()


def main():
    load_dotenv(Path('.env'))
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--lake-id',default='m4-gce-june')
    p.add_argument('--source',type=Path,default=Path('data/m3/m3-gce-june/events.sqlite'))
    p.add_argument('--rules',default='config/filter-rules.yaml')
    import os
    p.add_argument('--bootstrap-servers',default=os.getenv('KAFKA_LOCAL_BOOTSTRAP_SERVERS','localhost:9092'))
    p.add_argument('--topic',default=os.getenv('KAFKA_RAW_TOPIC','raw-events'))
    args=p.parse_args()
    root=Path('checkpoints/lake')/args.lake_id
    from lake.writer import valid_id
    valid_id(args.lake_id)
    root.mkdir(parents=True,exist_ok=True)
    with (root/'writer.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        result=migrate(Lake(Objects(),args.lake_id,load_rules(args.rules)),args.source,args.bootstrap_servers,args.topic)
    print(json.dumps(result,indent=2))


if __name__=='__main__':
    main()
