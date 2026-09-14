"""Drain Kafka into MinIO using a bounded Spark foreachBatch writer."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import uuid
from kafka.admin import KafkaAdminClient
from pyspark.sql import SparkSession
from lake.objects import Objects
from lake.writer import Lake, valid_id
from lake.gold import refresh
from streaming.rules import load_rules
from streaming.transforms import validated_frame
from streaming.job import ProgressLog


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--lake-id',default='m4-gce-june')
    p.add_argument('--rules',default='config/filter-rules.yaml')
    p.add_argument('--topic',default=os.getenv('KAFKA_RAW_TOPIC','raw-events'))
    p.add_argument('--bootstrap-servers',default=os.getenv('KAFKA_INTERNAL_BOOTSTRAP_SERVERS','kafka:29092'))
    p.add_argument('--max-offsets',type=int,default=1000)
    p.add_argument('--test-fail-after-zone',choices=['raw','silver','rejected'],help=argparse.SUPPRESS)
    p.add_argument('--test-crash-after-batch',type=int,help=argparse.SUPPRESS)
    args=p.parse_args()
    valid_id(args.lake_id)
    if not 1<=args.max_offsets<=10000:
        p.error('max-offsets must be between 1 and 10000')
    root=Path('checkpoints/lake')/args.lake_id
    checkpoint=root/'spark'
    logs=Path('logs/m4')/args.lake_id
    root.mkdir(parents=True,exist_ok=True);logs.mkdir(parents=True,exist_ok=True)
    with (root/'writer.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        rules=load_rules(args.rules)
        lake=Lake(Objects(),args.lake_id,rules)
        imported=lake.ready()
        admin=KafkaAdminClient(bootstrap_servers=args.bootstrap_servers)
        cluster=admin.describe_cluster()['cluster_id'];admin.close()
        lake.identity(args.topic,cluster)
        token=root/'sink-token'
        binding_key=f'{lake.control}/checkpoint.json'
        binding=lake.objects.json(binding_key)
        if binding is not None and not token.exists():
            raise ValueError('Missing checkpoint token: restore checkpoints/lake with MinIO; do not reset it')
        if not token.exists():
            token.write_text(str(uuid.uuid4()))
        lake.objects.put_json(binding_key,dict(token=token.read_text()),immutable=True)
        stream_commits=[c for c in lake.commits() if c['commit_id'].startswith('stream-')]
        if stream_commits and not (checkpoint/'metadata').exists():
            raise ValueError('Missing Spark checkpoint for committed stream; restore checkpoint or use a new lake ID')
        spark=(SparkSession.builder.appName(args.lake_id).config('spark.sql.session.timeZone','UTC')
               .config('spark.sql.shuffle.partitions','2').getOrCreate())
        spark.sparkContext.setLogLevel('WARN')
        spark._jvm.org.apache.logging.log4j.core.config.Configurator.setLevel(
            'org.apache.spark.sql.kafka010.consumer.KafkaDataConsumer',spark._jvm.org.apache.logging.log4j.Level.ERROR)
        listener=ProgressLog(logs/'progress.jsonl');spark.streams.addListener(listener)
        try:
            source=(spark.readStream.format('kafka').option('kafka.bootstrap.servers',args.bootstrap_servers)
                    .option('subscribe',args.topic).option('startingOffsets',json.dumps(imported['starting_offsets']))
                    .option('failOnDataLoss','true').option('maxOffsetsPerTrigger',args.max_offsets).load())
            def process(batch,batch_id):
                rows=[r.asDict(recursive=True) for r in validated_frame(batch,rules).orderBy('topic','partition','offset').collect()]
                result=lake.publish(f'stream-{batch_id:012d}',rows,fail_after_zone=args.test_fail_after_zone)
                with (logs/'batches.jsonl').open('a') as out:
                    out.write(json.dumps(result)+'\n')
                print(json.dumps(dict(batch=batch_id,counts=result['counts'],retried=result.get('retried',False))),flush=True)
                if args.test_crash_after_batch==batch_id and not result.get('retried'):
                    os._exit(86)
            query=(source.writeStream.foreachBatch(process).option('checkpointLocation',str(checkpoint))
                   .trigger(availableNow=True).start())
            query.awaitTermination()
        finally:
            spark.stop()
        gold=refresh(lake)
        print(json.dumps(dict(gold_snapshot=gold['snapshot_id'],updated_at=gold['updated_at'])),flush=True)

if __name__=='__main__':
    main()
