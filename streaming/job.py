"""Run Spark Structured Streaming into the local transactional M3 sink."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import signal
import uuid

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQueryListener
from streaming.rules import load_rules, rules_hash
from streaming.store import bind_identity, commit_batch
from streaming.transforms import validated_frame


class ProgressLog(StreamingQueryListener):
    def __init__(self, path):
        super().__init__()
        self.path = path
    def onQueryStarted(self, event):
        pass
    def onQueryTerminated(self, event):
        pass
    def onQueryIdle(self, event):
        pass
    def onQueryProgress(self, event):
        # Includes batch duration and Kafka source offsets/lag metrics when available.
        with self.path.open('a') as stream:
            stream.write(event.progress.json + '\n')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--pipeline-id', default='m3-gce-june')
    p.add_argument('--rules', type=Path, default=Path('config/filter-rules.yaml'))
    p.add_argument('--bootstrap-servers', default=os.getenv('KAFKA_INTERNAL_BOOTSTRAP_SERVERS', 'kafka:29092'))
    p.add_argument('--topic', default=os.getenv('KAFKA_RAW_TOPIC', 'raw-events'))
    p.add_argument('--max-offsets', type=int, default=1000)
    p.add_argument('--continuous', action='store_true', help='Default drains available data and exits')
    p.add_argument('--trigger', default='5 seconds')
    p.add_argument('--test-crash-after-batch', type=int, help=argparse.SUPPRESS)
    args = p.parse_args()
    import re
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,100}', args.pipeline_id) or not 1 <= args.max_offsets <= 10000:
        p.error('Invalid pipeline ID or max-offsets')
    rules = load_rules(args.rules)
    output = Path('data/m3') / args.pipeline_id
    checkpoint = Path('checkpoints/spark') / args.pipeline_id
    logs = Path('logs/m3') / args.pipeline_id
    for path in (output, checkpoint, logs):
        path.mkdir(parents=True, exist_ok=True)
    with (output / 'pipeline.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        token_path = checkpoint / 'sink-token'
        database = output / 'events.sqlite'
        if database.exists() != token_path.exists():
            raise ValueError('Sink and checkpoint must be kept together; use a new pipeline ID for a rebuild')
        if not token_path.exists():
            token_path.write_text(str(uuid.uuid4()))
        identity = dict(rules=rules_hash(rules), topic=args.topic,
                        bootstrap_servers=args.bootstrap_servers,
                        checkpoint=str(checkpoint.resolve()), token=token_path.read_text())
        bind_identity(database, identity)
        spark = (SparkSession.builder.appName(args.pipeline_id)
                 .config('spark.sql.session.timeZone', 'UTC')
                 .config('spark.sql.shuffle.partitions', '2').getOrCreate())
        spark.sparkContext.setLogLevel('WARN')
        # foreachBatch can emit this upstream Kafka thread warning per record.
        # Keep errors visible without flooding progress logs with identical warnings.
        spark._jvm.org.apache.logging.log4j.core.config.Configurator.setLevel(
            'org.apache.spark.sql.kafka010.consumer.KafkaDataConsumer',
            spark._jvm.org.apache.logging.log4j.Level.ERROR)
        listener = ProgressLog(logs / 'progress.jsonl')
        spark.streams.addListener(listener)
        query = None
        try:
            source = (spark.readStream.format('kafka')
                      .option('kafka.bootstrap.servers', args.bootstrap_servers)
                      .option('subscribe', args.topic)
                      .option('startingOffsets', 'earliest')
                      .option('failOnDataLoss', 'true')
                      .option('maxOffsetsPerTrigger', args.max_offsets).load())
            def process(batch, batch_id):
                typed = validated_frame(batch, rules).orderBy('topic', 'partition', 'offset')
                rows = (row.asDict(recursive=True) for row in typed.collect())
                result = commit_batch(database, batch_id, rows)
                print(json.dumps(result), flush=True)
                with (logs / 'batches.jsonl').open('a') as stream:
                    stream.write(json.dumps(result) + '\n')
                if args.test_crash_after_batch == batch_id and not result.get('already_committed'):
                    os._exit(86)  # Integration-only: sink committed, Spark offset not yet committed.
            writer = (source.writeStream.foreachBatch(process)
                      .option('checkpointLocation', str(checkpoint)))
            writer = writer.trigger(processingTime=args.trigger) if args.continuous else writer.trigger(availableNow=True)
            query = writer.start()
            def stop(signum, frame):
                if query is not None:
                    query.stop()
            signal.signal(signal.SIGTERM, stop)
            signal.signal(signal.SIGINT, stop)
            query.awaitTermination()
        finally:
            if query is not None and query.isActive:
                query.stop()
            spark.stop()


if __name__ == '__main__':
    main()
