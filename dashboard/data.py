"""Freeze one published Gold snapshot; all event views share its source coverage."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaConsumer, TopicPartition
from lake.objects import Objects
from lake.query import connection
from lake.writer import Lake, valid_id
from streaming.rules import load_rules

ROOT = Path(__file__).resolve().parents[1]


def json_file(path):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None


def batch_history(path):
    """Spark input counts can include rereads: use source offsets, not numInputRows."""
    values = {}
    if path.exists():
        for line in path.read_text().splitlines():
            try:
                row = json.loads(line)
                count = 0
                for source in row['sources']:
                    for topic, parts in (source.get('endOffset') or {}).items():
                        start = (source.get('startOffset') or {}).get(topic, {})
                        if not all(p in start for p in parts):
                            count = None
                            break
                        count += sum(end - start[p] for p, end in parts.items())
                ms = row.get('durationMs', {}).get('triggerExecution')
                values[(row['id'], row['batchId'])] = dict(
                    batch=row['batchId'], time=row['timestamp'], source_offsets=count,
                    duration_ms=ms, offsets_per_second=count / (ms / 1000) if count is not None and ms else None)
            except (ValueError, KeyError, TypeError):
                continue
    return list(values.values())


def kafka_status(topic, commits):
    ends = {}
    for commit in commits:
        for part, span in commit['ranges'].items():
            ends[int(part)] = max(ends.get(int(part), 0), span['end'])
    consumer = None
    try:
        consumer = KafkaConsumer(bootstrap_servers=os.getenv('KAFKA_LOCAL_BOOTSTRAP_SERVERS', 'localhost:9092'),
                                 group_id=None, enable_auto_commit=False, api_version_auto_timeout_ms=2500,
                                 request_timeout_ms=5000)
        parts = consumer.partitions_for_topic(topic)
        if not parts:
            raise ValueError('Topic unavailable')
        positions = consumer.end_offsets([TopicPartition(topic, p) for p in parts])
        return dict(available=True, partitions=[dict(partition=p.partition, kafka_end=end,
                    stored_end=ends.get(p.partition), offset_gap=end-ends[p.partition] if p.partition in ends else None)
                    for p, end in sorted(positions.items())])
    except Exception:
        return dict(available=False, partitions=[])
    finally:
        if consumer is not None:
            consumer.close()


def load_snapshot(lake_id):
    valid_id(lake_id)
    load_dotenv(ROOT / '.env')
    lake = Lake(Objects(), lake_id, load_rules(ROOT / 'config/filter-rules.yaml'))
    # Capture the pointer first. Later commits must not leak into this dashboard snapshot.
    gold = lake.objects.json(f'{lake.control}/gold/latest.json')
    if gold is None:
        raise ValueError('Gold is not available. Complete an M4 run, then refresh.')
    commits = lake.commits()
    by_id = {c['commit_id']: c for c in commits}
    if any(cid not in by_id for cid in gold['commits']):
        raise ValueError('Gold references missing source commits.')
    covered = [by_id[cid] for cid in gold['commits']]
    con = connection(lake, covered, gold_snapshot=gold)
    try:
        quality = con.execute('SELECT * FROM gold_data_quality').fetch_arrow_table().to_pandas()
        volume = con.execute('SELECT * FROM gold_event_volume').fetch_arrow_table().to_pandas()
        reasons = con.execute('SELECT * FROM gold_rejection_reasons').fetch_arrow_table().to_pandas()
        counts = {zone: con.execute(f'SELECT count(*) FROM {zone}').fetchone()[0] for zone in lake.schemas}
        if counts['silver'] > 100000 or counts['rejected'] > 100000:
            raise ValueError('This local dashboard supports up to 100,000 Silver and Rejected rows per snapshot. Server-side paging is required for a larger lake.')
        silver = con.execute('''SELECT * EXCLUDE(metadata), metadata.replay_run_id AS replay_run_id,
            metadata.event_id AS event_id, metadata.source_row AS source_row FROM silver''').fetch_arrow_table().to_pandas()
        rejected = con.execute('''SELECT source_id,event_date,metadata.replay_run_id AS replay_run_id,
            reason,reasons,details FROM rejected''').fetch_arrow_table().to_pandas()
        if any(int(quality[f'{z}_count'].sum()) != counts[z] for z in counts):
            raise ValueError('Gold counts do not reconcile with their source snapshot.')
        if int(volume.event_count.sum()) != counts['silver']:
            raise ValueError('Gold volume does not reconcile with Silver.')
    finally:
        con.close()
    files = [dict(f, commit_id=c['commit_id']) for c in covered for f in c['files']]
    history = batch_history(ROOT / 'logs/m4' / lake_id / 'progress.jsonl')
    # Only list producer state matching a run in this snapshot and its Kafka topic.
    topic = lake.ready()['topic']
    known_runs = set(silver.replay_run_id.dropna())
    replay = []
    for path in (ROOT / 'checkpoints/replay').glob('*.json'):
        state = json_file(path)
        if state and state.get('topic') == topic and state.get('replay_run_id') in known_runs:
            replay.append({k: state.get(k) for k in ('replay_run_id', 'last_acked_row', 'complete')})
    return dict(lake_id=lake_id, bucket=lake.objects.bucket, gold=gold, commits=covered,
                newer_commits=len(set(by_id)-set(gold['commits'])), quality=quality, volume=volume,
                reasons=reasons, silver=silver, rejected=rejected, files=files, history=history,
                kafka=kafka_status(topic, commits), replay=replay,
                comparison=json_file(ROOT / 'data/m4' / lake_id / 'verification.json'),
                loaded_at=datetime.now(timezone.utc).isoformat())


def scope(frame, start=None, end=None, date_scope='Dated + unknown', run=None, unidentified=False):
    result = frame
    if unidentified:
        result = result[result.replay_run_id.isna()]
    elif run is not None:
        result = result[result.replay_run_id == run]
    unknown = result.event_date.eq('unknown')
    dated = ~unknown
    if start:
        dated &= result.event_date.ge(str(start))
    if end:
        dated &= result.event_date.le(str(end))
    mask = unknown if date_scope == 'Unknown only' else dated if date_scope == 'Dated only' else dated | unknown
    return result.loc[mask].copy()


def totals(quality):
    result = {key: int(quality[key].sum()) for key in
              ('raw_count', 'silver_count', 'rejected_count', 'null_timestamp_count')}
    result['rejected_ratio'] = result['rejected_count']/result['raw_count'] if result['raw_count'] else None
    result['null_ratio'] = result['null_timestamp_count']/result['silver_count'] if result['silver_count'] else None
    return result


def breakdown(frame, field):
    # Preserve null/blank as explicit categories rather than dropping them in groupby.
    return frame[field].fillna('(Missing)').replace('', '(Empty)').value_counts().rename_axis('Category').reset_index(name='Events')
