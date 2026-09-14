"""Build a complete daily snapshot, including newly arrived historical events."""
import hashlib
from datetime import datetime, timezone
from lake.query import connection
from lake.schema import parquet_bytes


def refresh(lake):
    commits=lake.commits()
    fingerprint='\n'.join(c['commit_id']+':'+c['input_sha256'] for c in commits)
    snapshot=hashlib.sha256(fingerprint.encode()).hexdigest()[:24]
    key=f'{lake.control}/gold/snapshots/{snapshot}.json'
    existing=lake.objects.json(key)
    if existing is None:
        con=connection(lake,commits)
        queries={
            'event-volume': '''SELECT event_date, metadata.replay_run_id AS replay_run_id,
                              count(*) AS event_count FROM silver GROUP BY 1,2 ORDER BY 1,2''',
            'data-quality': '''SELECT r.event_date, r.metadata.replay_run_id AS replay_run_id,
                count(*) AS raw_count, count(s.source_id) AS silver_count,
                count(j.source_id) AS rejected_count,
                count(s.source_id) FILTER (WHERE s.event_time IS NULL) AS null_timestamp_count,
                count(s.source_id) FILTER (WHERE s.event_time IS NULL)::DOUBLE / nullif(count(s.source_id),0) AS null_timestamp_ratio
                FROM raw r LEFT JOIN silver s USING(source_id) LEFT JOIN rejected j USING(source_id)
                GROUP BY 1,2 ORDER BY 1,2''',
            'rejection-reasons': '''SELECT event_date, metadata.replay_run_id AS replay_run_id,
                unnest(reasons) AS reason, count(*) AS event_count FROM rejected GROUP BY ALL ORDER BY 1,2,3''',
        }
        files=[]
        try:
            for name,sql in queries.items():
                # UNNEST before grouping, so one event can contribute to multiple reasons.
                if name=='rejection-reasons':
                    sql='''SELECT event_date,replay_run_id,reason,count(*) AS event_count FROM
                        (SELECT event_date,metadata.replay_run_id AS replay_run_id,unnest(reasons) AS reason FROM rejected)
                        GROUP BY 1,2,3 ORDER BY 1,2,3'''
                table=con.execute(sql).fetch_arrow_table()
                files.append(dict(lake.objects.put(f'gold/{name}/{lake.id}/{snapshot}.parquet',parquet_bytes(table,table.schema),immutable=True),metric=name,rows=len(table)))
        finally:
            con.close()
        existing=dict(snapshot_id=snapshot,updated_at=datetime.now(timezone.utc).isoformat(),
                      timezone='America/Los_Angeles',commits=[c['commit_id'] for c in commits],
                      processed_ranges=[dict(commit_id=c['commit_id'],ranges=c['ranges']) for c in commits],files=files)
        lake.objects.put_json(key,existing,immutable=True)
    lake.objects.put_json(f'{lake.control}/gold/latest.json',existing)
    return existing
