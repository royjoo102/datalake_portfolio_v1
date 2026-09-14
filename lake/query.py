"""Query committed MinIO Parquet directly with DuckDB and Arrow."""
import argparse
import json
from pathlib import Path
import duckdb
import pyarrow as pa
import pyarrow.dataset as ds
from dotenv import load_dotenv
from lake.objects import Objects
from lake.writer import Lake
from streaming.rules import load_rules


def connection(lake, commits=None, gold_snapshot=...):
    commits = lake.commits() if commits is None else commits
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    fs = lake.objects.arrow_fs()
    for zone, schema in lake.schemas.items():
        paths = [f"{lake.objects.bucket}/{f['key']}" for c in commits for f in c['files'] if f['zone']==zone]
        data = ds.dataset(paths, filesystem=fs, format='parquet', schema=schema) if paths else pa.Table.from_pylist([], schema=schema)
        con.register(zone, data)
    latest = lake.objects.json(f'{lake.control}/gold/latest.json') if gold_snapshot is ... else gold_snapshot
    if latest is not None:
        for item in latest['files']:
            name = 'gold_' + item['metric'].replace('-', '_')
            data = ds.dataset([f"{lake.objects.bucket}/{item['key']}"], filesystem=fs, format='parquet')
            con.register(name, data)
    return con


def main():
    load_dotenv(Path('.env'))
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--lake-id', default='m4-gce-june')
    p.add_argument('--rules', default='config/filter-rules.yaml')
    p.add_argument('--sql', default='SELECT event_date, count(*) AS events FROM silver GROUP BY event_date ORDER BY event_date')
    args=p.parse_args()
    lake=Lake(Objects(),args.lake_id,load_rules(args.rules))
    con=connection(lake)
    try:
        result=con.execute(args.sql)
        print(json.dumps([dict(zip([d[0] for d in result.description],row)) for row in result.fetchall()], default=str,ensure_ascii=False,indent=2))
    finally:
        con.close()

if __name__=='__main__':
    main()
