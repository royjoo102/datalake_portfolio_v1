"""Open DuckDB UI with native SQL views of committed MinIO Parquet."""
import argparse
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
from urllib.parse import urlparse

import duckdb
import pyarrow as pa
from dotenv import load_dotenv
from lake.objects import Objects
from lake.writer import Lake
from streaming.rules import load_rules


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def setup_sql(lake):
    """Use shared catalog views, not connection-local Python Arrow registrations."""
    commits = lake.commits()
    settings = lake.objects.settings
    endpoint = urlparse(settings['endpoint'])
    sql = ['INSTALL httpfs;', 'LOAD httpfs;', "SET TimeZone='UTC';"]
    sql.append(f'''CREATE SECRET lake_minio (
        TYPE s3, KEY_ID {literal(settings['access'])}, SECRET {literal(settings['secret'])},
        REGION {literal(settings['region'])}, ENDPOINT {literal(endpoint.netloc)},
        URL_STYLE 'path', USE_SSL {'true' if endpoint.scheme == 'https' else 'false'}
    );''')
    for zone, schema in lake.schemas.items():
        paths = [f"s3://{lake.objects.bucket}/{f['key']}" for c in commits for f in c['files'] if f['zone'] == zone]
        if paths:
            source = 'read_parquet([' + ','.join(map(literal, paths)) + '], hive_partitioning=false)'
            sql.append(f'CREATE VIEW {zone} AS SELECT * FROM {source};')
        else:
            # Retain typed, queryable empty zones without inventing a data file.
            con = duckdb.connect()
            try:
                con.register('empty_zone', pa.Table.from_pylist([], schema=schema))
                columns = con.execute('DESCRIBE empty_zone').fetchall()
                fields = ', '.join(f'CAST(NULL AS {kind}) AS "{name}"' for name, kind, *_ in columns)
                sql.append(f'CREATE VIEW {zone} AS SELECT {fields} WHERE false;')
            finally:
                con.close()
    latest = lake.objects.json(f'{lake.control}/gold/latest.json')
    if latest:
        for item in latest['files']:
            name = 'gold_' + item['metric'].replace('-', '_')
            path = f"s3://{lake.objects.bucket}/{item['key']}"
            sql.append(f'CREATE VIEW {name} AS SELECT * FROM read_parquet({literal(path)}, hive_partitioning=false);')
    return '\n'.join(sql)


def main():
    load_dotenv(Path('.env'))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--lake-id', default='m4-gce-june')
    parser.add_argument('--rules', default='config/filter-rules.yaml')
    parser.add_argument('--port', type=int, default=4214)
    parser.add_argument('--check', action='store_true', help='Verify SQL views without opening the UI')
    args = parser.parse_args()
    if not 1024 <= args.port <= 65535:
        parser.error('port must be between 1024 and 65535')
    cli = shutil.which('duckdb')
    if cli is None:
        parser.error('DuckDB CLI is required (duckdb must be on PATH)')
    if not args.check:
        try:
            with socket.create_connection(('localhost', args.port), timeout=0.5):
                pass
        except OSError:
            pass
        else:
            parser.error(f'Port {args.port} is already in use. Exit its DuckDB terminal with .quit, or select another --port.')
    lake = Lake(Objects(), args.lake_id, load_rules(args.rules))
    sql = setup_sql(lake)
    if not args.check:
        # DuckDB's default UI notebook database is shared across processes.
        # A separate port alone does not prevent its file-lock collision.
        ui_directory = Path(__file__).resolve().parents[1] / 'data/duckdb-ui' / lake.id / str(args.port)
        ui_directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        sql += (f"\nINSTALL ui; LOAD ui;"
                f" ATTACH {literal(ui_directory / 'notebooks.duckdb')} AS _duckdb_ui;"
                f" SET ui_local_port={args.port};")
    # Private ephemeral setup file; credentials never appear in command arguments.
    with tempfile.TemporaryDirectory(prefix='lake-ui-') as directory:
        path = Path(directory) / 'setup.sql'
        with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as out:
            out.write(sql)
        command = [cli, '-bail', '-init', str(path)]
        if args.check:
            command += ['-c', "SELECT 'raw' AS zone, count(*) AS rows FROM raw UNION ALL SELECT 'silver', count(*) FROM silver UNION ALL SELECT 'rejected', count(*) FROM rejected;"]
        else:
            command += ['-ui']
            print(f'Opening http://localhost:{args.port} with raw, silver, rejected and Gold views.', flush=True)
            print('Keep this terminal open. Exit with .quit; restart to include newly committed files.', flush=True)
        raise SystemExit(subprocess.call(command))


if __name__ == '__main__':
    main()
