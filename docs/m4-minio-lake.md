# M4: MinIO Parquet Lake

M4 moves the existing M3 results into MinIO and continues from their Kafka offsets. New events follow **Kafka → Spark validation → MinIO**. SQLite remains an unchanged M3 archive, not an intermediate sink for new events.

## Agreed contract

- Preserve every original Kafka value, including invalid UTF-8 and tombstones, in Raw. `raw_value` is binary; a Kafka tombstone is null.
- Keep the 24 business fields selected in `config/filter-rules.yaml` as top-level Silver string columns. Keep `event_type` and `attributes_event_type` independently.
- Keep original replay tracking information in the nested `metadata` struct. Technical columns also include `source_id`, `topic`, `partition`, `offset`, UTC `event_time`, `event_date`, and Silver `quality_flags`.
- Compute `event_date` from `event_time` in `America/Los_Angeles`, including daylight saving time. For example, `2026-06-07T04:59:45Z` belongs to `2026-06-06` locally. Do not partition by either event-type field.
- Keep missing or invalid event timestamps in Silver with null `event_timestamp`/`event_time`, a quality flag, and `event_date=unknown`. Raw preserves the unmodified input. Structural/schema failures still go to Rejected, as in M3.
- Do not deduplicate business events or discard late arrivals. Identical events at different Kafka offsets remain separate rows.
- Use Snappy Parquet. Retain SQLite, Kafka backups, checkpoints, previous Gold snapshots, and incomplete uploads; there is no automatic deletion or compaction.

## Run and query

Run from the repository root with the existing Kafka and MinIO services running:

```bash
source datalake_env/bin/activate
python -m pip install -r requirements.txt -r requirements-lake.txt
docker compose --profile m4 build spark-lake

# Initial import. Repeating against the same unchanged source is safe.
python -m lake.migrate

# Process the available Kafka backlog and refresh Gold, then exit.
./scripts/run-lake.sh

# Query Parquet in MinIO directly.
python -m lake.query
python -m lake.query --sql 'SELECT event_date, event_type, attributes_event_type, count(*) AS events FROM silver GROUP BY 1,2,3 ORDER BY 1,2,3'
python -m lake.query --sql 'SELECT * FROM gold_event_volume ORDER BY event_date, replay_run_id'
python -m lake.query --sql 'SELECT * FROM gold_data_quality ORDER BY event_date, replay_run_id'
python -m lake.query --sql 'SELECT * FROM gold_rejection_reasons'
python -m lake.query --sql "SELECT source_id, decode(raw_value) AS event_json FROM raw WHERE event_date <> 'unknown' LIMIT 1"
```

`decode(raw_value)` is appropriate only for valid UTF-8; use the binary column when inspecting malformed input. MinIO Console at `http://localhost:9001` shows the objects and lets you download them. Parquet is a columnar binary format, so use DuckDB for row contents rather than expecting the Console to display JSON.

The host commands load `.env`. Docker uses `MINIO_INTERNAL_ENDPOINT` and `KAFKA_INTERNAL_BOOTSTRAP_SERVERS`. Credentials and bucket names come from the existing environment settings. DuckDB runs in memory and scans MinIO through Arrow's S3 filesystem; it does not create a local database or download a full persistent copy.

Defaults: `--lake-id m4-gce-june`, topic `raw-events`, rules `config/filter-rules.yaml`, migration source `data/m3/m3-gce-june/events.sqlite`. `--max-offsets` controls the Spark per-trigger target (default 1,000, configurable from 1 to 10,000). It controls batch size, not the total processing range. Use `--lake-id` with the query command to inspect isolated test runs.

## Interactive SQL in the DuckDB UI

From the repository root, run:

```bash
source datalake_env/bin/activate
python -m lake.ui
```

This opens the local DuckDB UI at `http://localhost:4214`. Use the newly opened tab, not an older UI at port 4213. Keep the terminal open while querying; enter `.quit` to exit. The host DuckDB CLI must be installed. The first run installs DuckDB's `httpfs` and `ui` extensions and requires internet access.

```sql
SHOW TABLES;
SELECT * FROM silver LIMIT 100;
SELECT * FROM gold_event_volume ORDER BY event_date;
```

`lake.ui` registers native SQL views over the manifest-listed S3 Parquet objects. These views are shared across connections in the DuckDB instance. The Python Arrow registrations used by `lake.query` are connection-local; starting a UI from that Python connection does not make those registrations available to the UI's separate query connection, and can produce `Table with name silver does not exist`.

No event data is copied into a local DuckDB database. The setup uses a temporary S3 secret and a private temporary SQL file removed when the launcher exits. View file lists are fixed at launch; restart the launcher after processing more events to include the newly committed data and current Gold snapshot. `python -m lake.ui --check` verifies the views without opening a browser. Use `--port` to choose another UI port.

## Storage and completion

```text
s3://data-lake/
  raw/events/<lake-id>/event_date=<YYYY-MM-DD|unknown>/<commit-id>.parquet
  silver/events/<lake-id>/event_date=<YYYY-MM-DD|unknown>/<commit-id>.parquet
  rejected/events/<lake-id>/event_date=<YYYY-MM-DD|unknown>/<commit-id>.parquet
  gold/event-volume/<lake-id>/<snapshot-id>.parquet
  gold/data-quality/<lake-id>/<snapshot-id>.parquet
  gold/rejection-reasons/<lake-id>/<snapshot-id>.parquet
  _control/<lake-id>/
    identity.json
    import.json
    checkpoint.json
    commits/<commit-id>.json
    gold/latest.json
    gold/snapshots/<snapshot-id>.json
```

Raw and Rejected also use the parsed event date where available. Events without a usable event time go into `unknown`; that folder is not deleted. Each batch may add a file to several dates. The data is partitioned by date, not globally sorted by event timestamp; specify `ORDER BY` in queries when order matters.

A writer uploads all Raw, Silver and Rejected objects, then writes one immutable commit manifest listing their keys, counts, SHA-256 hashes, and Kafka ranges. **Only files listed in committed manifests are visible to supported readers.** Do not glob the entire prefix: an interrupted upload can leave uncommitted files there. This is logical atomic publication by this application, not an S3 multi-object transaction or an Iceberg/Delta table.

Retries use the same deterministic object names. A matching committed batch is skipped; changed contents under the same batch ID fail rather than overwrite existing data. This protects offset retries, not business-level duplicates. A single writer per lake ID is enforced by a shared local file lock; this is a local development guarantee, not distributed multi-writer coordination.

The initial import takes a query-only SQLite transaction under the M3 pipeline lock. It preserves stored M3 classifications and values. It publishes `import.json` only after every import batch succeeds. All readers refuse an incomplete import. Continuation starts at each imported partition's maximum offset plus one. Kafka retention or reset gaps detected at import fail the operation.

The first job uses those explicit starting offsets. Subsequent runs use the **M4 Spark checkpoint** at `checkpoints/lake/<lake-id>/spark/`. The checkpoint token is bound to MinIO, and missing/mismatched checkpoints fail. Keep the entire `checkpoints/lake/<lake-id>/` directory with the MinIO data. Do not reuse M3's checkpoint, clear individual checkpoint files, change rules under an existing lake ID, or recreate the source topic in place. A deliberate rebuild needs a new lake ID and a suitable retained source. Do not run the old M3 consumer as the active production path after this cutover.

`availableNow` determines the Kafka end offsets at the start of the query and drains that finite range. New arrivals after this boundary are handled by the next invocation. A successful command means that Spark committed the captured range and Gold publication succeeded. It does **not** mean no more events can arrive for a calendar day. No event-time deadline or watermark closes a day permanently. Logs contain Spark source offsets in `logs/m4/<lake-id>/progress.jsonl` and MinIO batch results in `batches.jsonl`.

## Daily Gold

After successful draining, Gold is recomputed over every committed batch for this lake ID, including the migration. Late events update their original LA date on the next refresh. Each snapshot is immutable; the last publication step updates `gold/latest.json` with its timestamp, source commit IDs, exact processed Kafka ranges, and output files. Readers should use this pointer, not sum all historical snapshots. If Gold generation fails, the previous pointer remains valid and rerunning the job refreshes it.

| Query table | Grain | Measures |
| --- | --- | --- |
| `gold_event_volume` | LA event date + replay run | Silver event count |
| `gold_data_quality` | LA event date + replay run | Raw, Silver, Rejected, null Silver timestamp counts and null timestamp ratio |
| `gold_rejection_reasons` | LA event date + replay run + rejection reason | Events with that reason |

Unidentifiable replay runs group under SQL null; unknown event dates use `unknown`. The null timestamp ratio divides by Silver count and is null where that denominator is zero. An event with multiple rejection reasons contributes once to each reason, so the reason total need not equal the Rejected event count. Runs are kept separate, and both event-type columns remain available for ad hoc queries. A newer committed batch can be visible in Raw/Silver before the next Gold snapshot; inspect the Gold pointer's source coverage and update time when comparing them.

## Files and responsibilities

- `lake/schema.py`: typed Arrow schemas, timestamp conversion, date routing, and Snappy encoding.
- `lake/objects.py`: S3 access, immutable writes, object listing, and the Arrow filesystem.
- `lake/writer.py`: batch fingerprints, deterministic object paths, and commit manifests.
- `lake/migrate.py`: frozen M3 export, integrity checks, and Kafka cutover offsets.
- `lake/job.py`: Spark Kafka source, validation, bounded batch publication, checkpoint binding, and Gold refresh.
- `lake/query.py`: manifest-aware Arrow datasets exposed to DuckDB as `raw`, `silver`, `rejected`, and the latest Gold tables.
- `lake/gold.py`: daily aggregates and immutable Gold snapshot publication.
- `docker/spark-lake.Dockerfile`, `requirements-lake.txt`, and the `spark-lake` Compose service: separate M4 runtime dependencies.
- `scripts/run-lake.sh`: run the dedicated Spark container with `local[2]`.
- `scripts/verify-lake.py`: compare original M3 rows and selected values with MinIO, verify committed object hashes, and report physical file sizes and the fair 53-vs-24-column comparison.
- `tests/test_lake.py`, `scripts/test-lake-integration.py`: unit and real-service recovery tests.

Spark validates each microbatch, then its driver encodes the bounded batch with PyArrow and uploads directly using S3 APIs. There is no SQLite write on this path. It uses `local[2]`, not the separate Spark master/worker, and is intended for this local milestone's volume. Batch collection and full-snapshot Gold joins are not a design for unbounded production volume. Larger workloads need distributed Parquet writes, a table format with transactional commits, scalable aggregation, and compaction. Small files are measured here and retained; no files are merged or removed automatically.

## Validation

```bash
python -m unittest discover -s tests -v
python scripts/test-lake-integration.py
python scripts/verify-lake.py
```

The integration test uses a new `m4-test-*` topic with three partitions and its own MinIO prefix. It imports two records, injects an interruption after Raw upload, then crashes after a manifest commit but before the Spark checkpoint commit. It resumes, adds another historical event, verifies updated Gold, and restarts without extra rows. Invalid UTF-8, tombstones, invalid schemas, null timestamps, and identical business events are covered. Test data and logs remain for inspection.

The verification report is written to `data/m4/m4-gce-june/verification.json`. The storage comparison uses the same 12,589 original valid rows, identical Snappy settings, a 10,000-row group size, and business fields only. It compares 53 columns against the selected 24, not JSON/SQLite size against Parquet, and excludes technical metadata from both sides. Actual Raw/Silver object sizes include technical columns, binary envelopes, and per-file overhead and therefore are reported separately.

### Verified local result (2026-09-10)

- Imported M3: **12,594 Raw = 12,589 Silver + 5 Rejected**. All original Raw bytes and every selected Silver value matched the SQLite snapshot; stored rejection reasons/details and metadata also matched.
- Cutover next offsets: partition 0 = **12590**, partition 1 = **1**, partition 2 = **3** (end offsets are exclusive).
- Continued replay `m2-validation-20260908` for **20 records only**, CSV source rows 12590–12609. Full-file replay was not run. The producer checkpoint now has `last_acked_row=12609` and remains paused.
- Final MinIO: **12,614 Raw = 12,609 Silver + 5 Rejected**, with 12,614 unique Kafka source offsets. New Raw bytes matched Kafka and new Silver business values matched the CSV. The M3 snapshot hash remained unchanged.
- Final Kafka next offsets: partition 0 = **12610**, partition 1 = **1**, partition 2 = **3**. Gold totals matched the final Raw/Silver/Rejected counts.
- **26 unit tests passed**. Real-service recovery test `m4-test-6dedd3fd7c` passed with 13 Raw, 7 Silver, 6 Rejected, three null timestamps, all three Kafka partitions, and an updated historical daily aggregate.
- **42 committed data objects** passed SHA-256 verification. Raw: 21 files / 5,805,087 bytes; Silver: 19 / 1,826,016 bytes; Rejected: 2 / 10,483 bytes. All 19 Silver files are under 1 MiB; small files exist at this validation scale. The report records minimum, median, and maximum sizes. Gold files and retained older snapshots are additional objects.
- Fair business-column comparison: **525,771 bytes (53 columns) → 348,942 bytes (24 columns), a 33.63% reduction**, using the same 12,589 original valid rows and encoding settings. This is not the total lake storage reduction: Raw and historical snapshots are deliberately retained.

Local evidence:

- `data/m4/m4-gce-june/verification.json`
- `data/m4/m4-gce-june/continuation-acceptance.json`
- `logs/m4/m4-gce-june/initial.log`, `continuation.log`, and `replay-continuation.log`
- `logs/m4/m4-test-6dedd3fd7c/acceptance.json` and the associated failure/recovery logs

These generated data, checkpoint, and log directories are intentionally ignored by Git. The M4 job has finished; it is not continuously consuming Kafka. Run `./scripts/run-lake.sh` again after the next replay batch. Larger replay runs and M5 dashboard work have not been started.
