# M2: CSV Replay Service

M2 reads `GCE_EVO_Data/GCE_EVO_202606.csv` sequentially and sends one Kafka JSON message per CSV record. Only this CSV is used initially. Parquet input is deferred.

## Run

From the project root:

```bash
source datalake_env/bin/activate
python -m pip install -r requirements.txt
docker compose up -d
./scripts/create-topics.sh
python -m replay --run-id gce-june-001 --limit 10000
```

Default source: `GCE_EVO_Data/GCE_EVO_202606.csv`. Default rate: 100 events/sec. `.env` supplies `KAFKA_LOCAL_BOOTSTRAP_SERVERS` and `KAFKA_RAW_TOPIC`; existing shell environment variables take precedence. CLI options override both.

Continue the same run through the rest of the file:

```bash
python -m replay --run-id gce-june-001 --resume
```

Start an independent replay of the entire file:

```bash
python -m replay --run-id gce-june-002 --rate 100
```

`--limit` counts records for the current invocation, including when resuming. Omitting it reads all remaining records. No filtering, sampling, deduplication, sorting, type conversion, or empty-value normalization is applied. CSV is streamed; the file is not loaded into memory. A streaming SHA-256 pass precedes each invocation, and resume scans past already acknowledged CSV records.

## Message contract

```json
{
  "metadata": {
    "schema_version": "1.0",
    "event_id": "sha256 of file SHA-256 plus CSV record number",
    "replay_run_id": "gce-june-001",
    "replay_time": "UTC ISO-8601 timestamp",
    "source_file": "GCE_EVO_202606.csv",
    "source_sha256": "file SHA-256",
    "source_row": 1
  },
  "payload": {
    "event_timestamp": "original CSV value",
    "event_type": "original CSV value",
    "attributes_event_type": "original CSV value"
  }
}
```

The abbreviated payload above represents **all 53 original columns**. Values remain strings, including empty strings and timestamps. UTF-8 BOM, quoted commas, quotes, and embedded newlines are supported. Metadata is separate to avoid overwriting source columns. Structurally malformed CSV, duplicate headers or missing fields stop replay and preserve the checkpoint; nothing is silently skipped.

`event_id` identifies a source file record, not a business event. Identical rows at different positions remain distinct. IDs remain stable across replay runs of the same unchanged file. Business deduplication and event-type interpretation are deferred to M3.

## Order, retry and recovery

All records use partition 0 by default (`--partition` can change it). This preserves file order within a run even though the topic has multiple partitions. Concurrent independent runs can interleave on the partition. Event time is preserved, not used to sort or pace replay.

The producer requests `acks=all`, enables idempotence, permits five client retries, and waits for each acknowledgement before advancing. Rate is an upper limit; broker latency may lower achieved throughput. Progress logs report acknowledged counts and measured events/sec every five seconds. See the [Kafka Python producer documentation](https://kafka-python.readthedocs.io/en/2.2.13/apidoc/KafkaProducer.html).

Press `Ctrl+C` for a graceful pause; the current send finishes and the checkpoint is saved. Resume using the same run ID and `--resume`. `SIGTERM` also requests graceful shutdown. Checkpoints are written atomically every 100 acknowledgements and on normal stop/error. A lock prevents concurrent processes from using the same run in the same state directory. Reusing an existing run ID without `--resume`, changing file contents or changing destination during resume is refused.

Checkpoints: `checkpoints/replay/<run-id>.json`.
Failure records: `checkpoints/replay/<run-id>.failures.jsonl` (source identity, record number, error class and time). The source file retains the failed payload. Failures stop the run; resolve the cause, then resume. Source files must remain unchanged during a run and across resume.

Recovery is **at least once**: a forced process kill or ambiguous broker acknowledgement can cause retransmission after the last saved checkpoint. No source rows are intentionally lost. Cross-restart deduplication belongs to M3. The local single-broker stack does not protect against broker-volume loss.

## Verification

```bash
python -m unittest discover -s tests -v
python scripts/verify-replay.py --run-id gce-june-001 --expected 10000
```

Run verification after a 10,000-record limited replay, before continuing that run. It reads a bounded Kafka offset snapshot, filters by run ID, and compares every payload field, record order, stable ID and source metadata against the CSV. It requires all records for that run to remain within Kafka retention. Existing unrelated messages are ignored. It does not commit consumer offsets.

Inspect events:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic raw-events --from-beginning
```

M2 writes to Kafka. Spark filtering and MinIO persistence remain M3/M4 work.

## Local source inspection

The June CSV contains 864,935 records and 53 columns (630,081,197 bytes). A full streaming parse completed without a CSV structural error. This is a format check, not business-data validation. At 100 events/sec, replaying the whole file takes approximately 2 hours 24 minutes plus startup and broker overhead.

## M2 validation result (2026-09-08)

- Unit tests: 9 passed, including exact value/duplicate-row preservation, stable IDs, limit/resume, changed-source rejection, malformed CSV, acknowledgement failure, graceful SIGINT and pacing.
- Real Kafka run: `m2-validation-20260908`, topic `raw-events`, partition 0.
- Sent 4,000 records, resumed from record 4,001 and sent another 6,000.
- Resumed session: 99.9 events/sec against a configured 100 events/sec.
- Read back all 10,000 messages: every original payload field, source row, ID and source metadata matched; no missing or extra records.
- Consumer verification uses an explicit beginning offset; a deferred seek followed by `position()` stalled with the installed client during validation.
- At initial acceptance, the run was paused at row 10,000. A later recap verified 12,589 records; the full 864,935-record file has not been replayed. See the [recap and M3 handoff](m2-recap-and-m3-handoff.md) for the latest recorded status and the historical Kafka storage-path issue (resolved in M3). To continue the run from its saved checkpoint:

```bash
python -m replay --run-id m2-validation-20260908 --resume
```

M2's 10,000-record acceptance check is complete. Full-file load testing and cross-restart business deduplication are separate later checks.

## Recap and open follow-ups

See [M2 recap and M3 handoff](m2-recap-and-m3-handoff.md) for conversation decisions, current verification counts, M3 questions and the Kafka log-directory/volume mismatch. M2 replay acceptance is complete. The persistence issue was subsequently corrected and verified in [M3](m3-streaming-pipeline.md).
