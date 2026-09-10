# M3: Spark Data Refinement Pipeline

## Completed Scope and Agreed Rules

The following rules implement the user's decisions from 2026-09-09.

- The Silver `payload` retains only the 24 source columns listed below. Traceability information, normalized event time, and quality flags are stored separately.
- **Business-event deduplication is disabled.** Events with the same event_id or content are all preserved when they have different Kafka offsets.
- If `event_timestamp` is missing, empty, or cannot be parsed, both the Silver field and normalized `event_time` are stored as null. The reason is recorded in `quality_flags`. No substitution with `arrival_timestamp` is performed.
- All original Kafka message bytes are preserved in `raw_events`. This includes the 29 fields excluded from Silver and any original invalid timestamp values.
- JSON syntax errors, invalid M2 traceability metadata, unsupported envelope versions, and declared field type errors are routed to Rejected.
- No event-time sorting or watermark is applied. Valid events that arrive late are preserved.

| Silver Column | Silver Column |
| --- | --- |
| application_package_name | application_title |
| application_version_name | arrival_timestamp |
| attributes_channel | attributes_current_screen |
| attributes_device_id | attributes_event_type |
| attributes_extra | attributes_store_id |
| attributes_to_screen | device_model |
| device_platform_name | device_platform_version |
| event_timestamp | event_type |
| event_version | metrics_duration |
| session_session_id | session_start_timestamp |
| session_stop_timestamp | eventreceivedyear |
| eventreceivedmonth | eventreceivedday |

The configuration is in `config/filter-rules.yaml`. It explicitly declares 53 input fields and a separate allowlist of 24 output fields. Source values remain strings, except that timestamp exceptions are converted to null according to the policy above. Valid epoch milliseconds are converted to UTC `event_time`, while the original valid timestamp string is preserved.

## Execution Architecture

```text
Kafka raw-events (all partitions)
  → Spark Structured Streaming
  → Explicit schema + validation + allowlist
  → One SQLite transaction per micro-batch
      ├── raw_events: all received original messages
      ├── silver_events: the selected 24 fields
      └── rejected_events: rejection reasons + reference to the original message
```

M3 runs Spark in `local[2]` mode inside a dedicated Docker container. The existing Spark master/worker services do not participate in this execution. The environment uses pinned versions, includes YAML support, and uses Kafka connector `3.5.8`. Submission to a separate Spark cluster and throughput scaling are follow-up work.

M3 outputs are stored in a **local SQLite staging database**. Event results are not yet written to MinIO. Parquet, the MinIO partition layout, Gold aggregation, and dashboards belong to M4 and later milestones. This implementation also does not republish rejected events to the `rejected-events` Kafka topic.

## Running and Inspecting the Pipeline

From the project root:

```bash
source datalake_env/bin/activate
python -m pip install -r requirements.txt
docker compose up -d
./scripts/create-topics.sh
docker compose --profile m3 build spark-pipeline
./scripts/run-streaming.sh
```

By default, the job processes all data available in Kafka at startup and then exits (`availableNow`). Running the same command again processes new data from the saved Spark checkpoint. It does not automatically run Replay or publish the entire June CSV to Kafka again.

To keep receiving events:

```bash
./scripts/run-streaming.sh --continuous
```

Stop with Ctrl+C. Each micro-batch is limited to approximately 1,000 offsets by default; `--max-offsets` accepts values from 1 to 10,000. Results are collected on the driver and written to a local database, so this is not a large-scale distributed sink.

Inspect results after processing has finished:

```bash
python scripts/inspect-streaming.py
python scripts/inspect-streaming.py --sample 3
python scripts/inspect-streaming.py --zone rejected --sample 5
python scripts/inspect-streaming.py --zone raw --sample 1
```

Without `--sample`, the tool prints only counts, reasons, offsets, and similar summary information. Actual company data is displayed only when sample output is requested. The inspection tool uses a `query_only` read transaction. It opens the existing database in rw mode to allow SQLite WAL management files to be opened, but SQL data modifications are prohibited. Access the SQLite database from the host after the Spark container's job has finished.

Use a new pipeline ID for a different source or configuration:

```bash
./scripts/run-streaming.sh --pipeline-id m3-experiment --topic another-topic
python scripts/inspect-streaming.py --pipeline-id m3-experiment
```

Compose injects the `KAFKA_INTERNAL_BOOTSTRAP_SERVERS` and `KAFKA_RAW_TOPIC` environment variables. The input Kafka topic and address can also be changed through CLI options.

## Checkpoints and Reprocessing

| Path | Purpose |
| --- | --- |
| `data/m3/<pipeline-id>/events.sqlite` | Original messages, Silver, Rejected, and committed batch records |
| `checkpoints/spark/<pipeline-id>/` | Spark source offsets and batch commits |
| `logs/m3/<pipeline-id>/progress.jsonl` | Spark progress, batch duration, and Kafka lag |
| `logs/m3/<pipeline-id>/batches.jsonl` | Input, Silver, and Rejected counts for each batch |
| `checkpoints/replay/` | Separate checkpoints for the M2 Producer |

Original messages, classification results, and the batch completion marker are stored in one SQLite transaction. Spark then commits its own checkpoint. If Spark stops immediately after the database commit, the same batch may be delivered again on restart, but the database completion marker prevents duplicate writes. Kafka's `(topic, partition, offset)` is also managed as a unique key.

This provides **protection against duplicate writes caused by processing retries**; it is separate from business-event deduplication. An identical event arriving at a new offset becomes a new Silver row. An invalid event is retained as one original row and one Rejected row, maintaining the following relationship:

```text
raw_count = silver_count + rejected_count
```

Keep the SQLite database and Spark checkpoint together. If only one is missing, or the configuration or destination changes, the job refuses to continue automatically. When rules change, reprocess the source using a new pipeline ID. A new pipeline ID is also required when deleting and recreating a Kafka topic or switching to a new cluster. If Kafka retention has removed earlier messages, rebuilding the complete dataset requires a separate source or backup. The job uses `failOnDataLoss=true` to avoid silently skipping known missing offsets.

## Errors and Quality Flags

| Condition | Result |
| --- | --- |
| Valid JSON and M2 schema | Silver |
| Identical content/event_id at a new Kafka offset | Each event is preserved in Silver |
| Missing timestamp, empty string, or null | Silver null + `MISSING_EVENT_TIMESTAMP` |
| Invalid timestamp format, range, or type | Silver null + `INVALID_EVENT_TIMESTAMP` |
| Malformed JSON, invalid UTF-8, or tombstone (null value) | Rejected with `MALFORMED_JSON` |
| Missing metadata/payload object or required metadata | `MISSING_REQUIRED_FIELD` |
| Invalid type for a declared field | `INVALID_DATA_TYPE` |
| Unsupported envelope schema_version | `UNSUPPORTED_SCHEMA_VERSION` |

Rejected records contain the first reason (`reason`), all reasons (`reasons`), and field details (`details`). The original message is linked through `source_id` in `raw_events`. The source business field `event_version` is distinct from `metadata.schema_version`; the latter supports version `1.0` of the M2 envelope.

## Validation Results (2026-09-09)

- Unit tests: 9 Replay tests + 11 M3 tests, totaling 20 passing tests.
- Actual input: **12,594 records = 12,589 Silver records + 5 Rejected records**.
- The 5 Rejected records are earlier smoke-test messages: 3 JSON errors and 2 missing required M2 envelopes.
- All 24 selected values for each of the 12,589 replayed June CSV rows were compared directly against the original CSV and matched.
- Null `event_time` values in the actual Silver data: 0.
- After 13 micro-batches, lag was 0 across all partitions. The last batch trigger took approximately 1.39 seconds; this is an observation from this run, not a performance guarantee.
- Rerunning the same pipeline did not increase raw/Silver/Rejected counts.
- Synthetic Kafka/Spark test: 12 records → 6 Silver + 6 Rejected. Both duplicate events were preserved, 3 timestamp exceptions became null, and an old valid event was retained.
- In the synthetic test, the driver was forcibly terminated immediately after database batch 0 was committed. The Spark offset file existed while the commit file did not. After resuming, processing completed without duplicate output. A subsequent normal restart also preserved the counts.
- SQLite integrity and `raw = silver + rejected` were verified.

Reproduction commands:

```bash
python -m unittest discover -s tests -v
python scripts/test-streaming-integration.py
```

The synthetic integration test creates and retains a new `m3-test-*` Kafka topic and output directory. The crash injection option that terminates the driver immediately after storage is used only for this test. Synthetic test events are not mixed into the original raw-events topic.

## Kafka Persistent Storage Fix

The path mismatch discovered in M2 was resolved. After confirming that no Producer was running, the Broker was stopped gracefully and the entire `/tmp/kafka-logs` directory was backed up. The data was copied into the existing empty `kafka-data` volume, and SHA-256 hashes were compared for 323 files. The Broker was then recreated with `KAFKA_LOG_DIRS=/var/lib/kafka/data`.

- Original backup: `data/kafka-backups/m3-before-migration/`
- File hash manifest: `data/kafka-backups/m3-before-migration.sha256.json`
- Actual log path after recreation: `/var/lib/kafka/data/raw-events-0`
- Preserved raw-events end offsets: partition 0=12590, 1=1, 2=3
- The original values, order, and IDs of all 12,589 existing Replay records passed revalidation

Kafka now uses the named volume across normal container recreation. `docker compose down -v` deletes volumes, so do not use it when existing data must be retained. The deployment still has one Broker and a replication factor of 1; multi-Broker high availability has not been implemented.

## M4 Handoff

- Carry the selected 24 Silver columns and timestamp null policy into the Parquet schema.
- Define a separate date partition path for events with `event_time=null`.
- Implement storage layouts for original messages, Silver, Rejected, and Gold aggregates in MinIO.
- Keep business-event deduplication deferred until the user defines the criteria.
- Preserve both event_type and attributes_event_type. Selecting the field used for aggregation is a separate decision.
- Do not discard late events before defining an allowed-lateness/watermark policy based on the actual data.

References: [Spark Kafka Integration](https://spark.apache.org/docs/3.5.8/structured-streaming-kafka-integration.html), [foreachBatch and Output Retries](https://spark.apache.org/docs/3.5.8/structured-streaming-programming-guide.html#using-foreach-and-foreachbatch).
