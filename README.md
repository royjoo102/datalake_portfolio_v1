# Local Data Lake

This project builds a local data lake development environment for replaying event data, processing it through Kafka and Spark Structured Streaming, and storing curated outputs in an S3-compatible lake.

## Local Stack

- Kafka in KRaft mode
- Spark master and worker
- MinIO as an S3-compatible object store
- Topic and smoke-test scripts for local validation

## Prerequisites

- Docker Desktop
- Docker Compose v2
- Bash

## Quick Start

Create a local environment file:

```bash
cp .env.example .env
```

Start the local services:

```bash
docker compose up -d
```

Create the required Kafka topics:

```bash
./scripts/create-topics.sh
```

Run the smoke test:

```bash
./scripts/smoke-test.sh
```

Stop the local services while retaining the containers:

```bash
docker compose stop
```

Resume existing containers with `docker compose start`. Kafka now writes to its persistent `kafka-data` volume; the M2 path mismatch was corrected and existing events were verified after migration in M3.

Remove local service data when you need a clean environment:

```bash
docker compose down -v
```

## Milestone Notes

- [M1 Local Development Environment Recap](docs/m1-local-development-environment.md)
- [M2 Replay Service](docs/m2-replay-service.md)
- [M2 Recap and M3 Handoff](docs/m2-recap-and-m3-handoff.md)
- [M3 Streaming Pipeline](docs/m3-streaming-pipeline.md)

M2 replay and M3 Spark validation are complete under the agreed scope. M3 produced 12,589 Silver records with the selected 24 fields and preserved 5 prior smoke-test messages in Rejected. Business deduplication is deferred. Results are staged locally; MinIO/Parquet publication is M4 work.

## Service Endpoints

| Service | Local Endpoint |
| --- | --- |
| Kafka | `localhost:9092` |
| MinIO API | `http://localhost:9000` |
| MinIO Console | `http://localhost:9001` |
| Spark Master UI | `http://localhost:8080` |

Default MinIO credentials are defined in `.env.example`.

## Kafka Topics

| Topic | Purpose |
| --- | --- |
| `raw-events` | Raw replayed source events |
| `rejected-events` | Events rejected by validation or parsing |

## Object Storage Layout

The local MinIO bucket is initialized with the following folders:

```text
s3://data-lake/raw/
s3://data-lake/silver/
s3://data-lake/gold/
s3://data-lake/rejected/
```

## Configuration Rules

Application code should read environment variables instead of hard-coding storage endpoints, bucket names, credentials, or topic names. Use `STORAGE_MODE=local` for MinIO-backed development and reserve AWS-specific values for later deployment stages.

Inside Docker Compose, services should use internal hostnames such as `kafka:29092` and `minio:9000`. Tools running directly on the host machine should use `localhost:9092` and `localhost:9000`.

## CSV Replay (M2)

Replay all original fields from the June CSV into Kafka at 100 events/sec:

```bash
source datalake_env/bin/activate
python -m pip install -r requirements.txt
python -m replay --run-id gce-june-001 --limit 10000
python scripts/verify-replay.py --run-id gce-june-001 --expected 10000
# Continue the same run through the rest of the CSV:
python -m replay --run-id gce-june-001 --resume
```

See [M2 Replay Service](docs/m2-replay-service.md) for the message contract, full-file replay, pause/resume, recovery guarantees and tests. No source fields or duplicate rows are filtered in M2.

## Spark Processing (M3)

```bash
docker compose --profile m3 build spark-pipeline
./scripts/run-streaming.sh
python scripts/inspect-streaming.py --sample 3
```

The default command drains available Kafka events and exits; rerun to resume, or add `--continuous` for ongoing processing. The dedicated container uses Spark `local[2]`. Rules live in `config/filter-rules.yaml`; local outputs and Spark checkpoints live in `data/m3/` and `checkpoints/spark/`. See the M3 guide for null timestamps, raw preservation, restart guarantees and integration tests.
