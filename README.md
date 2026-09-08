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

Resume existing containers with `docker compose start`. The current Kafka log directory differs from its mounted data volume; preserve/migrate it before recreating containers. See the [storage-path finding](docs/m2-recap-and-m3-handoff.md#발견-사항-kafka-저장-경로-불일치).

Remove local service data when you need a clean environment:

```bash
docker compose down -v
```

## Milestone Notes

- [M1 Local Development Environment Recap](docs/m1-local-development-environment.md)
- [M2 Replay Service](docs/m2-replay-service.md)
- [M2 Recap and M3 Handoff](docs/m2-recap-and-m3-handoff.md)

M2 CSV replay acceptance is complete. The latest recorded read-back check verified 12,589 records with all 53 source fields preserved. Spark processing and MinIO event persistence are pending; Kafka persistence across container recreation has an open storage-path issue.

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
