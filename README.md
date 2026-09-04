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

Stop the local services:

```bash
docker compose down
```

Remove local service data when you need a clean environment:

```bash
docker compose down -v
```

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
