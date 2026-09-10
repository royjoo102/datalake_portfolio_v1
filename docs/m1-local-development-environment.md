# M1 Local Development Environment Recap

This document records the M1 completion state. CSV replay was subsequently implemented in M2. See [M2 recap and M3 handoff](m2-recap-and-m3-handoff.md) for the historical Kafka storage-path finding. It was corrected with verified data preservation in [M3](m3-streaming-pipeline.md).

## Objective

M1 establishes a repeatable local runtime for the data lake project. The goal is to run the core infrastructure locally before building producer, streaming, validation, and storage logic in later milestones.

## What Was Built

The local stack is defined in `docker-compose.yml` and includes:

| Component | Image | Role |
| --- | --- | --- |
| Kafka | `apache/kafka:3.7.2` | Local event broker for raw and rejected event streams |
| Spark master | `spark:3.5.8` | Spark cluster manager |
| Spark worker | `spark:3.5.8` | Worker node that can run Spark executor processes |
| MinIO | `minio/minio:latest` | Local S3-compatible object storage |
| MinIO init | `minio/mc:latest` | One-off bucket and folder initialization job |

## Key Files

| File | Purpose |
| --- | --- |
| `docker-compose.yml` | Defines the local Kafka, Spark, and MinIO services |
| `.env.example` | Provides local configuration defaults without committing secrets |
| `scripts/create-topics.sh` | Creates required Kafka topics |
| `scripts/smoke-test.sh` | Verifies basic service connectivity |
| `README.md` | Provides local setup and execution instructions |
| `.gitignore` | Prevents local data, generated outputs, and secrets from being committed |

## Current Runtime Shape

```text
Host machine
  |
  | localhost:9092
  v
Kafka container
  |
  | Docker Compose internal network
  |
Spark master container <-> Spark worker container
  |
  | Future streaming jobs will write curated data
  v
MinIO container
```

## Kafka Setup

Kafka runs in KRaft mode, so ZooKeeper is not required.

Configured topics:

| Topic | Purpose |
| --- | --- |
| `raw-events` | Receives raw source events from local replay producers |
| `rejected-events` | Receives rejected events or validation failures |

Kafka exposes two access patterns:

| Access Pattern | Endpoint |
| --- | --- |
| Host machine access | `localhost:9092` |
| Docker internal access | `kafka:29092` |

Topic auto-creation is disabled so topics must be created explicitly through `scripts/create-topics.sh`.

## Spark Setup

Spark is configured with one master and one worker.

The Spark master is the cluster manager, not the application driver. The Spark worker is a worker node, not the executor itself. Driver and executor processes are created when a Spark application is submitted later.

Current endpoints:

| Endpoint | Purpose |
| --- | --- |
| `http://localhost:8080` | Spark Master UI |
| `spark://spark-master:7077` | Spark internal cluster endpoint |

## MinIO Setup

MinIO provides local S3-compatible storage. It is initialized with one bucket and four lake folders:

```text
s3://data-lake/raw/
s3://data-lake/silver/
s3://data-lake/gold/
s3://data-lake/rejected/
```

Current endpoints:

| Endpoint | Purpose |
| --- | --- |
| `http://localhost:9000` | MinIO S3-compatible API |
| `http://localhost:9001` | MinIO browser console |

Default local credentials:

```text
Username: minioadmin
Password: minioadmin
```

## Configuration Register

This register tracks local configuration values that may affect pipeline operation, security posture, and verification scope.

| Configuration | Current Value | Purpose | Impact if Changed | Security | Verification |
| --- | --- | --- | --- | --- | --- |
| Kafka host endpoint | `localhost:9092` | Connects local producers and host-based tools to Kafka | Local replay services may fail to connect | Not public | Produce/consume smoke test |
| Kafka internal endpoint | `kafka:29092` | Connects services inside the Docker Compose network | Spark or containerized services may fail to connect | Internal only | Container connectivity check |
| Spark master | `spark://spark-master:7077` | Submits Spark jobs to the local Spark cluster | Spark jobs may not run | Internal only | Worker registration and Spark connectivity |
| MinIO API | `localhost:9000` | Provides local S3-compatible object storage access | Data lake reads and writes may fail | Requires credentials | Write/read/delete validation |
| Raw topic | `raw-events` | Receives raw source events | Both producers and Spark consumers are affected | General configuration | Topic configuration check |

## Smoke Test Flow

The smoke test validates the infrastructure wiring, not the full data pipeline.

Current flow:

```text
1. Check Docker Compose service status
2. Verify the Kafka `raw-events` topic exists
3. Produce one JSON smoke-test message to Kafka
4. Consume from Kafka and verify the message id is present
5. Verify the MinIO bucket is reachable
6. Verify the Spark worker can reach the Spark master
```

The smoke-test JSON event is written to Kafka only. It is not written to MinIO yet. Kafka-to-MinIO persistence will be implemented later through a Spark Structured Streaming job.

## Verified Commands

Start services:

```bash
docker compose up -d
```

Create Kafka topics:

```bash
./scripts/create-topics.sh
```

Run smoke test:

```bash
./scripts/smoke-test.sh
```

Inspect services:

```bash
docker compose ps
```

Inspect MinIO bucket structure:

```bash
docker compose run --rm --entrypoint /bin/sh minio-init -c \
  "mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null && mc tree local/data-lake"
```

Inspect Kafka messages:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic raw-events \
  --from-beginning
```

## Verification Result

M1 verification passed.

| Check | Result |
| --- | --- |
| Kafka container is running and healthy | Passed |
| MinIO container is running and healthy | Passed |
| Spark master container is running and healthy | Passed |
| Spark worker container is running | Passed |
| Kafka topics exist | Passed |
| Producer smoke test succeeds | Passed |
| Consumer smoke test succeeds | Passed |
| MinIO bucket exists | Passed |
| Spark worker can reach Spark master | Passed |

## M1 Exit Criteria Review

| Exit Criteria | Status | Notes |
| --- | --- | --- |
| Kafka, Spark, and MinIO run with a single command | Complete | `docker compose up -d` starts the local stack |
| Producer and consumer smoke test succeeds | Complete | `scripts/smoke-test.sh` passed |
| Bucket, endpoint, and credentials are not hard-coded in application code | Complete | Values are provided through `.env.example` and Docker Compose defaults |

## Known Limitations

The current setup only validates infrastructure readiness. It does not yet replay source CSV files, run Spark Structured Streaming, validate event schemas, or write event data to MinIO.

These items belong to later milestones:

| Future Work | Target Milestone |
| --- | --- |
| Replay local CSV data into Kafka | M2 |
| Parse and validate event records | M3 |
| Write raw, silver, gold, and rejected Parquet datasets to MinIO | M4 |

## Ready for M2

M1 is complete and the project is ready to move into M2: building the local producer that reads source files and publishes events to the Kafka `raw-events` topic.
