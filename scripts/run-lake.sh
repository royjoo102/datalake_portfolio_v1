#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose --profile m4 run --rm --no-deps spark-lake \
  --master 'local[2]' --conf spark.jars.ivy=/root/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 \
  lake/job.py "$@"
