#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p data/m3 checkpoints/spark logs/m3
# local[2] uses the dedicated Spark driver container for reproducible local M3 jobs.
# Existing Spark master/worker services are left available for cluster-mode work.
docker compose --profile m3 run --rm --no-deps spark-pipeline \
  --master 'local[2]' \
  --conf spark.jars.ivy=/root/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 \
  streaming/job.py "$@"
