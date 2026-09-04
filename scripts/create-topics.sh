#!/usr/bin/env bash
set -euo pipefail

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

RAW_TOPIC="${KAFKA_RAW_TOPIC:-raw-events}"
REJECTED_TOPIC="${KAFKA_REJECTED_TOPIC:-rejected-events}"
PARTITIONS="${KAFKA_PARTITIONS:-3}"
REPLICATION_FACTOR="${KAFKA_REPLICATION_FACTOR:-1}"
BOOTSTRAP_SERVER="${KAFKA_CONTAINER_BOOTSTRAP_SERVER:-localhost:9092}"

echo "Creating Kafka topics if they do not exist..."

docker compose exec -T kafka kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --create \
  --if-not-exists \
  --topic "${RAW_TOPIC}" \
  --partitions "${PARTITIONS}" \
  --replication-factor "${REPLICATION_FACTOR}"

docker compose exec -T kafka kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --create \
  --if-not-exists \
  --topic "${REJECTED_TOPIC}" \
  --partitions "${PARTITIONS}" \
  --replication-factor "${REPLICATION_FACTOR}"

echo "Current Kafka topics:"
docker compose exec -T kafka kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --list
