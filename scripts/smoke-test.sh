#!/usr/bin/env bash
set -euo pipefail

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

RAW_TOPIC="${KAFKA_RAW_TOPIC:-raw-events}"
MINIO_BUCKET="${MINIO_BUCKET:-data-lake}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
BOOTSTRAP_SERVER="${KAFKA_CONTAINER_BOOTSTRAP_SERVER:-localhost:9092}"
CONSUMER_GROUP="local-smoke-test-$(date +%s)"
MESSAGE_ID="smoke-test-$(date +%s)"
MESSAGE="{\"event_id\":\"${MESSAGE_ID}\",\"event_type\":\"smoke_test\",\"source\":\"local\",\"schema_version\":\"1.0\", \"Hello world\"}"

echo "Checking Docker Compose services..."
docker compose ps

echo "Verifying Kafka topic exists: ${RAW_TOPIC}"
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --describe \
  --topic "${RAW_TOPIC}" >/dev/null

echo "Producing one Kafka smoke-test message..."
printf '%s\n' "${MESSAGE}" | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --topic "${RAW_TOPIC}"

echo "Consuming the Kafka smoke-test message..."
CONSUMED_MESSAGE="$(
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --topic "${RAW_TOPIC}" \
    --group "${CONSUMER_GROUP}" \
    --from-beginning \
    --timeout-ms 10000 2>/dev/null || true
)"

if [[ "${CONSUMED_MESSAGE}" != *"${MESSAGE_ID}"* ]]; then
  echo "Kafka smoke test failed. Expected message id: ${MESSAGE_ID}"
  echo "Consumed output: ${CONSUMED_MESSAGE}"
  exit 1
fi

echo "Verifying MinIO bucket and lake folders..."
docker compose run --rm --entrypoint /bin/sh minio-init -c \
  "mc alias set local http://minio:9000 '${MINIO_ROOT_USER}' '${MINIO_ROOT_PASSWORD}' >/dev/null && mc ls 'local/${MINIO_BUCKET}' >/dev/null"

echo "Verifying Spark master is reachable from the worker network..."
docker compose exec -T spark-worker bash -lc '</dev/tcp/spark-master/8080'

echo "Smoke test passed."
