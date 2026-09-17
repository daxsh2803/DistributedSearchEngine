#!/bin/bash
# Distributed Search Engine - Kafka Topic Creation Script (Phase 19A)
#
# Creates the initial topics for the document event system.
# Must be run after Kafka is healthy.
#
# Prerequisites:
#   - Docker Desktop running
#   - Kafka container running and healthy
#   - kafka-topics.sh available in PATH or via docker exec
#
# Usage:
#   ./docker/kafka/create-topics.sh
#
# This script is also executed by the kafka-init container in Docker Compose.

set -euo pipefail

KAFKA_HOST="${KAFKA_HOST:-localhost}"
KAFKA_PORT="${KAFKA_PORT:-9092}"
BOOTSTRAP_SERVER="${KAFKA_HOST}:${KAFKA_PORT}"

echo "Creating Kafka topics on ${BOOTSTRAP_SERVER}..."
echo ""

# Create documents.mutations topic
echo "Creating topic: documents.mutations"
docker compose -f "$(dirname "$0")/docker-compose.kafka.yml" exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --create \
    --if-not-exists \
    --topic documents.mutations \
    --partitions 3 \
    --replication-factor 1

echo ""
echo "Verifying topics..."
docker compose -f "$(dirname "$0")/docker-compose.kafka.yml" exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --list

echo ""
echo "Topic details:"
for topic in documents.mutations; do
  echo ""
  echo "=== ${topic} ==="
  docker compose -f "$(dirname "$0")/docker-compose.kafka.yml" exec -T kafka \
    /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server "${BOOTSTRAP_SERVER}" \
      --describe \
      --topic "${topic}"
done

echo ""
echo "All topics created and verified successfully."
