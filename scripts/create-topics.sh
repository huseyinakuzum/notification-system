#!/usr/bin/env bash
set -euo pipefail
BROKER="${KAFKA_BROKER:-kafka:9092}"
create() {
  kafka-topics --bootstrap-server "$BROKER" --create --if-not-exists \
    --topic "$1" --partitions "${2:-1}" --replication-factor 1
}
create delivery.high 1
create delivery.normal 1
create delivery.low 1
create delivery.dlq 1
echo "topics ready"
kafka-topics --bootstrap-server "$BROKER" --list
