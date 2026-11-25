#!/bin/sh
# Применяет clickhouse/migrations/*.sql по порядку имён и записывает их в cinema.schema_migrations.
# Уже применённые файлы пропускаются, поэтому скрипт безопасно запускать при каждом старте.
# В SQL подставляются {{KAFKA_BROKER_LIST}}, {{KAFKA_TOPIC}}, {{SCHEMA_REGISTRY_URL}}, {{GRAFANA_PASSWORD}}.
set -eu

CH="clickhouse-client --host ${CLICKHOUSE_HOST} --user ${CLICKHOUSE_USER} --password ${CLICKHOUSE_PASSWORD}"

until $CH --query "SELECT 1" >/dev/null 2>&1; do
  echo "waiting for clickhouse at ${CLICKHOUSE_HOST}..."
  sleep 1
done

$CH --query "CREATE DATABASE IF NOT EXISTS cinema"
$CH --query "CREATE TABLE IF NOT EXISTS cinema.schema_migrations
             (version String, applied_at DateTime DEFAULT now())
             ENGINE = MergeTree ORDER BY version"

for file in $(ls /migrations/*.sql | sort); do
  version=$(basename "$file")
  applied=$($CH --query "SELECT count() FROM cinema.schema_migrations WHERE version = '${version}'")
  if [ "$applied" != "0" ]; then
    echo "skip    ${version}"
    continue
  fi
  echo "apply   ${version}"
  sed -e "s|{{KAFKA_BROKER_LIST}}|${KAFKA_BROKER_LIST}|g" \
      -e "s|{{KAFKA_TOPIC}}|${KAFKA_TOPIC}|g" \
      -e "s|{{SCHEMA_REGISTRY_URL}}|${SCHEMA_REGISTRY_URL}|g" \
      -e "s|{{GRAFANA_PASSWORD}}|${GRAFANA_CLICKHOUSE_PASSWORD}|g" \
      "$file" | $CH --multiquery
  $CH --query "INSERT INTO cinema.schema_migrations (version) VALUES ('${version}')"
done
echo "clickhouse migrations done"
