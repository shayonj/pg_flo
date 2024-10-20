#!/bin/bash

source internal/e2e_common.sh

run_sql "CREATE TABLE IF NOT EXISTS benchmark_test (id SERIAL PRIMARY KEY, data TEXT);"
$pg_flo_BIN worker postgres \
  --group "benchmark_group" \
  --nats-url "$NATS_URL" \
  --source-host "$PG_HOST" \
  --source-port "$PG_PORT" \
  --source-dbname "$PG_DB" \
  --source-user "$PG_USER" \
  --source-password "$PG_PASSWORD" \
  --target-host "$TARGET_PG_HOST" \
  --target-port "$TARGET_PG_PORT" \
  --target-dbname "$TARGET_PG_DB" \
  --target-user "$TARGET_PG_USER" \
  --target-password "$TARGET_PG_PASSWORD" \
  --target-sync-schema

$pg_flo_BIN replicator \
  --host "$PG_HOST" \
  --port "$PG_PORT" \
  --dbname "$PG_DB" \
  --user "$PG_USER" \
  --password "$PG_PASSWORD" \
  --group "benchmark_group" \
  --tables "benchmark_test" \
  --schema "public" \
  --nats-url "$NATS_URL" \
  --max-copy-workers-per-table 4

ruby internal/benchmark_test.rb

./internal/monitor_metrics.sh

nats-top -n 2
