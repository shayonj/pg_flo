#!/bin/bash
set -euo pipefail

source "$(dirname "$0")/e2e_common.sh"

create_users() {
  log "Creating initial test table..."
  run_sql "DROP TABLE IF EXISTS public.users;"
  run_sql "CREATE TABLE public.users (id serial PRIMARY KEY, data text);"
  success "Initial test table created"
}

start_pg_flo_replication() {
  log "Starting pg_flo replication..."
  if [ -f "$pg_flo_LOG" ]; then
    mv "$pg_flo_LOG" "${pg_flo_LOG}.bak"
    log "Backed up previous replicator log to ${pg_flo_LOG}.bak"
  fi
  $pg_flo_BIN replicator \
    --host "$PG_HOST" \
    --port "$PG_PORT" \
    --dbname "$PG_DB" \
    --user "$PG_USER" \
    --password "$PG_PASSWORD" \
    --group "group_ddl" \
    --tables "users" \
    --schema "public" \
    --nats-url "$NATS_URL" \
    --track-ddl \
    >"$pg_flo_LOG" 2>&1 &
  pg_flo_PID=$!
  log "pg_flo replicator started with PID: $pg_flo_PID"
  success "pg_flo replication started"
}

start_pg_flo_worker() {
  log "Starting pg_flo worker with file sink..."
  if [ -f "$pg_flo_WORKER_LOG" ]; then
    mv "$pg_flo_WORKER_LOG" "${pg_flo_WORKER_LOG}.bak"
    log "Backed up previous worker log to ${pg_flo_WORKER_LOG}.bak"
  fi
  $pg_flo_BIN worker file \
    --group "group_ddl" \
    --nats-url "$NATS_URL" \
    --file-output-dir "$OUTPUT_DIR" \
    >"$pg_flo_WORKER_LOG" 2>&1 &
  pg_flo_WORKER_PID=$!
  log "pg_flo worker started with PID: $pg_flo_WORKER_PID"
  success "pg_flo worker started"
}

perform_ddl_operations() {
  log "Performing DDL operations..."
  run_sql "ALTER TABLE users ADD COLUMN new_column int;"
  run_sql "CREATE INDEX CONCURRENTLY idx_users_data ON users (data);"
  run_sql "ALTER TABLE users RENAME COLUMN data TO old_data;"
  run_sql "DROP INDEX idx_users_data;"
  run_sql "ALTER TABLE users ADD COLUMN new_column_one int;"
  run_sql "ALTER TABLE users ALTER COLUMN old_data TYPE varchar(255);"
  success "DDL operations performed"
}

verify_ddl_changes() {
  log "Verifying DDL changes..."
  local ddl_events=$(jq -s '[.[] | select(.Type == "DDL")]' "$OUTPUT_DIR"/*.jsonl)
  local ddl_count=$(echo "$ddl_events" | jq 'length')
  log "DDL event count: $ddl_count (expected 6)"

  if [ "$ddl_count" -eq 6 ]; then
    success "DDL event count matches expected value"
  else
    error "DDL event count does not match expected value"
    return 1
  fi

  # Assert SQL statements
  local expected_commands=(
    "ALTER TABLE users ADD COLUMN new_column int;"
    "CREATE INDEX CONCURRENTLY idx_users_data ON users (data);"
    "ALTER TABLE users RENAME COLUMN data TO old_data;"
    "DROP INDEX idx_users_data;"
    "ALTER TABLE users ADD COLUMN new_column_one int;"
    "ALTER TABLE users ALTER COLUMN old_data TYPE varchar(255);"
  )

  for i in "${!expected_commands[@]}"; do
    local command=$(echo "$ddl_events" | jq -r ".[$i].NewTuple.ddl_command")
    if [[ "$command" == "${expected_commands[$i]}" ]]; then
      success "DDL command $((i + 1)) matches expected value"
    else
      error "DDL command $((i + 1)) does not match expected value"
      log "Expected: ${expected_commands[$i]}"
      log "Actual: $command"
      return 1
    fi
  done

  # Check for table_rewrite event
  local table_rewrite_event=$(echo "$ddl_events" | jq '.[] | select(.NewTuple.event_type == "table_rewrite")')
  if [ -n "$table_rewrite_event" ]; then
    success "table_rewrite event detected"
  else
    error "table_rewrite event not found"
    return 1
  fi

  # Check if internal table is empty
  local remaining_rows=$(run_sql "SELECT COUNT(*) FROM internal_pg_flo.ddl_log;")
  if [ "$remaining_rows" -eq 0 ]; then
    success "internal_pg_flo.ddl_log table is empty"
  else
    error "internal_pg_flo.ddl_log table is not empty. Remaining rows: $remaining_rows"
    return 1
  fi

  return 0
}

test_pg_flo_ddl() {
  setup_postgres
  create_users
  start_pg_flo_replication
  sleep 1
  perform_ddl_operations
  sleep 2
  start_pg_flo_worker
  sleep 2
  stop_pg_flo_gracefully
  verify_ddl_changes || return 1
}

log "Starting pg_flo CDC test with DDL tracking..."
if test_pg_flo_ddl; then
  success "DDL tracking test passed! 🎉"
  exit 0
else
  error "DDL tracking test failed. Please check the logs."
  show_pg_flo_logs
  exit 1
fi
