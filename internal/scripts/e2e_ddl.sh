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
  log "Starting pg_flo worker with PostgreSQL sink..."
  if [ -f "$pg_flo_WORKER_LOG" ]; then
    mv "$pg_flo_WORKER_LOG" "${pg_flo_WORKER_LOG}.bak"
    log "Backed up previous worker log to ${pg_flo_WORKER_LOG}.bak"
  fi
  $pg_flo_BIN worker postgres \
    --group "group_ddl" \
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
    --target-sync-schema \
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

  # Check table structure in target database
  local new_column_exists=$(run_sql_target "SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'users' AND column_name = 'new_column';")
  local new_column_one_exists=$(run_sql_target "SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'users' AND column_name = 'new_column_one';")
  local old_data_type=$(run_sql_target "SELECT data_type FROM information_schema.columns WHERE table_name = 'users' AND column_name = 'old_data';")
  old_data_type=$(echo "$old_data_type" | xargs)

  if [ "$new_column_exists" -eq 1 ]; then
    success "new_column exists in target database"
  else
    error "new_column does not exist in target database"
    return 1
  fi

  if [ "$new_column_one_exists" -eq 1 ]; then
    success "new_column_one exists in target database"
  else
    error "new_column_one does not exist in target database"
    return 1
  fi

  if [ "$old_data_type" = "character varying" ]; then
    success "old_data column type is character varying"
  else
    error "old_data column type is not character varying (got: '$old_data_type')"
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
  start_pg_flo_worker
  sleep 5
  start_pg_flo_replication
  sleep 3
  perform_ddl_operations
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
