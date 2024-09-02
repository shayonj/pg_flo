#!/bin/bash
set -euo pipefail

source "$(dirname "$0")/e2e_common.sh"

create_users() {
  log "Creating test table in source database..."
  run_sql "DROP TABLE IF EXISTS public.users;"
  run_sql "CREATE TABLE public.users (id serial PRIMARY KEY, data text, created_at timestamp DEFAULT current_timestamp);"
  success "Test table created in source database"
}

start_pg_flo_replication() {
  log "Starting pg_flo replication to PostgreSQL sink..."
  $pg_flo_BIN stream postgres \
    --host "$PG_HOST" \
    --port "$PG_PORT" \
    --dbname "$PG_DB" \
    --user "$PG_USER" \
    --password "$PG_PASSWORD" \
    --group "group_postgres_sink" \
    --tables "users" \
    --schema "public" \
    --status-dir "/tmp" \
    --target-host "$TARGET_PG_HOST" \
    --target-port "$TARGET_PG_PORT" \
    --target-dbname "$TARGET_PG_DB" \
    --target-user "$TARGET_PG_USER" \
    --target-password "$TARGET_PG_PASSWORD" \
    --sync-schema >"$pg_flo_LOG" 2>&1 &
  pg_flo_PID=$!
  log "pg_flo started with PID: $pg_flo_PID"
  success "pg_flo replication to PostgreSQL sink started"
}

simulate_changes() {
  log "Simulating changes..."
  local insert_count=1000
  local update_count=500
  local delete_count=250

  for i in $(seq 1 $insert_count); do
    run_sql "INSERT INTO public.users (data) VALUES ('Data $i');"
  done

  sleep 10
  post_insert_count=$(run_sql_target "SELECT COUNT(*) FROM public.users;")

  for i in $(seq 1 $update_count); do
    run_sql "UPDATE public.users SET data = 'Updated data $i' WHERE id = $i;"
  done
  sleep 10
  post_update_count=$(run_sql_target "SELECT COUNT(*) FROM public.users WHERE data LIKE 'Updated data %';")

  for i in $(seq 1 $delete_count); do
    run_sql "DELETE FROM public.users WHERE id = $i;"
  done
  sleep 10
  post_delete_count=$(run_sql_target "SELECT COUNT(*) FROM public.users;")

  success "Changes simulated"
}

verify_changes() {
  log "Verifying changes in target database..."
  local insert_count=$post_insert_count
  local update_count=$post_update_count
  local delete_count=$((post_insert_count - post_delete_count))

  log "INSERT count: $insert_count (expected 1000)"
  log "UPDATE count: $update_count (expected 500)"
  log "DELETE count: $delete_count (expected 250)"

  if [ "$insert_count" -eq 1000 ] && [ "$update_count" -eq 500 ] && [ "$delete_count" -eq 250 ]; then
    success "Change counts match expected values in target database"
    return 0
  else
    error "Change counts do not match expected values in target database"
    return 1
  fi
}

run_sql_target() {
  PGPASSWORD=$TARGET_PG_PASSWORD psql -h "$TARGET_PG_HOST" -U "$TARGET_PG_USER" -d "$TARGET_PG_DB" -p "$TARGET_PG_PORT" -q -t -c "$1"
}

test_pg_flo_postgres_sink() {
  setup_postgres
  create_users
  start_pg_flo_replication
  sleep 1
  simulate_changes

  log "Waiting for pg_flo to process changes..."
  sleep 2

  stop_pg_flo_gracefully
  verify_changes || return 1
}

# Run the test
log "Starting pg_flo CDC test with PostgreSQL sink..."
if test_pg_flo_postgres_sink; then
  success "All tests passed! 🎉"
  exit 0
else
  error "Some tests failed. Please check the logs."
  show_pg_flo_logs
  exit 1
fi
