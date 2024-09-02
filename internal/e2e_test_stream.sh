#!/bin/bash
set -euo pipefail

source "$(dirname "$0")/e2e_common.sh"

create_users() {
  log "Creating test table..."
  run_sql "DROP TABLE IF EXISTS public.users;"
  run_sql "CREATE TABLE public.users (id serial PRIMARY KEY, data text, created_at timestamp DEFAULT current_timestamp);"
  success "Test table created"
}

start_pg_flo_replication() {
  log "Starting pg_flo replication..."
  $pg_flo_BIN stream file \
    --host "$PG_HOST" \
    --port "$PG_PORT" \
    --dbname "$PG_DB" \
    --user "$PG_USER" \
    --password "$PG_PASSWORD" \
    --group "group-2" \
    --tables "users" \
    --schema "public" \
    --status-dir "/tmp" \
    --output-dir "$OUTPUT_DIR" >"$pg_flo_LOG" 2>&1 &
  pg_flo_PID=$!
  log "pg_flo started with PID: $pg_flo_PID"
  success "pg_flo replication started"
}

simulate_changes() {
  log "Simulating changes..."
  local insert_count=1000
  local update_count=500
  local delete_count=250

  for i in $(seq 1 $insert_count); do
    run_sql "INSERT INTO public.users (data) VALUES ('Data $i');"
  done

  for i in $(seq 1 $update_count); do
    run_sql "UPDATE public.users SET data = 'Updated data $i' WHERE id = $i;"
  done

  for i in $(seq 1 $delete_count); do
    run_sql "DELETE FROM public.users WHERE id = $i;"
  done

  success "Changes simulated"
}

verify_changes() {
  log "Verifying changes..."
  local insert_count=$(jq -s '[.[] | select(.type == "INSERT")] | length' "$OUTPUT_DIR"/*.jsonl)
  local update_count=$(jq -s '[.[] | select(.type == "UPDATE")] | length' "$OUTPUT_DIR"/*.jsonl)
  local delete_count=$(jq -s '[.[] | select(.type == "DELETE")] | length' "$OUTPUT_DIR"/*.jsonl)

  log "INSERT count: $insert_count (expected 1000)"
  log "UPDATE count: $update_count (expected 500)"
  log "DELETE count: $delete_count (expected 250)"

  if [ "$insert_count" -eq 1000 ] && [ "$update_count" -eq 500 ] && [ "$delete_count" -eq 250 ]; then
    success "Change counts match expected values"
    return 0
  else
    error "Change counts do not match expected values"
    return 1
  fi
}

# Main test function
test_pg_flo_cdc() {
  setup_postgres
  create_users
  start_pg_flo_replication
  sleep 1
  simulate_changes

  log "Waiting for pg_flo to process changes..."
  sleep 10

  stop_pg_flo_gracefully
  verify_changes || return 1
}

# Run the test
log "Starting pg_flo CDC test with changes..."
if test_pg_flo_cdc; then
  success "All tests passed! 🎉"
  exit 0
else
  error "Some tests failed. Please check the logs."
  show_pg_flo_logs
  exit 1
fi
