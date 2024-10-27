#!/bin/bash
set -euo pipefail

source "$(dirname "$0")/e2e_common.sh"

create_users() {
  log "Creating test table..."
  run_sql "DROP TABLE IF EXISTS public.users;"
  run_sql "CREATE TABLE public.users (
    id serial PRIMARY KEY,
    email text,
    phone text,
    age int,
    ssn text,
    created_at timestamp DEFAULT current_timestamp
  );"
  success "Test table created"
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
    --group "group_transform_filter" \
    --tables "users" \
    --schema "public" \
    --nats-url "$NATS_URL" \
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
    --group "group_transform_filter" \
    --nats-url "$NATS_URL" \
    --file-output-dir "$OUTPUT_DIR" \
    --rules-config "$(dirname "$0")/rules.yml" \
    >"$pg_flo_WORKER_LOG" 2>&1 &
  pg_flo_WORKER_PID=$!
  log "pg_flo worker started with PID: $pg_flo_WORKER_PID"
  success "pg_flo worker started"
}

simulate_changes() {
  log "Simulating changes..."
  run_sql "INSERT INTO public.users (email, phone, age, ssn) VALUES
    ('john@example.com', '1234567890', 25, '123-45-6789'),
    ('jane@example.com', '9876543210', 17, '987-65-4321'),
    ('bob@example.com', '5551234567', 30, '555-12-3456');"

  run_sql "UPDATE public.users SET email = 'updated@example.com', phone = '1112223333' WHERE id = 1;"
  run_sql "DELETE FROM public.users WHERE age = 30;"
  run_sql "DELETE FROM public.users WHERE age = 17;"

  success "Changes simulated"
}

verify_changes() {
  log "Verifying changes..."
  local insert_count=$(jq -s '[.[] | select(.Type == "INSERT")] | length' "$OUTPUT_DIR"/*.jsonl)
  local update_count=$(jq -s '[.[] | select(.Type == "UPDATE")] | length' "$OUTPUT_DIR"/*.jsonl)
  local delete_count=$(jq -s '[.[] | select(.Type == "DELETE")] | length' "$OUTPUT_DIR"/*.jsonl)

  log "INSERT count: $insert_count (expected 2)"
  log "UPDATE count: $update_count (expected 1)"
  log "DELETE count: $delete_count (expected 2)"

  if [ "$insert_count" -eq 2 ] && [ "$update_count" -eq 1 ] && [ "$delete_count" -eq 2 ]; then
    success "Change counts match expected values"
  else
    error "Change counts do not match expected values"
    return 1
  fi

  # Verify transformations and filters
  local masked_email=$(jq -r 'select(.Type == "INSERT" and .NewTuple.id == 1) | .NewTuple.email' "$OUTPUT_DIR"/*.jsonl)
  local formatted_phone=$(jq -r 'select(.Type == "INSERT" and .NewTuple.id == 1) | .NewTuple.phone' "$OUTPUT_DIR"/*.jsonl)
  local filtered_insert=$(jq -r 'select(.Type == "INSERT" and .NewTuple.id == 2) | .NewTuple.id' "$OUTPUT_DIR"/*.jsonl)
  local updated_email=$(jq -r 'select(.Type == "UPDATE") | .NewTuple.email' "$OUTPUT_DIR"/*.jsonl)
  local masked_ssn=$(jq -r 'select(.Type == "INSERT" and .NewTuple.id == 1) | .NewTuple.ssn' "$OUTPUT_DIR"/*.jsonl)
  local filtered_age=$(jq -r 'select(.Type == "INSERT" and .NewTuple.id == 2) | .NewTuple.age' "$OUTPUT_DIR"/*.jsonl)

  if [[ "$masked_email" == "j**************m" ]] &&
    [[ "$formatted_phone" == "(123) 456-7890" ]] &&
    [[ -z "$filtered_insert" ]] &&
    [[ "$updated_email" == "u*****************m" ]] &&
    [[ "$masked_ssn" == "1XXXXXXXXX9" ]] &&
    [[ -z "$filtered_age" ]]; then
    success "Transformations and filters applied correctly"
  else
    error "Transformations or filters not applied correctly"
    log "Masked email: $masked_email"
    log "Formatted phone: $formatted_phone"
    log "Filtered insert: $filtered_insert"
    log "Updated email: $updated_email"
    log "Masked SSN: $masked_ssn"
    log "Filtered age: $filtered_age"
    return 1
  fi
}

test_pg_flo_transform_filter() {
  setup_postgres
  create_users
  start_pg_flo_replication
  start_pg_flo_worker
  sleep 2
  simulate_changes

  log "Waiting for pg_flo to process changes..."

  stop_pg_flo_gracefully
  verify_changes || return 1
}

log "Starting pg_flo CDC test with transformations and filters..."
if test_pg_flo_transform_filter; then
  success "All tests passed! 🎉"
  exit 0
else
  error "Some tests failed. Please check the logs."
  show_pg_flo_logs
  exit 1
fi
