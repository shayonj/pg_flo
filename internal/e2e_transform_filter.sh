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
  $pg_flo_BIN stream file \
    --host "$PG_HOST" \
    --port "$PG_PORT" \
    --dbname "$PG_DB" \
    --user "$PG_USER" \
    --password "$PG_PASSWORD" \
    --group "group_transform_filter" \
    --tables "users" \
    --schema "public" \
    --status-dir "/tmp" \
    --output-dir "$OUTPUT_DIR" \
    --track-ddl \
    --rules-config "$(dirname "$0")/rules.yml" >"$pg_flo_LOG" 2>&1 &

  pg_flo_PID=$!
  log "pg_flo started with PID: $pg_flo_PID"
  success "pg_flo replication started"
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
  local insert_count=$(jq -s '[.[] | select(.type == "INSERT")] | length' "$OUTPUT_DIR"/*.jsonl)
  local update_count=$(jq -s '[.[] | select(.type == "UPDATE")] | length' "$OUTPUT_DIR"/*.jsonl)
  local delete_count=$(jq -s '[.[] | select(.type == "DELETE")] | length' "$OUTPUT_DIR"/*.jsonl)

  log "INSERT count: $insert_count (expected 2)"
  log "UPDATE count: $update_count (expected 1)"
  log "DELETE count: $delete_count (expected 2)"

  if [ "$insert_count" -eq 2 ] && [ "$update_count" -eq 1 ]; then
    success "Change counts match expected values"
  else
    error "Change counts do not match expected values"
    return 1
  fi

  # Verify transformations and filters
  local masked_email=$(jq -r 'select(.type == "INSERT" and .new_row.id.value == 1) | .new_row.email.value' "$OUTPUT_DIR"/*.jsonl)
  local formatted_phone=$(jq -r 'select(.type == "INSERT" and .new_row.id.value == 1) | .new_row.phone.value' "$OUTPUT_DIR"/*.jsonl)
  local filtered_insert=$(jq -r 'select(.type == "INSERT" and .new_row.id.value == 2) | .new_row.id.value' "$OUTPUT_DIR"/*.jsonl)
  local updated_email=$(jq -r 'select(.type == "UPDATE") | .new_row.email.value' "$OUTPUT_DIR"/*.jsonl)
  local masked_ssn=$(jq -r 'select(.type == "INSERT" and .new_row.id.value == 1) | .new_row.ssn.value' "$OUTPUT_DIR"/*.jsonl)
  local filtered_age=$(jq -r 'select(.type == "INSERT" and .new_row.id.value == 2) | .new_row.age.value' "$OUTPUT_DIR"/*.jsonl)

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
  sleep 1
  simulate_changes

  log "Waiting for pg_flo to process changes..."
  sleep 5

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
