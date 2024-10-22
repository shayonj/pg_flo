#!/bin/bash
set -euo pipefail

source "$(dirname "$0")/e2e_common.sh"

create_users() {
  log "Creating test table..."
  run_sql "DROP TABLE IF EXISTS public.users;"
  run_sql "CREATE TABLE public.users (
    id serial PRIMARY KEY,
    int_col integer,
    float_col float,
    text_col text,
    bool_col boolean,
    date_col date,
    timestamp_col timestamp with time zone,
    json_col jsonb,
    array_col integer[],
    bytea_col bytea
  );"
  success "Test table created"
}

populate_initial_data() {
  log "Populating initial data..."
  run_sql "INSERT INTO public.users (
    int_col, float_col, text_col, bool_col, date_col, timestamp_col, json_col, array_col, bytea_col
  ) SELECT
    generate_series(1, 500000),
    random() * 100,
    'Initial data ' || generate_series(1, 500000),
    (random() > 0.5),
    current_date + (random() * 365)::integer * interval '1 day',
    current_timestamp + (random() * 365 * 24 * 60 * 60)::integer * interval '1 second',
    json_build_object('key', 'value' || generate_series(1, 500000), 'number', generate_series(1, 500000)),
    ARRAY[generate_series(1, 3)],
    decode(lpad(to_hex(generate_series(1, 4)), 8, '0'), 'hex')
  ;"
  run_sql "UPDATE public.users SET text_col = text_col || ' - Updated';"

  log "Inserting large JSON data..."
  local large_json='{"data":['
  for i in {1..10000}; do
    if [ $i -ne 1 ]; then
      large_json+=','
    fi
    large_json+='{"id":'$i',"name":"Item '$i'","description":"This is a long description for item '$i'. It contains a lot of text to make the JSON larger.","attributes":{"color":"red","size":"large","weight":10.5,"tags":["tag1","tag2","tag3"]}}'
  done
  large_json+=']}'

  run_sql "INSERT INTO public.users (int_col, json_col) VALUES (1000001, '$large_json'::jsonb);"

  run_sql "ANALYZE public.users;"
  success "Initial data populated"
}

simulate_concurrent_changes() {
  log "Simulating concurrent changes..."
  for i in {1..3000}; do
    run_sql "INSERT INTO public.users (
      int_col, float_col, text_col, bool_col, date_col, timestamp_col, json_col, array_col, bytea_col
    ) VALUES (
      $i,
      $i * 1.5,
      'Concurrent data $i',
      ($i % 2 = 0),
      current_date + ($i % 365) * interval '1 day',
      current_timestamp + ($i % (365 * 24)) * interval '1 hour',
      '{\"key\": \"concurrent_$i\", \"value\": $i}',
      ARRAY[$i, $i+1, $i+2],
      decode(lpad(to_hex($i), 8, '0'), 'hex')
    );"
  done
  success "Concurrent changes simulated"
}

start_pg_flo_replication() {
  log "Starting pg_flo replication..."
  $pg_flo_BIN replicator \
    --host "$PG_HOST" \
    --port "$PG_PORT" \
    --dbname "$PG_DB" \
    --user "$PG_USER" \
    --password "$PG_PASSWORD" \
    --group "test_group" \
    --tables "users" \
    --schema "public" \
    --nats-url "$NATS_URL" \
    --copy-and-stream \
    --max-copy-workers-per-table 4 \
    >"$pg_flo_LOG" 2>&1 &
  pg_flo_PID=$!
  log "pg_flo started with PID: $pg_flo_PID"
  success "pg_flo replication started"
}

start_pg_flo_worker() {
  log "Starting pg_flo worker with file sink..."
  $pg_flo_BIN worker file \
    --group "test_group" \
    --nats-url "$NATS_URL" \
    --file-output-dir "$OUTPUT_DIR" \
    >"$pg_flo_WORKER_LOG" 2>&1 &
  pg_flo_WORKER_PID=$!
  log "pg_flo worker started with PID: $pg_flo_WORKER_PID"
  success "pg_flo worker started"
}

compare_row_counts() {
  log "Comparing row counts..."
  DB_COUNT=$(run_sql "SELECT COUNT(*) FROM public.users")
  JSON_COUNT=$(jq -s '[.[] | select(.Type == "INSERT")] | length' "$OUTPUT_DIR"/*.jsonl)

  log "Database row count: $DB_COUNT"
  log "JSON INSERT count: $JSON_COUNT"

  EXPECTED_COUNT=503000

  if [ "$DB_COUNT" -eq "$JSON_COUNT" ] && [ "$DB_COUNT" -eq "$EXPECTED_COUNT" ]; then
    success "Row counts match and total is correct ($EXPECTED_COUNT)"
    return 0
  else
    error "Row counts do not match or total is incorrect. Expected $EXPECTED_COUNT, DB: $DB_COUNT, JSON: $JSON_COUNT"
    return 1
  fi
}

verify_large_json() {
  log "Verifying large JSON data..."
  local json_length=$(jq '.[] | select(.Type == "INSERT" and .NewRow.int_col == 1000001) | .NewRow.json_col | length' "$OUTPUT_DIR"/*.jsonl | head -n 1)

  log "Large JSON length: $json_length"

  if [ "$json_length" -gt 1000000 ]; then
    success "Large JSON data verified successfully"
    return 0
  else
    error "Large JSON data verification failed. Expected length > 1000000, got $json_length"
    return 1
  fi
}

test_pg_flo_cdc() {
  setup_postgres
  create_users
  populate_initial_data

  start_pg_flo_replication
  start_pg_flo_worker
  simulate_concurrent_changes

  log "Waiting for pg_flo to process changes..."
  sleep 2

  stop_pg_flo_gracefully

  sleep 1

  compare_row_counts || return 1
  verify_large_json || return 1
}

log "Starting pg_flo CDC test..."
if test_pg_flo_cdc; then
  success "All tests passed! 🎉"
  exit 0
else
  error "Some tests failed. Please check the logs."
  show_pg_flo_logs
  exit 1
fi
