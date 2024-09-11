#!/bin/bash
set -euo pipefail

source "$(dirname "$0")/e2e_common.sh"

TOTAL_INSERTS=3000
INTERRUPT_AFTER=10
RESUME_WAIT_TIME=2
INSERT_COMPLETE_FLAG="/tmp/insert_complete"

create_users() {
  log "Creating test table..."
  run_sql "DROP TABLE IF EXISTS public.users;"
  run_sql "CREATE TABLE public.users (id serial PRIMARY KEY, data text, created_at timestamp DEFAULT current_timestamp);"
  success "Test table created"
}

start_pg_flo_replication() {
  log "Starting pg_flo replication..."
  $pg_flo_BIN replicator \
    --host "$PG_HOST" \
    --port "$PG_PORT" \
    --dbname "$PG_DB" \
    --user "$PG_USER" \
    --password "$PG_PASSWORD" \
    --group "group_resume" \
    --tables "users" \
    --schema "public" \
    --nats-url "$NATS_URL" \
    >"$pg_flo_LOG" 2>&1 &
  pg_flo_PID=$!
  log "pg_flo started with PID: $pg_flo_PID"
  success "pg_flo replication started"
}

start_pg_flo_worker() {
  log "Starting pg_flo worker with file sink..."
  $pg_flo_BIN worker file \
    --group "group_resume" \
    --nats-url "$NATS_URL" \
    --file-output-dir "$OUTPUT_DIR" \
    >"$pg_flo_WORKER_LOG" 2>&1 &
  pg_flo_WORKER_PID=$!
  log "pg_flo worker started with PID: $pg_flo_WORKER_PID"
  success "pg_flo worker started"
}

simulate_concurrent_inserts() {
  log "Starting concurrent inserts..."
  for i in $(seq 1 $TOTAL_INSERTS); do
    run_sql "INSERT INTO public.users (data) VALUES ('Data $i');"
  done
  touch $INSERT_COMPLETE_FLAG
  success "Concurrent inserts completed"
}

interrupt_pg_flo() {
  log "Interrupting pg_flo processes..."
  stop_pg_flo_gracefully
}

verify_results() {
  log "Verifying results..."
  local db_count=$(run_sql "SELECT COUNT(*) FROM public.users")
  local json_count=$(jq -s '[.[] | select(.Type == "INSERT")] | length' "$OUTPUT_DIR"/*.jsonl)

  log "Database row count: $db_count"
  log "JSON INSERT count: $json_count"

  if [ "$db_count" -eq "$TOTAL_INSERTS" ] && [ "$json_count" -eq "$TOTAL_INSERTS" ]; then
    success "All inserts accounted for without duplicates"
    return 0
  else
    error "Mismatch in insert counts. Expected $TOTAL_INSERTS, DB: $db_count, JSON: $json_count"
    return 1
  fi
}

test_pg_flo_resume() {
  setup_postgres
  create_users
  start_pg_flo_replication
  start_pg_flo_worker

  rm -f $INSERT_COMPLETE_FLAG

  sleep 1

  simulate_concurrent_inserts &
  local insert_pid=$!

  sleep $INTERRUPT_AFTER

  interrupt_pg_flo

  log "Waiting for $RESUME_WAIT_TIME seconds before resuming..."
  sleep $RESUME_WAIT_TIME

  start_pg_flo_replication
  start_pg_flo_worker

  log "Waiting for inserts to complete..."
  while [ ! -f $INSERT_COMPLETE_FLAG ]; do
    sleep 1
  done

  log "Inserts completed. Waiting for pg_flo to catch up..."
  sleep 5

  stop_pg_flo_gracefully

  if kill -0 $insert_pid 2>/dev/null; then
    wait $insert_pid
  fi

  verify_results || return 1
}

log "Starting pg_flo CDC resume test..."
if test_pg_flo_resume; then
  success "Resume test passed! 🎉"
  exit 0
else
  error "Resume test failed. Please check the logs."
  show_pg_flo_logs
  exit 1
fi
