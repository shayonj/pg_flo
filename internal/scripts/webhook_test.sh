#!/bin/bash
set -euo pipefail

source "$(dirname "$0")/e2e_common.sh"

WEBHOOK_URL="https://deep-article-49.webhook.cool"

create_users() {
  log "Creating initial test table..."
  run_sql "DROP TABLE IF EXISTS public.users;"
  run_sql "CREATE TABLE public.users (id serial PRIMARY KEY, data text);"
  success "Initial test table created"
}

start_pg_flo_replication() {
  log "Starting pg_flo replication..."
  $pg_flo_BIN stream webhook \
    --host "$PG_HOST" \
    --port "$PG_PORT" \
    --dbname "$PG_DB" \
    --user "$PG_USER" \
    --password "$PG_PASSWORD" \
    --group "group-webhook" \
    --tables "users" \
    --schema "public" \
    --status-dir "/tmp" \
    --webhook-url "$WEBHOOK_URL" \
    --track-ddl >"$pg_flo_LOG" 2>&1 &
  pg_flo_PID=$!
  log "pg_flo started with PID: $pg_flo_PID"
  success "pg_flo replication started"
}

simulate_changes() {
  log "Simulating changes..."
  local insert_count=10
  local update_count=5
  local delete_count=3

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

test_pg_flo_webhook() {
  setup_docker
  setup_postgres
  create_users
  start_pg_flo_replication
  sleep 2
  simulate_changes
  perform_ddl_operations

  log "Waiting for pg_flo to process changes..."
  sleep 10

  stop_pg_flo_gracefully
  log "Test completed. Please check https://webhook.site/#!/f5a9abdb-c779-44a2-98ce-0760b4a2fc5c for received events."
}

# Run the test
log "Starting pg_flo CDC test with webhook sink..."
if test_pg_flo_webhook; then
  success "Test completed successfully. Please verify the results on webhook.site"
  exit 0
else
  error "Test failed. Please check the logs."
  show_pg_flo_logs
  exit 1
fi
