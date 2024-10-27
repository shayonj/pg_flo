#!/bin/bash
set -euo pipefail

source "$(dirname "$0")/e2e_common.sh"

create_multi_tenant_table() {
  log "Creating multi-tenant test table..."
  run_sql "DROP TABLE IF EXISTS public.events;"
  run_sql "CREATE TABLE public.events (
    id serial PRIMARY KEY,
    tenant_id int NOT NULL,
    name text,
    email text,
    created_at timestamp DEFAULT current_timestamp
  );"
  success "Multi-tenant test table created"
}

start_pg_flo_replication() {
  log "Starting pg_flo replication..."
  $pg_flo_BIN replicator \
    --host "$PG_HOST" \
    --port "$PG_PORT" \
    --dbname "$PG_DB" \
    --user "$PG_USER" \
    --password "$PG_PASSWORD" \
    --group "group_multi_tenant" \
    --tables "events" \
    --schema "public" \
    --nats-url "$NATS_URL" \
    >"$pg_flo_LOG" 2>&1 &
  pg_flo_PID=$!
  log "pg_flo replicator started with PID: $pg_flo_PID"
  success "pg_flo replication started"
}

start_pg_flo_worker() {
  log "Starting pg_flo worker with PostgreSQL sink..."
  $pg_flo_BIN worker postgres \
    --group "group_multi_tenant" \
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
    --rules-config "$(dirname "$0")/multi_tenant_rules.yml" \
    >"$pg_flo_WORKER_LOG" 2>&1 &
  pg_flo_WORKER_PID=$!
  log "pg_flo worker started with PID: $pg_flo_WORKER_PID"
  success "pg_flo worker started"
}

simulate_multi_tenant_changes() {
  log "Simulating multi-tenant changes..."
  run_sql "INSERT INTO public.events (tenant_id, name, email) VALUES
    (1, 'Alice', 'alice@tenant1.com'),
    (2, 'Bob', 'bob@tenant2.com'),
    (3, 'Charlie', 'charlie@tenant3.com'),
    (3, 'David', 'david@tenant3.com'),
    (4, 'Eve', 'eve@tenant4.com'),
    (3, 'Frank', 'frank@tenant3.com');"
  success "Multi-tenant changes simulated"
}

verify_multi_tenant_changes() {
  log "Verifying multi-tenant changes in target database..."
  local tenant_3_count=$(run_sql_target "SELECT COUNT(*) FROM public.events WHERE tenant_id = 3;" | xargs)
  local total_count=$(run_sql_target "SELECT COUNT(*) FROM public.events;" | xargs)

  log "Tenant 3 count: $tenant_3_count (expected 3)"
  log "Total count: $total_count (expected 3)"

  if [ "$tenant_3_count" -eq 3 ] && [ "$total_count" -eq 3 ]; then
    success "Multi-tenant filtering verified successfully"
    return 0
  else
    error "Multi-tenant filtering verification failed"
    return 1
  fi
}

test_pg_flo_multi_tenant() {
  setup_postgres
  create_multi_tenant_table
  start_pg_flo_replication
  sleep 2
  start_pg_flo_worker
  simulate_multi_tenant_changes

  log "Waiting for pg_flo to process changes..."
  sleep 5

  stop_pg_flo_gracefully
  verify_multi_tenant_changes || return 1
}

# Run the test
log "Starting pg_flo CDC test with multi-tenant filtering..."
if test_pg_flo_multi_tenant; then
  success "All tests passed! 🎉"
  exit 0
else
  error "Some tests failed. Please check the logs."
  show_pg_flo_logs
  exit 1
fi
