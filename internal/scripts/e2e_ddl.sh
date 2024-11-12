#!/bin/bash
set -euo pipefail

source "$(dirname "$0")/e2e_common.sh"

create_test_tables() {
  log "Creating test schemas and tables..."
  run_sql "DROP SCHEMA IF EXISTS app CASCADE; CREATE SCHEMA app;"
  run_sql "DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;"

  run_sql "CREATE TABLE app.users (id serial PRIMARY KEY, data text);"
  run_sql "CREATE TABLE app.posts (id serial PRIMARY KEY, content text);"

  run_sql "CREATE TABLE app.comments (id serial PRIMARY KEY, text text);"
  run_sql "CREATE TABLE public.metrics (id serial PRIMARY KEY, value numeric);"
  success "Test tables created"
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
    --schema "app" \
    --tables "users,posts" \
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

  # Column operations on tracked tables
  run_sql "ALTER TABLE app.users ADD COLUMN email text;"
  run_sql "ALTER TABLE app.users ADD COLUMN status varchar(50) DEFAULT 'active';"
  run_sql "ALTER TABLE app.posts ADD COLUMN category text;"

  # Index operations on tracked tables
  run_sql "CREATE INDEX CONCURRENTLY idx_users_email ON app.users (email);"
  run_sql "CREATE UNIQUE INDEX idx_posts_unique ON app.posts (content) WHERE content IS NOT NULL;"

  # Column modifications on tracked tables
  run_sql "ALTER TABLE app.users ALTER COLUMN status SET DEFAULT 'pending';"
  run_sql "ALTER TABLE app.posts ALTER COLUMN category TYPE varchar(100);"

  # Rename operations on tracked tables
  run_sql "ALTER TABLE app.users RENAME COLUMN data TO profile;"

  # Drop operations on tracked tables
  run_sql "DROP INDEX CONCURRENTLY IF EXISTS idx_users_email;"
  run_sql "ALTER TABLE app.posts DROP COLUMN IF EXISTS category;"

  # Operations on non-tracked tables (should be ignored)
  run_sql "ALTER TABLE app.comments ADD COLUMN author text;"
  run_sql "CREATE INDEX idx_comments_text ON app.comments (text);"
  run_sql "ALTER TABLE public.metrics ADD COLUMN timestamp timestamptz;"

  success "DDL operations performed"
}

verify_ddl_changes() {
  log "Verifying DDL changes in target database..."
  local failures=0

  check_column() {
    local table=$1
    local column=$2
    local expected_exists=$3
    local expected_type=${4:-""}
    local expected_default=${5:-""}
    local query="
      SELECT COUNT(*),
             data_type,
             character_maximum_length,
             column_default
      FROM information_schema.columns
      WHERE table_schema='app'
        AND table_name='$table'
        AND column_name='$column'
      GROUP BY data_type, character_maximum_length, column_default;"

    local result
    result=$(run_sql_target "$query")

    if [ -z "$result" ]; then
      exists=0
      data_type=""
      char_length=""
      default_value=""
    else
      read exists data_type char_length default_value < <(echo "$result" | tr '|' ' ')
    fi

    exists=${exists:-0}

    if [ "$exists" -eq "$expected_exists" ]; then
      if [ "$expected_exists" -eq 1 ]; then
        local type_ok=true
        local default_ok=true

        if [ -n "$expected_type" ]; then
          # Handle character varying type specifically
          if [ "$expected_type" = "character varying" ]; then
            if [ "$data_type" = "character varying" ] || [ "$data_type" = "varchar" ] || [ "$data_type" = "character" ]; then
              type_ok=true
            else
              type_ok=false
            fi
          elif [ "$data_type" != "$expected_type" ]; then
            type_ok=false
          fi
        fi

        if [ -n "$expected_default" ]; then
          if [[ "$default_value" == *"$expected_default"* ]]; then
            default_ok=true
          else
            default_ok=false
          fi
        fi

        if [ "$type_ok" = true ] && [ "$default_ok" = true ]; then
          if [[ "$expected_type" == "character varying" && -n "$char_length" ]]; then
            success "Column app.$table.$column verification passed (type: $data_type($char_length), default: $default_value)"
          else
            success "Column app.$table.$column verification passed (type: $data_type, default: $default_value)"
          fi
        else
          if [ "$type_ok" = false ]; then
            error "Column app.$table.$column type mismatch (expected: $expected_type, got: $data_type)"
            failures=$((failures + 1))
          fi
          if [ "$default_ok" = false ]; then
            error "Column app.$table.$column default value mismatch (expected: $expected_default, got: $default_value)"
            failures=$((failures + 1))
          fi
        fi
      else
        success "Column app.$table.$column verification passed (not exists)"
      fi
    else
      error "Column app.$table.$column verification failed (expected: $expected_exists, got: $exists)"
      failures=$((failures + 1))
    fi
  }

  check_index() {
    local index=$1
    local expected=$2
    local exists=$(run_sql_target "SELECT COUNT(*) FROM pg_indexes WHERE schemaname='app' AND indexname='$index';")

    if [ "$exists" -eq "$expected" ]; then
      success "Index app.$index verification passed (expected: $expected)"
    else
      error "Index app.$index verification failed (expected: $expected, got: $exists)"
      failures=$((failures + 1))
    fi
  }

  # Verify app.users changes
  check_column "users" "email" 1 "text"
  check_column "users" "status" 1 "character varying" "'pending'"
  check_column "users" "data" 0
  check_column "users" "profile" 1 "text"

  # Verify app.posts changes
  check_column "posts" "category" 0
  check_column "posts" "content" 1 "text"
  check_index "idx_posts_unique" 1 "unique"

  # Verify non-tracked tables
  check_column "comments" "author" 0
  check_index "idx_comments_text" 0

  local remaining_rows=$(run_sql "SELECT COUNT(*) FROM internal_pg_flo.ddl_log;")
  if [ "$remaining_rows" -eq 0 ]; then
    success "internal_pg_flo.ddl_log table is empty"
  else
    error "internal_pg_flo.ddl_log table is not empty. Remaining rows: $remaining_rows"
    failures=$((failures + 1))
  fi

  if [ "$failures" -eq 0 ]; then
    success "All DDL changes verified successfully"
    return 0
  else
    error "DDL verification failed with $failures errors"
    return 1
  fi
}

test_pg_flo_ddl() {
  setup_postgres
  create_test_tables
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
