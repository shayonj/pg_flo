#!/bin/bash

PG_HOST="${PG_HOST:-localhost}"
PG_PORT="${PG_PORT:-5433}"
PG_USER="${PG_USER:-myuser}"
PG_PASSWORD="${PG_PASSWORD:-mypassword}"
PG_DB="${PG_DB:-mydb}"

TARGET_PG_HOST="${TARGET_PG_HOST:-localhost}"
TARGET_PG_PORT="${TARGET_PG_PORT:-5434}"
TARGET_PG_USER="${TARGET_PG_USER:-targetuser}"
TARGET_PG_PASSWORD="${TARGET_PG_PASSWORD:-targetpassword}"
TARGET_PG_DB="${TARGET_PG_DB:-targetdb}"

pg_flo_BIN="./bin/pg_flo"
OUTPUT_DIR="/tmp/pg_flo-output"
pg_flo_LOG="/tmp/pg_flo.log"

# Helper functions
log() { echo "🔹 $1"; }
success() { echo "✅ $1"; }
error() { echo "❌ $1"; }

run_sql() {
  PGPASSWORD=$PG_PASSWORD psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" -p "$PG_PORT" -q -t -c "$1"
}

setup_postgres() {
  log "Ensuring PostgreSQL is ready..."
  for i in {1..30}; do
    if PGPASSWORD=$PG_PASSWORD psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" -p "$PG_PORT" -c '\q' >/dev/null 2>&1; then
      success "PostgreSQL is ready"
      return 0
    fi
    sleep 1
  done
  error "PostgreSQL is not ready after 30 seconds"
  exit 1
}

stop_pg_flo_gracefully() {
  log "Stopping pg_flo..."
  if kill -0 "$pg_flo_PID" 2>/dev/null; then
    kill -TERM "$pg_flo_PID"
    wait "$pg_flo_PID" 2>/dev/null || true
    success "pg_flo stopped"
  else
    log "pg_flo process not found, it may have already completed"
  fi
}

show_pg_flo_logs() {
  log "pg_flo logs:"
  echo "----------------------------------------"
  cat $pg_flo_LOG
  echo "----------------------------------------"
}
