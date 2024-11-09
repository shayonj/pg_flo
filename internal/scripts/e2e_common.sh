#!/bin/bash

PG_HOST="${PG_HOST:-localhost}"
PG_PORT="${PG_PORT:-5433}"
PG_USER="${PG_USER:-myuser}"
PG_PASSWORD="${PG_PASSWORD:-mypassword!@#%1234}"
PG_DB="${PG_DB:-mydb}"

TARGET_PG_HOST="${TARGET_PG_HOST:-localhost}"
TARGET_PG_PORT="${TARGET_PG_PORT:-5434}"
TARGET_PG_USER="${TARGET_PG_USER:-targetuser}"
TARGET_PG_PASSWORD="${TARGET_PG_PASSWORD:-targetpassword!@#1234}"
TARGET_PG_DB="${TARGET_PG_DB:-targetdb}"

NATS_URL="${NATS_URL:-nats://localhost:4222}"

pg_flo_BIN="./bin/pg_flo"
OUTPUT_DIR="/tmp/pg_flo-output"
pg_flo_LOG="/tmp/pg_flo.log"
pg_flo_WORKER_LOG="/tmp/pg_flo_worker.log"

# Helper functions
log() { echo "🔹 $1"; }
success() { echo "✅ $1"; }
error() { echo "❌ $1"; }

run_sql() {
  if [ ${#1} -gt 1000 ]; then
    local temp_file=$(mktemp)
    echo "$1" >"$temp_file"
    PGPASSWORD=$PG_PASSWORD psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" -p "$PG_PORT" -q -t -f "$temp_file"
    rm "$temp_file"
  else
    PGPASSWORD=$PG_PASSWORD psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" -p "$PG_PORT" -q -t -c "$1"
  fi
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
  log "Stopping pg_flo replicator..."
  if kill -0 "$pg_flo_PID" 2>/dev/null; then
    kill -TERM "$pg_flo_PID"
    wait "$pg_flo_PID" 2>/dev/null || true
    success "pg_flo replicator stopped"
  else
    log "pg_flo replicator process not found, it may have already completed"
  fi

  log "Stopping pg_flo worker..."
  if kill -0 "$pg_flo_WORKER_PID" 2>/dev/null; then
    kill -TERM "$pg_flo_WORKER_PID"
    wait "$pg_flo_WORKER_PID" 2>/dev/null || true
    success "pg_flo worker stopped"
  else
    log "pg_flo worker process not found, it may have already completed"
  fi
}

show_pg_flo_logs() {
  log "pg_flo replicator logs:"
  echo "----------------------------------------"
  cat $pg_flo_LOG*
  echo "----------------------------------------"

  log "pg_flo worker logs:"
  echo "----------------------------------------"
  cat $pg_flo_WORKER_LOG*
  echo "----------------------------------------"
}

run_sql_target() {
  if [ ${#1} -gt 1000 ]; then
    local temp_file=$(mktemp)
    echo "$1" >"$temp_file"
    PGPASSWORD=$TARGET_PG_PASSWORD psql -h "$TARGET_PG_HOST" -U "$TARGET_PG_USER" -d "$TARGET_PG_DB" -p "$TARGET_PG_PORT" -q -t -f "$temp_file"
    rm "$temp_file"
  else
    PGPASSWORD=$TARGET_PG_PASSWORD psql -h "$TARGET_PG_HOST" -U "$TARGET_PG_USER" -d "$TARGET_PG_DB" -p "$TARGET_PG_PORT" -q -t -c "$1"
  fi
}

setup_docker() {
  rm -Rf /tmp/pg*
  log "Setting up Docker environment..."
  docker compose -f internal/docker-compose.yml down -v
  docker compose -f internal/docker-compose.yml up -d
  success "Docker environment is set up"
}
