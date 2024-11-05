#!/bin/bash
set -euo pipefail

source "$(dirname "$0")/e2e_common.sh"

setup_docker() {
  pkill -9 "pg_flo" || true
  rm -Rf /tmp/pg*
  log "Setting up Docker environment..."
  docker compose -f internal/docker-compose.yml down -v
  docker compose -f internal/docker-compose.yml up -d
  success "Docker environment is set up"
}

cleanup_data() {
  log "Cleaning up data..."
  run_sql "DROP TABLE IF EXISTS public.users;"
  run_sql "DROP SCHEMA IF EXISTS internal_pg_flo CASCADE;"
  rm -rf /tmp/pg_flo-output
  rm -f /tmp/pg_flo.log
  success "Data cleanup complete"
}

cleanup() {
  log "Cleaning up..."
  docker compose down -v
  success "Cleanup complete"
}

trap cleanup EXIT

make build

# setup_docker

setup_docker

log "Running e2e order tests..."
if CI=false ruby ./internal/scripts/e2e_order_test.rb; then
  success "Original e2e tests completed successfully"
else
  error "Original e2e tests failed"
  exit 1
fi

# log "Running e2e routing tests..."
# if CI=false ./internal/scripts/e2e_routing.sh; then
#   success "Original e2e tests completed successfully"
# else
#   error "Original e2e tests failed"
#   exit 1
# fi

# setup_docker

# log "Running e2e copy & stream tests..."
# if CI=false ./internal/scripts/e2e_copy_and_stream.sh; then
#   success "Original e2e tests completed successfully"
# else
#   error "Original e2e tests failed"
#   exit 1
# fi

# setup_docker

# log "Running e2e copy only tests..."
# if CI=false ./internal/scripts/e2e_copy_only.sh; then
#   success "Original e2e tests completed successfully"
# else
#   error "Original e2e tests failed"
#   exit 1
# fi

# setup_docker

# log "Running e2e copy & stream tests..."
# if CI=false ./internal/scripts/e2e_multi_tenant.sh; then
#   success "Original e2e tests completed successfully"
# else
#   error "Original e2e tests failed"
#   exit 1
# fi

# setup_docker

# log "Running new e2e stream tests with changes..."
# if ./internal/scripts/e2e_test_stream.sh; then
#   success "New e2e tests with changes completed successfully"
# else
#   error "New e2e tests with changes failed"
#   exit 1
# fi

# setup_docker

# Run new e2e resume test
# log "Running new e2e resume test..."
# if ./internal/scripts/e2e_resume.sh; then
#   success "E2E resume test completed successfully"
# else
#   error "E2E resume test failed"
#   exit 1
# fi

# setup_docker

# # Run new e2e test for transform & filter
# log "Running new e2e test for transform & filter..."
# if ./internal/scripts/e2e_transform_filter.sh; then
#   success "E2E test for transform & filter test completed successfully"
# else
#   error "E2E test for transform & filter test failed"
#   exit 1
# fi

# setup_docker

# # Run new e2e test for DDL changes
# log "Running new e2e test for DDL changes..."
# if ./internal/scripts/e2e_ddl.sh; then
#   success "E2E test for DDL changes completed successfully"
# else
#   error "E2E test for DDL changes failed"
#   exit 1
# fi

# setup_docker

# Run new e2e test for Postgres changes
# log "Running new e2e test Postgres Sink..."
# if ./internal/scripts/e2e_postgres.sh; then
#   success "E2E test for Postgres Sink completed successfully"
# else
#   error "E2E test for Postgres Sink failed"
#   exit 1
# fi

# setup_docker

# log "Running new e2e test Postgres Sink..."
# if ./internal/scripts/e2e_postgres_data_type.sh; then
#   success "E2E test for Postgres Sink completed successfully"
# else
#   error "E2E test for Postgres Sink failed"
#   exit 1
# fi

success "All local e2e tests completed successfully"
