#!/bin/bash
set -euo pipefail

source "$(dirname "$0")/e2e_common.sh"

ROUTING_CONFIG="/tmp/pg_flo_routing_test.yml"
RULES_CONFIG="/tmp/pg_flo_rules_test.yml"

# Ensure temporary files are removed on exit
trap 'rm -f "$ROUTING_CONFIG" "$RULES_CONFIG"' EXIT

create_tables() {
  log "Creating test tables in source and target databases..."

  # Source database tables
  run_sql "DROP TABLE IF EXISTS public.users CASCADE;"
  run_sql "DROP TABLE IF EXISTS public.orders CASCADE;"
  run_sql "DROP TABLE IF EXISTS public.products CASCADE;"

  run_sql "CREATE TABLE public.users (
    id bigserial PRIMARY KEY,
    username varchar(50) NOT NULL,
    email varchar(100) NOT NULL,
    subscription_type jsonb,
    metadata jsonb
  );"

  run_sql "CREATE TABLE public.orders (
    id bigserial PRIMARY KEY,
    user_id bigint REFERENCES public.users(id),
    total_amount numeric(15,2) NOT NULL,
    items jsonb
  );"

  run_sql "CREATE TABLE public.products (
    id bigserial PRIMARY KEY,
    name varchar(100) NOT NULL,
    stock jsonb,
    tags text[]
  );"

  # Target database tables
  run_sql_target "DROP TABLE IF EXISTS public.customers CASCADE;"
  run_sql_target "DROP TABLE IF EXISTS public.transactions CASCADE;"
  run_sql_target "DROP TABLE IF EXISTS public.items CASCADE;"

  run_sql_target "CREATE TABLE public.customers (
    customer_id bigserial PRIMARY KEY,
    customer_name varchar(50) NOT NULL,
    email varchar(100) NOT NULL,
    subscription_type jsonb,
    metadata jsonb
  );"

  run_sql_target "CREATE TABLE public.transactions (
    transaction_id bigserial PRIMARY KEY,
    user_id bigint,
    amount numeric(15,2) NOT NULL,
    items jsonb
  );"

  run_sql_target "CREATE TABLE public.items (
    item_id bigserial PRIMARY KEY,
    item_name varchar(100) NOT NULL,
    stock jsonb,
    tags text[]
  );"

  success "Test tables created in source and target databases"
}

create_routing_config() {
  log "Creating routing configuration..."
  cat <<EOF >"$ROUTING_CONFIG"
users:
  source_table: users
  destination_table: customers
  column_mappings:
    - source: id
      destination: customer_id
    - source: username
      destination: customer_name
    - source: email
      destination: email
    - source: subscription_type
      destination: subscription_type
    - source: metadata
      destination: metadata
  operations:
    - INSERT
    - UPDATE

orders:
  source_table: orders
  destination_table: transactions
  column_mappings:
    - source: id
      destination: transaction_id
    - source: total_amount
      destination: amount
    - source: items
      destination: items
  operations:
    - INSERT
    - UPDATE
    - DELETE

products:
  source_table: products
  destination_table: items
  column_mappings:
    - source: id
      destination: item_id
    - source: name
      destination: item_name
    - source: stock
      destination: stock
    - source: tags
      destination: tags
  operations:
    - INSERT
    - UPDATE
EOF
  success "Routing configuration created"
}

create_rules_config() {
  log "Creating rules configuration..."
  cat <<EOF >"$RULES_CONFIG"
users:
  - type: transform
    column: email
    parameters:
      operation: lowercase
  - type: transform
    column: metadata
    parameters:
      operation: jsonb_extract_path
      path: subscription_type
orders:
  - type: filter
    column: total_amount
    parameters:
      operator: gt
      value: 100
  - type: transform
    column: items
    parameters:
      operation: jsonb_array_length
products:
  - type: transform
    column: inventory
    parameters:
      operation: jsonb_extract_path
      path: stock
  - type: transform
    column: tags
    parameters:
      operation: array_length
EOF
  success "Rules configuration created"
}

create_config_files() {
  log "Creating config files..."

  # Create replicator config
  cat >"/tmp/pg_flo_replicator.yml" <<EOF
# Replicator PostgreSQL connection settings
host: "${PG_HOST}"
port: ${PG_PORT}
dbname: "${PG_DB}"
user: "${PG_USER}"
password: "${PG_PASSWORD}"
schema: "public"
group: "group_routing_test"
tables:
  - users
  - orders
  - products
nats-url: "${NATS_URL}"
EOF

  # Create worker config
  cat >"/tmp/pg_flo_worker.yml" <<EOF
# Worker settings
group: "group_routing_test"
nats-url: "${NATS_URL}"
batch-size: 5000

# Routing and rules configuration files
routing-config: "${ROUTING_CONFIG}"
rules-config: "${RULES_CONFIG}"

# Source connection for schema sync
source-host: "${PG_HOST}"
source-port: ${PG_PORT}
source-dbname: "${PG_DB}"
source-user: "${PG_USER}"
source-password: "${PG_PASSWORD}"

# Target PostgreSQL connection settings
target-host: "${TARGET_PG_HOST}"
target-port: ${TARGET_PG_PORT}
target-dbname: "${TARGET_PG_DB}"
target-user: "${TARGET_PG_USER}"
target-password: "${TARGET_PG_PASSWORD}"
target-sync-schema: true
EOF

  create_routing_config
  create_rules_config
  success "Config files created"
}

start_pg_flo_replication() {
  log "Starting pg_flo replicator..."
  $pg_flo_BIN replicator --config "/tmp/pg_flo_replicator.yml" >"$pg_flo_LOG" 2>&1 &
  pg_flo_PID=$!
  log "pg_flo replicator started with PID: $pg_flo_PID"
  success "pg_flo replicator started"
}

start_pg_flo_worker() {
  log "Starting pg_flo worker with routing and rules..."
  $pg_flo_BIN worker postgres --config "/tmp/pg_flo_worker.yml" >"$pg_flo_WORKER_LOG" 2>&1 &
  pg_flo_WORKER_PID=$!
  log "pg_flo worker started with PID: $pg_flo_WORKER_PID"
  success "pg_flo worker started"
}

simulate_changes() {
  log "Simulating changes..."

  # Users with specific email format and subscription type
  run_sql "INSERT INTO public.users (username, email, subscription_type)
    VALUES ('john_doe', 'updated@example.com', '{\"type\": \"enterprise\"}');"
  run_sql "INSERT INTO public.users (username, email)
    VALUES ('jane_smith', 'jane@example.com');"

  # Orders with specific amounts and items count
  run_sql "INSERT INTO public.orders (user_id, total_amount, items)
    VALUES (1, 50.00, '[{\"id\": 1}, {\"id\": 2}]');"
  run_sql "INSERT INTO public.orders (user_id, total_amount, items)
    VALUES (1, 150.00, '[{\"id\": 3}, {\"id\": 4}]');"

  # Products with stock and tags
  run_sql "INSERT INTO public.products (name, stock, tags)
    VALUES ('Widget', '{\"quantity\": 75}', '{\"electronic\",\"gadget\",\"premium\"}');"
  run_sql "INSERT INTO public.products (name, stock, tags)
    VALUES ('Gadget', '{\"quantity\": 50}', '{\"tool\",\"premium\"}');"

  success "Changes simulated"
}

verify_changes() {
  log "Verifying changes in target database..."
  local test_failed=false

  # Verify users (customers) table
  local user_count=$(run_sql_target "SELECT COUNT(*) FROM public.customers;" | xargs)
  local john_email=$(run_sql_target "SELECT email FROM public.customers WHERE customer_name = 'john_doe';" | xargs)
  local jane_email=$(run_sql_target "SELECT email FROM public.customers WHERE customer_name = 'jane_smith';" | xargs)
  local john_subscription=$(run_sql_target "SELECT subscription_type->>'type' FROM public.customers WHERE customer_name = 'john_doe';" | xargs)

  # Verify orders (transactions) table
  local order_count=$(run_sql_target "SELECT COUNT(*) FROM public.transactions;" | xargs)
  local high_value_order=$(run_sql_target "SELECT amount FROM public.transactions WHERE transaction_id = 2;" | xargs)
  local order_items_count=$(run_sql_target "SELECT jsonb_array_length(items) FROM public.transactions WHERE transaction_id = 2;" | xargs)

  # Verify products (items) table
  local product_count=$(run_sql_target "SELECT COUNT(*) FROM public.items;" | xargs)
  local widget_stock=$(run_sql_target "SELECT (stock->>'quantity')::int FROM public.items WHERE item_name = 'Widget';" | xargs)
  local widget_tags_count=$(run_sql_target "SELECT array_length(tags, 1) FROM public.items WHERE item_name = 'Widget';" | xargs)

  # Assertions
  assert_equals "$user_count" "2" "Customer count" || test_failed=true
  assert_equals "$john_email" "updated@example.com" "John's email" || test_failed=true
  assert_equals "$jane_email" "jane@example.com" "Jane's email" || test_failed=true
  assert_equals "$john_subscription" "enterprise" "John's subscription type" || test_failed=true

  assert_equals "$order_count" "2" "Transaction count" || test_failed=true
  assert_equals "$high_value_order" "150.00" "High-value order amount" || test_failed=true
  assert_equals "$order_items_count" "2" "Order items count" || test_failed=true

  assert_equals "$product_count" "2" "Item count" || test_failed=true
  assert_equals "$widget_stock" "75" "Widget stock" || test_failed=true
  assert_equals "$widget_tags_count" "3" "Widget tags count" || test_failed=true

  if [ "$test_failed" = true ]; then
    error "Verification failed: One or more assertions did not pass"
    return 1
  fi

  success "All changes verified successfully in target database"
  return 0
}

assert_equals() {
  if [ "$1" != "$2" ]; then
    error "Assertion failed: $3 - Expected '$2', but got '$1'"
    return 1
  fi
  log "Assertion passed: $3"
}

test_pg_flo_routing() {
  setup_postgres
  create_tables
  create_config_files
  start_pg_flo_replication
  sleep 2
  start_pg_flo_worker
  simulate_changes

  log "Waiting for pg_flo to process changes..."
  sleep 2

  stop_pg_flo_gracefully
  verify_changes || return 1
}

# Run the test
log "Starting pg_flo CDC test with routing and rules..."
if test_pg_flo_routing; then
  success "All tests passed! 🎉"
  exit 0
else
  error "Some tests failed. Please check the logs."
  show_pg_flo_logs
  exit 1
fi
