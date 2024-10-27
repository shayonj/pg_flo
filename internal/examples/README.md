# pg_flo Examples

This guide demonstrates common use cases for pg_flo with practical examples.

## Basic Replication

Simple database-to-database replication:

```bash
# Start replicator
pg_flo replicator \
  --host source-db.example.com \
  --port 5432 \
  --dbname myapp \
  --user replicator \
  --password secret \
  --group users_orders \
  --tables users,orders \
  --nats-url nats://localhost:4222

# Start worker
pg_flo worker postgres \
  --group users_orders \
  --nats-url nats://localhost:4222 \
  --target-host dest-db.example.com \
  --target-dbname myapp \
  --target-user writer \
  --target-password secret
```

## Data Masking and Transformation

Mask sensitive data during replication:

```yaml
# rules.yaml
rules:
  - table: users
    type: transform
    column: email
    parameters:
      type: mask
      mask_char: "*"
    operations: [INSERT, UPDATE]
  - table: payments
    type: transform
    column: card_number
    parameters:
      type: regex_replace
      pattern: "(\d{12})(\d{4})"
      replacement: "************$2"
```

```bash
pg_flo worker postgres \
  --group sensitive_data \
  --rules-config /path/to/rules.yaml \
  # ... other postgres connection flags
```

## Custom Table Routing

Route and rename tables/columns:

```yaml
# routing.yaml
users:
  source_table: users
  destination_table: customers
  column_mappings:
    - source: user_id
      destination: customer_id
    - source: created_at
      destination: signup_date
  operations:
    - INSERT
    - UPDATE
```

```bash
pg_flo worker postgres \
  --group user_migration \
  --routing-config /path/to/routing.yaml \
  # ... other postgres connection flags
```

## Initial Load with Streaming

Perform parallel initial data load followed by continuous streaming:

```bash
pg_flo replicator \
  --copy-and-stream \
  --max-copy-workers-per-table 4 \
  --group full_sync \
  # ... other connection flags
```

## Multi-Destination Pipeline

Stream changes to multiple destinations simultaneously:

```bash
# Terminal 1: Stream to PostgreSQL
pg_flo worker postgres \
  --group audit \
  # ... postgres connection flags

# Terminal 2: Stream to files for archival
pg_flo worker file \
  --group audit \
  --file-output-dir /archive/changes

# Terminal 3: Stream to webhook for external processing
pg_flo worker webhook \
  --group audit \
  --webhook-url https://api.example.com/changes \
  --webhook-batch-size 100
```

## Schema Tracking

Enable DDL tracking to capture schema changes. DDLs are applied on the destination as they arrive:

```bash
pg_flo replicator \
  --track-ddl \
  --group schema_sync \
  # ... other connection flags

pg_flo worker postgres \
  --group schema_sync \
  --target-sync-schema true \
  # ... other postgres connection flags
```

## Configuration File

Instead of CLI flags, you can use a configuration file:

```yaml
# ~/.pg_flo.yaml
host: "source-db.example.com"
port: 5432
dbname: "myapp"
user: "replicator"
password: "secret"
group: "production"
tables:
  - users
  - orders
  - payments
nats-url: "nats://localhost:4222"
target-host: "dest-db.example.com"
target-dbname: "myapp"
target-user: "writer"
target-password: "secret"
```

```bash
pg_flo replicator --config /path/to/config.yaml
pg_flo worker postgres --config /path/to/config.yaml
```

## Environment Variables

All configuration options can also be set via environment variables:

```bash
export PG_FLO_HOST=source-db.example.com
export PG_FLO_PORT=5432
export PG_FLO_DBNAME=myapp
export PG_FLO_USER=replicator
export PG_FLO_PASSWORD=secret
export PG_FLO_GROUP=production
export PG_FLO_NATS_URL=nats://localhost:4222

pg_flo replicator
```
