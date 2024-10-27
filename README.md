# <img src="internal/pg_flo_logo.png" alt="pg_flo logo" width="40" align="center"> pg_flo

[![CI](https://github.com/shayonj/pg_flo/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/shayonj/pg_flo/actions/workflows/ci.yml)
[![Integration](https://github.com/shayonj/pg_flo/actions/workflows/integration.yml/badge.svg?branch=main)](https://github.com/shayonj/pg_flo/actions/workflows/integration.yml)

## Overview

`pg_flo` is the easiest way to move and transform data between PostgreSQL databases. Using PostgreSQL Logical Replication, it enables:

- Real-time Data Streaming of inserts, updates, deletes, and DDL changes in near real-time
- Fast initial data loads with parallel copy of existing data, automatic follow up by continuous replication
- Powerful transformations and filtering of data on-the-fly ([see rules](pkg/rules/README.md))
- Flexible routing to different tables and remapping of columns ([see routing](pkg/routing/README.md))
- Support for resumable streaming (deploys), DDL tracking, and more

ℹ️ `pg_flo` is in active development. The design and architecture is continuously improving. PRs/Issues are very much welcome 🙏

## Common Use Cases

- Real-time data replication between PostgreSQL databases
- ETL pipelines with data transformation
- Data re-routing, masking and filtering
- Database migration with zero downtime
- Event streaming from PostgreSQL

For detailed examples of these use cases, see our [Examples Guide](internal/examples/README.md).

## Quick setup

### Installation

```shell
go get https://github.com/shayonj/pg_flo.git
```

### Quick Start

```shell
# Start replicator
pg_flo replicator \
  --host source-db \
  --nats-url nats://localhost:4222 \
  --dbname myapp \
  --user replicator \
  --group users \
  --tables users

# Start worker
pg_flo worker postgres \
  --group users \
  --target-host dest-db \
  --nats-url nats://localhost:4222 \
```

### Configuration

Configure `pg_flo` using any of these methods:

1. CLI flags
2. YAML configuration file (default: `$HOME/.pg_flo.yaml`)
3. Environment variables

For all configuration options, see our [example configuration file](internal/pg-flo.yaml).

#### Sensitive Data

For passwords and sensitive data, use environment variables:

```shell
# Source database
export PG_FLO_PASSWORD=your_source_password
export PG_FLO_USER=your_source_user

# Target database (if using postgres sink)
export PG_FLO_TARGET_PASSWORD=your_target_password
export PG_FLO_TARGET_USER=your_target_user
```

### Groups

The `group` parameter is crucial for:

- Identifying replication processes
- Isolating replication slots and publications
- Running multiple instances on the same database
- Maintaining state for resumability
- Enabling parallel processing

Example:

```shell
# Replicate users and orders tables
pg_flo replicator --group users_orders --tables users,orders

# Replicate products table separately
pg_flo replicator --group products --tables products
```

## Architecture

pg_flo operates using two main components:

- **Replicator**: Captures changes from PostgreSQL using logical replication
- **Worker**: Processes and routes changes to destinations through NATS

For a detailed explanation of how pg_flo works internally, including:

- Publication and replication slot management
- Initial bulk copy process
- Streaming changes
- Message processing and transformation
- State management

See our [How it Works](internal/how-it-works.md) guide.

### Streaming Modes

1. **Stream Mode** (default)

   ```shell
   pg_flo replicator
   ```

2. **Copy and Stream Mode**
   ```shell
   pg_flo replicator --copy-and-stream --max-copy-workers-per-table 4
   ```

### Supported Destinations

- **stdout**: Output to console
- **file**: Write to files
- **postgres**: Replicate to another database
- **webhook**: Send to HTTP endpoints

For details, see [Supported Destinations](pkg/sinks/README.md).

## Features

### Message Routing

Route and transform data between tables:

```yaml
users:
  source_table: users
  destination_table: customers
  column_mappings:
    - source: id
      destination: customer_id
```

See [Message Routing](pkg/routing/README.md) for more.

### Transformation Rules

Apply data transformations during replication:

```shell
pg_flo worker file --rules-config /path/to/rules.yaml
```

See [Transformation Rules](pkg/rules/README.md) for available options.

## Scaling

To maintain the correct order of data changes as they occurred in the source database, it's recommended to run a single worker instance per group. If you need to replicate different tables or sets of tables independently at a faster rate, you can use the `--group` parameter to create separate groups for each table set.

**Steps to Scale Using Groups:**

1. **Define Groups**: Assign a unique group name to each set of tables you want to replicate independently using the `--group` parameter.

2. **Start a Replicator for Each Group**: Run the replicator for each group to capture changes for its respective tables.

   ```shell
   pg_flo replicator --group group_name --tables table1,table2
   ```

3. **Run a Worker for Each Group**: Start a worker for each group to process the data changes.

   ```shell
   pg_flo worker <sink_type> --group group_name [additional_flags]
   ```

**Example:**

If you have two sets of tables, `sales` and `inventory`, you can set up two groups:

- Group `sales`:

  ```shell
  pg_flo replicator --group sales --tables sales
  pg_flo worker postgres --group sales
  ```

- Group `inventory`:

  ```shell
  pg_flo replicator --group inventory --tables inventory
  pg_flo worker postgres --group inventory
  ```

## Limits and Considerations

- NATS message size limit: 8MB (configurable)
- Single worker per group recommended
- PostgreSQL logical replication prerequisites

For details, see [Limits and Considerations](#limits-and-considerations).

## Development

```shell
make build
make test
make lint

# Run end-to-end tests
./internal/scripts/e2e_local.sh
```

## Contributing

Contributions welcome! Please open an issue or submit a pull request.

## License

Apache License 2.0. See [LICENSE](LICENSE) file.
