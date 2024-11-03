# <img src="internal/pg_flo_logo.png" alt="pg_flo logo" width="40" align="center"> pg_flo

[![CI](https://github.com/shayonj/pg_flo/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/shayonj/pg_flo/actions/workflows/ci.yml)
[![Integration](https://github.com/shayonj/pg_flo/actions/workflows/integration.yml/badge.svg?branch=main)](https://github.com/shayonj/pg_flo/actions/workflows/integration.yml)
[![Release](https://img.shields.io/github/v/release/shayonj/pg_flo?sort=semver)](https://github.com/shayonj/pg_flo/releases/latest)
[![Docker Image](https://img.shields.io/docker/v/shayonj/pg_flo?label=docker&sort=semver)](https://hub.docker.com/r/shayonj/pg_flo/tags)

## Table of Contents

- [Overview](#overview)
- [Common Use Cases](#common-use-cases)
- [Quick Setup](#quick-setup)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Configuration](#configuration)
  - [Quick Start](#quick-start)
- [Architecture](#architecture)
  - [Streaming Modes](#streaming-modes)
  - [Supported Destinations](#supported-destinations)
- [Features](#features)
  - [Message Routing](#message-routing)
  - [Transformation Rules](#transformation-rules)
- [Scaling](#scaling)
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)

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

## Quick Setup

### Prerequisites

- Docker
- PostgreSQL database with logical replication enabled (`wal_level=logical`)

### Installation

The easiest way to run pg_flo is using Docker:

```shell
docker pull shayonj/pg_flo:latest
```

### Configuration

pg_flo can be configured using any of these methods:

1. Environment variables
2. YAML configuration file
3. CLI flags

For a complete list of configuration options, see our [example configuration file](internal/pg-flo.yaml).

### Quick Start

Here's a minimal example to get started:

```shell
# Start NATS server with JetStream configuration
docker run -d --name pg_flo_nats \
  --network host \
  -v /path/to/nats-server.conf:/etc/nats/nats-server.conf \
  nats:latest \
  -c /etc/nats/nats-server.conf

# Start replicator with config file
docker run -d --name pg_flo_replicator \
  --network host \
  -v /path/to/config.yaml:/etc/pg_flo/config.yaml \
  shayonj/pg_flo:latest \
  replicator --config /etc/pg_flo/config.yaml

# Or start replicator with environment variables
docker run -d --name pg_flo_replicator \
  --network host \
  -e PG_FLO_HOST=localhost \
  -e PG_FLO_DBNAME=myapp \
  -e PG_FLO_USER=replicator \
  -e PG_FLO_PASSWORD=secret \
  -e PG_FLO_GROUP=users \
  -e PG_FLO_TABLES=users \
  shayonj/pg_flo:latest \
  replicator --nats-url nats://localhost:4222

# Start worker with postgres as the destination
docker run -d --name pg_flo_worker \
  --network host \
  -v /path/to/config.yaml:/etc/pg_flo/config.yaml \
  shayonj/pg_flo:latest \
  worker postgres --config /etc/pg_flo/config.yaml
```

> **Note**:
>
> - pg_flo needs network access to both PostgreSQL and NATS
> - The examples above use `--network host` for local development
> - For production, we recommend using proper container networking (Docker networks, Kubernetes, etc.)
> - NATS server must be configured with JetStream enabled and appropriate message size limits
> - See our [example configuration](internal/pg-flo.yaml) for all available options for setting up pg_flo
> - See our [example NATS configuration](internal/nats-server.conf) for NATS server settings

### Example Configuration File

Create a `config.yaml` file:

```yaml
# Replicator settings
host: "localhost"
port: 5432
dbname: "myapp"
user: "replicator"
password: "secret"
group: "users"
tables:
  - "users"

# Worker settings (for postgres sink)
target-host: "dest-db"
target-dbname: "myapp"
target-user: "writer"
target-password: "secret"

# Common settings
nats-url: "nats://localhost:4222"
```

For a complete list of configuration options, see our [example configuration file](internal/pg-flo.yaml).

The rest of this documentation uses `pg_flo` commands directly for clarity. When running via Docker, simply prefix the commands with `docker run shayonj/pg_flo:latest`.

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
