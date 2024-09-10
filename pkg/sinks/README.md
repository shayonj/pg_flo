# Supported Sinks in pg_flo

pg_flo supports various sink types (destinations) for streaming data changes. This document provides an overview of the supported sinks and how to use them via the command-line interface.

- [Available Sinks](#available-sinks)
- [Common Flags](#common-flags)
- [STDOUT Sink](#stdout-sink)
  - [Usage](#usage)
  - [Example](#example)
- [File Sink](#file-sink)
  - [Usage](#usage-1)
  - [Additional Flags](#additional-flags)
  - [Example](#example-1)
- [PostgreSQL Sink](#postgresql-sink)
  - [Usage](#usage-2)
  - [Additional Flags](#additional-flags-1)
  - [Example](#example-2)
  - [Additional Behavior](#additional-behavior)
- [Webhook Sink](#webhook-sink)
  - [Usage](#usage-3)
  - [Additional Flags](#additional-flags-2)
  - [Example](#example-3)
  - [Additional Behavior](#additional-behavior-1)
- [Sink Interface](#sink-interface)

## Available Sinks

1. STDOUT
2. File
3. PostgreSQL
4. Webhook

## Common Flags

These flags are common to all sink types:

- `--host`: PostgreSQL source host
- `--port`: PostgreSQL source port
- `--dbname`: PostgreSQL source database name
- `--user`: PostgreSQL source user
- `--password`: PostgreSQL source password
- `--group`: Group name for replication
- `--tables`: Tables to replicate (comma-separated)
- `--status-dir`: Directory to store status files

## STDOUT Sink

The STDOUT sink writes changes directly to the console output.

### Usage

```shell
pg_flo stream stdout [common flags]
```

### Example

```shell
pg_flo stream stdout \
  --host localhost \
  --port 5432 \
  --dbname your_database \
  --user your_user \
  --password your_password \
  --group your_group \
  --tables table1,table2 \
  --status-dir /tmp/pg_flo-status
```

## File Sink

The File sink writes changes to files in the specified output directory.

### Usage

```shell
pg_flo stream file [common flags] --output-dir <output_directory>
```

### Additional Flags

- `--output-dir`: Output directory for file sink

### Example

```shell
pg_flo stream file \
  --host localhost \
  --port 5432 \
  --dbname your_database \
  --user your_user \
  --password your_password \
  --group your_group \
  --tables table1,table2 \
  --status-dir /tmp/pg_flo-status \
  --output-dir /tmp/pg_flo-output
```

## PostgreSQL Sink

The PostgreSQL sink replicates changes to another PostgreSQL database. To ensure accurate replication of updates and deletes, all tables must have a primary key defined.

### Usage

```shell
pg_flo stream postgres [common flags] [postgres sink flags]
```

### Additional Flags

- `--target-host`: Target PostgreSQL host
- `--target-port`: Target PostgreSQL port
- `--target-dbname`: Target PostgreSQL database name
- `--target-user`: Target PostgreSQL user
- `--target-password`: Target PostgreSQL password
- `--sync-schema`: Sync schema from source to target via `pg_dump` (boolean flag)

### Example

```shell
pg_flo stream postgres \
  --host localhost \
  --port 5432 \
  --dbname source_db \
  --user source_user \
  --password source_password \
  --group replication_group \
  --tables table1,table2 \
  --schema public \
  --status-dir /tmp/pg_flo-status \
  --target-host target.host.com \
  --target-port 5433 \
  --target-dbname target_db \
  --target-user target_user \
  --target-password target_password \
  --sync-schema
```

### Additional Behavior

- Supports schema synchronization between source and target databases using `pg_dump` when the `--sync-schema` flag is set.
- Creates an `internal_pg_flo` schema and `lsn_status` table to keep track of the last processed LSN.
- Handles `INSERT`, `UPDATE`, `DELETE`, and `DDL` operations.
- Uses `UPSERT` (`INSERT ... ON CONFLICT DO UPDATE`) for handling both `INSERT` and `UPDATE` operations efficiently.
- Executes operations within a transaction for each batch of changes.
- Rolls back the transaction and logs an error if any operation in the batch fails.

## Webhook Sink

The Webhook sink sends changes as HTTP POST requests to a specified URL.

### Usage

```shell
pg_flo stream webhook [common flags] --webhook-url <webhook_url>
```

### Additional Flags

- `--webhook-url`: URL to send webhook POST requests

### Example

```shell
pg_flo stream webhook \
  --host localhost \
  --port 5432 \
  --dbname your_database \
  --user your_user \
  --password your_password \
  --group your_group \
  --tables table1,table2 \
  --schema public \
  --status-dir /tmp/pg_flo-status \
  --webhook-url https://your-webhook-endpoint.com/receive
```

### Additional Behavior

- Sends each change as a separate HTTP POST request to the specified webhook URL.
- Implements a retry mechanism with up to 3 attempts for failed requests.
- Considers both network errors and non-2xx status codes as failures that trigger retries.
- Maintains a status file to keep track of the last processed LSN.
- The status file is stored in the specified status directory with the name `pg_flo_webhook_last_lsn.json`.

## Sink Interface

`pg_flo` uses a common interface for all sink types, allowing for easy implementation of new sinks. The `Sink` interface defines the following methods:

- `WriteBatch(data []interface{}) error`: Writes a batch of changes to the sink.
- `Close() error`: Closes the sink, releasing any resources or connections.

Sinks can save the last processed `LSN` at the destination (as appropriate). This ensures that if a `pg_flo` process shuts down (for example, during a deployment) and starts again, it knows where to resume from.
