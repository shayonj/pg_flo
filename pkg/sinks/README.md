# Supported Sinks in pg_flo

pg_flo supports various sink types (destinations) for streaming data changes. This document provides an overview of the supported sinks and how to use them via the command-line interface.

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

The PostgreSQL sink streams changes to another PostgreSQL database.

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

### Behavior

- The Webhook sink sends each change as a separate HTTP POST request to the specified URL.
- It includes a custom User-Agent header: "pg_flo/1.0".
- In case of failure, it will retry the request up to 3 times before giving up.
- The payload is sent as JSON in the request body.

### Error Handling

- If a non-2xx status code is received, the sink will retry the request up to 3 times.
- If all retries fail, an error will be logged, and the replication process will stop.

## Copy and Stream Mode

pg_flo also supports a "copy and stream" mode, which performs an initial parallelizable bulk copy of the data before starting the streaming process without any data loss or duplication. This mode is available for all sink types.

### Usage

Replace `stream` with `copy-and-stream` in the commands above.

### Additional Flags

- `--max-copy-workers`: Number of parallel connections for copy and stream mode (default: 4)

### Example (File Sink with Copy and Stream)

```shell
pg_flo copy-and-stream file \
  --host localhost \
  --port 5432 \
  --dbname your_database \
  --user your_user \
  --password your_password \
  --group your_group \
  --tables table1,table2 \
  --schema public \
  --status-dir /tmp/pg_flo-status \
  --output-dir /tmp/pg_flo-output \
  --max-copy-workers 8
```
