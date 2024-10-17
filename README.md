# 🌊 pg_flo

-## ![](internal/demo.gif)

[![CI](https://github.com/shayonj/pg_flo/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/shayonj/pg_flo/actions/workflows/ci.yml)
[![Integration  ](https://github.com/shayonj/pg_flo/actions/workflows/integration.yml/badge.svg?branch=main)](https://github.com/shayonj/pg_flo/actions/workflows/integration.yml)

`pg_flo` is the easiest way to move and transform data between PostgreSQL databases. It uses PostgreSQL Logical Replication to stream inserts, updates, deletes, and DDL changes to multiple destinations. With support for parallelizable bulk copy, near real-time streaming, and powerful transformation and filtering rules, `pg_flo` simplifies data sync and ETL processes.

⚠️ CURRENTLY UNDER ACTIVE DEVELOPMENT. ACCEPTING FEEDBACK/ISSUES/PULL REQUESTS 🚀

## Features

- Stream data changes in near real-time
- Parallelizable bulk copy for initial data sync
- Powerful transformation and filtering rules:
  - Regex-based transformations for string data
  - Masking sensitive information
  - Filtering based on column values using various comparison operators
  - Support for multiple data types (integers, floats, strings, timestamps and booleans)
  - Ability to apply rules selectively to INSERT, UPDATE, or DELETE operations
- Flexible data routing:
  - 🔜 Re-route data to different tables (as long as the schema matches)
  - 🔜 Ability to split or duplicate data streams based on custom logic
- Support for DDL change tracking
- Resumable streaming from last position
- Multiple destination support

I invite you to take a look through [issues](https://github.com/shayonj/pg_flo/issues) to see what's coming next 🤗.

## Quick Start

1. Install `pg_flo`:

```shell
go get https://github.com/shayonj/pg_flo.git
```

2. Create a configuration file (e.g., `pg_flo.yaml`) based on the [example configuration](internal/pg-flo.yaml).

   Note: All configuration options can also be set using environment variables. See the example configuration file for the corresponding environment variable names.

3. Start the replicator:

```shell
pg_flo replicator --config /path/to/pg_flo.yaml
```

4. Create a rules configuration file (e.g., `rules.yaml`):

```yaml
- type: transform
  column: email
  parameters:
    type: mask
    mask_char: "*"
  operations: [INSERT, UPDATE]

- type: filter
  column: age
  parameters:
    operator: gte
    value: 18
  operations: [INSERT, UPDATE, DELETE]
```

5. Start the worker with rules:

```shell
pg_flo worker stdout --config /path/to/pg_flo.yaml --rules-config /path/to/rules.yaml
```

This setup will start a replicator that captures changes from PostgreSQL and publishes them to NATS, and a worker that applies the specified rules before outputting the data to stdout.

## Usage

### Configuration

You can configure `pg_flo` using CLI flags, a YAML configuration file, or environment variables. By default, `pg_flo` looks for a configuration file at `$HOME/.pg_flo.yaml`. You can reference the example configuration file at [internal/pg-flo.yaml](internal/pg-flo.yaml).

### Group

The `--group` parameter is used to identify each replication process, which can contain one or more tables. It allows you to run multiple instances of `pg_flo` on the same database or across different databases without conflicts. The group name is used to isolate replication slots and publications in PostgreSQL and for internal state keeping for resumability.

## Streaming Modes

`pg_flo` supports two modes of operation: **Stream Mode** and **Copy and Stream Mode**.

### Stream Mode

In Stream mode, `pg_flo` continuously streams changes from the source PostgreSQL database to NATS, starting from the last known WAL position.

#### Running the Replicator in Stream Mode

```shell
pg_flo replicator --config /path/to/pg_flo.yaml
```

### Copy and Stream Mode

Copy-and-stream mode performs an initial parallelizable bulk copy of the existing data before starting the streaming process (without data loss or duplication). This ensures that the destination has a complete copy of the data.

#### Running the Replicator in Copy and Stream Mode

```shell
pg_flo replicator --config /path/to/pg_flo.yaml --copy-and-stream --max-copy-workers-per-table 4
```

### Running the Worker

The worker command remains the same for both modes since it processes messages from NATS.

```shell
pg_flo worker <sink_type> --config /path/to/pg_flo.yaml [additional_flags]
```

## Transformation and Filtering Rules

`pg_flo` supports powerful transformation and filtering rules that allow you to modify data on-the-fly or selectively process certain records before they reach the destination. These rules can be applied to various data types and operations.

To use these rules, create a YAML configuration file and specify its path using the `--rules-config` flag when running the worker:

```shell
pg_flo worker file \
  --config /path/to/pg_flo.yaml \
  --rules-config /path/to/rules-config.yaml
```

For detailed information about available transformation and filtering rules, their properties, and usage examples, please refer to the [rules README file](pkg/rules/README.md).

An example rules configuration file can be found at [internal/rules.yml](internal/rules.yml).

## Supported Destinations

`pg_flo` supports various sink types (destinations) for streaming data changes, including:

- **stdout**: Outputs data to standard output.
- **file**: Writes data changes to files.
- **postgres**: Replicates data to another PostgreSQL database.

Examples:

1. Using stdout sink:

```shell
pg_flo worker stdout --config /path/to/pg_flo.yaml
```

2. Using PostgreSQL sink:

```shell
pg_flo worker postgres --config /path/to/pg_flo.yaml
```

You can read more about the supported sinks with examples and the interface [here](pkg/sinks/README.md).

## How it Works

You can read about how the tool works briefly [here](internal/how-it-works.md).

## Development

`pg_flo` uses a Makefile to simplify common development tasks. Here are the available commands:

- `make build`
- `make test`
- `make lint`

### End-to-End Tests

For running end-to-end tests, use the provided script:

```shell
./internal/e2e_local.sh
```

This script orchestrates multiple integration tests covering scenarios like data replication, handling of DDL changes, resume functionality, transformations, and filtering.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request on GitHub.

## License

`pg_flo` is licensed under Elastic License 2.0 (ELv2). Please see the LICENSE file for additional information.
