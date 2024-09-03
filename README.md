# 🌊 pg_flo

## ![](internal/demo.gif)

[![CI](https://github.com/shayonj/pg_flo/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/shayonj/pg_flo/actions/workflows/ci.yml)

`pg_flo` is the easiest way to move and transform data from PostgreSQL. It users PostgreSQL Logical Replication to stream inserts, updates, deletes, and DDL changes to multiple destinations. With support for parallelizable bulk copy, near real-time streaming, and powerful transformation and filtering rules, `pg_flo` simplifies data sync and ETL processes.

⚠️ CURRENTLY UNDER ACTIVE DEVELOPMENT. ACCEPTING FEEDBACK/ISSUES/PULL REQUESTS 🚀

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
  - [Basic Usage](#basic-usage)
  - [Configuration](#configuration)
  - [Group](#group)
- [Streaming Modes](#streaming-modes)
  - [Stream Mode](#stream-mode)
    - [Usage](#usage-1)
  - [Copy and Stream Mode](#copy-and-stream-mode)
    - [Usage](#usage-2)
    - [Additional Flags](#additional-flags)
  - [Examples](#examples)
- [Transformation and Filtering Rules](#transformation-and-filtering-rules)
  - [Transformation Rules](#transformation-rules)
  - [Filtering Rules](#filtering-rules)
  - [Rule Properties](#rule-properties)
- [Supported Destinations](#supported-destinations)
- [How it Works](#how-it-works)
- [Development](#development)
  - [End-to-End Tests](#end-to-end-tests)
- [Contributing](#contributing)
- [License](#license)

## Features

- Stream data changes (`INSERT`/`UPDATE`/`DELETE`) from PostgreSQL to multiple destinations.
- Type aware transformation and filtering rules so only filtered and transformed data reaches the destination.
- Supports tracking DDL changes (`ALTER`, `CREATE INDEX`, `DROP INDEX`).
- Configurable via command-line flags or environment variables.
- Supports copy and stream mode to parallelize bulk copy and stream changes.
- Resumable streaming from last `lsn` position.

I invite you to take a look through [issues](https://github.com/shayonj/pg_flo/issues) to see what's coming next 🤗.

## Installation

To install `pg_flo`, clone the repository and build the binary:

```shell
go get https://github.com/your-repo/pg_flo.git
```

## Usage

### Basic Usage

To start streaming to `STDOUT`, use `stream` command:

```shell
pg_flo stream stdout \
  --host localhost \
  --port 5432 \
  --dbname your_database \
  --user your_user \
  --password your_password \
  --group your_group \
  --schema public \
  --tables table1,table2
```

### Configuration

You can configure `pg_flo` using CLI flags, a YAML configuration file or environment variables. By default, `pg_flo` looks for a configuration file at `$HOME/.pg_flo.yaml`. You can reference the example configuration file at [internal/pg-flo.yaml](internal/pg-flo.yaml)

### Group

`--group`: This parameter is used to identify each replication process which can contain one or more or all tables. It allows you to run multiple instances of `pg_flo` on the same database or across different databases without conflicts. The group name is used to isolate replication slots and publications in PostgreSQL, and can also be used to for some internal state keeping for resumability sake.

## Streaming Modes

`pg_flo` supports two modes of operation: `stream` mode and `copy-and-stream` mode.

### Stream Mode

With Stream mode `pg_flo` continuously streams changes from the source PostgreSQL database to the specified sink, it will start streaming changes from the last known WAL Position.

#### Usage

```shell
pg_flo stream <sink_type> [flags]
```

### Copy and Stream Mode

Copy-and-stream mode performs an initial parallelizable bulk copy of the existing data before starting the streaming process (without data loss or duplication). This ensures that the destination has a complete copy of the data without any loss or duplication.

#### Usage

```shell
pg_flo copy-and-stream <sink_type> [flags]
```

#### Additional Flags

- `--max-copy-workers`: Number of parallel connections for the copy operation (default: 4)

### Examples

1. Stream mode with STDOUT sink:

```shell
pg_flo stream stdout \
  --host localhost \
  --port 5432 \
  --dbname your_database \
  --user your_user \
  --password your_password \
  --group your_group \
  --schema public \
  --tables table1,table2
```

2. Copy-and-stream mode with File sink:

```shell
pg_flo copy-and-stream file \
  --host localhost \
  --port 5432 \
  --dbname your_database \
  --user your_user \
  --password your_password \
  --group your_group \
  --schema public \
  --tables table1,table2 \
  --output-dir /tmp/pg_flo-output \
  --max-copy-workers 8
```

Both modes are available for all supported [sink types](pkg/sinks/README.md), and any configured rules or transformations are applied to operations in both modes.

## Transformation and Filtering Rules

With pg_flo, you can apply powerful transformation and filtering rules to your data streams. These rules allow you to modify data on-the-fly (transformation) or selectively process only certain records (filtering) based on specified conditions.

### Transformation Rules

- **Mask**: Replace characters in a column with a specified mask character.
- **Regex**: Apply a regular expression pattern to transform column values.

### Filtering Rules

- **Comparison Operators**: Filter rows based on column values or operation types (e.g., greater than or equal to). Only operations that match these the filtering rules will be emitted to the destination.

### Rule Properties

- `type`: The type of rule (transform or filter).
- `column`: The column to apply the rule to.
- `parameters`: Specific parameters for the rule type.
- `operations`: The database operations to apply the rule to (INSERT, UPDATE, DELETE).
- `allow_empty_deletes`: Allow the rule to process delete operations even if the column value is empty.

You can read more about available transformation and filtering rules [here](pkg/rules/README.md).

To use these rules, specify the path to your rules configuration file using the `--rules-config` flag when running `pg_flo`. You can also read the example file at [internal/rules.yml](internal/rules.yml)

```shell
pg_flo stream file \
  --host localhost \
  --port 5432 \
  --dbname your_database \
  --user your_user \
  --password your_password \
  --group your_group \
  --tables table1,table2 \
  --schema public \
  --output-dir /tmp/pg_flo-output \
  --rules-config /path/to/rules-config.yaml
```

## Supported Destinations

`pg_flo` supports various sink types (destinations) for streaming data changes. You can read more about the supported Sinks with examples and the interface [here](pkg/sinks/README.md).

## How it Works

You can read about how the tool works briefly here [here](internal/how-it-works.md).

## Development

`pg_flo` uses a Makefile to simplify common development tasks. Here are the available commands:

- `make build`
- `make test`
- `make lint`

### End-to-End Tests

For running end-to-end tests, use the provided script:

```shell
./internal/e2e_test_local.sh
```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request on GitHub.

## License

`pg_flo` is licensed under Elastic License 2.0 (ELv2). Please see the LICENSE file for additional information.
