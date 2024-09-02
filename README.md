# 🌊 pg_flo

`pg_flo` is the easiest way to move and transform data from PostgreSQL. It users PostgreSQL Logical Replication to stream inserts, updates, deletes, and DDL changes to multiple destinations. With support for parallelizable bulk copy, near real-time streaming, and powerful transformation and filtering rules, `pg_flo` simplifies data sync and ETL processes.

⚠️ CURRENTLY UNDER ACTIVE DEVELOPMENT. ACCEPTING FEEDBACK/ISSUES/PULL REQUESTS 🚀

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
  - [Basic Usage](#basic-usage)
  - [Configuration](#configuration)
  - [Environment Variables](#environment-variables)
- [Examples](#examples)
  - [Group](#group)
  - [Example 1: Basic streaming of changes to STDOUT](#example-1-basic-streaming-of-changes-to-stdout)
  - [Example 2: Using Configuration File](#example-2-using-configuration-file)
  - [Example 3: With Transformation and Filtering Rules](#example-3-with-transformation-and-filtering-rules)
    - [Transformation Rules](#transformation-rules)
    - [Filtering Rules](#filtering-rules)
    - [Rule Properties](#rule-properties)
- [Supported Sinks](#supported-sinks)
- [Development](#development)
  - [End-to-End Tests](#end-to-end-tests)
- [Contributing](#contributing)

## Features

- Stream data changes from PostgreSQL to multiple destinations.
- Type aware transformation and filtering rules so only filtered and transformed data reaches the destination.
- Supports tracking DDL changes.
- Configurable via command-line flags or environment variables.
- Supports copy and stream mode to parallelize bulk copy and stream changes.

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

You can configure `pg_flo` using a YAML configuration file or environment variables. By default, `pg_flo` looks for a configuration file at `$HOME/.pg_flo.yaml`. You can reference the example configuration file at [internal/pg-flo.yaml](internal/pg-flo.yaml)

### Environment Variables

`pg_flo` also supports environment variables for configuration:

- `PGHOST`
- `PGPORT`
- `PGDATABASE`
- `PGUSER`
- `PGPASSWORD`

## Examples

### Group

`--group`: This parameter is used to identify each replication process which can contain one or more or all tables. It allows you to run multiple instances of `pg_flo` on the same database or across different databases without conflicts. The group name is used to isolate replication slots and publications in PostgreSQL, and can also be used to for some internal state keeping for resumability sake.

### Example 1: Basic streaming of changes to STDOUT

````shell
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
````

### Example 2: Using Configuration File

```shell
pg_flo stream stdout --config /path/to/pg_flo.yaml
```

### Example 3: With Transformation and Filtering Rules

#### Transformation Rules

- **Mask**: Replace characters in a column with a specified mask character.
- **Regex**: Apply a regular expression pattern to transform column values.

#### Filtering Rules

- **Comparison Operators**: Filter rows based on column values or operation types (e.g., greater than or equal to). Only operations that match these the filtering rules will be emitted to the destination.

#### Rule Properties

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

## Supported Sinks

`pg_flo` supports various sink types (destinations) for streaming data changes. You can read more about the supported Sinks and the interface [here](pkg/sinks/README.md).

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
