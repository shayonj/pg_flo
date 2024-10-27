# Message Routing

Table routing allows you to map source tables and columns to different destinations while preserving data types.

## Configuration

Create a YAML file (e.g., `routing.yaml`) with your routing rules:

```yaml
users:
  source_table: users
  destination_table: customers
  column_mappings:
    - source: id
      destination: customer_id
    - source: username
      destination: customer_name
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
  operations:
    - INSERT
    - UPDATE
    - DELETE
```

## Usage with Routing

Start the worker with the routing configuration:

```shell
pg_flo worker postgres --routing-config routing.yaml ...
```

## Routing Rules

Each table configuration supports:

- `source_table`: Original table name (required)
- `destination_table`: Target table name (optional, defaults to source_table)
- `column_mappings`: List of column name mappings (optional)
  - `source`: Original column name
  - `destination`: New column name in target
- `operations`: List of operations to replicate (required)
  - Supported: `INSERT`, `UPDATE`, `DELETE`

## Important Notes

- Column data types must match between source and destination
- Primary keys are automatically mapped
- All specified columns must exist in both tables
- Operations not listed in `operations` will be ignored. Defaults to all operations.
- Unlisted columns are preserved with their original names
- Complex types (jsonb, arrays) are preserved during mapping

## Examples

### Basic Table Mapping

```yaml
users:
  source_table: users
  destination_table: customers
  operations:
    - INSERT
    - UPDATE
```

### Column Remapping

```yaml
products:
  source_table: products
  destination_table: items
  column_mappings:
    - source: id
      destination: item_id
    - source: name
      destination: item_name
  operations:
    - INSERT
    - UPDATE
    - DELETE
```
