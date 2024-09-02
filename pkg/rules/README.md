## Transformation Rules

There are two types of transformation rules available:

1. **Regex Transform**

   - Type: `"regex"`
   - Parameters:
     - `pattern`: The regular expression pattern to match
     - `replace`: The replacement string
   - Description: Applies a regular expression replacement on string values in the specified column.

2. **Mask Transform**
   - Type: `"mask"`
   - Parameters:
     - `mask_char`: The character to use for masking
   - Description: Masks the content of string values, keeping the first and last characters visible and replacing the rest with the specified mask character.

## Filtering Rules

Filtering rules use various comparison operators to determine whether a row should be included in the output. The available operators are:

1. **Equality** (`"eq"`)
2. **Inequality** (`"ne"`)
3. **Greater Than** (`"gt"`)
4. **Less Than** (`"lt"`)
5. **Greater Than or Equal To** (`"gte"`)
6. **Less Than or Equal To** (`"lte"`)
7. **Contains** (`"contains"`)

## Rule Properties

Both transformation and filtering rules share these common properties:

- `type`: Specifies whether it's a "transform" or "filter" rule.
- `column`: The name of the column to apply the rule to.
- `operations`: An array of operations (INSERT, UPDATE, DELETE) to which the rule should be applied. If not specified, it applies to all operations.
- `allow_empty_deletes`: A boolean flag that, when set to true, allows the rule to process delete operations even if the column value is empty.

## Additional Notes

- The rules support various data types, including integers, floats, strings, timestamps, booleans, and numeric (decimal) values.
- For filtering rules, the comparison is type-aware, ensuring that values are compared appropriately based on their data type.
- The `contains` operator for filtering only works on string values.
- Transformation rules currently only work on string values. If a non-string value is encountered, the transformation is skipped and a warning is logged.

To use these rules, you would define them in a YAML configuration file and specify the path to this file using the `--rules-config` flag when running `pg_flo`. The exact structure of the YAML file should match the rule properties and parameters described above.
