# Cookbook

## Writing Custom Rules

You can supply your own SQL and have the runner fail when that query returns rows.

```yaml
- expectation_type: SqlErrorRows
  sql: |
    SELECT * FROM your_table WHERE bad_condition
  max_error_rows: 10  # optional
```

The validator will attach `error_row_count` and a sample of rows to the validation result.
