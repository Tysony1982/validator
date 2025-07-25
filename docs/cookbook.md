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

## Validating Files Directly

The `FileEngine` exposes one or more files as a regular SQL table backed by DuckDB.
Create the engine with a file path (globs allowed) and a table name, then run validators
like you would against any database table.

```python
from src.expectations.engines.file import FileEngine
from src.expectations.runner import ValidationRunner
from src.expectations.validators.column import ColumnNotNull

eng = FileEngine("/data/myfile.csv", table="data")
runner = ValidationRunner({"file": eng})
results = runner.run([("file", "data", ColumnNotNull(column="id"))])
```

Wildcards such as `"/data/*.parquet"` combine many files. DuckDB scans the files lazily,
so only the columns required by each validator are read into memory.
