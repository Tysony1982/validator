# Cookbook

## Installation

Install the dependencies with:

```bash
pip install -r requirements.txt
```


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
from src.expectations.config.expectation import SLAConfig
from src.expectations.result_model import RunMetadata
from src.expectations.validators.column import ColumnNotNull

eng = FileEngine("/data/myfile.csv", table="data")
runner = ValidationRunner({"file": eng})
run = RunMetadata(suite_name="demo")
results = runner.run([("file", "data", ColumnNotNull(column="id"))], run_id=run.run_id)
```

Wildcards such as `"/data/*.parquet"` combine many files. DuckDB scans the files lazily,
so only the columns required by each validator are read into memory.

## Grouping Suites into SLAs

Multiple expectation suites can be bundled under a single SLA configuration.
Each SLA lists the suites it contains and `build_validators()` will aggregate all
validators for execution.

```yaml
sla_name: nightly_checks
suites:
  - suite_name: users_basic
    engine: duck
    table: users
    expectations:
      - expectation_type: ColumnNotNull
        column: id
  - suite_name: orders_basic
    engine: duck
    table: orders
    expectations:
      - expectation_type: ColumnNotNull
          column: order_id
  ```

## Persisting Validation Results

Validation results can be stored for later analysis using pluggable stores.
The `DuckDBResultStore` writes run metadata and results into a DuckDB
database:

```python
from src.expectations.engines.duckdb import DuckDBEngine
from src.expectations.store import DuckDBResultStore
from src.expectations.runner import ValidationRunner
from src.expectations.result_model import RunMetadata

engine = DuckDBEngine("results.db")
store = DuckDBResultStore(engine)
runner = ValidationRunner({"duck": DuckDBEngine()})
run = RunMetadata(suite_name="demo", sla_name="nightly")
results = runner.run(bindings, run_id=run.run_id)
# persist results with optional SLA configuration
sla_cfg = SLAConfig(sla_name="nightly", suites=[])
store.persist_run(run, results, sla_cfg)
```

## Collecting Table Statistics

`TableStatsCollector` computes basic metrics for every column using the same
metric builders that power the validators. Statistics can be persisted alongside
validation results and later queried to derive reasonable thresholds or SLOs.

```python
from src.expectations.stats import TableStatsCollector
from src.expectations.store import DuckDBResultStore
from src.expectations.engines.duckdb import DuckDBEngine
from src.expectations.result_model import RunMetadata

engine = DuckDBEngine()
store = DuckDBResultStore(engine)
collector = TableStatsCollector({"duck": engine})

run = RunMetadata(suite_name="stats_demo")
stats = collector.collect("duck", "orders", run_id=run.run_id)
store.persist_stats(run, stats)
```

Persisted statistics are indexed by engine, schema, table and column which makes
looking up historical ranges for a given column trivial.
