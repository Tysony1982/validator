# Validator

Lightweight data validation utilities powered by DuckDB and sqlglot.

## Concurrency Model

Metric registration and execution engines are safe to use from multiple threads.
`MetricRegistry` is implemented as a process local singleton protected by an
`RLock` so concurrent registrations will not corrupt the registry. DuckDB
connections are pooled inside `DuckDBEngine` which hands out a dedicated
connection per statement. Each process maintains its own registry and pool so it
is safe to run tests under `pytest -n auto` or spawn threads in your application.

