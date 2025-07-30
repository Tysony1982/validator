# Validator

Lightweight data validation utilities powered by DuckDB and sqlglot.

## Concurrency Model

Metric registration and execution engines are safe to use from multiple threads.
`MetricRegistry` is implemented as a process local singleton protected by an
`RLock` so concurrent registrations will not corrupt the registry. DuckDB
connections are pooled inside `DuckDBEngine` which hands out a dedicated
connection per statement. The pool size defaults to one connection but can be
increased via the ``pool_size`` argument to accommodate concurrent queries.
Each process maintains its own registry and pool so it is safe to run tests
under `pytest -n auto` or spawn threads in your application.


## Documentation

Install development requirements including Sphinx and run:

```bash
make docs
```

This will generate HTML documentation under `docs/_build`. In particular
`docs/validators.html` lists all available validator classes with their
signatures and docstrings.

## Service Module

The optional ``validator.service`` package exposes a small FastAPI application
that lets you manage expectation suites and trigger runs through HTTP. Results
are read from the configured ``BaseResultStore`` so any of the included stores
can be plugged in. The service is intended as a foundation for building a
custom UI or automation around validation workflows.
