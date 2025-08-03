# Validator

Lightweight data validation utilities powered by DuckDB and sqlglot.

## Quickstart

Install the Python dependencies:

```bash
pip install -r requirements.txt
```

Create a small `service_app.py` to start the API:

```python
from validator.service import Service, SuiteStore
from src.expectations.runner import ValidationRunner
from src.expectations.engines.duckdb import DuckDBEngine
from src.expectations.store import DuckDBResultStore

runner = ValidationRunner({"duck": DuckDBEngine("example.db")})
store = DuckDBResultStore(DuckDBEngine("results.db"))
service = Service(runner, store, SuiteStore("suites"))
app = service.app
```

Run the service with `uvicorn`:

```bash
uvicorn service_app:app --reload
```

Start the Streamlit UI in another terminal:

```bash
SERVICE_URL=http://localhost:8000 RESULT_DB=results.db \
    streamlit run src/service/streamlit_app.py
```


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

## Streamlit Front-end

A lightweight Streamlit app can serve as the user facing layer on top of the
service. Two common scenarios are:

1. **Explorer / dashboard for validation history** – query the ``/runs`` and
   ``/runs/{id}`` endpoints or attach directly to the DuckDB result store to
   render tables and trend charts.
2. **Interactive suite builder / editor** – use ``st.text_area`` or a form
   wizard to compose expectation suites and ``POST /suites`` to save them.

Streamlit does not replace the FastAPI service. It only provides a thin UX
layer, so validations continue to run even if the app is offline.

## Reconciliation

Built-in validators can reconcile data between a primary source and a
comparer engine.  Start with a row-count check and then drill into
individual columns.

Example configuration::

    expectations:
      - expectation_type: TableReconciliationValidator
        comparer_engine: warehouse
        comparer_table: users_copy
      - expectation_type: ColumnReconciliationValidator
        column_map:
          primary: id
        primary_engine: duck
        primary_table: users
        comparer_engine: warehouse
        comparer_table: users_copy

Best practices:

* Run the table validator first to detect large discrepancies early.
* Apply matching ``where`` clauses on both engines when filtering data.
* Use :class:`ColumnMapping` to handle renamed columns or type conversions.
