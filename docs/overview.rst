Validator Overview
==================

Validator provides lightweight data validation powered by DuckDB and
sqlglot.  It offers reusable building blocks for composing validation
workflows:

- :doc:`validators` define the individual checks.
- :doc:`engines` connect to data sources.
- :doc:`runner` orchestrates executions across engines.
- :doc:`stores` persist run results and optional statistics.
- A FastAPI service with an optional Streamlit UI exposes validations over
  HTTP (:doc:`streamlit`).

Quickstart
----------

1. Install dependencies::

      pip install -r requirements.txt

2. Wire up the service:

   .. code-block:: python

      from validator.service import Service, SuiteStore
      from src.expectations.runner import ValidationRunner
      from src.expectations.engines.duckdb import DuckDBEngine
      from src.expectations.store import DuckDBResultStore

      runner = ValidationRunner({"duck": DuckDBEngine("example.db")})
      store = DuckDBResultStore(DuckDBEngine("results.db"))
      service = Service(runner, store, SuiteStore("suites"))
      app = service.app

3. Launch the API::

      uvicorn service_app:app --reload

4. Start the Streamlit UI::

      SERVICE_URL=http://localhost:8000 RESULT_DB=results.db \
          streamlit run src/service/streamlit_app.py

For more detailed examples, consult the :doc:`cookbook`.
