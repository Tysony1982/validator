Is a Streamlit front-end a good fit? ✅
====================================

Yes – two complementary use-cases:

Explorer / dashboard for validation history
------------------------------------------
Streamlit excels at quick dashboards. You can either query the ``/runs`` and ``/runs/{id}`` endpoints from the service or, if the app runs on the same host, attach directly to the DuckDB result store. Typical widgets include a dropdown for the suite name, multi-select filters for severity and a time-range slider. Results can be rendered with ``st.dataframe`` and trend lines with ``altair`` or ``matplotlib``.

Interactive suite builder / editor
---------------------------------
Use a YAML text editor via ``st.text_area`` with syntax highlighting (for example using ``streamlit-ace``) or a form based wizard for non-expert users. The wizard might let the user select a table, pick a validator type and set parameters with a live preview of the YAML. Hitting "Save" should call ``POST /suites`` and provide immediate feedback via a toast notification.

Architectural sketch
--------------------

.. mermaid::

    graph LR
        browser((User))
        subgraph Front-end
            streamlit(Streamlit app)
        end
        subgraph API layer
            fastapi(FastAPI service\nValidator Service)
        end
        db[(DuckDB / JSONL\nResultStore)]
        fs[(Suite YAML dir)]

        browser --> streamlit
        streamlit -->|REST: list runs, create suite, trigger run| fastapi
        fastapi --> db
        fastapi --> fs
        streamlit -->|read-only DuckDB (optional)| db

Why keep both?
--------------
FastAPI remains the programmatic interface for pipelines or CI, while Streamlit acts purely as a thin user interface layer. If Streamlit goes down, validations still run.

Implementation tips
-------------------

.. list-table::
   :header-rows: 1

   * - Need
     - Streamlit snippet
   * - Auto-refresh run table every ``N`` seconds
     - ``st_autorefresh(interval=30_000)``
   * - Large tables
     - ``st.dataframe(df, use_container_width=True)`` with DuckDB's ``to_arrow()`` result
   * - Code editor
     - ``from streamlit_ace import st_ace`` – returns YAML string
   * - Form ↔ Pydantic validation
     - ``ExpectationSuiteConfig.model_validate_yaml(text)`` to catch errors client-side

Running the app
---------------
First start the FastAPI service which exposes suite management and run endpoints. A small example using the DuckDB result store::

    from validator.service import Service, SuiteStore
    from src.expectations.engines.duckdb import DuckDBEngine
    from src.expectations.runner import ValidationRunner
    from src.expectations.store import DuckDBResultStore

    runner = ValidationRunner({"duck": DuckDBEngine("example.db")})
    store = DuckDBResultStore(DuckDBEngine("results.db"))
    service = Service(runner, store, SuiteStore("suites"))
    app = service.app

Launch the API with ``uvicorn``:

.. code-block:: bash

    uvicorn service_app:app --reload

With the service running, open the Streamlit UI:

.. code-block:: bash

    SERVICE_URL=http://localhost:8000 \
    RESULT_DB=results.db \
    streamlit run src/service/streamlit_app.py

``SERVICE_URL`` points to the service base URL while ``RESULT_DB`` allows the UI
to read the DuckDB history directly.
