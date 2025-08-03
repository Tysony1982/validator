Result Stores
=============

Validator separates validation execution from persistence using a small
:class:`~src.expectations.store.base.BaseResultStore` interface.  A store
receives a :class:`~src.expectations.result_model.RunMetadata` object and
all produced :class:`~src.expectations.result_model.ValidationResult`
instances and decides how to save them.

Base interface
--------------

.. code-block:: python

    class BaseResultStore(ABC):
        """Abstract interface for persistence backends."""

        @abstractmethod
        def persist_run(
            self,
            run: RunMetadata,
            results: Sequence[ValidationResult],
            sla_config: SLAConfig | None = None,
        ) -> None:
            """Persist a run and all its validation results."""

Built-in stores
---------------

``DuckDBResultStore``
~~~~~~~~~~~~~~~~~~~~~
Persists all artefacts into a DuckDB database using
:class:`~src.expectations.engines.duckdb.DuckDBEngine`.  The schema is
created automatically and contains four tables:

``slas``
    SLA configurations keyed by ``sla_name``.
``runs``
    Run metadata including timestamps and associated SLA.
``results``
    One row per validator execution with severity and optional details.
``statistics``
    Optional column level metrics produced by
    :class:`~src.expectations.stats.TableStatsCollector`.

Because data lives in a database it can be pruned with regular SQL, e.g.::

    DELETE FROM runs WHERE started_at < DATE '2024-01-01';

The store exposes a read-only ``connection`` property.  Use plain SQL or
``src.utils.store_context.store_connection`` to perform ad-hoc queries.

``FileResultStore``
~~~~~~~~~~~~~~~~~~~
Writes JSON artefacts to a directory on disk.  Each run generates
``runs/<run_id>.json`` and ``results/<run_id>.jsonl`` while SLA configs and
statistics are saved under ``slas`` and ``statistics`` respectively.
Cleanup simply means deleting the corresponding files.  To analyse data
later, read the JSON files with standard Python tools such as
``pandas.read_json``.

Persisting results
------------------

Stores integrate with both the high level workflow helper and the service
API.  The :func:`~src.expectations.workflow.run_validations` function takes
any ``BaseResultStore`` and persists a run automatically::

    from src.expectations.engines.duckdb import DuckDBEngine
    from src.expectations.runner import ValidationRunner
    from src.expectations.store import DuckDBResultStore
    from src.expectations.workflow import run_validations

    runner = ValidationRunner({"duck": DuckDBEngine("example.db")})
    store = DuckDBResultStore(DuckDBEngine("results.db"))
    run, results = run_validations(
        suite_name="demo",
        bindings=[("duck", "orders", [])],
        runner=runner,
        store=store,
    )

The :class:`~src.service.api.Service` wires the same store into its REST
endpoints so validations triggered via ``POST /runs/{suite_name}`` are
persisted in exactly the same way.

Querying history
----------------

For databases, issue SQL queries against the ``runs`` and ``results``
tables to build dashboards or derive trends.  With the file store, load
the JSON artefacts and aggregate with your tool of choice.  Regular
deletes or file removal keep the history at a manageable size.

