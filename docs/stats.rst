Table Statistics
================

The :class:`~src.expectations.stats.TableStatsCollector` computes basic metrics for every column in a table.
Statistics are useful for exploring datasets and establishing baselines for validation rules.

Default Metrics
---------------

Table-level metrics and column-level metrics are collected separately.  By default the
collector gathers:

* table metrics: ``row_cnt``
* column metrics: ``null_pct``, ``min``, ``max``

Custom metric names can be supplied via the ``table_metrics`` and ``column_metrics``
arguments to :meth:`~src.expectations.stats.TableStatsCollector.collect`.

Collecting and Persisting Stats
-------------------------------

.. code-block:: python

   from src.expectations.stats import TableStatsCollector
   from src.expectations.store import DuckDBResultStore, FileResultStore
   from src.expectations.engines.duckdb import DuckDBEngine
   from src.expectations.result_model import RunMetadata

   engine = DuckDBEngine()
   collector = TableStatsCollector({"duck": engine})

   run = RunMetadata(suite_name="stats_demo")
   stats = collector.collect("duck", "orders", run_id=run.run_id)

   # Persist into DuckDB
   store = DuckDBResultStore(engine)
   store.persist_stats(run, stats)

   # Or persist to plain JSON files
   file_store = FileResultStore("/tmp/stats")
   file_store.persist_stats(run, stats)

Further Reading
---------------

- :doc:`cookbook#registering-predicate-metrics` – extend and register new metrics.
- :doc:`cookbook#reconciling-data-between-engines` – reconciliation validators for comparing datasets.

