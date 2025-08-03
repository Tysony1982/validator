Engine Reference
================

Engines connect data sources to validators and plug into the :class:`~src.expectations.runner.ValidationRunner` which orchestrates execution across multiple back ends. For examples of wiring engines into a runner, see the :doc:`streamlit` guide. To reconcile tables across engines using reconciliation validators, see the ``Reconciling Data Between Engines`` section of the :doc:`cookbook`.

BaseEngine
----------

All engines derive from :class:`src.expectations.engines.base.BaseEngine`, a minimal interface consisting of:

* ``run_sql(sql)`` – execute a query and return a :class:`pandas.DataFrame`.
* ``list_columns(table)`` – return column names for configuration validation.
* ``get_dialect()`` – name of the SQL dialect understood by ``sqlglot``.
* ``close()`` – release any underlying resources.
* ``run_many(statements)`` – optional helper that calls ``run_sql`` for each statement.

DuckDBEngine
------------

A lightweight engine backed by an embedded DuckDB database.

.. code-block:: python

    from src.expectations.engines.duckdb import DuckDBEngine
    duck = DuckDBEngine(database="example.db", read_only=True, pool_size=2)

The engine accepts an in-memory database by default. ``pool_size`` controls connection pooling for concurrent validations.

FileEngine
----------

Wraps files on disk and exposes them as a SQL table using DuckDB.

.. code-block:: python

    from src.expectations.engines.file import FileEngine
    files = FileEngine("/data/*.parquet", table="events", pool_size=2)

The ``path`` may be a glob. A DuckDB view named by ``table`` is created so validators can query the files directly.

MSSQLEngine
-----------

Placeholder for a future engine targeting Microsoft SQL Server. The stub illustrates the intended API:

.. code-block:: python

    from src.expectations.engines.mssql import MSSQLEngine
    mssql = MSSQLEngine("Driver={ODBC Driver 18 for SQL Server};Server=host;Database=db;UID=user;PWD=pass")

``MSSQLEngine`` is currently unimplemented in the codebase.

