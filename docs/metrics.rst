Metrics Framework
=================

The :mod:`src.expectations.metrics` package provides a small framework for
building and executing metric expressions against a database.  Metrics are
registered globally and can then be referenced by validators or composed into
batch queries.

Metric registry
---------------

The module :mod:`src.expectations.metrics.registry` exposes a thread‑safe
registry keyed by short metric names.  Builders are ordinary callables that
receive one or more column names and return a
:class:`sqlglot.expressions.Expression`.

.. code-block:: python

    from sqlglot import exp
    from src.expectations.metrics.registry import register_metric

    @register_metric("avg_length")
    def average_length(column: str) -> exp.Expression:
        return exp.Avg(this=exp.Length(this=exp.column(column)))

The decorator stores the builder in the registry so it can later be retrieved
with :func:`get_metric` or used by validators.  The project ships with a small
set of built‑in metrics such as ``null_pct``, ``distinct_cnt``,
``row_cnt`` and ``duplicate_row_cnt``.

Registering ``pct_where`` metrics
---------------------------------

A convenience helper is available for metrics that compute the percentage of
rows matching a predicate.  :func:`register_pct_where` takes a metric key and a
SQL predicate and registers a builder that divides the count of matching rows by
the total row count.

.. code-block:: python

    from src.expectations.metrics.registry import register_pct_where

    # percentage of rows where status = 'active'
    register_pct_where("active_pct", "status = 'active'")

    # later …
    builder = get_metric("active_pct")
    builder("status").sql()  # -> SUM(CASE WHEN ...) / COUNT(*)

Referencing metrics in validators
---------------------------------

Column validators typically subclass
:class:`src.expectations.validators.column.ColumnMetricValidator`.  They point to
a metric via the ``_metric_key`` attribute and interpret the resulting value.

.. code-block:: python

    from src.expectations.validators.column import ColumnMetricValidator

    class ColumnActivePct(ColumnMetricValidator):
        _metric_key = "active_pct"

        def interpret(self, value) -> bool:
            self.active_pct = float(value)
            return self.active_pct > 0.95

.. _metrics-batching:

Batch execution
---------------

The :mod:`src.expectations.metrics.batch_builder` module groups many metric
requests into a single SQL query.  Each :class:`MetricRequest` specifies a
metric name, the target column and an alias.  :class:`MetricBatchBuilder` looks
up each metric in the registry and constructs one ``SELECT`` statement:

.. code-block:: python

    from src.expectations.metrics.batch_builder import MetricBatchBuilder, MetricRequest

    requests = [MetricRequest(column="id", metric="row_cnt", alias="r")]
    sql = MetricBatchBuilder(table="users", requests=requests).sql()
    # SELECT COUNT(*) AS r FROM users

Filtering metrics
-----------------

Each :class:`MetricRequest` also accepts a ``filter_sql`` argument that applies
an optional per-metric row filter.  Instead of emitting a single ``WHERE``
clause, the builder rewrites each metric expression so that multiple requests
can use different predicates simultaneously.

.. code-block:: python

    requests = [
        MetricRequest(
            column="*",
            metric="row_cnt",
            alias="active_rows",
            filter_sql="status = 'active'",
        )
    ]
    MetricBatchBuilder(table="users", requests=requests).sql()
    # SELECT SUM(CASE WHEN status = 'active' THEN 1 END) AS active_rows FROM users

Flow from validator ``where`` clauses
-------------------------------------

Validators expose a ``where`` argument that is passed through to
``MetricRequest.filter_sql``.  When batching, each validator's filter becomes
part of its metric expression:

.. code-block:: python

    from src.expectations.validators.column import ColumnNonNullCnt, ColumnDistinctCnt

    v1 = ColumnNonNullCnt(column="email", where="status = 'active'")
    v2 = ColumnDistinctCnt(column="id")
    sql = MetricBatchBuilder(
        table="users",
        requests=[v.metric_request() for v in (v1, v2)],
    ).sql()
    # SELECT SUM(CASE WHEN status = 'active' THEN 1 END) AS v1,
    #        COUNT(DISTINCT id) AS v2
    # FROM users

This design lets different validators operate on different subsets of a table
while still being combined into a single query.

