"""
validator.metrics.registry
~~~~~~~~~~~~~~~~~~~~~~~~~~

Global **metric registry** mapping a *metric key* (e.g. ``"null_pct"``)
to a *builder* – a callable that receives a **column name** and returns a
sqlglot :class:`sqlglot.expressions.Expression`.

The registry is intentionally tiny; packages or user-code may register
additional metrics at import time via the :func:`register_metric` decorator.

Built-ins provided out-of-the-box
---------------------------------
key           | meaning                               | expression
--------------|---------------------------------------|------------------------------------
``null_pct``  | percentage of NULL values             | ``SUM(CASE WHEN col IS NULL THEN 1 END) / COUNT(*)``
``distinct_cnt`` | count of distinct values            | ``COUNT(DISTINCT col)``
``row_cnt``   | total row count (ignores *col* arg)   | ``COUNT(*)``

"""

from __future__ import annotations

from typing import Callable, Dict, Tuple

from sqlglot import exp, parse_one

# --------------------------------------------------------------------------- #
# Public typing alias                                                         #
# --------------------------------------------------------------------------- #
MetricBuilder = Callable[[str], exp.Expression]

# --------------------------------------------------------------------------- #
# Internal registry dictionary                                                #
# --------------------------------------------------------------------------- #
_METRICS: Dict[str, MetricBuilder] = {}


# --------------------------------------------------------------------------- #
# Helper functions                                                             #
# --------------------------------------------------------------------------- #
def register_metric(name: str) -> Callable[[MetricBuilder], MetricBuilder]:
    """
    Decorator that registers a *metric builder* under **name**.

    Example
    -------
    >>> @register_metric("non_null_cnt")
    ... def _non_null_cnt(col):
    ...     return sqlglot.exp.Count(
    ...         sqlglot.exp.Case().when(sqlglot.exp.column(col).is_(sqlglot.exp.null()), None).else_(1)
    ...     )
    """

    def _decorator(fn: MetricBuilder) -> MetricBuilder:
        if name in _METRICS:
            raise KeyError(f"Metric key '{name}' already registered")
        _METRICS[name] = fn
        return fn

    return _decorator


def get_metric(name: str) -> MetricBuilder:
    """
    Return the builder registered for *name*.

    Raises
    ------
    KeyError
        If *name* is unknown.
    """
    try:
        return _METRICS[name]
    except KeyError as exc:  # pragma: no cover
        raise KeyError(
            f"Unknown metric key '{name}'. " f"Available: {', '.join(sorted(_METRICS))}"
        ) from exc


def available_metrics() -> Tuple[str, ...]:
    """Return a **tuple** of all registered metric keys (read-only)."""
    return tuple(_METRICS)


# ------------------------------------------------------------------ #
# Built-in metric builders                                           #
# ------------------------------------------------------------------ #
@register_metric("null_pct")
def _null_pct(column: str) -> exp.Expression:
    """
    SUM(CASE WHEN col IS NULL THEN 1 ELSE 0 END) / COUNT(*)
    """
    col_expr = exp.column(column)
    case_expr = (
        exp.Case()
        .when(exp.Is(this=col_expr, expression=exp.null()), exp.Literal.number(1))
        .else_(exp.Literal.number(0))
    )
    sum_nulls = exp.Sum(this=case_expr)
    count_rows = exp.Count(this=exp.Star())

    # Use exp.Div (division) – correct class name in sqlglot
    return exp.Div(this=sum_nulls, expression=count_rows)


@register_metric("distinct_cnt")
def _distinct_cnt(column: str) -> exp.Expression:
    """COUNT(DISTINCT col)"""
    distinct = exp.Distinct(expressions=[exp.column(column)])
    return exp.Count(this=distinct)


@register_metric("row_cnt")
def _row_cnt(_: str) -> exp.Expression:
    """COUNT(*)"""
    return exp.Count(this=exp.Star())


@register_metric("min")
def _min(column: str) -> exp.Expression:
    """MIN(col)"""
    return exp.Min(this=exp.column(column))


@register_metric("max")
def _max(column: str) -> exp.Expression:
    """MAX(col)"""
    return exp.Max(this=exp.column(column))


@register_metric("non_null_cnt")
def _non_null_cnt(column: str) -> exp.Expression:
    """COUNT(CASE WHEN col IS NOT NULL THEN 1 END)"""
    col_expr = exp.column(column)
    case_expr = (
        exp.Case()
        .when(exp.Is(this=col_expr, expression=exp.null()), exp.null())
        .else_(exp.Literal.number(1))
    )
    return exp.Count(this=case_expr)


@register_metric("avg")
def _avg(column: str) -> exp.Expression:
    """AVG(col)"""
    return exp.Avg(this=exp.column(column))


@register_metric("stddev")
def _stddev(column: str) -> exp.Expression:
    """STDDEV_SAMP(col)"""
    return exp.StddevSamp(this=exp.column(column))


def pct_where(predicate_sql: str) -> MetricBuilder:
    """Return a metric builder for ``pct_where`` using *predicate_sql*."""

    def _builder(_: str) -> exp.Expression:
        case_expr = (
            exp.Case()
            .when(parse_one(predicate_sql), exp.Literal.number(1))
            .else_(exp.Literal.number(0))
        )
        sum_true = exp.Sum(this=case_expr)
        count_rows = exp.Count(this=exp.Star())
        return exp.Div(this=sum_true, expression=count_rows)

    return _builder
