"""
src.expectations.metrics.registry
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Global **metric registry** mapping a *metric key* (e.g. ``"null_pct"``)
to a *builder* – a callable that receives a **column name** and returns a
sqlglot :class:`sqlglot.expressions.Expression`.

The registry is intentionally tiny; packages or user-code may register
additional metrics at import time via the :func:`register_metric` decorator.

Built-ins provided out-of-the-box
---------------------------------
key                | meaning                               | expression
------------------|---------------------------------------|------------------------------------
``null_pct``       | percentage of NULL values             | ``SUM(CASE WHEN col IS NULL THEN 1 END) / COUNT(*)``
``distinct_cnt``   | count of distinct values              | ``COUNT(DISTINCT col)``
``row_cnt``        | total row count (ignores *col* arg)   | ``COUNT(*)``
``duplicate_cnt``  | count of duplicate values             | ``COUNT(*) - COUNT(DISTINCT col)``

"""

from __future__ import annotations

import threading
from typing import Callable, Dict, Tuple, Optional

from sqlglot import exp
from src.expectations.metrics.utils import validate_filter_sql

# --------------------------------------------------------------------------- #
# Public typing alias                                                         #
# --------------------------------------------------------------------------- #
# ``MetricBuilder`` functions may accept additional arguments beyond a single
# column name (e.g. multiple columns or a filter expression).  We therefore
# type it as a generic callable and leave runtime validation to the builders
# themselves.
MetricBuilder = Callable[..., exp.Expression]


class MetricRegistry:
    """Thread-safe singleton registry of metric builders."""

    _instance: "MetricRegistry | None" = None
    _instance_lock = threading.RLock()

    def __init__(self) -> None:
        self._metrics: Dict[str, MetricBuilder] = {}
        self._lock = threading.RLock()

    @classmethod
    def instance(cls) -> "MetricRegistry":
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    # ------------------------------------------------------------------ #
    # Public API                                                        #
    # ------------------------------------------------------------------ #
    def register(self, name: str, builder: MetricBuilder) -> None:
        with self._lock:
            if name in self._metrics:
                raise KeyError(f"Metric key '{name}' already registered")
            self._metrics[name] = builder

    def get(self, name: str) -> MetricBuilder:
        with self._lock:
            try:
                return self._metrics[name]
            except KeyError as exc:  # pragma: no cover
                raise KeyError(
                    f"Unknown metric key '{name}'. Available: {', '.join(sorted(self._metrics))}"
                ) from exc

    def keys(self) -> Tuple[str, ...]:
        with self._lock:
            return tuple(self._metrics)


# --------------------------------------------------------------------------- #
# Helper functions wrapping the singleton                                      #
# --------------------------------------------------------------------------- #
def register_metric(name: str) -> Callable[[MetricBuilder], MetricBuilder]:
    def _decorator(fn: MetricBuilder) -> MetricBuilder:
        MetricRegistry.instance().register(name, fn)
        return fn

    return _decorator


def get_metric(name: str) -> MetricBuilder:
    return MetricRegistry.instance().get(name)


def available_metrics() -> Tuple[str, ...]:
    """Return a **tuple** of all registered metric keys (read-only)."""
    return MetricRegistry.instance().keys()


# ------------------------------------------------------------------ #
# Built-in metric builders                                           #
# ------------------------------------------------------------------ #
@register_metric("null_pct")
def _null_pct(column: str) -> exp.Expression:
    """Percentage of ``NULL`` values in *column*.

    Examples
    --------
    >>> _null_pct("a").sql()
    "SUM(CASE WHEN a IS NULL THEN 1 ELSE 0 END) / COUNT(*)"
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
    """Count of distinct values in *column*.

    Examples
    --------
    >>> _distinct_cnt("a").sql()
    'COUNT(DISTINCT a)'
    """
    distinct = exp.Distinct(expressions=[exp.column(column)])
    return exp.Count(this=distinct)


@register_metric("row_cnt")
def _row_cnt(_: str) -> exp.Expression:
    """Total row count for the current table.

    Examples
    --------
    >>> _row_cnt("irrelevant").sql()
    'COUNT(*)'
    """
    return exp.Count(this=exp.Star())


@register_metric("duplicate_cnt")
def _duplicate_cnt(column: str) -> exp.Expression:
    """Number of duplicate values in ``column``."""

    total = _row_cnt(column)
    distinct = _distinct_cnt(column)
    return exp.Sub(this=total, expression=distinct)


@register_metric("duplicate_row_cnt")
def _duplicate_row_cnt(columns: str) -> exp.Expression:
    """Count duplicate groups based on ``columns``.

    ``columns`` may be a comma-separated list of key columns. The metric returns
    the number of groups that contain more than one row.

    Examples
    --------
    >>> _duplicate_row_cnt("a,b").sql()
    'COUNT(*) - COUNT(DISTINCT a, b)'
    """

    keys = [c.strip() for c in columns.split(",") if c.strip()]
    if not keys:
        raise ValueError("duplicate_row_cnt requires at least one column")

    row_cnt = exp.Count(this=exp.Star())
    distinct = exp.Count(
        this=exp.Distinct(expressions=[exp.column(k) for k in keys])
    )
    return exp.Sub(this=row_cnt, expression=distinct)


@register_metric("min")
def _min(column: str) -> exp.Expression:
    """Minimum value of *column*.

    Examples
    --------
    >>> _min("a").sql()
    'MIN(a)'
    """
    return exp.Min(this=exp.column(column))


@register_metric("max")
def _max(column: str) -> exp.Expression:
    """Maximum value of *column*.

    Examples
    --------
    >>> _max("a").sql()
    'MAX(a)'
    """
    return exp.Max(this=exp.column(column))


@register_metric("non_null_cnt")
def _non_null_cnt(column: str) -> exp.Expression:
    """Count of non-``NULL`` values in *column*.

    Examples
    --------
    >>> _non_null_cnt("a").sql()
    'COUNT(CASE WHEN a IS NOT NULL THEN 1 END)'
    """
    col_expr = exp.column(column)
    case_expr = (
        exp.Case()
        .when(exp.Is(this=col_expr, expression=exp.null()), exp.null())
        .else_(exp.Literal.number(1))
    )
    return exp.Count(this=case_expr)


@register_metric("avg")
def _avg(column: str) -> exp.Expression:
    """Average (mean) of *column*.

    Examples
    --------
    >>> _avg("a").sql()
    'AVG(a)'
    """
    return exp.Avg(this=exp.column(column))


@register_metric("stddev")
def _stddev(column: str) -> exp.Expression:
    """Sample standard deviation of *column*.

    Examples
    --------
    >>> _stddev("a").sql()
    'STDDEV_SAMP(a)'
    """
    return exp.StddevSamp(this=exp.column(column))


def percentile(column: str, q: float) -> exp.Expression:
    """Continuous percentile of *column* at quantile *q*.

    Parameters
    ----------
    column:
        Column name to compute the percentile for.
    q:
        Desired quantile in the ``0-1`` range.

    Examples
    --------
    >>> percentile("a", 0.9).sql()
    'PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY a)'
    """
    if not 0 <= q <= 1:
        raise ValueError("q must be between 0 and 1")

    order = exp.Order(expressions=[exp.Ordered(this=exp.column(column))])
    within = exp.WithinGroup(
        this=exp.PercentileCont(this=exp.Literal.number(q)),
        expression=order,
    )
    return within


def register_percentile(q: float) -> MetricBuilder:
    """Register a ``percentile`` metric for quantile *q*.

    The metric key follows the ``pct_<q>`` convention where ``<q>`` is the
    percentile on a ``0-100`` scale (integer).
    """

    name = f"pct_{int(q * 100)}"

    def _builder(column: str) -> exp.Expression:
        return percentile(column, q)

    registry = MetricRegistry.instance()
    try:
        registry.register(name, _builder)
        return _builder
    except KeyError:
        # Metric already registered; return the existing builder
        return registry.get(name)


def pct_where(predicate_sql: str) -> MetricBuilder:
    """Return a metric builder for ``pct_where`` using *predicate_sql*.

    Examples
    --------
    >>> pct_red = pct_where("color = 'red'")
    >>> pct_red("color").sql()
    "SUM(CASE WHEN color = 'red' THEN 1 ELSE 0 END) / COUNT(*)"
    """

    def _builder(_: str) -> exp.Expression:
        condition = validate_filter_sql(predicate_sql)
        case_expr = (
            exp.Case()
            .when(condition, exp.Literal.number(1))
            .else_(exp.Literal.number(0))
        )
        sum_true = exp.Sum(this=case_expr)
        count_rows = exp.Count(this=exp.Star())
        return exp.Div(this=sum_true, expression=count_rows)

    return _builder


def register_pct_where(name: str, predicate_sql: str) -> MetricBuilder:
    """Register a ``pct_where`` metric under ``name``.

    Parameters
    ----------
    name:
        Key used when looking up the metric via :func:`get_metric`.
    predicate_sql:
        SQL predicate evaluated per row. The resulting metric expresses the
        percentage of rows for which the predicate is true.

    Returns
    -------
    MetricBuilder
        The registered builder, equivalent to :func:`pct_where(predicate_sql)`.

    Examples
    --------
    >>> register_pct_where("b_is_one_pct", "b = 1")
    >>> builder = get_metric("b_is_one_pct")
    >>> builder("a")  # expression for percentage of rows where b equals 1
    """

    builder = pct_where(predicate_sql)
    MetricRegistry.instance().register(name, builder)
    return builder


# --------------------------------------------------------------------------- #
# Set comparison metrics                                                      #
# --------------------------------------------------------------------------- #

def _resolve_columns(col1: str, col2: Optional[str]) -> Tuple[str, str]:
    """Return a pair of column names.

    Builders may be invoked either with two separate column arguments or with a
    single comma-separated string containing both column names.  This helper
    normalizes the input to a two-item tuple and raises ``ValueError`` if the
    input is malformed.
    """

    if col2 is not None:
        return col1, col2

    parts = [p.strip() for p in col1.split(",")]
    if len(parts) != 2:
        raise ValueError("Expected two column names separated by a comma")
    return parts[0], parts[1]


@register_metric("set_overlap_pct")
def set_overlap_pct(
    column_a: str,
    column_b: Optional[str] = None,
    *,
    filter_sql: Optional[str] = None,
) -> exp.Expression:
    """Return the percentage of overlapping values between two columns.

    Examples
    --------
    >>> expr = set_overlap_pct("a", "b")
    >>> expr.sql()
    "SUM(CASE WHEN a IS NOT NULL AND b IS NOT NULL THEN 1 END) / "
    "SUM(CASE WHEN a IS NOT NULL OR b IS NOT NULL THEN 1 END)"
    """

    col_a, col_b = _resolve_columns(column_a, column_b)
    a = exp.column(col_a)
    b = exp.column(col_b)

    a_not_null = exp.Not(this=exp.Is(this=a, expression=exp.null()))
    b_not_null = exp.Not(this=exp.Is(this=b, expression=exp.null()))

    filt = validate_filter_sql(filter_sql) if filter_sql else None

    inter_cond = exp.and_(a_not_null, b_not_null)
    if filt is not None:
        inter_cond = exp.and_(filt, inter_cond)
    inter_case = exp.Case().when(inter_cond, exp.Literal.number(1))
    inter_cnt = exp.Sum(this=inter_case)

    union_cond = exp.or_(a_not_null, b_not_null)
    if filt is not None:
        union_cond = exp.and_(filt, union_cond)
    union_case = exp.Case().when(union_cond, exp.Literal.number(1))
    union_cnt = exp.Sum(this=union_case)

    zero_union = exp.EQ(this=union_cnt.copy(), expression=exp.Literal.number(0))
    div_expr = exp.Div(this=inter_cnt, expression=union_cnt.copy())

    return exp.Case().when(zero_union, exp.null()).else_(div_expr)


@register_metric("missing_values_cnt")
def missing_values_cnt(
    column_a: str,
    column_b: Optional[str] = None,
    *,
    filter_sql: Optional[str] = None,
) -> exp.Expression:
    """Count values present in *column_b* but missing from *column_a*.

    Examples
    --------
    >>> expr = missing_values_cnt("expected", "actual")
    >>> expr.sql()
    "SUM(CASE WHEN expected IS NULL AND actual IS NOT NULL THEN 1 END)"
    """

    col_a, col_b = _resolve_columns(column_a, column_b)
    a = exp.column(col_a)
    b = exp.column(col_b)

    cond = exp.and_(
        exp.Is(this=a, expression=exp.null()),
        exp.Not(this=exp.Is(this=b, expression=exp.null())),
    )
    if filter_sql:
        cond = exp.and_(validate_filter_sql(filter_sql), cond)

    case_expr = exp.Case().when(cond, exp.Literal.number(1))
    return exp.Sum(this=case_expr)


@register_metric("extra_values_cnt")
def extra_values_cnt(
    column_a: str,
    column_b: Optional[str] = None,
    *,
    filter_sql: Optional[str] = None,
) -> exp.Expression:
    """Count values present in *column_a* but missing from *column_b*.

    Examples
    --------
    >>> expr = extra_values_cnt("actual", "expected")
    >>> expr.sql()
    "SUM(CASE WHEN actual IS NOT NULL AND expected IS NULL THEN 1 END)"
    """

    col_a, col_b = _resolve_columns(column_a, column_b)
    a = exp.column(col_a)
    b = exp.column(col_b)

    cond = exp.and_(
        exp.Not(this=exp.Is(this=a, expression=exp.null())),
        exp.Is(this=b, expression=exp.null()),
    )
    if filter_sql:
        cond = exp.and_(validate_filter_sql(filter_sql), cond)

    case_expr = exp.Case().when(cond, exp.Literal.number(1))
    return exp.Sum(this=case_expr)
