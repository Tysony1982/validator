"""
validator.validators.column
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ready-to-use **column-level** validators that integrate with the new
batch-execution architecture.

* All classes inherit :class:`ColumnMetricValidator` which implements
  the boilerplate for metric-type validators.
* Each validator registers (or re-uses) a metric key in
  ``validator.metrics.registry`` and provides `interpret()` logic.

New metrics added here:
    - ``min``      → MIN(column)
    - ``max``      → MAX(column)
    - ``row_cnt``  → COUNT(*)
"""

from __future__ import annotations

from typing import Any, Optional

from sqlglot import exp

from src.expectations.metrics.batch_builder import MetricRequest
from src.expectations.metrics.registry import register_metric, get_metric
from src.expectations.validators.base import ValidatorBase


# --------------------------------------------------------------------------- #
# Helper mix-in                                                               #
# --------------------------------------------------------------------------- #
class ColumnMetricValidator(ValidatorBase):
    """
    Convenience base class for *metric* validators that target a single column.
    Child classes must provide:

    * class attribute ``_metric_key``
    * ``interpret(self, value)`` implementation
    """

    _metric_key: str  # must be overridden

    def __init__(self, *, column: str, where: str | None = None, **kwargs):
        super().__init__(where=where)
        self.column = column
        self.extra_params = kwargs  # keep whatever sub-class needs

    # ---- ValidatorBase interface ------------------------------------
    @classmethod
    def kind(cls):
        return "metric"

    def metric_request(self) -> MetricRequest:
        return MetricRequest(
            column=self.column,
            metric=self._metric_key,
            alias=self.runtime_id,
            filter_sql=self.where_condition,
        )


# --------------------------------------------------------------------------- #
# Concrete column validators                                                  #
# --------------------------------------------------------------------------- #
class ColumnNotNull(ColumnMetricValidator):
    """
    Passes when *no* NULLs are present in the column.
    """

    _metric_key = "null_pct"

    def interpret(self, value) -> bool:
        self.null_pct = float(value)
        return self.null_pct == 0.0


class ColumnNullPct(ColumnMetricValidator):
    """
    Passes when NULL percentage ≤ ``max_null_pct`` (0-1 range).
    """

    _metric_key = "null_pct"

    def __init__(self, *, column: str, max_null_pct: float, **kw):
        if not 0 <= max_null_pct <= 1:
            raise ValueError("max_null_pct must be between 0 and 1")
        super().__init__(column=column, **kw)
        self.max_null_pct = max_null_pct

    def interpret(self, value) -> bool:
        self.null_pct = float(value)
        return self.null_pct <= self.max_null_pct


class ColumnDistinctCount(ColumnMetricValidator):
    """
    Compares COUNT(DISTINCT column) with an *expected* value
    (== by default, or ≥ / ≤ via ``op`` parameter).
    """

    _metric_key = "distinct_cnt"

    def __init__(
        self,
        *,
        column: str,
        expected: int,
        op: str = "==",
        **kw,
    ):
        if op not in {"==", ">=", "<=", ">", "<"}:
            raise ValueError("op must be one of ==, >=, <=, >, <")
        super().__init__(column=column, **kw)
        self.expected = expected
        self.op = op

    def interpret(self, value) -> bool:
        self.distinct_cnt = int(value)
        return eval(f"{self.distinct_cnt} {self.op} {self.expected}")  # noqa: S307


class ColumnMin(ColumnMetricValidator):
    """
    Passes when MIN(column) ≥ ``min_value`` (inclusive by default).
    """

    _metric_key = "min"

    def __init__(self, *, column: str, min_value: Any, strict: bool = False, **kw):
        super().__init__(column=column, **kw)
        self.min_value = min_value
        self.strict = strict

    def interpret(self, value) -> bool:
        self.observed_min = value
        if self.strict:
            return self.observed_min > self.min_value
        return self.observed_min >= self.min_value


class ColumnMax(ColumnMetricValidator):
    """
    Passes when MAX(column) ≤ ``max_value`` (inclusive by default).
    """

    _metric_key = "max"

    def __init__(self, *, column: str, max_value: Any, strict: bool = False, **kw):
        super().__init__(column=column, **kw)
        self.max_value = max_value
        self.strict = strict

    def interpret(self, value) -> bool:
        self.observed_max = value
        if self.strict:
            return self.observed_max < self.max_value
        return self.observed_max <= self.max_value


class ColumnValueInSet(ColumnMetricValidator):
    """Passes when all values are within ``allowed_values``.

    Example YAML::

        - expectation_type: ColumnValueInSet
          column: status
          allowed_values: [OPEN, CLOSED]
    """

    _metric_key = "row_cnt"

    def __init__(
        self, *, column: str, allowed_values: list[str], allow_null: bool = False, **kw
    ):
        if not allowed_values:
            raise ValueError("allowed_values must not be empty")
        super().__init__(column=column, **kw)
        self.allowed_values = allowed_values
        self.allow_null = allow_null

    def metric_request(self) -> MetricRequest:
        vals = ", ".join(f"'{v}'" for v in self.allowed_values)
        cond = f"{self.column} NOT IN ({vals})"
        if not self.allow_null:
            cond += f" OR {self.column} IS NULL"
        return MetricRequest(
            column=self.column,
            metric=self._metric_key,
            alias=self.runtime_id,
            filter_sql=cond,
        )

    def interpret(self, value) -> bool:
        if value is None or (isinstance(value, float) and value != value):
            self.invalid_cnt = 0
        else:
            self.invalid_cnt = int(value)
        return self.invalid_cnt == 0


class ColumnMatchesRegex(ColumnMetricValidator):
    """Passes when every value matches ``pattern``.

    Example YAML::

        - expectation_type: ColumnMatchesRegex
          column: email
          pattern: "^[A-Za-z]+@example.com$"
    """

    _metric_key = "row_cnt"

    def __init__(self, *, column: str, pattern: str, **kw):
        super().__init__(column=column, **kw)
        self.pattern = pattern

    def metric_request(self) -> MetricRequest:
        cond = f"NOT REGEXP_LIKE({self.column}, '{self.pattern}')"
        return MetricRequest(
            column=self.column,
            metric=self._metric_key,
            alias=self.runtime_id,
            filter_sql=cond,
        )

    def interpret(self, value) -> bool:
        if value is None or (isinstance(value, float) and value != value):
            self.invalid_cnt = 0
        else:
            self.invalid_cnt = int(value)
        return self.invalid_cnt == 0


class ColumnRange(ColumnMetricValidator):
    """Passes when values fall between ``min_value`` and ``max_value``.

    Example YAML::

        - expectation_type: ColumnRange
          column: price
          min_value: 0
          max_value: 100
    """

    _metric_key = "row_cnt"

    def __init__(
        self, *, column: str, min_value: Any, max_value: Any, strict: bool = False, **kw
    ):
        super().__init__(column=column, **kw)
        self.min_value = min_value
        self.max_value = max_value
        self.strict = strict

    def metric_request(self) -> MetricRequest:
        if self.strict:
            cond = f"{self.column} <= {self.min_value} OR {self.column} >= {self.max_value}"
        else:
            cond = (
                f"{self.column} < {self.min_value} OR {self.column} > {self.max_value}"
            )
        return MetricRequest(
            column=self.column,
            metric=self._metric_key,
            alias=self.runtime_id,
            filter_sql=cond,
        )

    def interpret(self, value) -> bool:
        if value is None or (isinstance(value, float) and value != value):
            self.out_of_range_cnt = 0
        else:
            self.out_of_range_cnt = int(value)
        return self.out_of_range_cnt == 0


class ColumnGreaterEqual(ColumnMetricValidator):
    """Passes when ``column`` ≥ ``other_column`` row-wise.

    Example YAML::

        - expectation_type: ColumnGreaterEqual
          column: end_date
          other_column: start_date
    """

    _metric_key = "row_cnt"

    def __init__(self, *, column: str, other_column: str, **kw):
        super().__init__(column=column, **kw)
        self.other_column = other_column

    def metric_request(self) -> MetricRequest:
        cond = f"{self.column} < {self.other_column}"
        return MetricRequest(
            column=self.column,
            metric=self._metric_key,
            alias=self.runtime_id,
            filter_sql=cond,
        )

    def interpret(self, value) -> bool:
        if value is None or (isinstance(value, float) and value != value):
            self.invalid_cnt = 0
        else:
            self.invalid_cnt = int(value)
        return self.invalid_cnt == 0
