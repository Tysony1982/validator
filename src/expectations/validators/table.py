"""
src.expectations.validators.table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Table-level validators.

* `RowCountValidator` – metric-based, folds into batch query.
* `DuplicateRowValidator` – metric-based duplicate check across a set of
  key columns.
"""

from __future__ import annotations

from typing import List, Sequence

import pandas as pd

from sqlglot import exp

from src.expectations.metrics.batch_builder import MetricRequest
from src.expectations.validators.base import ValidatorBase


# --------------------------------------------------------------------------- #
# Row-count validator                                                         #
# --------------------------------------------------------------------------- #
class RowCountValidator(ValidatorBase):
    """
    Passes when the table row count is within [min_rows, max_rows] bounds.
    Either bound can be ``None`` to disable that side.
    """

    def __init__(
        self,
        *,
        min_rows: int | None = None,
        max_rows: int | None = None,
        where: str | None = None,
    ):
        super().__init__(where=where)
        if min_rows is None and max_rows is None:
            raise ValueError("At least one of min_rows / max_rows must be provided")
        self.min_rows = min_rows
        self.max_rows = max_rows

    # ---- ValidatorBase interface ------------------------------------
    @classmethod
    def kind(cls):
        return "metric"

    def metric_request(self) -> MetricRequest:
        return MetricRequest(
            column="*",  # ignored by row_cnt metric builder
            metric="row_cnt",
            alias=self.runtime_id,
            filter_sql=self.where_condition,
        )

    def interpret(self, value) -> bool:
        self.row_cnt = int(value)
        ok = True
        if self.min_rows is not None:
            ok &= self.row_cnt >= self.min_rows
        if self.max_rows is not None:
            ok &= self.row_cnt <= self.max_rows
        return ok


# --------------------------------------------------------------------------- #
# Duplicate-row validator                                                     #
# --------------------------------------------------------------------------- #
class DuplicateRowValidator(ValidatorBase):
    """Passes when no duplicate rows exist across ``key_columns``."""

    def __init__(self, *, key_columns: Sequence[str]):
        super().__init__()
        if not key_columns:
            raise ValueError("key_columns must be a non-empty list")
        self.key_cols: List[str] = list(key_columns)

    # ---- ValidatorBase interface ------------------------------------
    @classmethod
    def kind(cls):
        return "metric"

    def metric_request(self) -> MetricRequest:
        return MetricRequest(
            column=self.key_cols,
            metric="duplicate_row_cnt",
            alias=self.runtime_id,
        )

    def interpret(self, value) -> bool:
        if isinstance(value, pd.DataFrame):
            dup_cnt = int(value.iloc[0, 0]) if not value.empty else 0
        else:
            dup_cnt = int(value or 0)
        self.duplicate_cnt = dup_cnt
        return dup_cnt == 0


class PrimaryKeyUniquenessValidator(ValidatorBase):
    """Passes when the set of ``key_columns`` uniquely identifies each row.

    Example YAML::

        - expectation_type: PrimaryKeyUniquenessValidator
          key_columns: [id]
    """

    def __init__(self, *, key_columns: Sequence[str]):
        super().__init__()
        if not key_columns:
            raise ValueError("key_columns must be a non-empty list")
        self.key_cols = list(key_columns)

    @classmethod
    def kind(cls):
        return "custom"

    def custom_sql(self, table: str):
        distinct = exp.Count(
            this=exp.Distinct(expressions=[exp.column(c) for c in self.key_cols])
        )
        diff = exp.Sub(this=exp.Count(this=exp.Star()), expression=distinct).as_(
            "dup_cnt"
        )
        return exp.select(diff).from_(table)

    def interpret(self, value) -> bool:
        if isinstance(value, pd.DataFrame):
            dup_cnt = int(value.iloc[0, 0]) if not value.empty else 0
        else:
            dup_cnt = int(value or 0)
        self.duplicate_cnt = dup_cnt
        return dup_cnt == 0


# --------------------------------------------------------------------------- #
# Table freshness validator                                                    #
# --------------------------------------------------------------------------- #
class TableFreshnessValidator(ValidatorBase):
    """Passes when the most recent ``timestamp_column`` is within ``threshold``.

    ``threshold`` may be any value accepted by :func:`pandas.Timedelta`, e.g.
    ``"1h"`` or ``pd.Timedelta(hours=1)``.
    """

    def __init__(self, *, timestamp_column: str, threshold, where: str | None = None):
        super().__init__(where=where)
        self.timestamp_column = timestamp_column
        self.threshold = pd.Timedelta(threshold)

    # ---- ValidatorBase interface ------------------------------------
    @classmethod
    def kind(cls):
        return "metric"

    def metric_request(self) -> MetricRequest:
        return MetricRequest(
            column=self.timestamp_column,
            metric="max",
            alias=self.runtime_id,
            filter_sql=self.where_condition,
        )

    def interpret(self, value) -> bool:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            self.max_timestamp = None
            return False

        self.max_timestamp = pd.Timestamp(value)
        now = pd.Timestamp.utcnow()
        return self.max_timestamp >= now - self.threshold
