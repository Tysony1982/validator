"""
validator.validators.table
~~~~~~~~~~~~~~~~~~~~~~~~~~

Table-level validators.

* `RowCountValidator` – metric-based, folds into batch query.
* `DuplicateRowValidator` – custom SQL example (non-batchable) that
   checks for duplicates across a set of key columns.
"""

from __future__ import annotations

from typing import List, Sequence

from sqlglot import exp

from src.expectations.metrics.batch_builder import MetricRequest
from src.expectations.validators.base import ValidatorBase
from src.expectations.metrics.registry import register_metric
from src.expectations.metrics.registry import available_metrics as _avail


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
# Duplicate-row validator (custom SQL)                                        #
# --------------------------------------------------------------------------- #
class DuplicateRowValidator(ValidatorBase):
    """
    Checks for duplicate rows based on a list of *key_columns*.

    Passes when the duplicate count == 0.

    *kind()* returns "custom" → executed in its own query.
    """

    def __init__(self, *, key_columns: Sequence[str]):
        super().__init__()
        if not key_columns:
            raise ValueError("key_columns must be a non-empty list")
        self.key_cols: List[str] = list(key_columns)

    # ---- ValidatorBase interface ------------------------------------
    @classmethod
    def kind(cls):
        return "custom"

    def custom_sql(self, table: str):
        """
        Build:
            SELECT COUNT(*) AS dup_cnt
            FROM (
                SELECT <cols>, COUNT(*) c
                FROM table
                GROUP BY <cols>
                HAVING COUNT(*) > 1
            ) d
        """
        inner = (
            exp.select(*map(exp.column, self.key_cols), exp.Count(this=exp.Star()).as_("c"))
            .from_(table)
            .group_by(*map(exp.column, self.key_cols))
            .having(exp.GT(this=exp.column("c"), expression=exp.Literal.number(1)))
        )
        return exp.select(exp.Count(this=exp.Star()).as_("dup_cnt")).from_(inner.subquery("d"))

    def interpret(self, value) -> bool:
        self.duplicate_cnt = int(value)
        return self.duplicate_cnt == 0
