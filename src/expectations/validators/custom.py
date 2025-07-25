"""Custom SQL validators."""

from __future__ import annotations

from typing import Literal

import pandas as pd

from src.expectations.validators.base import ValidatorBase


class SqlErrorRowsValidator(ValidatorBase):
    """Run ad-hoc SQL that returns error rows.

    Example YAML::

        - expectation_type: SqlErrorRows
          sql: |
            SELECT * FROM my_table WHERE bad_condition
          max_error_rows: 10
    """

    def __init__(
        self,
        *,
        sql: str,
        max_error_rows: int = 20,
        severity: str = "FAIL",
        tags: list[str] | None = None,
    ):
        super().__init__()
        self.sql_text = sql
        self.max_error_rows = max_error_rows
        self.severity = severity
        self.tags = tags or []
        self.error_row_count = 0
        self.details: dict = {}

    @classmethod
    def kind(cls) -> Literal["custom"]:
        return "custom"

    def custom_sql(self, table: str):
        return self.sql_text

    def interpret(self, df: pd.DataFrame) -> bool:
        self.error_row_count = len(df)
        self.details = {
            "error_row_count": self.error_row_count,
            "error_rows_sample": df.head(self.max_error_rows).to_dict("records"),
        }
        return self.error_row_count == 0
