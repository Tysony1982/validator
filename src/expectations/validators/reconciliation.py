from __future__ import annotations

"""Validators for reconciling tables and columns across engines."""

from typing import Sequence

import pandas as pd

from src.expectations.engines.base import BaseEngine
from src.expectations.metrics.batch_builder import MetricBatchBuilder, MetricRequest
from src.expectations.validators.base import ValidatorBase
from src.expectations.validators.column import ColumnMetricValidator


class ColumnReconciliationValidator(ColumnMetricValidator):
    """Compare simple column metrics between two engines.

    The validator runs a set of basic metrics on the *primary* engine and the
    provided ``comparer_engine`` and succeeds when all metrics match exactly.

    Parameters
    ----------
    column : str
        Column name on the primary table.
    comparer_engine : BaseEngine
        Engine used for the comparison query.
    comparer_table : str
        Table name on the comparer engine.
    comparer_column : str, optional
        Column on the comparer table; defaults to ``column``.
    where : str, optional
        Optional SQL filter for the primary engine.
    comparer_where : str, optional
        Optional SQL filter for the comparer engine.
    """

    _metric_key = "row_cnt"  # unused but required by ``ColumnMetricValidator``
    _metrics: Sequence[str] = (
        "row_cnt",
        "min",
        "max",
    )

    def __init__(
        self,
        *,
        column: str,
        comparer_engine: BaseEngine,
        comparer_table: str,
        comparer_column: str | None = None,
        where: str | None = None,
        comparer_where: str | None = None,
    ) -> None:
        super().__init__(column=column, where=where)
        self.comparer_engine = comparer_engine
        self.comparer_table = comparer_table
        self.comparer_column = comparer_column or column
        self.comparer_where = comparer_where

    # ---- ValidatorBase interface ------------------------------------
    @classmethod
    def kind(cls) -> str:
        return "custom"

    def custom_sql(self, table: str):
        requests = []
        for metric in self._metrics:
            col = self.column if metric != "row_cnt" else "*"
            requests.append(
                MetricRequest(
                    column=col,
                    metric=metric,
                    alias=metric,
                    filter_sql=self.where_condition,
                )
            )
        return MetricBatchBuilder(table=table, requests=requests).build_query_ast()

    def interpret(self, df: pd.DataFrame) -> bool:
        row = df.iloc[0]
        primary = {m: row[m] for m in self._metrics}

        cmp_requests = []
        for metric in self._metrics:
            col = self.comparer_column if metric != "row_cnt" else "*"
            cmp_requests.append(
                MetricRequest(
                    column=col,
                    metric=metric,
                    alias=metric,
                    filter_sql=self.comparer_where,
                )
            )
        sql = MetricBatchBuilder(
            table=self.comparer_table, requests=cmp_requests
        ).build_query_ast()
        cmp_df = self.comparer_engine.run_sql(sql)
        cmp_row = cmp_df.iloc[0]
        comparer = {m: cmp_row[m] for m in self._metrics}

        self.details = {"primary": primary, "comparer": comparer}
        return primary == comparer


class TableReconciliationValidator(ValidatorBase):
    """Compare table row counts between two engines."""

    def __init__(
        self,
        *,
        comparer_engine: BaseEngine,
        comparer_table: str,
        where: str | None = None,
        comparer_where: str | None = None,
    ) -> None:
        super().__init__(where=where)
        self.comparer_engine = comparer_engine
        self.comparer_table = comparer_table
        self.comparer_where = comparer_where

    @classmethod
    def kind(cls) -> str:
        return "custom"

    def custom_sql(self, table: str):
        request = MetricRequest(
            column="*",
            metric="row_cnt",
            alias="row_cnt",
            filter_sql=self.where_condition,
        )
        return MetricBatchBuilder(table=table, requests=[request]).build_query_ast()

    def interpret(self, df: pd.DataFrame) -> bool:
        primary_cnt = int(df.iloc[0]["row_cnt"]) if not df.empty else 0

        cmp_request = MetricRequest(
            column="*",
            metric="row_cnt",
            alias="row_cnt",
            filter_sql=self.comparer_where,
        )
        sql = MetricBatchBuilder(
            table=self.comparer_table, requests=[cmp_request]
        ).build_query_ast()
        cmp_df = self.comparer_engine.run_sql(sql)
        comparer_cnt = int(cmp_df.iloc[0]["row_cnt"]) if not cmp_df.empty else 0

        self.details = {"primary": primary_cnt, "comparer": comparer_cnt}
        return primary_cnt == comparer_cnt
