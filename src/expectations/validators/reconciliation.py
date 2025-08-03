from __future__ import annotations

"""Validators for reconciling tables and columns across engines."""

from typing import Sequence

import pandas as pd

from src.expectations.engines.base import BaseEngine
from src.expectations.metrics.batch_builder import MetricBatchBuilder, MetricRequest
from src.expectations.utils.mappings import ColumnMapping, validate_column_mapping
from src.expectations.validators.base import ValidatorBase
from src.expectations.validators.column import ColumnMetricValidator


class ColumnReconciliationValidator(ColumnMetricValidator):
    """Compare simple column metrics between two engines.

    The validator runs a set of basic metrics on the *primary* engine and the
    provided ``comparer_engine`` and succeeds when all metrics match exactly.

    Parameters
    ----------
    column_map : :class:`~src.expectations.utils.mappings.ColumnMapping`
        Mapping between the primary and comparer columns.  Allows name
        remapping and value type conversions.
    primary_engine : BaseEngine
        Engine for the primary table used for validation of the mapping.
    primary_table : str
        Table name on the primary engine.
    comparer_engine : BaseEngine
        Engine used for the comparison query.
    comparer_table : str
        Table name on the comparer engine.
    where : str, optional
        Optional SQL filter for the primary engine.
    comparer_where : str, optional
        Optional SQL filter for the comparer engine.

    Examples
    --------
    Basic usage compares the same column on two engines:

    >>> mapping = ColumnMapping("a")
    >>> ColumnReconciliationValidator(
    ...     column_map=mapping,
    ...     primary_engine=primary,
    ...     primary_table="t1",
    ...     comparer_engine=comparer,
    ...     comparer_table="t2",
    ... )
    <ColumnReconciliationValidator>
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
        column_map: ColumnMapping,
        primary_engine: BaseEngine,
        primary_table: str,
        comparer_engine: BaseEngine,
        comparer_table: str,
        where: str | None = None,
        comparer_where: str | None = None,
    ) -> None:
        super().__init__(column=column_map.primary, where=where)
        self.column_map = column_map
        self.comparer_engine = comparer_engine
        self.comparer_table = comparer_table
        self.comparer_where = comparer_where

        validate_column_mapping(
            column_map,
            primary_engine,
            primary_table,
            comparer_engine,
            comparer_table,
        )

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
        comparer_col = self.column_map.comparer or self.column_map.primary
        for metric in self._metrics:
            col = comparer_col if metric != "row_cnt" else "*"
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

        for metric in self._metrics:
            p_val, c_val = self.column_map.convert(primary[metric], comparer[metric])
            primary[metric] = p_val
            comparer[metric] = c_val

        self.details = {"primary": primary, "comparer": comparer}
        return primary == comparer


class TableReconciliationValidator(ValidatorBase):
    """Compare table row counts between two engines.

    Examples
    --------
    >>> TableReconciliationValidator(
    ...     comparer_engine=comparer,
    ...     comparer_table="t2",
    ... )
    <TableReconciliationValidator>
    """

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
