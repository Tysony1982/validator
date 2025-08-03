from __future__ import annotations

"""Validators for reconciling tables and columns across engines."""

from typing import Sequence

import pandas as pd
import re

from src.expectations.engines.base import BaseEngine
from src.expectations.metrics.batch_builder import MetricBatchBuilder, MetricRequest
from src.expectations.metrics.registry import get_metric
from src.expectations.utils.mappings import ColumnMapping, validate_column_mapping
from src.expectations.utils.comparer import run_metrics
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
    Basic usage compares the same column on two engines::

        mapping = ColumnMapping("a")
        ColumnReconciliationValidator(
            column_map=mapping,
            primary_engine=primary,
            primary_table="t1",
            comparer_engine=comparer,
            comparer_table="t2",
        )

    Column mappings can rename and cast values::

        mapping = ColumnMapping(
            primary="id",
            comparer="user_id",
            comparer_type=int,
        )
        ColumnReconciliationValidator(
            column_map=mapping,
            primary_engine=primary,
            primary_table="users",
            comparer_engine=comparer,
            comparer_table="users_copy",
            where="active = 1",
            comparer_where="status = 'active'",
        )
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

        comparer_col = self.column_map.comparer or self.column_map.primary
        cmp_requests = []
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
        comparer = run_metrics(
            self.comparer_engine, self.comparer_table, cmp_requests
        )

        for metric in self._metrics:
            p_val, c_val = self.column_map.convert(primary[metric], comparer[metric])
            primary[metric] = p_val
            comparer[metric] = c_val

        self.details = {"primary": primary, "comparer": comparer}
        return primary == comparer


class TableReconciliationValidator(ValidatorBase):
    """Compare table row counts between two engines.

    Parameters
    ----------
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
    Basic usage::

        TableReconciliationValidator(
            comparer_engine=comparer,
            comparer_table="t2",
        )

    Apply filters when validating a subset of rows::

        TableReconciliationValidator(
            comparer_engine=comparer,
            comparer_table="t2",
            where="active = 1",
            comparer_where="status = 'active'",
        )
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
        cmp = run_metrics(self.comparer_engine, self.comparer_table, [cmp_request])
        comparer_cnt = int(cmp.get("row_cnt") or 0)

        self.details = {"primary": primary_cnt, "comparer": comparer_cnt}
        return primary_cnt == comparer_cnt


class ForeignKeyReconciliationValidator(ValidatorBase):
    """Validate foreign key references between two tables on the same engine.

    The validator checks that all values in the *foreign* table exist in the
    *primary* table using the :func:`missing_values_cnt` metric.  The validator
    succeeds when there are no missing references.

    Parameters
    ----------
    column_map : :class:`~src.expectations.utils.mappings.ColumnMapping`
        Mapping between the primary key column and the foreign key column.  If
        ``comparer`` is ``None`` the primary column name is used for both
        tables.
    primary_engine : BaseEngine
        Engine used to validate the column mapping on construction.
    primary_table : str
        Table containing the primary key values.
    foreign_table : str
        Table containing foreign key references.
    """

    def __init__(
        self,
        *,
        column_map: ColumnMapping,
        primary_engine: BaseEngine,
        primary_table: str,
        foreign_table: str,
    ) -> None:
        super().__init__()
        self.column_map = column_map
        self.primary_table = primary_table
        self.foreign_table = foreign_table

        # Validate that both columns exist on the engine
        validate_column_mapping(
            column_map,
            primary_engine,
            primary_table,
            primary_engine,
            foreign_table,
        )

    # ---- ValidatorBase interface ------------------------------------
    @classmethod
    def kind(cls) -> str:
        return "custom"

    def custom_sql(self, table: str):
        foreign_table = table or self.foreign_table
        primary_col = self.column_map.primary
        foreign_col = self.column_map.comparer or self.column_map.primary
        expr = get_metric("missing_values_cnt")("a", "b").sql()
        expr = re.sub(r"\ba\b", f"p.{primary_col}", expr)
        expr = re.sub(r"\bb\b", f"f.{foreign_col}", expr)
        return (
            f"SELECT {expr} AS missing FROM {foreign_table} f "
            f"LEFT JOIN {self.primary_table} p ON p.{primary_col} = f.{foreign_col}"
        )

    def interpret(self, df: pd.DataFrame) -> bool:
        missing = 0
        if not df.empty:
            val = df.iloc[0]["missing"]
            if val is not None and not pd.isna(val):
                missing = int(val)
        self.details = {"missing_values_cnt": missing}
        return missing == 0
