"""
src.expectations.metrics.batch_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Creates **one** SELECT statement that evaluates *n* metric expressions
and aliases them with the validator's unique runtime IDs.  Dialect
compilation is delegated to sqlglot.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence

from sqlglot import exp, select

from src.expectations.errors import ValidationConfigError
from src.expectations.metrics.utils import validate_filter_sql

from src.expectations.metrics.registry import get_metric, available_metrics


# --------------------------------------------------------------------------- #
# Public request model                                                        #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class MetricRequest:
    column: str
    metric: str
    alias: str
    filter_sql: Optional[str] = None  # optional per-metric WHERE


# --------------------------------------------------------------------------- #
# Batch builder                                                               #
# --------------------------------------------------------------------------- #
class MetricBatchBuilder:
    """Convert many :class:`MetricRequest` objects into a single query."""

    def __init__(
        self,
        *,
        table: str,
        requests: Sequence[MetricRequest],
        dialect: str = "ansi",
    ):
        self.table = table
        self.requests = list(requests)
        self.dialect = dialect

        known = set(available_metrics())
        unknown = {r.metric for r in self.requests if r.metric not in known}
        if unknown:
            raise ValueError(f"Unknown metrics: {', '.join(sorted(unknown))}")

    # ------------------------------------------------------------------ #
    # Private helpers                                                    #
    # ------------------------------------------------------------------ #
    @staticmethod
    def _apply_filter(
        expr: exp.Expression, filter_sql: Optional[str]
    ) -> exp.Expression:
        if not filter_sql:
            return expr

        filter_exp = validate_filter_sql(filter_sql)

        # COUNT(*) or COUNT(col) → SUM(CASE WHEN condition THEN 1 END)
        if isinstance(expr, exp.Count):
            case_expr = exp.Case().when(filter_exp, exp.Literal.number(1))
            return exp.Sum(this=case_expr)

        # MIN/MAX/AVG/STDDEV → aggregate(CASE WHEN condition THEN arg END)
        if isinstance(
            expr,
            (
                exp.Min,
                exp.Max,
                exp.Avg,
                exp.Stddev,
                exp.StddevSamp,
                exp.StddevPop,
            ),
        ):
            new_arg = (
                exp.Case()
                .when(filter_exp, expr.args.get("this"))
            )
            expr = expr.copy()
            expr.set("this", new_arg)
            return expr

        # Fallback: wrap entire expression in SUM(CASE WHEN … END)
        return exp.Sum(
            this=exp.Case().when(filter_exp, expr.copy())
        )

    # ------------------------------------------------------------------ #
    # Public API                                                         #
    # ------------------------------------------------------------------ #
    def build_query_ast(self) -> exp.Expression:
        projections: List[exp.Expression] = []
        for req in self.requests:
            raw_expr = get_metric(req.metric)(req.column).copy()
            final_expr = self._apply_filter(raw_expr, req.filter_sql)
            projections.append(exp.alias_(final_expr, req.alias))

        return select(*projections).from_(self.table)

    def sql(self) -> str:
        return self.build_query_ast().sql(dialect=self.dialect, pretty=False)
