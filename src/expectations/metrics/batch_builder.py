"""
validator.metrics.batch_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Creates **one** SELECT statement that evaluates *n* metric expressions
and aliases them with the validator's unique runtime IDs.  Dialect
compilation is delegated to sqlglot.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence

from sqlglot import exp, parse_one, select

from src.expectations.metrics.registry import get_metric

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

        unknown = {r.metric for r in self.requests if r.metric not in get_metric.__wrapped__.__closure__[0].cell_contents}  # type: ignore
        if unknown:
            raise ValueError(f"Unknown metrics: {', '.join(sorted(unknown))}")

    # ------------------------------------------------------------------ #
    # Private helpers                                                    #
    # ------------------------------------------------------------------ #
    @staticmethod
    def _apply_filter(expr: exp.Expression, filter_sql: Optional[str]) -> exp.Expression:
        if not filter_sql:
            return expr

        filter_exp = parse_one(filter_sql)
        if isinstance(expr, exp.Count) and not expr.args.get("filter"):
            expr.set("filter", filter_exp)
            return expr

        # generic SUM(CASE WHEN …) fallback
        return exp.Function(
            this="SUM",
            expressions=[exp.Case().when(filter_exp, expr).else_(exp.null())],
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
