from __future__ import annotations

"""Helpers for running metrics on a secondary comparer engine."""

from typing import Any, Dict, Sequence

import pandas as pd

from src.expectations.engines.base import BaseEngine
from src.expectations.metrics.batch_builder import MetricBatchBuilder, MetricRequest


def run_metrics(
    engine: BaseEngine,
    table: str,
    requests: Sequence[MetricRequest],
) -> Dict[str, Any]:
    """Execute *requests* on *engine* and return a mapping of alias to value."""
    if not requests:
        return {}

    sql = MetricBatchBuilder(table=table, requests=requests).build_query_ast()
    df: pd.DataFrame = engine.run_sql(sql)
    if df.empty:
        return {r.alias: None for r in requests}
    row = df.iloc[0]
    return {r.alias: row[r.alias] for r in requests}
