from __future__ import annotations

from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd

from src.expectations.engines.base import BaseEngine
from src.expectations.metrics.batch_builder import MetricBatchBuilder, MetricRequest

from .models import MetricStat


class TableStatsCollector:
    """Compute statistics for all columns of a table."""

    DEFAULT_COLUMN_METRICS: Tuple[str, ...] = ("null_pct", "min", "max")
    DEFAULT_TABLE_METRICS: Tuple[str, ...] = ("row_cnt",)

    def __init__(self, engine_map: Dict[str, BaseEngine]):
        self.engine_map = engine_map

    def collect(
        self,
        engine_key: str,
        table: str,
        *,
        run_id: str,
        column_metrics: Sequence[str] | None = None,
        table_metrics: Sequence[str] | None = None,
    ) -> List[MetricStat]:
        """Return statistics for *table* as a list of :class:`MetricStat`."""

        col_metrics = tuple(column_metrics or self.DEFAULT_COLUMN_METRICS)
        tbl_metrics = tuple(table_metrics or self.DEFAULT_TABLE_METRICS)

        engine = self.engine_map[engine_key]
        columns = engine.list_columns(table)

        alias_map: Dict[str, Tuple[Optional[str], str]] = {}
        requests: List[MetricRequest] = []

        idx = 0
        # table-level metrics
        for metric in tbl_metrics:
            alias = f"m{idx}"
            idx += 1
            requests.append(MetricRequest(column="*", metric=metric, alias=alias))
            alias_map[alias] = (None, metric)

        # column metrics
        for col in columns:
            for metric in col_metrics:
                alias = f"m{idx}"
                idx += 1
                requests.append(
                    MetricRequest(column=col, metric=metric, alias=alias)
                )
                alias_map[alias] = (col, metric)

        sql = MetricBatchBuilder(
            table=table, requests=requests, dialect=engine.get_dialect()
        ).sql()
        df: pd.DataFrame = engine.run_sql(sql)
        row = df.iloc[0]

        stats: List[MetricStat] = []
        schema = None
        if "." in table:
            schema = table.rsplit(".", 1)[0]
        for alias, (col, metric) in alias_map.items():
            stats.append(
                MetricStat(
                    run_id=run_id,
                    table=table,
                    column=col,
                    metric=metric,
                    value=row[alias],
                    engine_name=engine_key,
                    schema=schema,
                )
            )
        return stats
