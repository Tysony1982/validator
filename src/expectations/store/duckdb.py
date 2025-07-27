from __future__ import annotations

import json
from pathlib import Path
from typing import Sequence, Optional

from src.expectations.config.expectation import SLAConfig

import duckdb

from src.expectations.result_model import RunMetadata, ValidationResult
from src.expectations.stats import MetricStat
from .base import BaseResultStore
from src.expectations.engines.duckdb import DuckDBEngine


class DuckDBResultStore(BaseResultStore):
    """Persist results into a DuckDB database using :class:`DuckDBEngine`."""

    def __init__(self, engine: Optional[DuckDBEngine] = None, *, database: str | Path = ":memory:"):
        """Create a result store backed by *engine* or a new DuckDBEngine."""
        self._engine = engine or DuckDBEngine(database)
        self._init_schema()

    def _init_schema(self) -> None:
        self._engine.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS slas(
                sla_name TEXT PRIMARY KEY,
                config TEXT
            )
            """
        )
        self._engine.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS runs(
                run_id TEXT PRIMARY KEY,
                suite_name TEXT,
                sla_name TEXT REFERENCES slas(sla_name),
                engine_name TEXT,
                schema TEXT,
                started_at TIMESTAMP,
                finished_at TIMESTAMP
            )
            """
        )
        self._engine.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS results(
                run_id TEXT,
                validator TEXT,
                table_name TEXT,
                column_name TEXT,
                engine_name TEXT,
                schema TEXT,
                metric TEXT,
                success BOOLEAN,
                value TEXT,
                severity TEXT,
                filter_sql TEXT,
                details TEXT
            )
            """
        )
        self._engine.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS statistics(
                run_id TEXT,
                table_name TEXT,
                column_name TEXT,
                engine_name TEXT,
                schema TEXT,
                metric TEXT,
                value TEXT
            )
            """
        )
        self._engine.connection.execute(
            "CREATE INDEX IF NOT EXISTS stats_lookup ON statistics(engine_name, schema, table_name, column_name, metric)"
        )

    # ------------------------------------------------------------------ #
    # BaseResultStore interface
    # ------------------------------------------------------------------ #
    def persist_run(
        self,
        run: RunMetadata,
        results: Sequence[ValidationResult],
        sla_config: SLAConfig | None = None,
    ) -> None:
        if run.sla_name and sla_config is not None:
            self._engine.connection.execute(
                "INSERT OR REPLACE INTO slas VALUES (?, ?)",
                (run.sla_name, json.dumps(sla_config.model_dump())),
            )
        self._engine.connection.execute(
            "INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                run.run_id,
                run.suite_name,
                run.sla_name,
                run.engine_name,
                run.db_schema,
                run.started_at,
                run.finished_at,
            ),
        )
        for r in results:
            self._engine.connection.execute(
                "INSERT INTO results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    r.run_id,
                    r.validator,
                    r.table,
                    r.column,
                    r.engine_name,
                    r.db_schema,
                    r.metric,
                    r.success,
                    r.value,
                    r.severity,
                    r.filter_sql,
                    json.dumps(r.details),
                ),
            )

    def persist_stats(
        self, run: RunMetadata, stats: Sequence["MetricStat"]
    ) -> None:
        """Persist statistics for a run."""

        for s in stats:
            self._engine.connection.execute(
                "INSERT INTO statistics VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    s.run_id,
                    s.table,
                    s.column,
                    s.engine_name or run.engine_name,
                    s.db_schema or run.db_schema,
                    s.metric,
                    s.value,
                ),
            )

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:  # pragma: no cover - helper
        return self._engine.connection

    def close(self) -> None:  # pragma: no cover
        self._engine.close()
