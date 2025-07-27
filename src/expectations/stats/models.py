from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel


class MetricStat(BaseModel):
    """Single metric value computed for a table/column."""

    run_id: str
    table: str
    column: Optional[str] = None
    metric: str
    value: Any
    engine_name: Optional[str] = None
    db_schema: Optional[str] = None

    # ------------------------------------------------------------------
    # Backwards compatibility alias
    # ------------------------------------------------------------------
    @property
    def schema(self) -> Optional[str]:
        """Alias for :attr:`db_schema` for backwards compatibility."""
        return self.db_schema

    @schema.setter
    def schema(self, value: Optional[str]) -> None:  # pragma: no cover - simple
        self.db_schema = value
