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
    schema: Optional[str] = None
