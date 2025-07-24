"""
validator.result_models
~~~~~~~~~~~~~~~~~~~~~~~

Typed artefacts passed from runner to downstream stores / reporters.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class RunMetadata(BaseModel):
    run_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    suite_name: str
    started_at: datetime = Field(default_factory=datetime.utcnow)
    finished_at: Optional[datetime] = None

    class Config:
        json_encoders = {datetime: lambda dt: dt.isoformat()}


class ValidationResult(BaseModel):
    run_id: str
    validator: str
    table: str
    column: Optional[str] = None
    metric: Optional[str] = None
    success: bool
    value: Any
    severity: str = "FAIL"
    filter_sql: Optional[str] = None
    details: Dict[str, Any] = Field(default_factory=dict)
