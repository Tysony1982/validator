"""
src.expectations.validators.base
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Common parent for all validators.  Two *kinds* are supported:

1. **metric**  – wraps a single expression produced by the metric registry
2. **custom**  – supplies its own SQL (as sqlglot AST or raw string)

The runner handles both transparently.
"""

from __future__ import annotations

import uuid
from abc import ABC, abstractmethod
from typing import Literal, Optional

from sqlglot import exp

from src.expectations.metrics.batch_builder import MetricRequest



class ValidatorBase(ABC):
    """Foundation for every validator."""

    # ------------------------------------------------------------------ #
    # Construction                                                       #
    # ------------------------------------------------------------------ #
    def __init__(self, *, where: str | None = None):
        self.where_condition: Optional[str] = where
        self.runtime_id: str = f"v_{uuid.uuid4().hex[:8]}"

    # ------------------------------------------------------------------ #
    # Classification                                                     #
    # ------------------------------------------------------------------ #
    @classmethod
    @abstractmethod
    def kind(cls) -> Literal["metric", "custom"]:
        ...

    # ------------------------------------------------------------------ #
    # Metric validators                                                  #
    # ------------------------------------------------------------------ #
    def metric_request(self) -> MetricRequest:  # noqa: D401
        raise NotImplementedError

    # ------------------------------------------------------------------ #
    # Custom SQL validators                                              #
    # ------------------------------------------------------------------ #
    def custom_sql(self, table: str) -> str | exp.Expression:  # noqa: D401
        raise NotImplementedError

    # ------------------------------------------------------------------ #
    # Result interpretation                                              #
    # ------------------------------------------------------------------ #
    @abstractmethod
    def interpret(self, value) -> bool:  # noqa: D401
        """Convert the raw DB value (scalar/DataFrame row) into pass/fail."""

    # ------------------------------------------------------------------ #
    # Pretty repr                                                        #
    # ------------------------------------------------------------------ #
    def __repr__(self) -> str:  # pragma: no cover
        return f"<{self.__class__.__name__} id={self.runtime_id}>"
