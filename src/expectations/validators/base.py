"""
src.expectations.validators.base
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Common parent for all validators.  Two *kinds* are supported:

1. **metric**  – wraps a single expression produced by the metric registry
2. **custom**  – supplies its own SQL (as sqlglot AST or raw string)

The runner handles both transparently.
"""

from __future__ import annotations

import os
import ulid
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
        # ULID string is 26 chars -> "v<pid>_<ulid>" still well under 63 chars
        self.runtime_id: str = f"v{os.getpid()}_{ulid.new().str}"

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
