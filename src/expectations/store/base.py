from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Sequence

from src.expectations.config.expectation import SLAConfig

from src.expectations.result_model import RunMetadata, ValidationResult


class BaseResultStore(ABC):
    """Abstract interface for persistence backends."""

    @abstractmethod
    def persist_run(
        self,
        run: RunMetadata,
        results: Sequence[ValidationResult],
        sla_config: "SLAConfig | None" = None,
    ) -> None:
        """Persist a run and all its validation results.

        ``sla_config`` is stored when provided and ``run.sla_name`` is set.
        """
        raise NotImplementedError
