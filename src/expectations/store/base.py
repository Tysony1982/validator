from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Sequence

from src.expectations.result_model import RunMetadata, ValidationResult


class BaseResultStore(ABC):
    """Abstract interface for persistence backends."""

    @abstractmethod
    def persist_run(self, run: RunMetadata, results: Sequence[ValidationResult]) -> None:
        """Persist a run and all its validation results."""
        raise NotImplementedError
