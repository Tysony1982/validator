from __future__ import annotations

"""Utility helpers for building execution engines."""

from typing import Any, Dict, Type

from src.expectations.engines.base import BaseEngine
from src.expectations.engines import DuckDBEngine, FileEngine

_ENGINE_REGISTRY: Dict[str, Type[BaseEngine]] = {
    "duckdb": DuckDBEngine,
    "file": FileEngine,
}


def create_engine(kind: str, **kwargs: Any) -> BaseEngine:
    """Instantiate an engine by *kind* using registered implementations."""
    try:
        cls = _ENGINE_REGISTRY[kind]
    except KeyError as exc:  # pragma: no cover - defensive
        raise ValueError(f"Unknown engine type: {kind}") from exc
    return cls(**kwargs)


def create_comparer_engine(kind: str, **kwargs: Any) -> BaseEngine:
    """Thin wrapper around :func:`create_engine` for clarity."""
    return create_engine(kind, **kwargs)
