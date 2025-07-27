"""
src.expectations.config.expectation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Pydantic models that describe an *expectation suite* and a helper that
translates it into concrete `ValidatorBase` instances.
"""

from __future__ import annotations

from importlib import import_module
from pathlib import Path
from typing import Any, Dict, List, Sequence
import sys
import yaml
from pydantic import BaseModel, Field, model_validator

from src.expectations.validators.base import ValidatorBase
from src.expectations.runner import ValidatorBinding


# --------------------------------------------------------------------------- #
# Model definitions                                                           #
# --------------------------------------------------------------------------- #
class ExpectationConfig(BaseModel):
    expectation_type: str  # class name of the validator
    column: str | None = None
    where: str | None = None
    sql: str | None = None
    max_error_rows: int | None = None
    kwargs: Dict[str, Any] = Field(default_factory=dict)
    tags: List[str] = Field(default_factory=list)
    severity: str | None = None  # INFO / WARN / FAIL
    threshold: float | int | None = None

    @model_validator(mode="after")
    def strip_empty_kwargs(self):
        if not self.kwargs:
            delattr(self, "kwargs")
        return self


class ExpectationSuiteConfig(BaseModel):
    suite_name: str
    engine: str
    table: str
    expectations: List[ExpectationConfig]

    # ------------------------------------------------------------------ #
    # Factory helpers                                                    #
    # ------------------------------------------------------------------ #
    def build_validators(self) -> Sequence[ValidatorBinding]:
        """
        Dynamically import the validator classes, instantiate them, and
        return the bindings the runner understands.
        """
        bindings: List[ValidatorBinding] = []

        from src.expectations.validators.custom import SqlErrorRowsValidator

        for cfg in self.expectations:
            if cfg.expectation_type == "SqlErrorRows":
                cls = SqlErrorRowsValidator
            else:
                cls = _resolve_validator_class(cfg.expectation_type)
            if not issubclass(cls, ValidatorBase):
                raise TypeError(f"{cfg.expectation_type} is not a ValidatorBase")

            init_kwargs = dict(getattr(cfg, "kwargs", {}) or {})
            if cfg.column:
                init_kwargs["column"] = cfg.column
            if cfg.where:
                init_kwargs["where"] = cfg.where
            if cfg.sql:
                init_kwargs["sql"] = cfg.sql
            if cfg.max_error_rows is not None:
                init_kwargs["max_error_rows"] = cfg.max_error_rows
            if cfg.threshold is not None:
                init_kwargs["threshold"] = cfg.threshold
            if cfg.severity:
                init_kwargs["severity"] = cfg.severity

            validator = cls(**init_kwargs)
            bindings.append((self.engine, self.table, validator))

        return bindings

    # ------------------------------------------------------------------ #
    # I/O                                                                 #
    # ------------------------------------------------------------------ #
    @classmethod
    def from_yaml(cls, path: str | Path) -> "ExpectationSuiteConfig":
        with open(path, "r") as fh:
            return cls.model_validate(yaml.safe_load(fh))

    @classmethod
    def from_json(cls, path: str | Path) -> "ExpectationSuiteConfig":
        with open(path, "r") as fh:
            return cls.model_validate_json(fh.read())

    # optional “smart” loader ---------------------
    @classmethod
    def from_file(cls, path: str | Path) -> "ExpectationSuiteConfig":
        ext = Path(path).suffix.lower()
        if ext in {".yml", ".yaml"}:
            return cls.from_yaml(path)
        if ext == ".json":
            return cls.from_json(path)
        raise ValueError(f"Unsupported config extension: {ext}")


class SLAConfig(BaseModel):
    """Group multiple expectation suites under a single SLA."""

    sla_name: str
    suites: List[ExpectationSuiteConfig]

    # ------------------------------------------------------------------ #
    # Factory helpers                                                    #
    # ------------------------------------------------------------------ #
    def build_validators(self) -> Sequence[ValidatorBinding]:
        """Aggregate bindings for all contained suites."""
        bindings: List[ValidatorBinding] = []
        for suite in self.suites:
            bindings.extend(suite.build_validators())
        return bindings

    # ------------------------------------------------------------------ #
    # I/O                                                                 #
    # ------------------------------------------------------------------ #
    @classmethod
    def from_yaml(cls, path: str | Path) -> "SLAConfig":
        with open(path, "r") as fh:
            return cls.model_validate(yaml.safe_load(fh))

    @classmethod
    def from_json(cls, path: str | Path) -> "SLAConfig":
        with open(path, "r") as fh:
            return cls.model_validate_json(fh.read())

    @classmethod
    def from_file(cls, path: str | Path) -> "SLAConfig":
        ext = Path(path).suffix.lower()
        if ext in {".yml", ".yaml"}:
            return cls.from_yaml(path)
        if ext == ".json":
            return cls.from_json(path)
        raise ValueError(f"Unsupported config extension: {ext}")


# --------------------------------------------------------------------------- #
# Internal helpers                                                            #
# --------------------------------------------------------------------------- #
def _resolve_validator_class(name: str) -> type[ValidatorBase]:
    """
    Resolve *name* (e.g. "ColumnNotNull") to an actual class.

    Strategy:
      1. Look in `src.expectations.validators` sub-modules already imported.
      2. Fallback to dynamic import.
    """
    # optimistic: already in sys.modules
    for mod_name in list(sys.modules):
        if mod_name.startswith("src.expectations.validators."):
            mod = sys.modules[mod_name]
            if hasattr(mod, name):
                return getattr(mod, name)

    # fallback – import checker modules lazily
    parts = name.split(".")
    if len(parts) == 1:
        # assume it's in the default column module
        mod = import_module("src.expectations.validators.column")
        return getattr(mod, name)
    *pkg, cls_name = parts
    mod = import_module(".".join(pkg))
    return getattr(mod, cls_name)
