"""Mapping helpers for table and column relationships."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from src.expectations.engines.base import BaseEngine


@dataclass(frozen=True, slots=True)
class TableMapping:
    """Map a primary table to a comparer table."""

    primary: str
    comparer: str


@dataclass(frozen=True, slots=True)
class ColumnMapping:
    """Map a column between primary and comparer tables.

    Parameters
    ----------
    primary: str
        Column name on the primary table.
    comparer: str, optional
        Column name on the comparer table. If ``None`` uses the primary
        name.
    primary_type: callable, optional
        Callable used to convert metric results from the primary column.
    comparer_type: callable, optional
        Callable used to convert metric results from the comparer column.
    primary_case: str, optional
        Normalize string case for primary values ("lower" or "upper").
    comparer_case: str, optional
        Normalize string case for comparer values ("lower" or "upper").
    """

    primary: str
    comparer: str | None = None
    primary_type: Callable[[Any], Any] | None = None
    comparer_type: Callable[[Any], Any] | None = None
    primary_case: str | None = None
    comparer_case: str | None = None

    def convert(self, primary_value: Any, comparer_value: Any) -> tuple[Any, Any]:
        """Apply type conversions to metric results."""

        if self.primary_type is not None:
            primary_value = self.primary_type(primary_value)
        if self.comparer_type is not None:
            comparer_value = self.comparer_type(comparer_value)

        if self.primary_case and isinstance(primary_value, str):
            if self.primary_case.lower() == "lower":
                primary_value = primary_value.lower()
            elif self.primary_case.lower() == "upper":
                primary_value = primary_value.upper()

        if self.comparer_case and isinstance(comparer_value, str):
            if self.comparer_case.lower() == "lower":
                comparer_value = comparer_value.lower()
            elif self.comparer_case.lower() == "upper":
                comparer_value = comparer_value.upper()

        return primary_value, comparer_value


def validate_column_mapping(
    mapping: ColumnMapping,
    primary_engine: BaseEngine,
    primary_table: str,
    comparer_engine: BaseEngine,
    comparer_table: str,
) -> None:
    """Ensure mapped columns exist on both engines."""

    primary_cols = set(primary_engine.list_columns(primary_table))
    comparer_cols = set(comparer_engine.list_columns(comparer_table))

    if mapping.primary not in primary_cols:
        raise ValueError(
            f"Column '{mapping.primary}' not found on table '{primary_table}'"
        )

    comparer_name = mapping.comparer or mapping.primary
    if comparer_name not in comparer_cols:
        raise ValueError(
            f"Column '{comparer_name}' not found on table '{comparer_table}'"
        )
