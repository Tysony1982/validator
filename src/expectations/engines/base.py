"""
validator.engines.base
~~~~~~~~~~~~~~~~~~~~~~

Minimal contract every execution engine must satisfy.
Keeps the API surface tiny: new back-ends only need to provide *three*
methods plus a `close()`.

All query content is supplied as either a raw SQL string or a sqlglot
Expression – the engine may choose to stringify if required.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, List, Sequence

import pandas as pd
from sqlglot import exp


class BaseEngine(ABC):
    """Abstract base for all SQL/DF execution engines."""

    # ------------------------------------------------------------------ #
    # Mandatory hooks                                                    #
    # ------------------------------------------------------------------ #
    @abstractmethod
    def run_sql(self, sql: str | exp.Expression) -> pd.DataFrame:  # noqa: D401
        """Execute *sql* and return the result as a pandas DataFrame."""

    @abstractmethod
    def list_columns(self, table: str) -> List[str]:
        """Return column names for *table* (used by config validation)."""

    @abstractmethod
    def get_dialect(self) -> str:  # noqa: D401
        """Return the SQL dialect identifier understood by sqlglot."""

    @abstractmethod
    def close(self) -> None:  # noqa: D401
        """Clean up all open resources (connections, cursors, etc.)."""

    # ------------------------------------------------------------------ #
    # Optional sugar – engines may override if they can batch multiple   #
    # queries in a single round-trip, but the default is fine.           #
    # ------------------------------------------------------------------ #
    def run_many(self, sql_statements: Sequence[str | exp.Expression]) -> List[pd.DataFrame]:
        """Execute *each* statement individually using :py:meth:`run_sql`."""
        return [self.run_sql(stmt) for stmt in sql_statements]

    # ------------------------------------------------------------------ #
    # Repr                                                               #
    # ------------------------------------------------------------------ #
    def __repr__(self) -> str:  # pragma: no cover
        return f"<{self.__class__.__name__} dialect={self.get_dialect()!r}>"
