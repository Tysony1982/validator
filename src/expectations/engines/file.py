from __future__ import annotations

import contextlib
import glob
import os
from pathlib import Path
from typing import List, Sequence

import pandas as pd
from sqlglot import exp

from .base import BaseEngine
from .duckdb import DuckDBEngine


class FileEngine(BaseEngine):
    """Expose one or more data files as a SQL table via DuckDB.

    Parameters
    ----------
    path : str | Path
        File path or glob pointing to the data files.
    table : str, default "t"
        Name of the view exposing the files.
    database : str | Path, optional
        DuckDB database for the underlying engine.
    pool_size : int, default 1
        Passed through to :class:`DuckDBEngine`.
    """

    def __init__(self, path: str | Path, *, table: str = "t", database: str | Path = ":memory:", pool_size: int = 1):
        self.path = str(path)
        self.table = table
        self._duck = DuckDBEngine(database, pool_size=pool_size)
        # Register view pointing to the file path or glob
        quoted_path = self.path.replace("'", "''")
        self._duck.run_sql(f"CREATE VIEW {self.table} AS SELECT * FROM '{quoted_path}'")
        self._dialect = self._duck.get_dialect()
        self.file_metadata = self._collect_metadata()

    def _collect_metadata(self):
        meta = []
        for p in glob.glob(self.path):
            try:
                st = os.stat(p)
                meta.append({
                    "path": os.path.abspath(p),
                    "size": st.st_size,
                    "modified": st.st_mtime,
                })
            except OSError:
                continue
        return meta

    # ------------------------------------------------------------------ #
    # BaseEngine interface                                               #
    # ------------------------------------------------------------------ #
    def run_sql(self, sql: str | exp.Expression) -> pd.DataFrame:
        return self._duck.run_sql(sql)

    def run_many(self, sql_statements: Sequence[str | exp.Expression]):
        return self._duck.run_many(sql_statements)

    def list_columns(self, table: str) -> List[str]:
        return self._duck.list_columns(table)

    def get_dialect(self) -> str:
        return self._dialect

    def close(self) -> None:
        with contextlib.suppress(Exception):
            self._duck.run_sql(f"DROP VIEW IF EXISTS {self.table}")
        self._duck.close()

    # ------------------------------------------------------------------ #
    # Repr                                                               #
    # ------------------------------------------------------------------ #
    def __repr__(self) -> str:  # pragma: no cover
        return f"<FileEngine path={self.path!r} table={self.table!r}>"
