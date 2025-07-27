"""
src.expectations.engines.duckdb
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Concrete implementation of :class:`src.expectations.engines.base.BaseEngine`
for an embedded DuckDB database.  Perfect for unit-tests, local
experimentation, and small-scale production jobs.

Highlights
----------
* Accepts either **in-memory** (default) or on-disk database file.
* Understands both **raw SQL strings** and **sqlglot Expressions**.
* Implements ``run_many()`` with a *single* `duckdb.sql` call to reduce
  Python/DB round-trips when the engine receives several stand-alone
  validators.
"""

from __future__ import annotations

import contextlib
from pathlib import Path
from queue import Queue
from typing import List, Sequence

import duckdb
import pandas as pd
from sqlglot import exp

from src.expectations.engines.base import BaseEngine


class DuckDBEngine(BaseEngine):
    """
    Parameters
    ----------
    database : str | Path, optional
        Filepath for a persistent database, or ``":memory:"`` (default)
        for an ephemeral one.
    read_only : bool, default False
        Open the database in read-only mode (ignored for in-memory DBs).
    pool_size : int, default 1
        Number of connections to keep in the internal pool.
    """

    def __init__(
        self,
        database: str | Path = ":memory:",
        *,
        read_only: bool = False,
        pool_size: int = 1,
    ):
        if pool_size < 1:
            raise ValueError("pool_size must be >= 1")
        self._dialect = "duckdb"
        self._conns: List[duckdb.DuckDBPyConnection] = [
            duckdb.connect(str(database), read_only=read_only)
            for _ in range(pool_size)
        ]
        self._pool: "Queue[duckdb.DuckDBPyConnection]" = Queue()
        for conn in self._conns:
            self._pool.put(conn)

    # ------------------------------------------------------------------ #
    # BaseEngine interface                                               #
    # ------------------------------------------------------------------ #
    def run_sql(self, sql: str | exp.Expression) -> pd.DataFrame:  # noqa: D401
        """
        Execute *sql* and return the result as a pandas DataFrame.

        *sql* may be:
        * a raw SQL string
        * a sqlglot Expression – automatically compiled to the DuckDB
          dialect via ``sqlglot.Expression.sql()``.
        """
        if isinstance(sql, exp.Expression):
            sql = sql.sql(dialect=self._dialect, pretty=False)
        conn = self._pool.get()
        try:
            return conn.execute(str(sql)).fetchdf()
        except Exception as exc:  # pragma: no cover
            raise RuntimeError(f"DuckDB query failed: {sql}\n{exc}") from exc
        finally:
            self._pool.put(conn)

    def run_many(self, sql_statements: Sequence[str | exp.Expression]):  # noqa: D401
        """
        Execute *sql_statements* sequentially and collect the results.

        DuckDB only returns a single result set when multiple statements are
        chained together, so we simply loop over the statements and call
        :py:meth:`run_sql` for each one.
        """
        if not sql_statements:
            return []

        # DuckDB returns a single result set – we only use this method
        # for stand-alone validators that each expect a scalar result,
        # so we run them one-by-one and collect.
        dfs: List[pd.DataFrame] = []
        for stmt in sql_statements:
            dfs.append(self.run_sql(stmt))
        return dfs

    def list_columns(self, table: str) -> List[str]:  # noqa: D401
        """
        Return column names for *table* (schema-qualified ok).

        Uses DuckDB's system PRAGMA: ``PRAGMA table_info('tbl')``.
        """
        schema, t = (None, table)
        if "." in table:
            schema, t = table.rsplit(".", 1)

        pragma = (
            f"PRAGMA table_info('{schema}.{t}')" if schema else f"PRAGMA table_info('{t}')"
        )
        conn = self._pool.get()
        try:
            df = conn.execute(pragma).fetchdf()
            return df["name"].tolist()
        finally:
            self._pool.put(conn)

    def get_dialect(self) -> str:  # noqa: D401
        return self._dialect

    def close(self):  # noqa: D401
        for conn in self._conns:
            with contextlib.suppress(Exception):
                conn.close()

    # ------------------------------------------------------------------ #
    # Convenience helpers                                                #
    # ------------------------------------------------------------------ #
    @property
    def connection(self) -> duckdb.DuckDBPyConnection:  # pragma: no cover
        """Expose one DuckDB connection (first in the pool)."""
        return self._conns[0]

    def register_dataframe(self, name: str, df: pd.DataFrame) -> None:
        """Register *df* as a DuckDB view for ad-hoc testing."""
        for conn in self._conns:
            conn.register(name, df)

    def __repr__(self) -> str:  # pragma: no cover

        #db_name = getattr(self._conn, "database_name", ":memory:")
        #loc = ":memory:" if db_name == ":memory:" else Path(db_name).name

        sample = self._conns[0]
        loc = ":memory:" if sample.database_name == ":memory:" else Path(sample.database_name).name

        return f"<DuckDBEngine db={loc!r}>"
