"""Expose a read-only connection from a :class:`DuckDBResultStore`.

The initial implementation simply surfaces the underlying DuckDB connection.
One alternative would be to wrap the store in the same ``BaseEngine``
interface used by the validation runner. While that would unify the APIs it
also introduces another abstraction layer without much benefit because the
historical validators only need to issue lightweight ``SELECT`` queries.  This
helper keeps things straightforward and leaves room to add a proper engine
later should the store grow more complex.
"""

from contextlib import contextmanager
from src.expectations.store.duckdb import DuckDBResultStore


@contextmanager
def store_connection(store: DuckDBResultStore):
    """Yield the underlying connection for ad-hoc read-only queries.

    The connection is intentionally left open after the context exits so it can
    be reused by callers. This helper merely provides syntactic sugar around
    the store's connection without managing its lifecycle.
    """
    conn = store.connection
    try:
        yield conn
    finally:
        # The DuckDB connection remains open for the caller to manage.
        pass
