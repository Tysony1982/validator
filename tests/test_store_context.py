from src.expectations.store.duckdb import DuckDBResultStore
from src.utils.store_context import store_connection


def test_store_connection_context_keeps_connection_open():
    store = DuckDBResultStore()
    with store_connection(store) as conn:
        assert conn.execute("SELECT 1").fetchone()[0] == 1
    # Connection should remain usable after the context manager exits
    assert conn.execute("SELECT 1").fetchone()[0] == 1
