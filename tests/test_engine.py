import pandas as pd

from src.expectations.engines.base import BaseEngine


def test_duckdb_list_columns(duckdb_engine):
    df = pd.DataFrame({"a": [1], "b": [2]})
    duckdb_engine.register_dataframe("t", df)
    cols = set(duckdb_engine.list_columns("t"))
    assert cols == {"a", "b"}


def test_run_many_fallback():
    class DummyEngine(BaseEngine):
        def run_sql(self, sql):
            return pd.DataFrame({"v": [sql]})

        def list_columns(self, table):
            return []

        def get_dialect(self):
            return "ansi"

        def close(self):
            pass

    eng = DummyEngine()
    res = eng.run_many(["SELECT 1", "SELECT 2"])
    assert len(res) == 2
