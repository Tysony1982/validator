import pandas as pd
from sqlglot import select

from src.expectations.metrics.registry import get_metric


def _run_expr(engine, table: str, expr):
    sql = select(expr).from_(table)
    return engine.run_sql(sql).iloc[0, 0]


def test_duplicate_row_metric_counts_groups(duckdb_engine):
    df = pd.DataFrame({"a": [1, 1, 2, 3, 3], "b": [1, 1, 2, 3, 3]})
    duckdb_engine.register_dataframe("t", df)
    expr = get_metric("duplicate_row_cnt")("a,b")
    val = _run_expr(duckdb_engine, "t", expr)
    assert val == 2


def test_duplicate_row_metric_counts_all_duplicates(duckdb_engine):
    df = pd.DataFrame({"a": [1, 1, 1, 2, 2, 3], "b": [1, 1, 1, 2, 2, 3]})
    duckdb_engine.register_dataframe("t", df)
    expr = get_metric("duplicate_row_cnt")("a,b")
    val = _run_expr(duckdb_engine, "t", expr)
    assert val == 3


def test_duplicate_row_metric_no_dups(duckdb_engine):
    df = pd.DataFrame({"a": [1, 2], "b": [1, 2]})
    duckdb_engine.register_dataframe("t", df)
    expr = get_metric("duplicate_row_cnt")("a,b")
    val = _run_expr(duckdb_engine, "t", expr)
    assert val == 0


def test_duplicate_cnt_metric_counts_duplicates(duckdb_engine):
    df = pd.DataFrame({"a": [1, 1, 2, 3, 3]})
    duckdb_engine.register_dataframe("t", df)
    expr = get_metric("duplicate_cnt")("a")
    val = _run_expr(duckdb_engine, "t", expr)
    assert val == 2


def test_duplicate_cnt_metric_no_dups(duckdb_engine):
    df = pd.DataFrame({"a": [1, 2]})
    duckdb_engine.register_dataframe("t", df)
    expr = get_metric("duplicate_cnt")("a")
    val = _run_expr(duckdb_engine, "t", expr)
    assert val == 0
