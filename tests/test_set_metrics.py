import pandas as pd
import pytest
from pytest import approx
from sqlglot import select

from src.expectations.metrics.registry import get_metric


def _run_expr(engine, table: str, expr):
    sql = select(expr).from_(table)
    return engine.run_sql(sql).iloc[0, 0]


def test_set_overlap_pct_with_nulls(duckdb_engine):
    duckdb_engine.register_dataframe("t", pd.DataFrame({"a": [1, None, 2], "b": [1, 2, 3]}))
    expr = get_metric("set_overlap_pct")("a", "b")
    val = _run_expr(duckdb_engine, "t", expr)
    assert val == approx(2 / 3)


def test_set_overlap_pct_mismatched_schema(duckdb_engine):
    duckdb_engine.register_dataframe("t", pd.DataFrame({"a": [1, 2]}))
    expr = get_metric("set_overlap_pct")("a", "b")
    with pytest.raises(RuntimeError):
        _run_expr(duckdb_engine, "t", expr)


def test_set_overlap_pct_all_nulls(duckdb_engine):
    duckdb_engine.register_dataframe("t", pd.DataFrame({"a": [None, None], "b": [None, None]}))
    expr = get_metric("set_overlap_pct")("a", "b")
    val = _run_expr(duckdb_engine, "t", expr)
    assert pd.isna(val)


def test_set_overlap_pct_empty_table(duckdb_engine):
    duckdb_engine.register_dataframe("t", pd.DataFrame({"a": [], "b": []}))
    expr = get_metric("set_overlap_pct")("a", "b")
    val = _run_expr(duckdb_engine, "t", expr)
    assert pd.isna(val)


def test_missing_values_cnt_with_nulls(duckdb_engine):
    duckdb_engine.register_dataframe("t", pd.DataFrame({"a": [None, 1, 3], "b": [1, 2, None]}))
    expr = get_metric("missing_values_cnt")("a", "b")
    val = _run_expr(duckdb_engine, "t", expr)
    assert val == 1


def test_missing_values_cnt_mismatched_schema(duckdb_engine):
    duckdb_engine.register_dataframe("t", pd.DataFrame({"a": [1, 2]}))
    expr = get_metric("missing_values_cnt")("a", "b")
    with pytest.raises(RuntimeError):
        _run_expr(duckdb_engine, "t", expr)


def test_missing_values_cnt_empty_table(duckdb_engine):
    duckdb_engine.register_dataframe("t", pd.DataFrame({"a": [], "b": []}))
    expr = get_metric("missing_values_cnt")("a", "b")
    val = _run_expr(duckdb_engine, "t", expr)
    assert pd.isna(val)


def test_extra_values_cnt_with_nulls(duckdb_engine):
    duckdb_engine.register_dataframe("t", pd.DataFrame({"a": [None, 1, 3], "b": [1, 2, None]}))
    expr = get_metric("extra_values_cnt")("a", "b")
    val = _run_expr(duckdb_engine, "t", expr)
    assert val == 1


def test_extra_values_cnt_mismatched_schema(duckdb_engine):
    duckdb_engine.register_dataframe("t", pd.DataFrame({"a": [1, 2]}))
    expr = get_metric("extra_values_cnt")("a", "b")
    with pytest.raises(RuntimeError):
        _run_expr(duckdb_engine, "t", expr)


def test_extra_values_cnt_empty_table(duckdb_engine):
    duckdb_engine.register_dataframe("t", pd.DataFrame({"a": [], "b": []}))
    expr = get_metric("extra_values_cnt")("a", "b")
    val = _run_expr(duckdb_engine, "t", expr)
    assert pd.isna(val)
