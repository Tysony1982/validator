import pandas as pd
from pytest import approx
from sqlglot import select

from src.expectations.engines.duckdb import DuckDBEngine
from src.expectations.metrics.registry import get_metric


def _run_expr(engine: DuckDBEngine, table: str, expr):
    sql = select(expr).from_(table)
    return engine.run_sql(sql).iloc[0, 0]


def test_null_pct_sql():
    eng = DuckDBEngine()
    eng.register_dataframe("t", pd.DataFrame({"a": [1, 2, None]}))
    expr = get_metric("null_pct")("a")
    val = _run_expr(eng, "t", expr)
    assert val == approx(1 / 3)


def test_distinct_cnt():
    eng = DuckDBEngine()
    eng.register_dataframe("t", pd.DataFrame({"a": [1, 1, 2]}))
    expr = get_metric("distinct_cnt")("a")
    val = _run_expr(eng, "t", expr)
    assert val == 2


def test_min_max():
    eng = DuckDBEngine()
    eng.register_dataframe("t", pd.DataFrame({"a": [3, 1, 5]}))
    min_expr = get_metric("min")("a").as_("mn")
    max_expr = get_metric("max")("a").as_("mx")
    df = eng.run_sql(select(min_expr, max_expr).from_("t"))
    assert df.iloc[0]["mn"] == 1
    assert df.iloc[0]["mx"] == 5


def test_extra_metrics():
    eng = DuckDBEngine()
    eng.register_dataframe("t", pd.DataFrame({"a": [1, 2, None]}))

    nn_expr = get_metric("non_null_cnt")("a")
    avg_expr = get_metric("avg")("a")
    stddev_expr = get_metric("stddev")("a")

    df = eng.run_sql(
        select(nn_expr.as_("nn"), avg_expr.as_("avg"), stddev_expr.as_("sd")).from_("t")
    )
    assert df.iloc[0]["nn"] == 2
    assert df.iloc[0]["avg"] == approx(1.5)
    assert df.iloc[0]["sd"] == approx((0.5) ** 0.5)


def test_pct_where_builder(tmp_path):
    eng = DuckDBEngine()
    df = pd.DataFrame({"a": [1, 2, 3], "b": [1, 0, 1]})
    eng.register_dataframe("t", df)
    from src.expectations.metrics.registry import pct_where

    builder = pct_where("b = 1")
    expr = builder("a")
    val = _run_expr(eng, "t", expr)
    assert val == approx(2 / 3)
