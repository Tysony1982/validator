import pandas as pd
from sqlglot import select

from src.expectations.metrics.batch_builder import MetricBatchBuilder, MetricRequest
from src.expectations.engines.duckdb import DuckDBEngine


def test_alias_ordering():
    reqs = [
        MetricRequest(column="a", metric="row_cnt", alias="r1"),
        MetricRequest(column="a", metric="distinct_cnt", alias="d1"),
    ]
    builder = MetricBatchBuilder(table="t", requests=reqs)
    aliases = [exp.args["alias"].name for exp in builder.build_query_ast().expressions]
    assert aliases == ["r1", "d1"]


def test_sql_compiles_and_runs():
    eng = DuckDBEngine()
    eng.register_dataframe("t", pd.DataFrame({"a": [1, 2, 2]}))
    reqs = [
        MetricRequest(column="a", metric="row_cnt", alias="rc"),
        MetricRequest(column="a", metric="distinct_cnt", alias="dc"),
    ]
    builder = MetricBatchBuilder(table="t", requests=reqs, dialect="duckdb")
    sql = builder.sql()
    df = eng.run_sql(sql)
    assert set(df.columns) == {"rc", "dc"}

def test_apply_filter_generates_conditional_aggregates():
    req = MetricRequest(column="*", metric="row_cnt", alias="rc", filter_sql="a > 1")
    builder = MetricBatchBuilder(table="t", requests=[req], dialect="duckdb")
    sql = builder.sql()
    assert "CASE WHEN a > 1" in sql
    assert "SUM(CASE WHEN a > 1 THEN 1 END)" in sql

    req2 = MetricRequest(column="a", metric="max", alias="mx", filter_sql="b < 5")
    builder2 = MetricBatchBuilder(table="t", requests=[req2], dialect="duckdb")
    sql2 = builder2.sql()
    assert "CASE WHEN b < 5" in sql2
    assert "MAX(CASE WHEN b < 5 THEN a END)" in sql2

