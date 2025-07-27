import pandas as pd
import pytest
from sqlglot import select

from src.expectations.metrics.batch_builder import MetricBatchBuilder, MetricRequest
from src.expectations.errors import ValidationConfigError
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


def test_query_ast_compiles_multiple_dialects():
    reqs = [
        MetricRequest(column="a", metric="row_cnt", alias="rc"),
        MetricRequest(column="a", metric="distinct_cnt", alias="dc"),
    ]
    builder = MetricBatchBuilder(table="t", requests=reqs)
    ast = builder.build_query_ast()

    for dialect in ["duckdb", "postgres", "snowflake"]:
        ast.sql(dialect=dialect, pretty=False)



def test_malicious_filter_rejected():
    req = MetricRequest(column="a", metric="row_cnt", alias="rc", filter_sql="1; DROP TABLE x")
    builder = MetricBatchBuilder(table="t", requests=[req])
    with pytest.raises(ValidationConfigError):
        builder.sql()


def test_filtered_distinct_and_non_null_counts():
    eng = DuckDBEngine()
    df = pd.DataFrame({"a": [1, None, 2, 3], "b": [1, 1, 0, 1]})
    eng.register_dataframe("t", df)

    req1 = MetricRequest(column="a", metric="distinct_cnt", alias="dc", filter_sql="b = 1")
    req2 = MetricRequest(column="a", metric="non_null_cnt", alias="nn", filter_sql="b = 1")

    builder = MetricBatchBuilder(table="t", requests=[req1, req2], dialect="duckdb")
    sql = builder.sql()
    df_res = eng.run_sql(sql)

    assert df_res.loc[0, "dc"] == 2
    assert df_res.loc[0, "nn"] == 2
    assert "COUNT(DISTINCT CASE WHEN b = 1 THEN a END)" in sql
    assert "COUNT(CASE WHEN b = 1 AND NOT a IS NULL THEN 1 END)" in sql

