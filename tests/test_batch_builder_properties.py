import hypothesis.strategies as st
from hypothesis import given

from src.expectations.metrics.batch_builder import MetricBatchBuilder, MetricRequest
from src.expectations.metrics.registry import available_metrics


# Strategy for simple WHERE predicates
_columns = st.sampled_from(["a", "b", "c"])
_ops = st.sampled_from([">", "<", "=", ">=", "<=", "!="])
_numbers = st.integers(min_value=0, max_value=10)

simple_predicate = st.one_of(
    st.builds(lambda c, op, n: f"{c} {op} {n}", _columns, _ops, _numbers),
    st.builds(lambda c: f"{c} IS NULL", _columns),
    st.builds(lambda c: f"{c} IS NOT NULL", _columns),
)

filter_strategy = st.one_of(st.none(), simple_predicate)
# Metrics like set comparisons require multiple columns. The batch builder only
# supports single-column metrics, so exclude those multi-column metrics from the
# strategy.
_single_col_metrics = [
    m
    for m in available_metrics()
    if m not in {"set_overlap_pct", "missing_values_cnt", "extra_values_cnt"}
]
metric_strategy = st.sampled_from(_single_col_metrics)


@given(metric=metric_strategy, filter_sql=filter_strategy)
def test_batch_builder_generates_sql(metric, filter_sql):
    req = MetricRequest(column="col", metric=metric, alias="m", filter_sql=filter_sql)
    builder = MetricBatchBuilder(table="t", requests=[req])
    ast = builder.build_query_ast()
    for dialect in ["duckdb", "postgres", "snowflake"]:
        ast.sql(dialect=dialect, pretty=False)
