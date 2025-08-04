import pandas as pd
import pytest
import hypothesis.strategies as st
from hypothesis import HealthCheck, given, settings
from src.expectations.engines.duckdb import DuckDBEngine

DuckDBEngine.__repr__ = lambda self: "<DuckDBEngine>"

from src.expectations.validators.column import (
    ColumnNotNull,
    ColumnNullPct,
    ColumnDistinctCount,
    ColumnMin,
    ColumnMax,
    ColumnPercentile,
    ColumnValueInSet,
    ColumnMatchesRegex,
    ColumnRange,
    ColumnLength,
    ColumnGreaterEqual,
    ColumnUniquenessValidator,
)
from src.expectations.validators.table import RowCountValidator


def _run(runner, table, validator):
    return runner.run([("duck", table, validator)], run_id="test")[0]


def test_column_not_null_pass_fail(duckdb_engine, validation_runner):
    duckdb_engine.register_dataframe("t1", pd.DataFrame({"a": [1, 2]}))
    duckdb_engine.register_dataframe("t2", pd.DataFrame({"a": [1, None]}))

    assert _run(validation_runner, "t1", ColumnNotNull(column="a")).success is True
    assert _run(validation_runner, "t2", ColumnNotNull(column="a")).success is False


def test_column_null_pct_threshold(duckdb_engine, validation_runner):
    df = pd.DataFrame({"a": [1, None, None]})
    duckdb_engine.register_dataframe("t", df)

    assert _run(validation_runner, "t", ColumnNullPct(column="a", max_null_pct=0.7)).success is True
    assert _run(validation_runner, "t", ColumnNullPct(column="a", max_null_pct=0.6)).success is False


def test_column_distinct_count_ops(duckdb_engine, validation_runner):
    df = pd.DataFrame({"a": [1, 1, 2, 3]})
    duckdb_engine.register_dataframe("t", df)

    cases = [
        ("==", True),
        (">=", True),
        ("<=", True),
        (">", False),
        ("<", False),
    ]
    for op, expected in cases:
        res = _run(validation_runner, "t", ColumnDistinctCount(column="a", expected=3, op=op))
        assert res.success is expected


@pytest.mark.parametrize(
    "op, expected",
    [
        ("==", True),
        (">=", True),
        ("<=", True),
        (">", False),
        ("<", False),
    ],
)
def test_column_distinct_count_parametrized(duckdb_engine, validation_runner, op, expected):
    df = pd.DataFrame({"a": [1, 1, 2, 3]})
    duckdb_engine.register_dataframe("t", df)
    res = _run(validation_runner, "t", ColumnDistinctCount(column="a", expected=3, op=op))
    assert res.success is expected


def test_column_min_max_strict_vs_inclusive(duckdb_engine, validation_runner):
    df = pd.DataFrame({"a": [1, 2, 3]})
    duckdb_engine.register_dataframe("t", df)

    assert _run(validation_runner, "t", ColumnMin(column="a", min_value=1)).success is True
    assert (
        _run(validation_runner, "t", ColumnMin(column="a", min_value=1, strict=True)).success
        is False
    )
    assert _run(validation_runner, "t", ColumnMax(column="a", max_value=3)).success is True
    assert (
        _run(validation_runner, "t", ColumnMax(column="a", max_value=3, strict=True)).success
        is False
    )


def test_column_value_in_set(duckdb_engine, validation_runner):
    df = pd.DataFrame({"a": ["A", "B", "A"]})
    duckdb_engine.register_dataframe("t", df)
    ok = _run(validation_runner, "t", ColumnValueInSet(column="a", allowed_values=["A", "B"]))
    assert ok.success is True
    fail = _run(validation_runner, "t", ColumnValueInSet(column="a", allowed_values=["A"]))
    assert fail.success is False


def test_column_matches_regex(duckdb_engine, validation_runner):
    df = pd.DataFrame({"a": ["x1", "y2", "z"]})
    duckdb_engine.register_dataframe("t", df)
    v = ColumnMatchesRegex(column="a", pattern="^[a-z][0-9]")
    assert _run(validation_runner, "t", v).success is False


def test_column_range(duckdb_engine, validation_runner):
    df = pd.DataFrame({"a": [1, 2, 3]})
    duckdb_engine.register_dataframe("t", df)
    assert (
        _run(validation_runner, "t", ColumnRange(column="a", min_value=1, max_value=3)).success
        is True
    )
    assert (
        _run(
            validation_runner,
            "t",
            ColumnRange(column="a", min_value=1, max_value=3, strict=True),
        ).success
        is False
    )


def test_column_greater_equal(duckdb_engine, validation_runner):
    df = pd.DataFrame({"a": [1, 2], "b": [2, 1]})
    duckdb_engine.register_dataframe("t", df)
    v = ColumnGreaterEqual(column="b", other_column="a")
    assert _run(validation_runner, "t", v).success is False


def test_column_uniqueness_validator(duckdb_engine, validation_runner):
    df_unique = pd.DataFrame({"a": [1, 2, 3]})
    duckdb_engine.register_dataframe("t1", df_unique)
    ok = _run(validation_runner, "t1", ColumnUniquenessValidator(column="a"))
    assert ok.success is True

    df_dup = pd.DataFrame({"a": [1, 1, 2]})
    duckdb_engine.register_dataframe("t2", df_dup)
    fail = _run(validation_runner, "t2", ColumnUniquenessValidator(column="a"))
    assert fail.success is False


def test_column_length_basic_pass_fail(duckdb_engine, validation_runner):
    df = pd.DataFrame({"a": ["abc", "d", "efgh"]})
    duckdb_engine.register_dataframe("t", df)
    validator = ColumnLength(column="a", min_length=2, max_length=3)
    res = _run(validation_runner, "t", validator)
    assert res.success is False
    assert validator.invalid_cnt == 2


def test_column_length_trim_option(duckdb_engine, validation_runner):
    df = pd.DataFrame({"a": [" a ", "bb ", "cc "]})
    duckdb_engine.register_dataframe("t", df)
    v_trim = ColumnLength(column="a", max_length=2, trim=True)
    assert _run(validation_runner, "t", v_trim).success is True
    v_no_trim = ColumnLength(column="a", max_length=2, trim=False)
    assert _run(validation_runner, "t", v_no_trim).success is False


def test_column_length_min_only(duckdb_engine, validation_runner):
    df_pass = pd.DataFrame({"a": ["ab", "cd"]})
    duckdb_engine.register_dataframe("t1", df_pass)
    assert _run(validation_runner, "t1", ColumnLength(column="a", min_length=2)).success is True

    df_fail = pd.DataFrame({"a": ["a", "bc"]})
    duckdb_engine.register_dataframe("t2", df_fail)
    assert _run(validation_runner, "t2", ColumnLength(column="a", min_length=2)).success is False


def test_column_length_max_only(duckdb_engine, validation_runner):
    df_pass = pd.DataFrame({"a": ["a", "bb"]})
    duckdb_engine.register_dataframe("t1", df_pass)
    assert _run(validation_runner, "t1", ColumnLength(column="a", max_length=2)).success is True

    df_fail = pd.DataFrame({"a": ["abc", "d"]})
    duckdb_engine.register_dataframe("t2", df_fail)
    assert _run(validation_runner, "t2", ColumnLength(column="a", max_length=2)).success is False


def test_column_length_requires_bounds():
    with pytest.raises(ValueError):
        ColumnLength(column="a")


def test_column_length_where_clause_filtering(duckdb_engine, validation_runner):
    df = pd.DataFrame({"a": ["x", "long"], "b": [0, 1]})
    duckdb_engine.register_dataframe("t", df)
    v_pass = ColumnLength(column="a", max_length=1, where="b = 0")
    assert _run(validation_runner, "t", v_pass).success is True
    v_fail = ColumnLength(column="a", max_length=1, where="b = 1")
    assert _run(validation_runner, "t", v_fail).success is False


def test_column_min_where_clause(duckdb_engine, validation_runner):
    df = pd.DataFrame({"a": [1, 3], "b": [0, 1]})
    duckdb_engine.register_dataframe("t", df)
    v_pass = ColumnMin(column="a", min_value=3, where="b = 1")
    assert _run(validation_runner, "t", v_pass).success is True
    v_fail = ColumnMin(column="a", min_value=2, where="b = 0")
    assert _run(validation_runner, "t", v_fail).success is False


def test_column_max_where_clause(duckdb_engine, validation_runner):
    df = pd.DataFrame({"a": [3, 10], "b": [0, 1]})
    duckdb_engine.register_dataframe("t", df)
    v_pass = ColumnMax(column="a", max_value=3, where="b = 0")
    assert _run(validation_runner, "t", v_pass).success is True
    v_fail = ColumnMax(column="a", max_value=5, where="b = 1")
    assert _run(validation_runner, "t", v_fail).success is False


def test_where_clause_filters_rows(duckdb_engine, validation_runner):
    df = pd.DataFrame({"a": [1, None], "b": [0, 1]})
    duckdb_engine.register_dataframe("t", df)
    v_pass = RowCountValidator(min_rows=1, max_rows=1, where="b = 0")
    res_pass = _run(validation_runner, "t", v_pass)
    assert res_pass.success is True
    v_fail = RowCountValidator(min_rows=1, max_rows=0, where="b = 1")
    res_fail = _run(validation_runner, "t", v_fail)
    assert res_fail.success is False


def test_row_count_where_clause(duckdb_engine, validation_runner):
    df = pd.DataFrame({"a": [1, 2, 3], "b": [1, 1, 2]})
    duckdb_engine.register_dataframe("t", df)
    v = RowCountValidator(min_rows=1, max_rows=1, where="b = 2")
    res = _run(validation_runner, "t", v)
    assert res.success is True


def test_column_percentile(duckdb_engine, validation_runner):
    df = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
    duckdb_engine.register_dataframe("t", df)
    expected = float(df["a"].quantile(0.9))
    v_pass = ColumnPercentile(column="a", q=0.9, expected=expected, tolerance=1e-6)
    assert _run(validation_runner, "t", v_pass).success is True
    v_fail = ColumnPercentile(column="a", q=0.9, expected=expected - 1.0, tolerance=1e-6)
    assert _run(validation_runner, "t", v_fail).success is False


def test_column_uniqueness_from_yaml(tmp_path, duckdb_engine, validation_runner):
    yaml_content = """
suite_name: s
engine: duck
table: t
expectations:
  - expectation_type: ColumnUniquenessValidator
    column: a
"""
    path = tmp_path / "suite.yml"
    path.write_text(yaml_content)

    from src.expectations.config.expectation import ExpectationSuiteConfig

    cfg = ExpectationSuiteConfig.from_yaml(path)
    duckdb_engine.register_dataframe("t", pd.DataFrame({"a": [1, 2, 3]}))
    res = validation_runner.run(list(cfg.build_validators()), run_id="test")[0]
    assert res.success is True


# ---------------------------------------------------------------------------
# Hypothesis-based property tests
# ---------------------------------------------------------------------------


@st.composite
def range_with_edges(draw):
    min_v = draw(st.integers(-1000, 1000))
    max_v = draw(st.integers(min_v + 1, min_v + 1000))
    inner = draw(st.lists(st.integers(min_v, max_v), min_size=1))
    values = inner + [min_v, max_v]
    return min_v, max_v, values


@st.composite
def strict_range_values(draw):
    min_v = draw(st.integers(-1000, 1000))
    max_v = draw(st.integers(min_v + 2, min_v + 1000))
    values = draw(st.lists(st.integers(min_v + 1, max_v - 1), min_size=1))
    return min_v, max_v, values


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(
    data=st.lists(st.one_of(st.none(), st.integers()), min_size=1),
    max_null_pct=st.floats(
        min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False
    ),
)
def test_column_null_pct_property(
    duckdb_engine, validation_runner, data, max_null_pct
):
    df = pd.DataFrame({"a": data})
    duckdb_engine.register_dataframe("t", df)
    validator = ColumnNullPct(column="a", max_null_pct=max_null_pct)
    result = _run(validation_runner, "t", validator)
    actual_null_pct = df["a"].isna().mean()
    assert result.success == (actual_null_pct <= max_null_pct)
    assert 0.0 <= validator.null_pct <= 1.0


@given(
    max_null_pct=st.floats(allow_nan=False, allow_infinity=False).filter(
        lambda x: x < 0 or x > 1
    )
)
def test_column_null_pct_invalid_bounds(max_null_pct):
    with pytest.raises(ValueError):
        ColumnNullPct(column="a", max_null_pct=max_null_pct)


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(range_with_edges())
def test_column_range_strict_bounds(duckdb_engine, validation_runner, data):
    min_v, max_v, values = data
    df = pd.DataFrame({"a": values})
    duckdb_engine.register_dataframe("t", df)
    inclusive = ColumnRange(column="a", min_value=min_v, max_value=max_v)
    strict = ColumnRange(column="a", min_value=min_v, max_value=max_v, strict=True)
    assert _run(validation_runner, "t", inclusive).success is True
    assert _run(validation_runner, "t", strict).success is False


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(strict_range_values())
def test_column_range_strict_passes(duckdb_engine, validation_runner, data):
    min_v, max_v, values = data
    df = pd.DataFrame({"a": values})
    duckdb_engine.register_dataframe("t", df)
    strict = ColumnRange(column="a", min_value=min_v, max_value=max_v, strict=True)
    assert _run(validation_runner, "t", strict).success is True
