import pandas as pd
import pytest

from src.expectations.engines.duckdb import DuckDBEngine
from src.expectations.runner import ValidationRunner
from src.expectations.validators.column import (
    ColumnNotNull,
    ColumnNullPct,
    ColumnDistinctCount,
    ColumnMin,
    ColumnMax,
    ColumnValueInSet,
    ColumnMatchesRegex,
    ColumnRange,
    ColumnGreaterEqual,
)
from src.expectations.validators.table import RowCountValidator


def _run(eng, table, validator):
    runner = ValidationRunner({"duck": eng})
    return runner.run([("duck", table, validator)], run_id="test")[0]


def test_column_not_null_pass_fail():
    eng = DuckDBEngine()
    eng.register_dataframe("t1", pd.DataFrame({"a": [1, 2]}))
    eng.register_dataframe("t2", pd.DataFrame({"a": [1, None]}))

    assert _run(eng, "t1", ColumnNotNull(column="a")).success is True
    assert _run(eng, "t2", ColumnNotNull(column="a")).success is False


def test_column_null_pct_threshold():
    eng = DuckDBEngine()
    df = pd.DataFrame({"a": [1, None, None]})
    eng.register_dataframe("t", df)

    assert _run(eng, "t", ColumnNullPct(column="a", max_null_pct=0.7)).success is True
    assert _run(eng, "t", ColumnNullPct(column="a", max_null_pct=0.6)).success is False


def test_column_distinct_count_ops():
    eng = DuckDBEngine()
    df = pd.DataFrame({"a": [1, 1, 2, 3]})
    eng.register_dataframe("t", df)

    cases = [
        ("==", True),
        (">=", True),
        ("<=", True),
        (">", False),
        ("<", False),
    ]
    for op, expected in cases:
        res = _run(eng, "t", ColumnDistinctCount(column="a", expected=3, op=op))
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
def test_column_distinct_count_parametrized(op, expected):
    eng = DuckDBEngine()
    df = pd.DataFrame({"a": [1, 1, 2, 3]})
    eng.register_dataframe("t", df)
    res = _run(eng, "t", ColumnDistinctCount(column="a", expected=3, op=op))
    assert res.success is expected


def test_column_min_max_strict_vs_inclusive():
    eng = DuckDBEngine()
    df = pd.DataFrame({"a": [1, 2, 3]})
    eng.register_dataframe("t", df)

    assert _run(eng, "t", ColumnMin(column="a", min_value=1)).success is True
    assert (
        _run(eng, "t", ColumnMin(column="a", min_value=1, strict=True)).success is False
    )
    assert _run(eng, "t", ColumnMax(column="a", max_value=3)).success is True
    assert (
        _run(eng, "t", ColumnMax(column="a", max_value=3, strict=True)).success is False
    )


def test_column_value_in_set():
    eng = DuckDBEngine()
    df = pd.DataFrame({"a": ["A", "B", "A"]})
    eng.register_dataframe("t", df)
    ok = _run(eng, "t", ColumnValueInSet(column="a", allowed_values=["A", "B"]))
    assert ok.success is True
    fail = _run(eng, "t", ColumnValueInSet(column="a", allowed_values=["A"]))
    assert fail.success is False


def test_column_matches_regex():
    eng = DuckDBEngine()
    df = pd.DataFrame({"a": ["x1", "y2", "z"]})
    eng.register_dataframe("t", df)
    v = ColumnMatchesRegex(column="a", pattern="^[a-z][0-9]")
    assert _run(eng, "t", v).success is False


def test_column_range():
    eng = DuckDBEngine()
    df = pd.DataFrame({"a": [1, 2, 3]})
    eng.register_dataframe("t", df)
    assert (
        _run(eng, "t", ColumnRange(column="a", min_value=1, max_value=3)).success
        is True
    )
    assert (
        _run(
            eng, "t", ColumnRange(column="a", min_value=1, max_value=3, strict=True)
        ).success
        is False
    )


def test_column_greater_equal():
    eng = DuckDBEngine()
    df = pd.DataFrame({"a": [1, 2], "b": [2, 1]})
    eng.register_dataframe("t", df)
    v = ColumnGreaterEqual(column="b", other_column="a")
    assert _run(eng, "t", v).success is False


def test_column_min_where_clause():
    eng = DuckDBEngine()
    df = pd.DataFrame({"a": [1, 3], "b": [0, 1]})
    eng.register_dataframe("t", df)
    v_pass = ColumnMin(column="a", min_value=3, where="b = 1")
    assert _run(eng, "t", v_pass).success is True
    v_fail = ColumnMin(column="a", min_value=2, where="b = 0")
    assert _run(eng, "t", v_fail).success is False


def test_column_max_where_clause():
    eng = DuckDBEngine()
    df = pd.DataFrame({"a": [3, 10], "b": [0, 1]})
    eng.register_dataframe("t", df)
    v_pass = ColumnMax(column="a", max_value=3, where="b = 0")
    assert _run(eng, "t", v_pass).success is True
    v_fail = ColumnMax(column="a", max_value=5, where="b = 1")
    assert _run(eng, "t", v_fail).success is False

def test_where_clause_filters_rows():
    eng = DuckDBEngine()
    df = pd.DataFrame({"a": [1, None], "b": [0, 1]})
    eng.register_dataframe("t", df)
    v_pass = RowCountValidator(min_rows=1, max_rows=1, where="b = 0")
    res_pass = _run(eng, "t", v_pass)
    assert res_pass.success is True
    v_fail = RowCountValidator(min_rows=1, max_rows=0, where="b = 1")
    res_fail = _run(eng, "t", v_fail)
    assert res_fail.success is False


def test_row_count_where_clause():
    eng = DuckDBEngine()
    df = pd.DataFrame({"a": [1, 2, 3], "b": [1, 1, 2]})
    eng.register_dataframe("t", df)
    v = RowCountValidator(min_rows=1, max_rows=1, where="b = 2")
    res = _run(eng, "t", v)
    assert res.success is True


