import pandas as pd

from src.expectations.engines.duckdb import DuckDBEngine
from src.expectations.runner import ValidationRunner
from src.expectations.validators.column import (
    ColumnNotNull,
    ColumnNullPct,
    ColumnDistinctCount,
    ColumnMin,
    ColumnMax,
)


def _run(eng, table, validator):
    runner = ValidationRunner({"duck": eng})
    return runner.run([("duck", table, validator)])[0]


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


def test_column_min_max_strict_vs_inclusive():
    eng = DuckDBEngine()
    df = pd.DataFrame({"a": [1, 2, 3]})
    eng.register_dataframe("t", df)

    assert _run(eng, "t", ColumnMin(column="a", min_value=1)).success is True
    assert _run(eng, "t", ColumnMin(column="a", min_value=1, strict=True)).success is False
    assert _run(eng, "t", ColumnMax(column="a", max_value=3)).success is True
    assert _run(eng, "t", ColumnMax(column="a", max_value=3, strict=True)).success is False
