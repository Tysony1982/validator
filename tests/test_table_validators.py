import pandas as pd

from src.expectations.engines.duckdb import DuckDBEngine
from src.expectations.runner import ValidationRunner
from src.expectations.validators.table import RowCountValidator, DuplicateRowValidator


def _run(eng, table, validator):
    runner = ValidationRunner({"duck": eng})
    return runner.run([("duck", table, validator)])[0]


def test_row_count_bounds():
    eng = DuckDBEngine()
    eng.register_dataframe("t0", pd.DataFrame({"a": []}))
    eng.register_dataframe("t1", pd.DataFrame({"a": [1]}))
    eng.register_dataframe("t5", pd.DataFrame({"a": range(5)}))

    v = RowCountValidator(min_rows=1, max_rows=3)
    assert _run(eng, "t0", RowCountValidator(min_rows=1, max_rows=3)).success is False
    assert _run(eng, "t1", RowCountValidator(min_rows=1, max_rows=3)).success is True
    assert _run(eng, "t5", RowCountValidator(min_rows=1, max_rows=3)).success is False


def test_duplicate_row_validator():
    eng = DuckDBEngine()
    df_dup = pd.DataFrame({"a": [1, 1, 2], "b": [1, 1, 2]})
    df_ok = pd.DataFrame({"a": [1, 2, 3], "b": [1, 2, 3]})
    eng.register_dataframe("dup", df_dup)
    eng.register_dataframe("ok", df_ok)

    assert _run(eng, "dup", DuplicateRowValidator(key_columns=["a", "b"])).success is False
    assert _run(eng, "ok", DuplicateRowValidator(key_columns=["a", "b"])).success is True
