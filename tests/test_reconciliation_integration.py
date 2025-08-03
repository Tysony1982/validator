import pandas as pd

from src.expectations.engines.duckdb import DuckDBEngine
from src.expectations.engines.file import FileEngine
from src.expectations.runner import ValidationRunner
from src.expectations.validators.reconciliation import (
    ColumnReconciliationValidator,
    TableReconciliationValidator,
)
from src.expectations.utils.mappings import ColumnMapping


def test_duckdb_file_reconciliation_success(tmp_path):
    df = pd.DataFrame({"a": [1, None, 2]})
    csv = tmp_path / "data.csv"
    df.to_csv(csv, index=False)

    duck = DuckDBEngine()
    duck.register_dataframe("t", df)
    file_eng = FileEngine(csv, table="f")

    v_table = TableReconciliationValidator(
        comparer_engine=file_eng, comparer_table="f"
    )
    v_col = ColumnReconciliationValidator(
        column_map=ColumnMapping("a"),
        primary_engine=duck,
        primary_table="t",
        comparer_engine=file_eng,
        comparer_table="f",
    )
    runner = ValidationRunner({"primary": duck, "file": file_eng})
    res_table, res_col = runner.run(
        [("primary", "t", v_table), ("primary", "t", v_col)], run_id="test"
    )
    assert res_table.success is True
    assert res_col.success is True
    file_eng.close()


def test_duckdb_file_reconciliation_mismatch(tmp_path):
    primary_df = pd.DataFrame({"a": [1, 2]})
    comparer_df = pd.DataFrame({"a": [1, 2, 3]})
    csv = tmp_path / "data.csv"
    comparer_df.to_csv(csv, index=False)

    duck = DuckDBEngine()
    duck.register_dataframe("t", primary_df)
    file_eng = FileEngine(csv, table="f")

    v_table = TableReconciliationValidator(
        comparer_engine=file_eng, comparer_table="f"
    )
    v_col = ColumnReconciliationValidator(
        column_map=ColumnMapping("a"),
        primary_engine=duck,
        primary_table="t",
        comparer_engine=file_eng,
        comparer_table="f",
    )
    runner = ValidationRunner({"primary": duck, "file": file_eng})
    res_table, res_col = runner.run(
        [("primary", "t", v_table), ("primary", "t", v_col)], run_id="test"
    )
    assert res_table.success is False
    assert res_table.details["primary"] == 2
    assert res_table.details["comparer"] == 3
    assert res_col.success is False
    file_eng.close()
