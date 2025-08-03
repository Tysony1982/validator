import pandas as pd

import pytest


from src.expectations.engines.duckdb import DuckDBEngine
from src.expectations.runner import ValidationRunner
from src.expectations.utils.mappings import ColumnMapping
from src.expectations.validators.reconciliation import (
    ColumnReconciliationValidator,
    TableReconciliationValidator,
)


def _run(eng_map, table, validator):
    runner = ValidationRunner(eng_map)
    # primary engine key assumed to be 'primary'
    return runner.run([("primary", table, validator)], run_id="test")[0]


def test_table_reconciliation_validator():
    primary = DuckDBEngine()
    comparer = DuckDBEngine()
    primary.register_dataframe("t1", pd.DataFrame({"a": [1, 2, 3]}))
    comparer.register_dataframe("t2", pd.DataFrame({"a": [1, 2, 3]}))

    v_pass = TableReconciliationValidator(
        comparer_engine=comparer, comparer_table="t2"
    )
    res_pass = _run({"primary": primary, "comp": comparer}, "t1", v_pass)
    assert res_pass.success is True

    comparer.register_dataframe("t3", pd.DataFrame({"a": [1]}))
    v_fail = TableReconciliationValidator(
        comparer_engine=comparer, comparer_table="t3"
    )
    res_fail = _run({"primary": primary, "comp": comparer}, "t1", v_fail)
    assert res_fail.success is False
    assert res_fail.details["primary"] == 3
    assert res_fail.details["comparer"] == 1


def test_column_reconciliation_validator():
    primary = DuckDBEngine()
    comparer = DuckDBEngine()
    primary.register_dataframe("t1", pd.DataFrame({"a": [1, 2, 3]}))
    comparer.register_dataframe("t2", pd.DataFrame({"a": [1, 2, 3]}))

    v_pass = ColumnReconciliationValidator(
        column_map=ColumnMapping("a"),
        primary_engine=primary,
        primary_table="t1",
        comparer_engine=comparer,
        comparer_table="t2",
    )
    res_pass = _run({"primary": primary, "comp": comparer}, "t1", v_pass)
    assert res_pass.success is True

    comparer.register_dataframe("t3", pd.DataFrame({"a": [1, 5]}))
    v_fail = ColumnReconciliationValidator(
        column_map=ColumnMapping("a"),
        primary_engine=primary,
        primary_table="t1",
        comparer_engine=comparer,
        comparer_table="t3",
    )
    res_fail = _run({"primary": primary, "comp": comparer}, "t1", v_fail)
    assert res_fail.success is False
    assert res_fail.details["primary"]["row_cnt"] == 3
    assert res_fail.details["comparer"]["row_cnt"] == 2



def test_column_mapping_with_renames_and_conversion():
    primary = DuckDBEngine()
    comparer = DuckDBEngine()
    primary.register_dataframe("t1", pd.DataFrame({"a": [1, 2, 3]}))
    comparer.register_dataframe("t2", pd.DataFrame({"b": ["1", "2", "3"]}))

    mapping = ColumnMapping("a", comparer="b", primary_type=int, comparer_type=int)
    v = ColumnReconciliationValidator(
        column_map=mapping,
        primary_engine=primary,
        primary_table="t1",
        comparer_engine=comparer,
        comparer_table="t2",
    )
    res = _run({"primary": primary, "comp": comparer}, "t1", v)
    assert res.success is True



def test_column_reconciliation_mismatched_schema():
    primary = DuckDBEngine()
    comparer = DuckDBEngine()
    primary.register_dataframe("t1", pd.DataFrame({"a": [1, 2]}))
    comparer.register_dataframe("t2", pd.DataFrame({"b": [1, 2]}))

    v = ColumnReconciliationValidator(
        column="a", comparer_engine=comparer, comparer_table="t2"
    )
    res = _run({"primary": primary, "comp": comparer}, "t1", v)
    assert res.success is False
    assert "error" in res.details


def test_column_reconciliation_empty_tables():
    primary = DuckDBEngine()
    comparer = DuckDBEngine()
    primary.register_dataframe("t1", pd.DataFrame({"a": []}))
    comparer.register_dataframe("t2", pd.DataFrame({"a": []}))

    v = ColumnReconciliationValidator(
        column="a", comparer_engine=comparer, comparer_table="t2"
    )
    res = _run({"primary": primary, "comp": comparer}, "t1", v)
    assert res.success is False
    assert res.details["primary"]["row_cnt"] == 0
    assert res.details["comparer"]["row_cnt"] == 0


def test_table_reconciliation_mismatched_schema():
    primary = DuckDBEngine()
    comparer = DuckDBEngine()
    primary.register_dataframe("t1", pd.DataFrame({"a": [1, 2]}))
    comparer.register_dataframe("t2", pd.DataFrame({"b": [1, 2]}))

    v = TableReconciliationValidator(
        comparer_engine=comparer, comparer_table="t2"
    )
    res = _run({"primary": primary, "comp": comparer}, "t1", v)
    assert res.success is True


def test_table_reconciliation_empty_tables():
    primary = DuckDBEngine()
    comparer = DuckDBEngine()
    primary.register_dataframe("t1", pd.DataFrame({"a": []}))
    comparer.register_dataframe("t2", pd.DataFrame({"a": []}))

    v = TableReconciliationValidator(
        comparer_engine=comparer, comparer_table="t2"
    )
    res = _run({"primary": primary, "comp": comparer}, "t1", v)
    assert res.success is True
    assert res.details == {"primary": 0, "comparer": 0}


def test_table_reconciliation_one_empty():
    primary = DuckDBEngine()
    comparer = DuckDBEngine()
    primary.register_dataframe("t1", pd.DataFrame({"a": [1]}))
    comparer.register_dataframe("t2", pd.DataFrame({"a": []}))

    v = TableReconciliationValidator(
        comparer_engine=comparer, comparer_table="t2"
    )
    res = _run({"primary": primary, "comp": comparer}, "t1", v)
    assert res.success is False
    assert res.details["primary"] == 1
    assert res.details["comparer"] == 0

def test_column_mapping_validation():
    primary = DuckDBEngine()
    comparer = DuckDBEngine()
    primary.register_dataframe("t1", pd.DataFrame({"a": [1]}))
    comparer.register_dataframe("t2", pd.DataFrame({"a": [1]}))

    with pytest.raises(ValueError):
        ColumnReconciliationValidator(
            column_map=ColumnMapping("missing"),
            primary_engine=primary,
            primary_table="t1",
            comparer_engine=comparer,
            comparer_table="t2",
        )

    with pytest.raises(ValueError):
        ColumnReconciliationValidator(
            column_map=ColumnMapping("a", comparer="missing"),
            primary_engine=primary,
            primary_table="t1",
            comparer_engine=comparer,
            comparer_table="t2",
        )

