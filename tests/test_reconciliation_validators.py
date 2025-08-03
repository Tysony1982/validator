import pandas as pd

import pandas as pd

from src.expectations.engines.duckdb import DuckDBEngine
from src.expectations.runner import ValidationRunner
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
        column="a", comparer_engine=comparer, comparer_table="t2"
    )
    res_pass = _run({"primary": primary, "comp": comparer}, "t1", v_pass)
    assert res_pass.success is True

    comparer.register_dataframe("t3", pd.DataFrame({"a": [1, 5]}))
    v_fail = ColumnReconciliationValidator(
        column="a", comparer_engine=comparer, comparer_table="t3"
    )
    res_fail = _run({"primary": primary, "comp": comparer}, "t1", v_fail)
    assert res_fail.success is False
    assert res_fail.details["primary"]["row_cnt"] == 3
    assert res_fail.details["comparer"]["row_cnt"] == 2
