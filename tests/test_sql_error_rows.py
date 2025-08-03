import pandas as pd

from src.expectations.validators.custom import SqlErrorRowsValidator
from src.expectations.validators.column import ColumnNotNull
from src.expectations.config.expectation import ExpectationSuiteConfig


def _run(runner, table, validator):
    return runner.run([("duck", table, validator)], run_id="test")[0]


def test_sql_error_rows_pass(duckdb_engine, validation_runner):
    duckdb_engine.register_dataframe("t", pd.DataFrame({"a": [1]}))
    v = SqlErrorRowsValidator(sql="SELECT * FROM t WHERE 1=0")
    res = _run(validation_runner, "t", v)
    assert res.success is True


def test_sql_error_rows_fail_details(duckdb_engine, validation_runner):
    duckdb_engine.register_dataframe("t", pd.DataFrame({"a": [1, 2, 3]}))
    v = SqlErrorRowsValidator(sql="SELECT * FROM t", max_error_rows=2)
    res = _run(validation_runner, "t", v)
    assert res.success is False
    assert res.details["error_row_count"] == 3
    assert len(res.details["error_rows_sample"]) <= 2


def test_runner_integration_with_config(tmp_path, duckdb_engine, validation_runner):
    duckdb_engine.register_dataframe("t", pd.DataFrame({"a": [1], "b": [1]}))
    yaml_content = """
suite_name: demo_custom
engine: duck
table: t
expectations:
  - expectation_type: SqlErrorRows
    sql: |
      SELECT * FROM t WHERE 1=0
  - expectation_type: ColumnNotNull
    column: b
"""
    path = tmp_path / "suite.yml"
    path.write_text(yaml_content)
    cfg = ExpectationSuiteConfig.from_yaml(path)
    results = validation_runner.run(cfg.build_validators(), run_id="test")
    assert len(results) == 2
