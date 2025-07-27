import pandas as pd
import sys
from src.expectations.engines.duckdb import DuckDBEngine
from src.expectations.runner import ValidationRunner
from src.expectations.config.expectation import ExpectationSuiteConfig
from src.expectations.validators.custom import SqlErrorRowsValidator
from src.expectations.validators.table import RowCountValidator, DuplicateRowValidator


def test_suite_execution_with_multiple_validations(tmp_path):
    eng = DuckDBEngine()
    df = pd.DataFrame({"a": [1, 2, 3, 3], "b": [1, 2, 2, 4]})
    eng.register_dataframe("t", df)
    yaml_content = """
suite_name: demo_suite
engine: duck
table: t
expectations:
  - expectation_type: RowCountValidator
    where: "a >= 2"
    kwargs:
      min_rows: 3
  - expectation_type: RowCountValidator
    where: "a = 1"
    kwargs:
      max_rows: 0
  - expectation_type: DuplicateRowValidator
    kwargs:
      key_columns: [a]
  - expectation_type: SqlErrorRows
    sql: SELECT * FROM t WHERE a < 0
"""
    path = tmp_path / "suite.yml"
    path.write_text(yaml_content)

    cfg = ExpectationSuiteConfig.from_yaml(path)
    runner = ValidationRunner({"duck": eng})
    results = runner.run(cfg.build_validators(), run_id="test")
    statuses = [r.success for r in results]
    assert statuses == [True, False, False, True]
    assert results[0].filter_sql == "a >= 2"


