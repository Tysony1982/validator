import pandas as pd

from src.expectations.store import DuckDBResultStore
from src.expectations.validators.column import MetricDriftValidator
from src.expectations.validators.custom import ColumnZScoreOutlierRowsValidator


def _run(runner, table, validator):
    return runner.run([("duck", table, validator)], run_id="test")[0]


def test_metric_drift_validator(duckdb_engine, validation_runner):
    store = DuckDBResultStore(duckdb_engine)
    store.connection.execute("DELETE FROM statistics")
    # insert synthetic history
    for i, val in enumerate([11, 9, 10, 11, 9, 10, 10, 10, 11, 9]):
        store.connection.execute(
            "INSERT INTO statistics VALUES (?, ?, ?, ?, ?, ?, ?)",
            (f"r{i}", "t", None, None, None, "row_cnt", val),
        )
    duckdb_engine.register_dataframe("t", pd.DataFrame({"a": range(15)}))
    v = MetricDriftValidator(metric="row_cnt", column=None, result_store=store, window=10, z_thresh=3.0)
    res = _run(validation_runner, "t", v)
    assert res.success is False
    assert v.details["z"] > 3


def test_column_zscore_outlier_rows_validator(duckdb_engine, validation_runner):
    df = pd.DataFrame({"a": [1, 2, 3, 100]})
    duckdb_engine.register_dataframe("t", df)
    v = ColumnZScoreOutlierRowsValidator(column="a", z_thresh=1.0)
    res = _run(validation_runner, "t", v)
    assert res.success is False
    assert res.details["error_row_count"] == 1
