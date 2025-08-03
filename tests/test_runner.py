import pandas as pd
import pytest

from src.expectations.validators.column import ColumnNotNull, ColumnNullPct
from src.expectations.validators.table import DuplicateRowValidator
from src.expectations.validators.base import ValidatorBase


class FaultyValidator(ValidatorBase):
    @classmethod
    def kind(cls):
        return "custom"

    def custom_sql(self, table: str):
        return "SELECT * FROM nonexistent"

    def interpret(self, value):
        return True


def test_metric_grouping(duckdb_engine, validation_runner, monkeypatch):
    duckdb_engine.register_dataframe("t", pd.DataFrame({"a": [1, 2, None]}))

    calls = []
    original = duckdb_engine.run_sql

    def spy(sql):
        calls.append(sql)
        return original(sql)

    monkeypatch.setattr(duckdb_engine, "run_sql", spy)

    bindings = [
        ("duck", "t", ColumnNotNull(column="a")),
        ("duck", "t", ColumnNullPct(column="a", max_null_pct=1.0)),
    ]
    validation_runner.run(bindings, run_id="test")
    assert len(calls) == 1


def test_metric_and_custom_calls(duckdb_engine, validation_runner, monkeypatch):
    duckdb_engine.register_dataframe("t", pd.DataFrame({"a": [1, 1]}))
    calls = []
    original = duckdb_engine.run_sql

    def spy(sql):
        calls.append(sql)
        return original(sql)

    monkeypatch.setattr(duckdb_engine, "run_sql", spy)

    bindings = [
        ("duck", "t", ColumnNotNull(column="a")),
        ("duck", "t", DuplicateRowValidator(key_columns=["a"])),
    ]
    validation_runner.run(bindings, run_id="test")
    assert len(calls) == 2


def test_error_propagation(duckdb_engine, validation_runner):
    duckdb_engine.register_dataframe("t", pd.DataFrame({"a": [1]}))

    res = validation_runner.run([("duck", "t", FaultyValidator())], run_id="test")[0]
    assert res.success is False
    assert "error" in res.details
    assert "traceback" in res.details


def test_metric_error_capture(duckdb_engine, validation_runner):
    res = validation_runner.run([("duck", "missing", ColumnNotNull(column="a"))], run_id="test")[0]
    assert res.success is False
    assert "error" in res.details
    assert "traceback" in res.details
