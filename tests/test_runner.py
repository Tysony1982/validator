import pandas as pd
import pytest

from src.expectations.engines.duckdb import DuckDBEngine
from src.expectations.runner import ValidationRunner
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


def test_metric_grouping(monkeypatch):
    eng = DuckDBEngine()
    eng.register_dataframe("t", pd.DataFrame({"a": [1, 2, None]}))

    calls = []
    original = eng.run_sql

    def spy(sql):
        calls.append(sql)
        return original(sql)

    monkeypatch.setattr(eng, "run_sql", spy)

    runner = ValidationRunner({"duck": eng})
    bindings = [
        ("duck", "t", ColumnNotNull(column="a")),
        ("duck", "t", ColumnNullPct(column="a", max_null_pct=1.0)),
    ]
    runner.run(bindings, run_id="test")
    assert len(calls) == 1


def test_metric_and_custom_calls(monkeypatch):
    eng = DuckDBEngine()
    eng.register_dataframe("t", pd.DataFrame({"a": [1, 1]}))
    calls = []
    original = eng.run_sql

    def spy(sql):
        calls.append(sql)
        return original(sql)

    monkeypatch.setattr(eng, "run_sql", spy)

    runner = ValidationRunner({"duck": eng})
    bindings = [
        ("duck", "t", ColumnNotNull(column="a")),
        ("duck", "t", DuplicateRowValidator(key_columns=["a"])),
    ]
    runner.run(bindings, run_id="test")
    assert len(calls) == 2


def test_error_propagation(monkeypatch):
    eng = DuckDBEngine()
    eng.register_dataframe("t", pd.DataFrame({"a": [1]}))

    runner = ValidationRunner({"duck": eng})
    res = runner.run([("duck", "t", FaultyValidator())], run_id="test")[0]
    assert res.success is False
    assert "error" in res.details
