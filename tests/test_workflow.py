import json

from src.expectations.workflow import run_validations
from src.expectations.runner import ValidationRunner
from src.expectations.validators.column import ColumnNotNull
from src.expectations.store import FileResultStore


def test_run_validations_empty_bindings(tmp_path):
    runner = ValidationRunner({})
    store = FileResultStore(tmp_path)

    run, results = run_validations(
        suite_name="suite1",
        bindings=[],
        runner=runner,
        store=store,
    )

    assert run.engine_name is None
    assert results == []

    run_file = tmp_path / "runs" / f"{run.run_id}.json"
    res_file = tmp_path / "results" / f"{run.run_id}.jsonl"
    assert run_file.exists()
    assert res_file.exists()

    data = json.loads(run_file.read_text())
    assert data["engine_name"] is None
    assert res_file.read_text() == ""


def test_run_validations_schema_extraction(tmp_path, duckdb_engine):
    duckdb_engine.run_sql("CREATE SCHEMA s; CREATE TABLE s.t(a INT); INSERT INTO s.t VALUES (1);")
    runner = ValidationRunner({"duck": duckdb_engine})
    store = FileResultStore(tmp_path)

    run, results = run_validations(
        suite_name="suite1",
        bindings=[("duck", "s.t", ColumnNotNull(column="a"))],
        runner=runner,
        store=store,
    )

    assert run.db_schema == "s"
    assert run.engine_name == "duck"
    assert results and results[0].db_schema == "s"

    run_file = tmp_path / "runs" / f"{run.run_id}.json"
    res_file = tmp_path / "results" / f"{run.run_id}.jsonl"
    assert run_file.exists()
    assert res_file.exists()
    assert json.loads(run_file.read_text())["db_schema"] == "s"
    assert json.loads(res_file.read_text().splitlines()[0])["db_schema"] == "s"
