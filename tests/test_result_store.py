import pandas as pd

from src.expectations.result_model import RunMetadata, ValidationResult
from src.expectations.store import DuckDBResultStore
from src.expectations.engines.duckdb import DuckDBEngine


def test_duckdb_store_persist(tmp_path):
    db = tmp_path / "res.db"
    if db.exists():
        db.unlink()
    engine = DuckDBEngine(db)
    store = DuckDBResultStore(engine)

    # ensure clean tables if they already exist
    store.connection.execute("DELETE FROM runs")
    store.connection.execute("DELETE FROM results")

    run = RunMetadata(suite_name="suite1")
    results = [
        ValidationResult(
            run_id=run.run_id,
            validator="ColumnNotNull",
            table="t",
            column="a",
            metric="null_pct",
            success=True,
            value=0.0,
        )
    ]

    store.persist_run(run, results)

    df_runs = store.connection.execute("SELECT * FROM runs").fetchdf()
    df_results = store.connection.execute("SELECT * FROM results").fetchdf()

    assert len(df_runs) == 1
    assert len(df_results) == 1
    assert bool(df_results.loc[0, "success"]) is True


def test_duckdb_store_persist_sla(tmp_path):
    db = tmp_path / "res.db"
    if db.exists():
        db.unlink()
    engine = DuckDBEngine(db)
    store = DuckDBResultStore(engine)

    store.connection.execute("DELETE FROM runs")
    store.connection.execute("DELETE FROM results")
    store.connection.execute("DELETE FROM slas")

    run = RunMetadata(suite_name="suite1", sla_name="sla1")
    results = [
        ValidationResult(
            run_id=run.run_id,
            validator="ColumnNotNull",
            table="t",
            column="a",
            metric="null_pct",
            success=True,
            value=0.0,
        )
    ]

    from src.expectations.config.expectation import SLAConfig, ExpectationSuiteConfig

    sla_cfg = SLAConfig(
        sla_name="sla1",
        suites=[ExpectationSuiteConfig(suite_name="suite1", engine="duck", table="t", expectations=[])]
    )

    store.persist_run(run, results, sla_cfg)

    df_runs = store.connection.execute("SELECT * FROM runs").fetchdf()
    df_slas = store.connection.execute("SELECT * FROM slas").fetchdf()

    assert len(df_runs) == 1
    assert df_runs.loc[0, "sla_name"] == "sla1"
    assert len(df_slas) == 1
