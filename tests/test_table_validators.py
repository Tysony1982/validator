import pandas as pd

from src.expectations.validators.table import (
    RowCountValidator,
    DuplicateRowValidator,
    PrimaryKeyUniquenessValidator,
    TableFreshnessValidator,
)


def _run(runner, table, validator):
    return runner.run([("duck", table, validator)], run_id="test")[0]


def test_row_count_bounds(duckdb_engine, validation_runner):
    duckdb_engine.register_dataframe("t0", pd.DataFrame({"a": []}))
    duckdb_engine.register_dataframe("t1", pd.DataFrame({"a": [1]}))
    duckdb_engine.register_dataframe("t5", pd.DataFrame({"a": range(5)}))

    assert (
        _run(validation_runner, "t0", RowCountValidator(min_rows=1, max_rows=3)).success
        is False
    )
    assert (
        _run(validation_runner, "t1", RowCountValidator(min_rows=1, max_rows=3)).success
        is True
    )
    assert (
        _run(validation_runner, "t5", RowCountValidator(min_rows=1, max_rows=3)).success
        is False
    )


def test_duplicate_row_validator(duckdb_engine, validation_runner):
    df_dup = pd.DataFrame({"a": [1, 1, 2], "b": [1, 1, 2]})
    df_ok = pd.DataFrame({"a": [1, 2, 3], "b": [1, 2, 3]})
    duckdb_engine.register_dataframe("dup", df_dup)
    duckdb_engine.register_dataframe("ok", df_ok)

    assert (
        _run(
            validation_runner, "dup", DuplicateRowValidator(key_columns=["a", "b"])
        ).success
        is False
    )
    assert (
        _run(
            validation_runner, "ok", DuplicateRowValidator(key_columns=["a", "b"])
        ).success
        is True
    )


def test_primary_key_uniqueness(duckdb_engine, validation_runner):
    duckdb_engine.register_dataframe("t", pd.DataFrame({"id": [1, 1, 2]}))
    v = PrimaryKeyUniquenessValidator(key_columns=["id"])
    assert _run(validation_runner, "t", v).success is False
    duckdb_engine.register_dataframe("t2", pd.DataFrame({"id": [1, 2]}))
    assert (
        _run(
            validation_runner,
            "t2",
            PrimaryKeyUniquenessValidator(key_columns=["id"]),
        ).success
        is True
    )


def test_table_freshness(duckdb_engine, validation_runner):
    now = pd.Timestamp.utcnow()
    fresh = pd.DataFrame({"ts": [now]})
    stale = pd.DataFrame({"ts": [now - pd.Timedelta(hours=2)]})
    duckdb_engine.register_dataframe("fresh", fresh)
    duckdb_engine.register_dataframe("stale", stale)

    v_fresh = TableFreshnessValidator(timestamp_column="ts", threshold="1h")
    assert _run(validation_runner, "fresh", v_fresh).success is True

    v_stale = TableFreshnessValidator(timestamp_column="ts", threshold="1h")
    assert _run(validation_runner, "stale", v_stale).success is False
