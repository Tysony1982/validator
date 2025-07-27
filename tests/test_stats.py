import pandas as pd

from src.expectations.engines.duckdb import DuckDBEngine
from src.expectations.stats import TableStatsCollector
from src.expectations.store import DuckDBResultStore
from src.expectations.result_model import RunMetadata


def test_collect_and_persist_stats(tmp_path):
    eng = DuckDBEngine()
    df = pd.DataFrame({"a": [1, 2, None], "b": [5, 6, 7]})
    eng.register_dataframe("t", df)

    store = DuckDBResultStore(eng)
    store.connection.execute("DELETE FROM statistics")

    collector = TableStatsCollector({"duck": eng})
    run = RunMetadata(suite_name="stats")
    stats = collector.collect("duck", "t", run_id=run.run_id)
    store.persist_stats(run, stats)

    df_stats = store.connection.execute(
        "SELECT metric, column_name, value FROM statistics WHERE run_id = ?",
        (run.run_id,),
    ).fetchdf()

    assert {"row_cnt", "null_pct", "min", "max"}.issubset(set(df_stats["metric"]))
    row_cnt = float(df_stats[df_stats["metric"] == "row_cnt"]["value"].iloc[0])
    assert row_cnt == 3.0
