import pandas as pd
from concurrent.futures import ThreadPoolExecutor

from src.expectations.metrics import registry
from src.expectations.engines.duckdb import DuckDBEngine
from src.expectations.runner import ValidationRunner
from src.expectations.validators.column import ColumnNotNull


def _worker(i: int) -> bool:
    name = f"dyn_metric_{i}"

    @registry.register_metric(name)
    def _metric(col: str):
        # simple identity metric
        from sqlglot import exp
        return exp.column(col)

    eng = DuckDBEngine(pool_size=2)
    eng.register_dataframe("t", pd.DataFrame({"a": [1, 2]}))
    runner = ValidationRunner({"duck": eng})
    res = runner.run([("duck", "t", ColumnNotNull(column="a"))], run_id=f"test_{i}")[0]

    # cleanup
    registry.MetricRegistry.instance()._metrics.pop(name, None)
    return res.success


def test_parallel_registry_and_engine():
    with ThreadPoolExecutor(max_workers=4) as exe:
        results = list(exe.map(_worker, range(4)))
    assert all(results)
