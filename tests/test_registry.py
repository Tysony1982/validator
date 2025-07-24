import pytest
from sqlglot import exp

from src.expectations.metrics import registry


def test_register_metric_duplicate_key():
    @registry.register_metric("_dup")
    def _metric(col: str) -> exp.Expression:
        return exp.column(col)

    with pytest.raises(KeyError):
        @registry.register_metric("_dup")
        def _metric2(col: str) -> exp.Expression:  # pragma: no cover - should not be executed
            return exp.column(col)
    # cleanup
    del registry._METRICS["_dup"]


def test_builtin_metric_retrieval():
    for name in registry.available_metrics():
        expr = registry.get_metric(name)("col")
        assert isinstance(expr, exp.Expression)
