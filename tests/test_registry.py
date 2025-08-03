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
    registry.MetricRegistry.instance()._metrics.pop("_dup", None)


def test_builtin_metric_retrieval():
    for name in registry.available_metrics():
        builder = registry.get_metric(name)
        if name in {"set_overlap_pct", "missing_values_cnt", "extra_values_cnt"}:
            with pytest.raises(ValueError):
                builder("col")
            expr = builder("col1", "col2")
        else:
            expr = builder("col")
        assert isinstance(expr, exp.Expression)
