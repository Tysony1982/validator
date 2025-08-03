import pytest
from src.expectations.engines.duckdb import DuckDBEngine
from src.expectations.runner import ValidationRunner
from .utils import sample_orders_df, sample_customers_df


@pytest.fixture
def duckdb_engine():
    """Return a fresh DuckDBEngine instance."""
    eng = DuckDBEngine()
    yield eng
    # ensure resources are released
    eng.close()


@pytest.fixture
def sample_tables(duckdb_engine: DuckDBEngine):
    """Register sample orders and customers tables on the provided engine."""
    duckdb_engine.register_dataframe("orders", sample_orders_df())
    duckdb_engine.register_dataframe("customers", sample_customers_df())
    return duckdb_engine


@pytest.fixture
def validation_runner(sample_tables: DuckDBEngine):
    """Provide a ValidationRunner wired to the sample DuckDB engine."""
    return ValidationRunner({"duck": sample_tables})
