import pandas as pd
from faker import Faker
from src.expectations.engines.duckdb import DuckDBEngine


fake = Faker()
Faker.seed(0)


def sample_orders_df() -> pd.DataFrame:
    """Return a small orders DataFrame with typical issues using Faker."""
    statuses = ["open", "shipped", "cancelled"]
    rows = [
        {
            "order_id": 1,
            "customer_id": 1,
            "amount": fake.pyfloat(min_value=50, max_value=200, right_digits=2),
            "status": fake.random_element(statuses),
        },
        {
            "order_id": 2,
            "customer_id": 2,
            "amount": fake.pyfloat(min_value=100, max_value=300, right_digits=2),
            "status": fake.random_element(statuses),
        },
        {
            "order_id": 3,
            "customer_id": 2,
            "amount": -50,
            "status": fake.random_element(statuses),
        },
        {
            "order_id": 4,
            "customer_id": 3,
            "amount": fake.pyfloat(min_value=100, max_value=400, right_digits=2),
            "status": fake.random_element(statuses),
        },
        {
            "order_id": 4,
            "customer_id": 4,
            "amount": None,
            "status": fake.random_element(statuses),
        },
    ]
    return pd.DataFrame(rows)


def sample_customers_df() -> pd.DataFrame:
    """Return a small customers DataFrame with duplicates and bad emails using Faker."""
    emails = [fake.email(), fake.email(), None, "invalid_email"]
    return pd.DataFrame({"customer_id": [1, 2, 3, 2], "email": emails})


def setup_sample_engine() -> DuckDBEngine:
    """Create a DuckDB engine preloaded with sample tables."""
    eng = DuckDBEngine()
    eng.register_dataframe("orders", sample_orders_df())
    eng.register_dataframe("customers", sample_customers_df())
    return eng
