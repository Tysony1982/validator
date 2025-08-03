import pandas as pd

from src.expectations.config.expectation import SLAConfig
from src.expectations.store import DuckDBResultStore
from src.expectations.workflow import run_validations

def test_full_end_to_end_workflow(tmp_path, sample_tables, validation_runner):
    eng = sample_tables

    sla_yaml = """
    sla_name: ecommerce
    suites:
      - suite_name: orders_suite
        engine: duck
        table: orders
        expectations:
          - expectation_type: RowCountValidator
            kwargs:
              min_rows: 1
          - expectation_type: ColumnNotNull
            column: order_id
          - expectation_type: ColumnMin
            column: amount
            kwargs:
              min_value: 0
          - expectation_type: DuplicateRowValidator
            kwargs:
              key_columns: [order_id]
          - expectation_type: SqlErrorRows
            sql: SELECT * FROM orders WHERE amount < 0
            max_error_rows: 5
      - suite_name: customers_suite
        engine: duck
        table: customers
        expectations:
          - expectation_type: PrimaryKeyUniquenessValidator
            kwargs:
              key_columns: [customer_id]
          - expectation_type: ColumnMatchesRegex
            column: email
            kwargs:
              pattern: '^[^@]+@[^@]+\\.[^@]+$'
          - expectation_type: ColumnNotNull
            column: email
          - expectation_type: ColumnDistinctCount
            column: customer_id
            kwargs:
              expected: 3
              op: '=='
    """
    path = tmp_path / "sla.yml"
    path.write_text(sla_yaml)

    sla_cfg = SLAConfig.from_yaml(path)

    runner = validation_runner
    store = DuckDBResultStore(eng)

    store.connection.execute("DELETE FROM runs")
    store.connection.execute("DELETE FROM results")
    store.connection.execute("DELETE FROM slas")

    for suite in sla_cfg.suites:
        run_validations(
            suite_name=suite.suite_name,
            bindings=suite.build_validators(),
            runner=runner,
            store=store,
            sla_config=sla_cfg,
        )

    df_runs = store.connection.execute("SELECT * FROM runs").fetchdf()
    df_results = store.connection.execute("SELECT * FROM results").fetchdf()
    df_slas = store.connection.execute("SELECT * FROM slas").fetchdf()

    assert len(df_slas) == 1
    assert df_slas.loc[0, "sla_name"] == "ecommerce"

    assert len(df_runs) == 2
    assert set(df_runs["suite_name"]) == {"orders_suite", "customers_suite"}
    assert set(df_runs["engine_name"]) == {"duck"}

    assert len(df_results) == 9
    assert set(df_results["engine_name"]) == {"duck"}
    assert df_results["success"].sum() == 3
