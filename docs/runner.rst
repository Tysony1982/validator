Validation Runner
=================

The :class:`~src.expectations.runner.ValidationRunner` executes
expectations produced from configuration. It groups metric validators
per engine/table into a single SQL query and runs custom validators
individually. See :doc:`Validator Reference <validators>` for the list of
available expectation types.

Validator Bindings
------------------

Bindings are a three-part tuple ``(engine, table, validator)``.  They are
usually created from an :class:`ExpectationSuiteConfig`::

    suite:
      suite_name: user_table
      engine: duck
      table: analytics.users
      expectations:
        - expectation_type: ColumnNotNull
          column: id

.. code-block:: python

    from src.expectations.config.expectation import ExpectationSuiteConfig

    suite = ExpectationSuiteConfig.from_file("suite.yml")
    bindings = suite.build_validators()
    runner = ValidationRunner({"duck": engine})
    results = runner.run(bindings, run_id="demo")

Suite and SLA Configuration
---------------------------

Multiple suites can be orchestrated through
:func:`~src.expectations.workflow.run_validations` and optionally grouped
under an :class:`SLAConfig`.

Example suite YAML::

    suite_name: users
    engine: duck
    table: analytics.users
    expectations:
      - expectation_type: ColumnNotNull
        column: id

Example SLA YAML::

    sla_name: nightly-sla
    suites:
      - suite_name: users
        engine: duck
        table: analytics.users
        expectations:
          - expectation_type: ColumnNotNull
            column: id
      - suite_name: orders
        engine: duck
        table: analytics.orders
        expectations:
          - expectation_type: RowCountValidator
            min_rows: 1

.. code-block:: python

    from src.expectations.workflow import run_validations
    from src.expectations.config.expectation import SLAConfig

    sla = SLAConfig.from_file("sla.yml")
    bindings = sla.build_validators()
    run, results = run_validations(
        suite_name="nightly",
        bindings=bindings,
        runner=runner,
        store=my_store,
        sla_config=sla,
    )

Concurrency and Error Handling
------------------------------

``ValidationRunner`` executes bindings synchronously. Metric validators
for the same engine and table are batched into one SQL statement while
custom validators run one at a time. SQL or runtime errors are caught and
recorded in each :class:`ValidationResult` so the remaining validators can
continue running.

