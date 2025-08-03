Reconciliation
==============

Reconciliation validators compare data across two engines to ensure that
records remain consistent when copied or migrated.  They operate on a
``primary`` engine and a ``comparer`` engine and are useful when the same
dataset is stored in multiple systems.

Table Reconciliation
--------------------

Use :class:`~src.expectations.validators.reconciliation.TableReconciliationValidator`
when you only need to confirm that both tables contain the same number of rows.

Example YAML::

    - expectation_type: TableReconciliationValidator
      comparer_engine: warehouse
      comparer_table: users_copy
      where: "active = 1"
      comparer_where: "status = 'active'"

Column Reconciliation
---------------------

:class:`~src.expectations.validators.reconciliation.ColumnReconciliationValidator`
compares basic metrics (row count, minimum and maximum) between two columns.
Mappings can rename columns and cast values to handle type differences.

Example YAML::

    - expectation_type: ColumnReconciliationValidator
      column_map:
        primary: id
        comparer: user_id
        comparer_type: int
      primary_engine: duck
      primary_table: users
      comparer_engine: warehouse
      comparer_table: users_copy
      where: "active = 1"
      comparer_where: "status = 'active'"

Best Practices
--------------

* Run a table reconciliation first to catch major discrepancies early.
* Apply identical ``where`` filters on both engines when validating a subset
  of data.
* Use ``column_map`` to handle renamed columns or type conversions.
* Reconcile one column at a time to keep failures easy to interpret.

Troubleshooting
---------------

* Row count mismatches usually indicate missing or duplicate records.
  Verify filters and ensure both engines are reading the same data.
* If metrics do not match, check the mapped column types and confirm that
  conversions (e.g., ``int`` vs ``str``) are correct.
* Inspect the validator ``details`` for the primary and comparer values to
  pinpoint differences.
* Ensure both engines are reachable and have permissions to read the relevant
  tables.
