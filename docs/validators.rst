Validator Reference
===================

The validator system is organized into categories that mirror the Python
modules under ``src/expectations/validators``:

* ``column`` – column-level checks such as :class:`ColumnNotNull`.
* ``table`` – whole-table checks like :class:`RowCountValidator`.
* ``custom`` – ad-hoc or user-defined SQL validators.
* ``reconciliation`` – cross-table comparisons.

Validators are executed by the :doc:`runner` and can emit useful
:doc:`metrics` or participate in :doc:`reconciliation` workflows.

ColumnDistinctCount
-------------------

**Signature**::

    ColumnDistinctCount(column: 'str', expected: 'int', op: 'str' = '==', **kw)

Compares COUNT(DISTINCT column) with an *expected* value
(== by default, or ≥ / ≤ via ``op`` parameter).

ColumnGreaterEqual
------------------

**Signature**::

    ColumnGreaterEqual(column: 'str', other_column: 'str', **kw)

Passes when ``column`` ≥ ``other_column`` row-wise.

Example YAML:

.. code-block:: yaml

   - expectation_type: ColumnGreaterEqual
     column: end_date
     other_column: start_date

ColumnMatchesRegex
------------------

**Signature**::

    ColumnMatchesRegex(column: 'str', pattern: 'str', **kw)

Passes when every value matches ``pattern``.

Example YAML:

.. code-block:: yaml

   - expectation_type: ColumnMatchesRegex
     column: email
     pattern: "^[A-Za-z]+@example.com$"

ColumnMax
---------

**Signature**::

    ColumnMax(column: 'str', max_value: 'Any', strict: 'bool' = False, **kw)

Passes when MAX(column) ≤ ``max_value`` (inclusive by default).

ColumnMin
---------

**Signature**::

    ColumnMin(column: 'str', min_value: 'Any', strict: 'bool' = False, **kw)

Passes when MIN(column) ≥ ``min_value`` (inclusive by default).

ColumnNotNull
-------------

**Signature**::

    ColumnNotNull(column: 'str', where: 'str | None' = None, **kwargs)

Passes when *no* NULLs are present in the column.

ColumnNullPct
-------------

**Signature**::

    ColumnNullPct(column: 'str', max_null_pct: 'float', **kw)

Passes when NULL percentage ≤ ``max_null_pct`` (0-1 range).

ColumnRange
-----------

**Signature**::

    ColumnRange(column: 'str', min_value: 'Any', max_value: 'Any', strict: 'bool' = False, **kw)

Passes when values fall between ``min_value`` and ``max_value``.

Example YAML:

.. code-block:: yaml

   - expectation_type: ColumnRange
     column: price
     min_value: 0
     max_value: 100

ColumnReconciliationValidator
-----------------------------

**Signature**::

    ColumnReconciliationValidator(column_map: 'ColumnMapping', primary_engine: 'BaseEngine', primary_table: 'str', comparer_engine: 'BaseEngine', comparer_table: 'str', where: 'str | None' = None, comparer_where: 'str | None' = None)

Compare simple column metrics between two engines.

The validator runs a set of basic metrics on the *primary* engine and the
provided ``comparer_engine`` and succeeds when all metrics match exactly.

Parameters
----------
column_map : :class:`~src.expectations.utils.mappings.ColumnMapping`
    Mapping between the primary and comparer columns.  Allows name
    remapping and value type conversions.
primary_engine : BaseEngine
    Engine for the primary table used for validation of the mapping.
primary_table : str
    Table name on the primary engine.
comparer_engine : BaseEngine
    Engine used for the comparison query.
comparer_table : str
    Table name on the comparer engine.
where : str, optional
    Optional SQL filter for the primary engine.
comparer_where : str, optional
    Optional SQL filter for the comparer engine.

Examples
--------
Basic usage compares the same column on two engines::

    mapping = ColumnMapping("a")
    ColumnReconciliationValidator(
        column_map=mapping,
        primary_engine=primary,
        primary_table="t1",
        comparer_engine=comparer,
        comparer_table="t2",
    )

Column mappings can rename and cast values::

    mapping = ColumnMapping(
        primary="id",
        comparer="user_id",
        comparer_type=int,
    )
    ColumnReconciliationValidator(
        column_map=mapping,
        primary_engine=primary,
        primary_table="users",
        comparer_engine=comparer,
        comparer_table="users_copy",
        where="active = 1",
        comparer_where="status = 'active'",
    )
    <ColumnReconciliationValidator>

ColumnValueInSet
----------------

**Signature**::

    ColumnValueInSet(column: 'str', allowed_values: 'list[str]', allow_null: 'bool' = False, **kw)

Passes when all values are within ``allowed_values``.

Example YAML:

.. code-block:: yaml

   - expectation_type: ColumnValueInSet
     column: status
     allowed_values: [OPEN, CLOSED]

ColumnZScoreOutlierRowsValidator
--------------------------------

**Signature**::

    ColumnZScoreOutlierRowsValidator(column: 'str', z_thresh: 'float' = 3.0, max_error_rows: 'int' = 20, **kw)

Return rows where ``ABS((col - μ)/σ)`` exceeds ``z_thresh``.

DuplicateRowValidator
---------------------

**Signature**::

    DuplicateRowValidator(key_columns: 'Sequence[str]')

Checks for duplicate rows based on a list of *key_columns*.

Passes when the duplicate count == 0.

*kind()* returns "custom" → executed in its own query.

MetricDriftValidator
--------------------

**Signature**::

    MetricDriftValidator(column: 'str | None', metric: 'str', window: 'int' = 20, z_thresh: 'float' = 3.0, result_store, **kw)

Detect drift in any registered metric via rolling z-score.

PrimaryKeyUniquenessValidator
-----------------------------

**Signature**::

    PrimaryKeyUniquenessValidator(key_columns: 'Sequence[str]')

Passes when the set of ``key_columns`` uniquely identifies each row.

Example YAML:

.. code-block:: yaml

   - expectation_type: PrimaryKeyUniquenessValidator
     key_columns: [id]

RowCountValidator
-----------------

**Signature**::

    RowCountValidator(min_rows: 'int | None' = None, max_rows: 'int | None' = None, where: 'str | None' = None)

Passes when the table row count is within [min_rows, max_rows] bounds.
Either bound can be ``None`` to disable that side.

SqlErrorRowsValidator
---------------------

**Signature**::

    SqlErrorRowsValidator(sql: 'str', max_error_rows: 'int' = 20, severity: 'str' = 'FAIL', tags: 'list[str] | None' = None)

Run ad-hoc SQL that returns error rows.

Example YAML:

.. code-block:: yaml

   - expectation_type: SqlErrorRows
     sql: |
       SELECT * FROM my_table WHERE bad_condition
     max_error_rows: 10

TableReconciliationValidator
----------------------------

**Signature**::

    TableReconciliationValidator(comparer_engine: 'BaseEngine', comparer_table: 'str', where: 'str | None' = None, comparer_where: 'str | None' = None)

Compare table row counts between two engines.

Parameters
----------
comparer_engine : BaseEngine
    Engine used for the comparison query.
comparer_table : str
    Table name on the comparer engine.
where : str, optional
    Optional SQL filter for the primary engine.
comparer_where : str, optional
    Optional SQL filter for the comparer engine.

Examples
--------
Basic usage::

    TableReconciliationValidator(
        comparer_engine=comparer,
        comparer_table="t2",
    )

Apply filters when validating a subset of rows::

    TableReconciliationValidator(
        comparer_engine=comparer,
        comparer_table="t2",
        where="active = 1",
        comparer_where="status = 'active'",
    )
    <TableReconciliationValidator>
