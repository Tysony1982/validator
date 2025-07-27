Validator Reference
===================

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

Example YAML::

    - expectation_type: ColumnGreaterEqual
      column: end_date
      other_column: start_date

ColumnMatchesRegex
------------------

**Signature**::

    ColumnMatchesRegex(column: 'str', pattern: 'str', **kw)

Passes when every value matches ``pattern``.

Example YAML::

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

Example YAML::

    - expectation_type: ColumnRange
      column: price
      min_value: 0
      max_value: 100

ColumnValueInSet
----------------

**Signature**::

    ColumnValueInSet(column: 'str', allowed_values: 'list[str]', allow_null: 'bool' = False, **kw)

Passes when all values are within ``allowed_values``.

Example YAML::

    - expectation_type: ColumnValueInSet
      column: status
      allowed_values: [OPEN, CLOSED]

DuplicateRowValidator
---------------------

**Signature**::

    DuplicateRowValidator(key_columns: 'Sequence[str]')

Checks for duplicate rows based on a list of *key_columns*.

Passes when the duplicate count == 0.

*kind()* returns "custom" → executed in its own query.

PrimaryKeyUniquenessValidator
-----------------------------

**Signature**::

    PrimaryKeyUniquenessValidator(key_columns: 'Sequence[str]')

Passes when the set of ``key_columns`` uniquely identifies each row.

Example YAML::

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

Example YAML::

    - expectation_type: SqlErrorRows
      sql: |
        SELECT * FROM my_table WHERE bad_condition
      max_error_rows: 10
