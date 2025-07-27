# src/expectations/metrics/utils.py
from __future__ import annotations

from sqlglot import exp, parse_one, ParseError
from src.expectations.errors import ValidationConfigError

# ------------------------------------------------------------------ #
#   Build a bullet-proof "bad node" tuple                         #
# ------------------------------------------------------------------ #
_DDL_NODE_NAMES = (
    # canonical names (exist in >=27)
    "Drop", "Create", "Insert", "Update", "Delete", "Alter", "Merge",
    # TRUNCATE/Table variants
    "TruncateTable", "Truncate",
    # privilege verbs (may or may not exist)
    "Grant", "Revoke",
    # generic catch-all for older versions
    "Command",
)

_BAD_NODE_TYPES = tuple(
    cls for name in _DDL_NODE_NAMES if (cls := getattr(exp, name, None))
)

# ------------------------------------------------------------------ #
#   Fallback bool-expr tester for nodes that lack .is_boolean      #
# ------------------------------------------------------------------ #
_FALLBACK_BOOLEAN_NODES = (
    exp.And, exp.Or, exp.Not,
    exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE,
    exp.Between, exp.In, exp.Like, exp.ILike, exp.RegexpLike,
    exp.Is, exp.Boolean,
)

def _is_boolean(expr: exp.Expression) -> bool:
    flag = getattr(expr, "is_boolean", None)
    return flag if flag is not None else isinstance(expr, _FALLBACK_BOOLEAN_NODES)

# ------------------------------------------------------------------ #
#   Public validator                                              #
# ------------------------------------------------------------------ #
def validate_filter_sql(sql: str) -> exp.Expression:
    """
    Ensure *sql* is a safe, BOOLEAN-returning WHERE predicate.

    Raises ValidationConfigError on any problem.
    """
    try:
        tree = parse_one(sql, error_level="raise")
    except ParseError as exc:
        raise ValidationConfigError(f"Invalid filter SQL: {exc}") from exc

    # No multi-statement delimiters
    if ";" in sql:
        raise ValidationConfigError("Semicolons are not allowed in filter clauses")

    # Disallow any DDL / DML constructs present in this sqlglot build
    if any(isinstance(node, _BAD_NODE_TYPES) for node in tree.walk()):
        raise ValidationConfigError("Filter contains disallowed SQL constructs")

    # Must be a boolean predicate
    if not _is_boolean(tree):
        raise ValidationConfigError("Filter clause must evaluate to BOOLEAN")

    return tree
