from __future__ import annotations

"""Helper utilities for metric helpers."""

from sqlglot import exp, parse_one

from src.expectations.errors import ValidationConfigError


_SUSPICIOUS_PATTERNS = (";", "--", "/*")


def validate_filter_sql(sql: str) -> exp.Expression:
    """Parse *sql* ensuring it doesn't contain obviously malicious constructs."""
    if any(p in sql for p in _SUSPICIOUS_PATTERNS):
        raise ValidationConfigError("Suspicious tokens detected in filter SQL")
    try:
        return parse_one(sql)
    except Exception as exc:  # pragma: no cover - just re-wrap
        raise ValidationConfigError(f"Invalid filter SQL: {exc}") from exc
