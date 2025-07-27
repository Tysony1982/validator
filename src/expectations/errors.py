"""Custom exception classes for the expectations package."""

class ValidationConfigError(ValueError):
    """Raised when configuration contains invalid or potentially malicious SQL."""

