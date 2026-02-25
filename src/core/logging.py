"""
Logging utilities with automatic credential redaction.
Ensures sensitive data is never exposed in logs.
"""

import logging
import re
from typing import Any

# Patterns to detect and redact sensitive information
SENSITIVE_PATTERNS = [
    # OAuth tokens and API keys
    (re.compile(r'"token":\s*"[^"]+'), '"token": "[REDACTED]'),
    (re.compile(r'"access_token":\s*"[^"]+'), '"access_token": "[REDACTED]'),
    (re.compile(r'"refresh_token":\s*"[^"]+'), '"refresh_token": "[REDACTED]'),
    (re.compile(r'"client_secret":\s*"[^"]+'), '"client_secret": "[REDACTED]'),
    (re.compile(r'"api_key":\s*"[^"]+'), '"api_key": "[REDACTED]'),
    (re.compile(r'"secret_key":\s*"[^"]+'), '"secret_key": "[REDACTED]'),
    # Authorization headers
    (re.compile(r'Authorization:\s*Bearer\s+\S+', re.IGNORECASE), 'Authorization: Bearer [REDACTED]'),
    (re.compile(r'Authorization:\s*\S+', re.IGNORECASE), 'Authorization: [REDACTED]'),
    # Password patterns
    (re.compile(r'"password":\s*"[^"]+'), '"password": "[REDACTED]'),
    (re.compile(r'password=\S+', re.IGNORECASE), 'password=[REDACTED]'),
    # Database connection strings with credentials
    (re.compile(r'postgresql://[^:]+:[^@]+@'), 'postgresql://[REDACTED]:[REDACTED]@'),
    # Generic key-value pairs that might contain secrets
    (re.compile(r'credentials=\{[^}]+\}'), 'credentials={[REDACTED]}'),
]


def redact_sensitive_data(message: str) -> str:
    """
    Redact sensitive information from log messages.

    Args:
        message: Log message that may contain sensitive data

    Returns:
        Message with sensitive data replaced with [REDACTED]
    """
    redacted = message
    for pattern, replacement in SENSITIVE_PATTERNS:
        redacted = pattern.sub(replacement, redacted)
    return redacted


class RedactingFormatter(logging.Formatter):
    """Custom logging formatter that redacts sensitive information."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record with sensitive data redacted."""
        # Format the record normally first
        formatted = super().format(record)
        # Redact sensitive information
        return redact_sensitive_data(formatted)


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance with redacting formatter.

    Args:
        name: Logger name (typically __name__)

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)

    # Only configure if no handlers exist
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = RedactingFormatter(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    return logger


def safe_repr(obj: Any, redact_keys: list[str] | None = None) -> str:
    """
    Create a safe string representation of an object with sensitive keys redacted.

    Args:
        obj: Object to represent
        redact_keys: Additional keys to redact (e.g., ['password', 'api_key'])

    Returns:
        String representation with sensitive data redacted
    """
    if redact_keys is None:
        redact_keys = []

    # Default sensitive keys
    default_redact_keys = [
        "password",
        "token",
        "access_token",
        "refresh_token",
        "client_secret",
        "api_key",
        "secret_key",
        "credentials",
    ]
    all_redact_keys = set(default_redact_keys + redact_keys)

    if isinstance(obj, dict):
        safe_dict = {}
        for key, value in obj.items():
            if key.lower() in all_redact_keys or any(k in key.lower() for k in all_redact_keys):
                safe_dict[key] = "[REDACTED]"
            else:
                safe_dict[key] = safe_repr(value, redact_keys)
        return str(safe_dict)
    elif isinstance(obj, (list, tuple)):
        return str([safe_repr(item, redact_keys) for item in obj])
    else:
        return str(obj)
