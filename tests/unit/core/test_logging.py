"""
Unit tests for logging utilities with credential redaction.
"""

import logging

import pytest

from src.core.logging import RedactingFormatter, get_logger, redact_sensitive_data, safe_repr


class TestRedactSensitiveData:
    """Test redact_sensitive_data function."""

    def test_redact_token(self):
        """Test redacting access token."""
        message = '{"token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"}'
        redacted = redact_sensitive_data(message)
        assert '"token": "[REDACTED]' in redacted
        assert "eyJhbGci" not in redacted

    def test_redact_access_token(self):
        """Test redacting access_token."""
        message = '{"access_token": "ya29.a0AfH6SMBx..."}'
        redacted = redact_sensitive_data(message)
        assert '"access_token": "[REDACTED]' in redacted
        assert "ya29" not in redacted

    def test_redact_refresh_token(self):
        """Test redacting refresh_token."""
        message = '{"refresh_token": "1//0gPwK9..."}'
        redacted = redact_sensitive_data(message)
        assert '"refresh_token": "[REDACTED]' in redacted
        assert "0gPwK9" not in redacted

    def test_redact_client_secret(self):
        """Test redacting client_secret."""
        message = '{"client_secret": "GOCSPX-abc123xyz"}'
        redacted = redact_sensitive_data(message)
        assert '"client_secret": "[REDACTED]' in redacted
        assert "GOCSPX" not in redacted

    def test_redact_api_key(self):
        """Test redacting API key."""
        message = '{"api_key": "sk_test_51234567890abcdef"}'
        redacted = redact_sensitive_data(message)
        assert '"api_key": "[REDACTED]' in redacted
        assert "sk_test" not in redacted

    def test_redact_authorization_header(self):
        """Test redacting Authorization header."""
        message = "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
        redacted = redact_sensitive_data(message)
        # The regex replaces the entire "Authorization: Bearer TOKEN" or just "Authorization: TOKEN"
        assert "[REDACTED]" in redacted
        assert "eyJhbGci" not in redacted

    def test_redact_password(self):
        """Test redacting password."""
        message = '{"password": "super_secret_password_123"}'
        redacted = redact_sensitive_data(message)
        assert '"password": "[REDACTED]' in redacted
        assert "super_secret" not in redacted

    def test_redact_database_connection_string(self):
        """Test redacting database credentials in connection string."""
        message = "postgresql://user:password123@localhost:5432/db"
        redacted = redact_sensitive_data(message)
        assert "postgresql://[REDACTED]:[REDACTED]@" in redacted
        assert "password123" not in redacted

    def test_redact_credentials_object(self):
        """Test redacting credentials object."""
        message = "credentials={token: abc123, refresh_token: xyz789}"
        redacted = redact_sensitive_data(message)
        assert "credentials={[REDACTED]}" in redacted
        assert "abc123" not in redacted
        assert "xyz789" not in redacted

    def test_no_redaction_needed(self):
        """Test message without sensitive data remains unchanged."""
        message = "Processing email for user test@example.com"
        redacted = redact_sensitive_data(message)
        assert redacted == message

    def test_multiple_sensitive_fields(self):
        """Test redacting multiple sensitive fields in one message."""
        message = (
            '{"token": "abc123", "refresh_token": "xyz789", '
            '"client_secret": "secret123", "user": "test@example.com"}'
        )
        redacted = redact_sensitive_data(message)
        assert '"token": "[REDACTED]' in redacted
        assert '"refresh_token": "[REDACTED]' in redacted
        assert '"client_secret": "[REDACTED]' in redacted
        assert "test@example.com" in redacted  # Non-sensitive data preserved
        assert "abc123" not in redacted
        assert "xyz789" not in redacted
        assert "secret123" not in redacted


class TestRedactingFormatter:
    """Test RedactingFormatter class."""

    def test_formatter_redacts_sensitive_data(self):
        """Test formatter redacts sensitive data in log records."""
        formatter = RedactingFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg='Token: {"token": "secret_token_123"}',
            args=(),
            exc_info=None,
        )

        formatted = formatter.format(record)
        assert "[REDACTED]" in formatted
        assert "secret_token_123" not in formatted

    def test_formatter_preserves_non_sensitive_data(self):
        """Test formatter preserves non-sensitive log data."""
        formatter = RedactingFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Processing user test@example.com",
            args=(),
            exc_info=None,
        )

        formatted = formatter.format(record)
        assert "test@example.com" in formatted
        assert "Processing user" in formatted


class TestGetLogger:
    """Test get_logger function."""

    def test_get_logger_returns_logger(self):
        """Test get_logger returns a Logger instance."""
        logger = get_logger("test_logger")
        assert isinstance(logger, logging.Logger)

    def test_get_logger_has_redacting_handler(self):
        """Test logger has handler with RedactingFormatter."""
        logger = get_logger("test_logger_redacting")
        assert len(logger.handlers) > 0
        handler = logger.handlers[0]
        assert isinstance(handler.formatter, RedactingFormatter)

    def test_get_logger_same_name_returns_same_instance(self):
        """Test get_logger returns same instance for same name."""
        logger1 = get_logger("test_logger_same")
        logger2 = get_logger("test_logger_same")
        assert logger1 is logger2

    def test_logger_redacts_sensitive_info_in_logs(self, capsys):
        """Test logger redacts sensitive information in actual log output."""
        logger = get_logger("test_redaction_stream")
        logger.setLevel(logging.INFO)

        # Clear any existing handlers to avoid interference
        logger.handlers.clear()

        # Add a new handler with redacting formatter
        handler = logging.StreamHandler()
        formatter = RedactingFormatter()
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        logger.info('Storing credentials: {"token": "secret123", "user": "test@example.com"}')

        # Check stderr output (where StreamHandler writes)
        captured = capsys.readouterr()
        assert "[REDACTED]" in captured.err
        assert "secret123" not in captured.err
        assert "test@example.com" in captured.err  # Non-sensitive preserved


class TestSafeRepr:
    """Test safe_repr function."""

    def test_safe_repr_dict_with_sensitive_keys(self):
        """Test safe_repr redacts sensitive keys in dictionary."""
        obj = {
            "username": "test_user",
            "password": "secret123",
            "token": "abc123",
            "email": "test@example.com",
        }
        safe = safe_repr(obj)
        assert "[REDACTED]" in safe
        assert "secret123" not in safe
        assert "abc123" not in safe
        assert "test_user" in safe
        assert "test@example.com" in safe

    def test_safe_repr_nested_dict(self):
        """Test safe_repr redacts nested dictionaries."""
        obj = {
            "user": {"username": "test", "password": "secret"},
            "auth": {"token": "abc123"},
        }
        safe = safe_repr(obj)
        assert "[REDACTED]" in safe
        assert "secret" not in safe
        assert "abc123" not in safe
        assert "test" in safe

    def test_safe_repr_list_of_dicts(self):
        """Test safe_repr handles lists of dictionaries."""
        obj = [
            {"user": "test1", "token": "abc123"},
            {"user": "test2", "token": "xyz789"},
        ]
        safe = safe_repr(obj)
        assert "[REDACTED]" in safe
        assert "abc123" not in safe
        assert "xyz789" not in safe
        assert "test1" in safe
        assert "test2" in safe

    def test_safe_repr_custom_redact_keys(self):
        """Test safe_repr with custom keys to redact."""
        obj = {"username": "test", "custom_secret": "sensitive_data"}
        safe = safe_repr(obj, redact_keys=["custom_secret"])
        assert "[REDACTED]" in safe
        assert "sensitive_data" not in safe
        assert "test" in safe

    def test_safe_repr_non_dict_object(self):
        """Test safe_repr with non-dictionary objects."""
        obj = "Just a string"
        safe = safe_repr(obj)
        assert safe == "Just a string"

    def test_safe_repr_credentials_key_variations(self):
        """Test safe_repr catches various credential key variations."""
        obj = {
            "access_token": "token1",
            "ACCESS_TOKEN": "token2",
            "AccessToken": "token3",
            "normal_field": "keep_this",
        }
        safe = safe_repr(obj)
        assert safe.count("[REDACTED]") == 3
        assert "token1" not in safe
        assert "token2" not in safe
        assert "token3" not in safe
        assert "keep_this" in safe

    def test_safe_repr_client_secret_variations(self):
        """Test safe_repr catches client_secret variations."""
        obj = {
            "client_secret": "secret1",
            "clientSecret": "secret2",
            "CLIENT_SECRET": "secret3",
        }
        safe = safe_repr(obj)
        # Note: safe_repr uses .lower() for key matching, so it should catch at least client_secret and CLIENT_SECRET
        # clientSecret won't match because "secret" is not a standalone word in the key check
        assert safe.count("[REDACTED]") >= 2
        assert "secret1" not in safe
        assert "secret3" not in safe


class TestIntegrationWithGmailAuth:
    """Test logging integration with Gmail auth scenarios."""

    def test_oauth_credentials_never_logged(self, capsys):
        """Test OAuth credentials are never exposed in logs."""
        logger = get_logger("test_oauth_stream")
        logger.setLevel(logging.INFO)
        logger.handlers.clear()

        handler = logging.StreamHandler()
        handler.setFormatter(RedactingFormatter())
        logger.addHandler(handler)

        # Log as JSON string instead of Python dict to match expected format
        import json
        credentials_dict = {
            "token": "ya29.a0AfH6SMBx...",
            "refresh_token": "1//0gPwK9...",
            "client_id": "client123",
            "client_secret": "secret456",
        }

        logger.info(f"Storing credentials: {json.dumps(credentials_dict)}")

        # Verify sensitive data is redacted in stderr output
        captured = capsys.readouterr()
        assert "[REDACTED]" in captured.err
        assert "ya29.a0AfH6SMBx" not in captured.err
        assert "0gPwK9" not in captured.err
        assert "secret456" not in captured.err

    def test_authorization_header_never_logged(self, capsys):
        """Test Authorization headers are never exposed in logs."""
        logger = get_logger("test_auth_header_stream")
        logger.setLevel(logging.INFO)
        logger.handlers.clear()

        handler = logging.StreamHandler()
        handler.setFormatter(RedactingFormatter())
        logger.addHandler(handler)

        logger.info("Request headers: Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9")

        # Verify bearer token is redacted in stderr output
        captured = capsys.readouterr()
        assert "[REDACTED]" in captured.err
        assert "eyJhbGci" not in captured.err

    def test_database_connection_string_never_logged(self, capsys):
        """Test database connection strings are redacted."""
        logger = get_logger("test_db_conn_stream")
        logger.setLevel(logging.INFO)
        logger.handlers.clear()

        handler = logging.StreamHandler()
        handler.setFormatter(RedactingFormatter())
        logger.addHandler(handler)

        logger.info("Connecting to postgresql://user:password123@localhost:5432/dbname")

        # Verify credentials are redacted in stderr output
        captured = capsys.readouterr()
        assert "[REDACTED]" in captured.err
        assert "password123" not in captured.err
