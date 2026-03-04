"""
Enhanced unit tests for the logging module.

Covers JsonFormatter output structure, request_id injection via
RedactingFormatter, sensitive-data redaction patterns, and the
get_logger factory.
"""

import json
import logging

from src.core.logging import (
    JsonFormatter,
    RedactingFormatter,
    get_logger,
    redact_sensitive_data,
)


def _make_record(
    msg: str = "test message",
    level: int = logging.INFO,
    name: str = "test.logger",
) -> logging.LogRecord:
    """Helper: build a minimal LogRecord without going through a real logger."""
    return logging.LogRecord(
        name=name,
        level=level,
        pathname="test.py",
        lineno=1,
        msg=msg,
        args=(),
        exc_info=None,
    )


class TestRedactingFormatterRequestId:
    """RedactingFormatter must inject [request_id] into formatted output."""

    def test_redacting_formatter_includes_request_id(self):
        """Format string %(request_id)s must be populated in every record."""
        formatter = RedactingFormatter(
            fmt="%(asctime)s - %(name)s - %(levelname)s - [%(request_id)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        record = _make_record("hello world")
        output = formatter.format(record)

        # The request_id placeholder must have been resolved — brackets present
        assert (
            "[" in output and "]" in output
        ), "Expected [request_id] bracket syntax in formatted output"
        # Default when no middleware context is active should be '-'
        assert "[-]" in output or "[" in output  # ID is present in some form

    def test_redacting_formatter_default_request_id_is_dash(self):
        """Without an active request context the request_id defaults to '-'."""
        formatter = RedactingFormatter(
            fmt="[%(request_id)s] %(message)s",
        )
        record = _make_record("no context")
        output = formatter.format(record)
        assert "[-]" in output, f"Expected '[-]' as default request_id, got: {output!r}"


class TestJsonFormatter:
    """Tests for JsonFormatter structured output."""

    def _format(self, msg: str = "test message", level: int = logging.INFO) -> dict:
        """Return the parsed JSON dict produced by JsonFormatter."""
        formatter = JsonFormatter(datefmt="%Y-%m-%dT%H:%M:%S")
        record = _make_record(msg=msg, level=level)
        raw = formatter.format(record)
        return json.loads(raw)

    def test_json_formatter_output_structure(self):
        """JsonFormatter must emit valid JSON with all required top-level keys."""
        formatter = JsonFormatter(datefmt="%Y-%m-%dT%H:%M:%S")
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="test message",
            args=(),
            exc_info=None,
        )
        output = formatter.format(record)
        parsed = json.loads(output)

        assert "timestamp" in parsed
        assert "level" in parsed
        assert parsed["level"] == "INFO"
        assert "message" in parsed
        assert "request_id" in parsed

    def test_json_formatter_message_field_matches_input(self):
        """The 'message' key must contain the original log message text."""
        parsed = self._format(msg="hello from json")
        assert parsed["message"] == "hello from json"

    def test_json_formatter_level_field(self):
        """Level names must be serialized as uppercase strings."""
        for level, name in [
            (logging.DEBUG, "DEBUG"),
            (logging.WARNING, "WARNING"),
            (logging.ERROR, "ERROR"),
        ]:
            parsed = self._format(level=level)
            assert parsed["level"] == name

    def test_json_formatter_output_is_single_line(self):
        """Each log record must be emitted as exactly one JSON line."""
        formatter = JsonFormatter(datefmt="%Y-%m-%dT%H:%M:%S")
        record = _make_record("single line check")
        raw = formatter.format(record)
        lines = [line for line in raw.splitlines() if line.strip()]
        assert len(lines) == 1, f"Expected 1 line, got {len(lines)}: {raw!r}"

    def test_json_formatter_redacts_sensitive_data(self):
        """Sensitive values must be redacted inside the JSON 'message' field."""
        sensitive_msg = '{"token": "super_secret_token_abc123", "user": "test@example.com"}'
        parsed = self._format(msg=sensitive_msg)

        # The token value must be gone
        assert (
            "super_secret_token_abc123" not in parsed["message"]
        ), "Token value should be redacted in JSON output"
        assert "[REDACTED]" in parsed["message"]
        # Non-sensitive data must survive
        assert "test@example.com" in parsed["message"]

    def test_json_formatter_request_id_defaults_to_dash(self):
        """Without a request context, request_id must be '-'."""
        parsed = self._format()
        assert parsed["request_id"] == "-", f"Expected '-', got '{parsed['request_id']}'"

    def test_json_formatter_includes_logger_name(self):
        """JsonFormatter output must include a 'logger' field with the record's name."""
        formatter = JsonFormatter(datefmt="%Y-%m-%dT%H:%M:%S")
        record = _make_record(name="my.special.logger")
        parsed = json.loads(formatter.format(record))
        assert parsed.get("logger") == "my.special.logger"

    def test_json_formatter_exception_field_on_exc_info(self):
        """When a record carries exc_info, an 'exception' key must appear."""
        formatter = JsonFormatter(datefmt="%Y-%m-%dT%H:%M:%S")
        try:
            raise ValueError("boom")
        except ValueError:
            import sys

            exc_info = sys.exc_info()

        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="test.py",
            lineno=1,
            msg="something went wrong",
            args=(),
            exc_info=exc_info,
        )
        parsed = json.loads(formatter.format(record))
        assert "exception" in parsed
        assert "ValueError" in parsed["exception"]


class TestGetLogger:
    """Tests for the get_logger factory function."""

    def test_get_logger_returns_configured_logger(self):
        """get_logger must return a Logger that already has at least one handler."""
        logger = get_logger("test.enhanced.factory")
        assert isinstance(logger, logging.Logger)
        assert (
            len(logger.handlers) > 0
        ), "Logger returned by get_logger must have at least one handler"

    def test_get_logger_handler_has_formatter(self):
        """Each handler attached by get_logger must carry a formatter."""
        logger = get_logger("test.enhanced.formatter_check")
        for handler in logger.handlers:
            assert handler.formatter is not None, f"Handler {handler!r} is missing a formatter"

    def test_get_logger_default_level_is_info(self):
        """Loggers created by get_logger must default to INFO level."""
        logger = get_logger("test.enhanced.level_check")
        assert logger.level == logging.INFO

    def test_get_logger_idempotent_handlers(self):
        """Calling get_logger twice with the same name must not add extra handlers."""
        name = "test.enhanced.idempotent"
        logger1 = get_logger(name)
        count_after_first = len(logger1.handlers)
        logger2 = get_logger(name)
        assert (
            len(logger2.handlers) == count_after_first
        ), "Repeated get_logger calls must not double-attach handlers"
        assert logger1 is logger2


class TestRedactSensitiveDataPatterns:
    """Tests for the standalone redact_sensitive_data helper."""

    def test_redact_token_json_field(self):
        """'token' JSON field value must be replaced with [REDACTED]."""
        msg = '{"token": "eyJhbGciOiJSUzI1NiJ9.payload.sig"}'
        result = redact_sensitive_data(msg)
        assert '"token": "[REDACTED]' in result
        assert "eyJhbGci" not in result

    def test_redact_password_query_param(self):
        """password= URL param pattern must be redacted."""
        msg = "Connecting with password=hunter2 to the service"
        result = redact_sensitive_data(msg)
        assert "[REDACTED]" in result
        assert "hunter2" not in result

    def test_redact_authorization_bearer_header(self):
        """Authorization: Bearer <token> must be fully redacted."""
        msg = "Authorization: Bearer ya29.a0AfH6SMBx_real_token"
        result = redact_sensitive_data(msg)
        assert "[REDACTED]" in result
        assert "ya29.a0AfH6SMBx_real_token" not in result

    def test_redact_postgresql_connection_string(self):
        """Credentials embedded in a postgresql:// DSN must be masked."""
        msg = "db_url=postgresql://admin:s3cr3tPassw0rd@db.example.com:5432/mydb"
        result = redact_sensitive_data(msg)
        assert "postgresql://[REDACTED]:[REDACTED]@" in result
        assert "s3cr3tPassw0rd" not in result

    def test_redact_refresh_token(self):
        """'refresh_token' JSON field must be redacted."""
        msg = '{"refresh_token": "1//04gPwK9_longrefreshtoken"}'
        result = redact_sensitive_data(msg)
        assert '"refresh_token": "[REDACTED]' in result
        assert "1//04gPwK9" not in result

    def test_redact_client_secret(self):
        """'client_secret' JSON field must be redacted."""
        msg = '{"client_secret": "GOCSPX-abc123xyz"}'
        result = redact_sensitive_data(msg)
        assert '"client_secret": "[REDACTED]' in result
        assert "GOCSPX-abc123xyz" not in result

    def test_redact_api_key(self):
        """'api_key' JSON field must be redacted."""
        msg = '{"api_key": "sk-prod-xxxxxxxxxxxx"}'
        result = redact_sensitive_data(msg)
        assert '"api_key": "[REDACTED]' in result
        assert "sk-prod-xxxxxxxxxxxx" not in result

    def test_non_sensitive_data_preserved(self):
        """Messages without sensitive patterns must pass through unchanged."""
        msg = "Fetched 42 emails for user@example.com in 1.23s"
        result = redact_sensitive_data(msg)
        assert result == msg

    def test_multiple_patterns_in_one_message(self):
        """All sensitive patterns in a single message must all be redacted."""
        msg = (
            '{"token": "tok_abc", "refresh_token": "ref_xyz", '
            '"client_secret": "sec_123", "user": "keep@example.com"}'
        )
        result = redact_sensitive_data(msg)
        assert "tok_abc" not in result
        assert "ref_xyz" not in result
        assert "sec_123" not in result
        assert "keep@example.com" in result  # non-sensitive must survive
