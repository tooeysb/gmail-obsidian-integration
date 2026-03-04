"""
Unit tests for voice draft prompt formatting functions.
"""

from src.services.voice.draft_prompt import (
    format_emails_for_analysis,
    format_example_emails,
)


class TestFormatExampleEmails:
    """Test format_example_emails output."""

    def test_single_email(self):
        emails = [
            {
                "recipient_emails": "bob@example.com",
                "subject": "Quick question",
                "date": "2024-01-15",
                "body": "Hey Bob, just checking in.",
            }
        ]
        result = format_example_emails(emails)

        assert "To: bob@example.com" in result
        assert "Subject: Quick question" in result
        assert "Hey Bob, just checking in." in result

    def test_multiple_emails(self):
        emails = [
            {"subject": "Email 1", "body": "Body 1"},
            {"subject": "Email 2", "body": "Body 2"},
        ]
        result = format_example_emails(emails)

        assert "Email 1" in result
        assert "Email 2" in result
        assert result.count("---") >= 4  # Opening + closing for each

    def test_missing_fields_use_defaults(self):
        emails = [{}]
        result = format_example_emails(emails)

        assert "To: unknown" in result
        assert "Subject: (no subject)" in result

    def test_empty_list(self):
        result = format_example_emails([])
        assert result == ""

    def test_uses_summary_as_body_fallback(self):
        emails = [{"summary": "Summary text"}]
        result = format_example_emails(emails)
        assert "Summary text" in result


class TestFormatEmailsForAnalysis:
    """Test format_emails_for_analysis output."""

    def test_numbered_emails(self):
        emails = [
            {"subject": "First", "body": "Content 1"},
            {"subject": "Second", "body": "Content 2"},
        ]
        result = format_emails_for_analysis(emails)

        assert "### Email 1" in result
        assert "### Email 2" in result

    def test_truncates_long_bodies(self):
        long_body = "x" * 3000
        emails = [{"body": long_body}]
        result = format_emails_for_analysis(emails)

        assert "[...truncated]" in result
        # Should have at most 2000 chars of body + truncation marker
        assert "x" * 2000 in result
        assert "x" * 2001 not in result

    def test_missing_fields(self):
        emails = [{}]
        result = format_emails_for_analysis(emails)

        assert "To: unknown" in result
        assert "Subject: (no subject)" in result
