"""
Shared utility functions and constants used across the application.
"""

from datetime import datetime


def serialize_dt(dt: datetime | None) -> str | None:
    """Serialize a datetime to ISO format string, or None."""
    return dt.isoformat() if dt else None


def strip_markdown_codeblocks(text: str) -> str:
    """Strip markdown code block fences from Claude API responses."""
    text = text.strip()
    if text.startswith("```json"):
        text = text[7:]
    elif text.startswith("```"):
        text = text[3:]
    if text.endswith("```"):
        text = text[:-3]
    return text.strip()


# Generic/personal email domains shared across company_resolver and contact_discovery.
GENERIC_EMAIL_DOMAINS: frozenset[str] = frozenset(
    {
        "gmail.com",
        "yahoo.com",
        "hotmail.com",
        "outlook.com",
        "aol.com",
        "icloud.com",
        "me.com",
        "live.com",
        "msn.com",
        "protonmail.com",
        "mail.com",
        "comcast.net",
        "att.net",
        "verizon.net",
        "sbcglobal.net",
        "cox.net",
        "charter.net",
    }
)
