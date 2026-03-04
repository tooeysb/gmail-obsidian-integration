"""
Unit tests for Excel importer helper functions.
Tests pure Python parsing logic without requiring openpyxl or real files.
"""

from datetime import date, datetime

from src.services.enrichment.excel_importer import (
    _build_column_mapping,
    _clean_cell_value,
    _extract_hyperlink_text,
    _match_header,
)


class TestCleanCellValue:
    """Test _clean_cell_value normalization."""

    def test_none_returns_none(self):
        assert _clean_cell_value(None) is None

    def test_empty_string_returns_none(self):
        assert _clean_cell_value("") is None
        assert _clean_cell_value("   ") is None

    def test_string_stripped(self):
        assert _clean_cell_value("  hello  ") == "hello"

    def test_integer(self):
        assert _clean_cell_value(42) == "42"

    def test_float(self):
        assert _clean_cell_value(3.14) == "3.14"

    def test_boolean(self):
        assert _clean_cell_value(True) == "True"
        assert _clean_cell_value(False) == "False"

    def test_datetime(self):
        dt = datetime(2024, 3, 15, 10, 30, 0)
        assert _clean_cell_value(dt) == "2024-03-15"

    def test_date(self):
        d = date(2024, 3, 15)
        assert _clean_cell_value(d) == "2024-03-15"

    def test_hyperlink_formula(self):
        formula = '=HYPERLINK("https://example.com","Click Here")'
        assert _clean_cell_value(formula) == "Click Here"


class TestExtractHyperlinkText:
    """Test _extract_hyperlink_text parsing."""

    def test_standard_hyperlink(self):
        result = _extract_hyperlink_text('=HYPERLINK("https://example.com","Display Text")')
        assert result == "Display Text"

    def test_no_quotes_on_display(self):
        result = _extract_hyperlink_text('=HYPERLINK("https://example.com",Display Text)')
        assert result == "Display Text"

    def test_non_hyperlink_passthrough(self):
        result = _extract_hyperlink_text("Just plain text")
        assert result == "Just plain text"

    def test_case_insensitive(self):
        result = _extract_hyperlink_text('=hyperlink("url","text")')
        assert result == "text"


class TestMatchHeader:
    """Test _match_header keyword matching."""

    def test_exact_match(self):
        assert _match_header("account name", ["account name"]) is True

    def test_substring_match(self):
        assert _match_header("Account Name (Required)", ["account name"]) is True

    def test_underscore_normalized(self):
        assert _match_header("account_name", ["account name"]) is True

    def test_case_insensitive(self):
        assert _match_header("ACCOUNT NAME", ["account name"]) is True

    def test_no_match(self):
        assert _match_header("email address", ["account name"]) is False

    def test_multiple_candidates(self):
        assert _match_header("Phone Call", ["email", "phone call"]) is True


class TestBuildColumnMapping:
    """Test _build_column_mapping header-to-field resolution."""

    def test_basic_mapping(self):
        headers = ["Account Name", "Annual Access Value", "Revenue Segment"]
        config = {
            "company_name": ["account name"],
            "arr": ["annual access value"],
            "revenue_segment": ["revenue segment"],
        }
        mapping = _build_column_mapping(headers, config)

        assert mapping == {0: "company_name", 1: "arr", 2: "revenue_segment"}

    def test_skips_none_headers(self):
        headers = [None, "Account Name", None]
        config = {"company_name": ["account name"]}
        mapping = _build_column_mapping(headers, config)

        assert mapping == {1: "company_name"}

    def test_first_match_wins(self):
        """Each canonical name is only assigned once."""
        headers = ["Name", "Full Name", "Title"]
        config = {"name": ["name"], "title": ["title"]}
        mapping = _build_column_mapping(headers, config)

        # "name" matches index 0, should not also match index 1
        assert mapping == {0: "name", 2: "title"}

    def test_empty_headers(self):
        mapping = _build_column_mapping([], {"name": ["name"]})
        assert mapping == {}

    def test_no_matches(self):
        headers = ["foo", "bar", "baz"]
        config = {"name": ["name"], "email": ["email"]}
        mapping = _build_column_mapping(headers, config)
        assert mapping == {}
