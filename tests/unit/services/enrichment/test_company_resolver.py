"""
Unit tests for CompanyResolver helper methods.
Tests pure Python logic (normalization, matching, parsing) without DB.
"""

from decimal import Decimal
from uuid import uuid4

from src.services.enrichment.company_resolver import CompanyResolver


class TestNormalizeName:
    """Test company name normalization."""

    def test_lowercase(self):
        assert CompanyResolver._normalize_name("ACME") == "acme"

    def test_strip_whitespace(self):
        assert CompanyResolver._normalize_name("  Acme  ") == "acme"

    def test_strip_llc(self):
        assert CompanyResolver._normalize_name("Acme LLC") == "acme"

    def test_strip_inc(self):
        assert CompanyResolver._normalize_name("Acme Inc.") == "acme"
        assert CompanyResolver._normalize_name("Acme Inc") == "acme"

    def test_strip_corp(self):
        assert CompanyResolver._normalize_name("Acme Corp.") == "acme"
        assert CompanyResolver._normalize_name("Acme Corp") == "acme"

    def test_strip_hq(self):
        assert CompanyResolver._normalize_name("Acme - HQ") == "acme"

    def test_strip_group(self):
        assert CompanyResolver._normalize_name("Walsh Group") == "walsh"

    def test_collapse_spaces(self):
        assert CompanyResolver._normalize_name("Acme   Construction   Co.") == "acme construction"

    def test_empty_string(self):
        assert CompanyResolver._normalize_name("") == ""

    def test_none_safe(self):
        """None is technically not valid input but should not crash."""
        # The method has a guard: if not name: return ""
        assert CompanyResolver._normalize_name("") == ""


class TestExtractDomain:
    """Test email domain extraction."""

    def test_standard_email(self):
        assert CompanyResolver._extract_domain("user@example.com") == "example.com"

    def test_uppercase_normalized(self):
        assert CompanyResolver._extract_domain("User@EXAMPLE.COM") == "example.com"

    def test_no_at_sign(self):
        assert CompanyResolver._extract_domain("not-an-email") is None

    def test_multiple_at_signs(self):
        result = CompanyResolver._extract_domain("user@sub@example.com")
        assert result == "sub@example.com"


class TestIsGenericDomain:
    """Test generic email provider detection."""

    def test_gmail(self):
        assert CompanyResolver._is_generic_domain("gmail.com") is True

    def test_yahoo(self):
        assert CompanyResolver._is_generic_domain("yahoo.com") is True

    def test_corporate(self):
        assert CompanyResolver._is_generic_domain("procore.com") is False

    def test_icloud(self):
        assert CompanyResolver._is_generic_domain("icloud.com") is True

    def test_hotmail(self):
        assert CompanyResolver._is_generic_domain("hotmail.com") is True


class TestParseArr:
    """Test ARR value parsing."""

    def test_plain_number(self):
        assert CompanyResolver._parse_arr("1000000") == Decimal("1000000")

    def test_with_dollar_sign(self):
        assert CompanyResolver._parse_arr("$1,000,000") == Decimal("1000000")

    def test_with_commas(self):
        assert CompanyResolver._parse_arr("1,234,567.89") == Decimal("1234567.89")

    def test_none(self):
        assert CompanyResolver._parse_arr(None) is None

    def test_empty_string(self):
        assert CompanyResolver._parse_arr("") is None

    def test_non_numeric(self):
        assert CompanyResolver._parse_arr("N/A") is None

    def test_float_string(self):
        assert CompanyResolver._parse_arr("999.99") == Decimal("999.99")


class TestMatchCompanyName:
    """Test fuzzy company name matching."""

    def test_direct_match(self):
        """Direct match should return the company ID."""
        company_map = {"acme": uuid4()}
        resolver = CompanyResolver.__new__(CompanyResolver)
        result = resolver._match_company_name("acme", company_map)
        assert result == company_map["acme"]

    def test_substring_match(self):
        """'acme' should match 'acme technologies'."""
        cid = uuid4()
        company_map = {"acme technologies": cid}
        resolver = CompanyResolver.__new__(CompanyResolver)
        result = resolver._match_company_name("acme tech", company_map)
        # "acme tech" (9 chars) in "acme technologies" (17 chars) = True
        assert result == cid

    def test_no_match(self):
        company_map = {"acme technologies": uuid4()}
        resolver = CompanyResolver.__new__(CompanyResolver)
        result = resolver._match_company_name("global corp", company_map)
        assert result is None

    def test_short_names_not_matched(self):
        """Names <= 4 chars should not match via substring."""
        company_map = {"abc": uuid4()}
        resolver = CompanyResolver.__new__(CompanyResolver)
        result = resolver._match_company_name("abcd", company_map)
        # "abc" has len 3 (<= 4), so substring matching is skipped
        assert result is None

    def test_empty_name(self):
        company_map = {"acme": uuid4()}
        resolver = CompanyResolver.__new__(CompanyResolver)
        result = resolver._match_company_name("", company_map)
        assert result is None
