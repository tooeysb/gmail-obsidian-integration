"""
Tests for People list columns (last_email_received, last_email_sent)
and contact detail auto-enrichment of job title via Haiku.
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch
from uuid import uuid4

from src.api.routers.crm import _best_company_name, _build_linkedin_url, _search_linkedin_title


# ---------------------------------------------------------------------------
# Helper to build a mock contact
# ---------------------------------------------------------------------------
def _make_contact(**overrides):
    contact = MagicMock()
    defaults = {
        "id": uuid4(),
        "name": "Kasey Bevans",
        "email": "kasey@acme.com",
        "phone": None,
        "title": None,
        "contact_type": "Champion",
        "is_vip": False,
        "email_count": 10,
        "tags": [],
        "relationship_context": None,
        "company_id": uuid4(),
        "last_contact_at": datetime(2024, 6, 1, tzinfo=UTC),
        "notes": None,
        "personal_email": None,
        "account_sources": ["procore-main"],
        "salesforce_id": None,
        "address": None,
        "linkedin_url": None,
        "created_at": datetime(2024, 1, 1, tzinfo=UTC),
        "updated_at": datetime(2024, 6, 1, tzinfo=UTC),
    }
    defaults.update(overrides)
    for k, v in defaults.items():
        setattr(contact, k, v)
    return contact


# ---------------------------------------------------------------------------
# Tests: list_contacts returns last_email_received / last_email_sent
# ---------------------------------------------------------------------------
class TestListContactsEmailDates:
    """GET /crm/api/contacts should include last_email_received and last_email_sent."""

    def test_email_dates_included_in_response(self, test_client, mock_db):
        """Contacts in the list response contain the two new date fields."""
        contact = _make_contact()
        company_mock = MagicMock()
        company_mock.name = "Acme Corp"
        contact.company = company_mock

        query_mock = mock_db.query.return_value
        query_mock.count.return_value = 1
        query_mock.all.return_value = [contact]

        resp = test_client.get("/crm/api/contacts")
        assert resp.status_code == 200

        data = resp.json()
        items = data["items"]
        assert len(items) == 1
        assert "last_email_received" in items[0]
        assert "last_email_sent" in items[0]

    def test_email_dates_none_when_no_participants(self, test_client, mock_db):
        """Contacts with no EmailParticipant records get None for both dates."""
        contact = _make_contact()
        contact.company = None
        contact.company_id = None

        query_mock = mock_db.query.return_value
        query_mock.count.return_value = 1
        query_mock.all.return_value = [contact]

        resp = test_client.get("/crm/api/contacts")
        assert resp.status_code == 200

        item = resp.json()["items"][0]
        assert item["last_email_received"] is None
        assert item["last_email_sent"] is None


# ---------------------------------------------------------------------------
# Tests: POST /contacts/{id}/enrich-title (async title enrichment)
# ---------------------------------------------------------------------------
class TestEnrichTitle:
    """POST /crm/api/contacts/{id}/enrich-title — LinkedIn first, Haiku fallback."""

    def _setup_enrich_mocks(self, mock_db, contact):
        """Configure mock_db for the enrich-title endpoint."""
        query_mock = mock_db.query.return_value
        # .first() calls: 1. get_current_user -> User, 2. contact lookup -> Contact
        query_mock.first.side_effect = [
            mock_db.query.return_value.first.return_value,  # User (from conftest)
            contact,
        ]

    @patch("src.api.routers.crm._search_linkedin_title")
    def test_linkedin_finds_title(self, mock_linkedin, test_client, mock_db):
        """When LinkedIn finds a title, return it without calling Haiku."""
        contact = _make_contact(title=None)
        company_mock = MagicMock()
        company_mock.name = "Acme Corp"
        company_mock.aliases = None
        company_mock.domain = None
        contact.company = company_mock
        self._setup_enrich_mocks(mock_db, contact)

        mock_db.execute.return_value.fetchall.return_value = []
        mock_linkedin.return_value = "President"

        resp = test_client.post(f"/crm/api/contacts/{contact.id}/enrich-title")
        assert resp.status_code == 200
        assert resp.json()["title"] == "President"

        mock_linkedin.assert_called_once_with("Kasey Bevans", "Acme Corp", None, None)
        assert contact.title == "President"
        mock_db.commit.assert_called()

    @patch("src.api.routers.crm._enrich_with_haiku")
    def test_returns_existing_title_without_enrichment(self, mock_haiku, test_client, mock_db):
        """When contact already has a title, return it immediately."""
        contact = _make_contact(title="CEO")
        company_mock = MagicMock()
        company_mock.name = "Acme Corp"
        company_mock.aliases = None
        company_mock.domain = None
        contact.company = company_mock
        self._setup_enrich_mocks(mock_db, contact)

        resp = test_client.post(f"/crm/api/contacts/{contact.id}/enrich-title")
        assert resp.status_code == 200
        assert resp.json()["title"] == "CEO"

        mock_haiku.assert_not_called()

    @patch("src.api.routers.crm._enrich_with_haiku")
    @patch("src.api.routers.crm._search_linkedin_title")
    def test_falls_back_to_haiku_when_linkedin_fails(
        self, mock_linkedin, mock_haiku, test_client, mock_db
    ):
        """When LinkedIn finds nothing, falls back to Haiku email signature extraction."""
        contact = _make_contact(title=None)
        company_mock = MagicMock()
        company_mock.name = "Acme Corp"
        company_mock.aliases = None
        company_mock.domain = None
        contact.company = company_mock
        self._setup_enrich_mocks(mock_db, contact)

        sig_row = MagicMock()
        sig_row.sender_name = "Kasey Bevans"
        sig_row.sig_text = "Thanks!\n\nKasey Bevans\nVP of Operations\nAcme Corp"
        mock_db.execute.return_value.fetchall.return_value = [sig_row]

        mock_linkedin.return_value = None
        mock_haiku.return_value = {
            "kasey@acme.com": {
                "name": "Kasey Bevans",
                "title": "VP of Operations",
                "linkedin_url": None,
            }
        }

        resp = test_client.post(f"/crm/api/contacts/{contact.id}/enrich-title")
        assert resp.status_code == 200
        assert resp.json()["title"] == "VP of Operations"

        mock_linkedin.assert_called_once()
        mock_haiku.assert_called_once()
        assert contact.title == "VP of Operations"
        mock_db.commit.assert_called()

    @patch("src.api.routers.crm._enrich_with_haiku")
    @patch("src.api.routers.crm._search_linkedin_title")
    def test_returns_null_when_both_fail(self, mock_linkedin, mock_haiku, test_client, mock_db):
        """When both LinkedIn and Haiku find nothing, return null."""
        contact = _make_contact(title=None)
        company_mock = MagicMock()
        company_mock.name = "Acme Corp"
        company_mock.aliases = None
        company_mock.domain = None
        contact.company = company_mock
        self._setup_enrich_mocks(mock_db, contact)

        mock_db.execute.return_value.fetchall.return_value = []
        mock_linkedin.return_value = None

        resp = test_client.post(f"/crm/api/contacts/{contact.id}/enrich-title")
        assert resp.status_code == 200
        assert resp.json()["title"] is None

        assert contact.title is None
        mock_db.commit.assert_not_called()


# ---------------------------------------------------------------------------
# Tests: _build_linkedin_url helper
# ---------------------------------------------------------------------------
class TestBuildLinkedInUrl:
    def test_basic_url(self):
        url = _build_linkedin_url("John Smith", "Acme Corp")
        assert "linkedin.com/search/results/people" in url
        assert "John" in url
        assert "Smith" in url

    def test_none_name_returns_none(self):
        assert _build_linkedin_url(None, "Acme Corp") is None

    def test_empty_name_returns_none(self):
        assert _build_linkedin_url("", "Acme Corp") is None


# ---------------------------------------------------------------------------
# Tests: _search_linkedin_title helper
# ---------------------------------------------------------------------------
class TestSearchLinkedInTitle:
    def test_none_name_returns_none(self):
        assert _search_linkedin_title(None, "Acme Corp") is None

    @patch("src.api.routers.crm.httpx")
    def test_parses_three_part_title(self, mock_httpx):
        """Parses 'Name - Title - Company | LinkedIn' format."""
        html = """
        <div class="result">
            <a class="result__a">Sean Halpin - President - JT Magen | LinkedIn</a>
            <a class="result__url">https://www.linkedin.com/in/seanhalpin</a>
        </div>
        """
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = html
        mock_httpx.get.return_value = mock_resp

        result = _search_linkedin_title("Sean Halpin", "JT Magen & Company")
        assert result == "President"

    @patch("src.api.routers.crm.httpx")
    def test_parses_two_part_title(self, mock_httpx):
        """Parses 'Name - Title | LinkedIn' format."""
        html = """
        <div class="result">
            <a class="result__a">John Doe - VP of Operations | LinkedIn</a>
            <a class="result__url">https://www.linkedin.com/in/johndoe</a>
        </div>
        """
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = html
        mock_httpx.get.return_value = mock_resp

        result = _search_linkedin_title("John Doe", "Acme Corp")
        assert result == "VP of Operations"

    @patch("src.api.routers.crm.httpx")
    def test_returns_none_when_no_linkedin_results(self, mock_httpx):
        """Returns None when no LinkedIn profile found in results."""
        html = """
        <div class="result">
            <a class="result__a">Some random page</a>
            <a class="result__url">https://www.example.com/page</a>
        </div>
        """
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = html
        mock_httpx.get.return_value = mock_resp

        result = _search_linkedin_title("Nobody Special", "Unknown Corp")
        assert result is None


# ---------------------------------------------------------------------------
# Tests: _best_company_name helper
# ---------------------------------------------------------------------------
class TestBestCompanyName:
    def test_multi_word_name_returned_as_is(self):
        result = _best_company_name("McCarthy Holdings", None, None)
        assert result == "McCarthy Holdings"

    def test_single_word_prefers_alias(self):
        result = _best_company_name("Manhattan", ["Manhattan Construction"], None)
        assert result == "Manhattan Construction"

    def test_alias_strips_suffixes(self):
        result = _best_company_name("Manhattan", ["Manhattan Construction, Inc."], None)
        assert result == "Manhattan Construction"

    def test_falls_back_to_domain_derived_name(self):
        result = _best_company_name("Manhattan", None, "manhattanconstruction.com")
        assert result == "Manhattan Construction"

    def test_no_alias_no_domain_returns_cleaned_name(self):
        result = _best_company_name("Manhattan - HQ", None, None)
        assert result == "Manhattan"

    def test_empty_aliases_skipped(self):
        result = _best_company_name("Manhattan", [], None)
        assert result == "Manhattan"
