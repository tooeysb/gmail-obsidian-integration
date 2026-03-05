"""
Tests for People list columns (last_email_received, last_email_sent)
and contact detail auto-enrichment of job title via Haiku.
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch
from uuid import uuid4

from src.api.routers.crm import _build_linkedin_url


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
    """POST /crm/api/contacts/{id}/enrich-title should extract title via Haiku."""

    def _setup_enrich_mocks(self, mock_db, contact):
        """Configure mock_db for the enrich-title endpoint."""
        query_mock = mock_db.query.return_value
        # .first() calls: 1. get_current_user -> User, 2. contact lookup -> Contact
        query_mock.first.side_effect = [
            mock_db.query.return_value.first.return_value,  # User (from conftest)
            contact,
        ]

    @patch("src.api.routers.crm._enrich_with_haiku")
    def test_enriches_title_when_missing(self, mock_haiku, test_client, mock_db):
        """When contact has no title, Haiku is called and title is persisted."""
        contact = _make_contact(title=None)
        company_mock = MagicMock()
        company_mock.name = "Acme Corp"
        contact.company = company_mock
        self._setup_enrich_mocks(mock_db, contact)

        sig_row = MagicMock()
        sig_row.sender_name = "Kasey Bevans"
        sig_row.sig_text = "Thanks!\n\nKasey Bevans\nVP of Operations\nAcme Corp"
        mock_db.execute.return_value.fetchall.return_value = [sig_row]

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

        mock_haiku.assert_called_once()
        assert contact.title == "VP of Operations"
        mock_db.commit.assert_called()

    @patch("src.api.routers.crm._enrich_with_haiku")
    def test_returns_existing_title_without_haiku(self, mock_haiku, test_client, mock_db):
        """When contact already has a title, return it immediately without Haiku."""
        contact = _make_contact(title="CEO")
        company_mock = MagicMock()
        company_mock.name = "Acme Corp"
        contact.company = company_mock
        self._setup_enrich_mocks(mock_db, contact)

        resp = test_client.post(f"/crm/api/contacts/{contact.id}/enrich-title")
        assert resp.status_code == 200
        assert resp.json()["title"] == "CEO"

        mock_haiku.assert_not_called()

    @patch("src.api.routers.crm._enrich_with_haiku")
    def test_returns_null_when_no_emails(self, mock_haiku, test_client, mock_db):
        """When contact has no emails with body, return null without Haiku."""
        contact = _make_contact(title=None)
        contact.company = None
        contact.company_id = None
        self._setup_enrich_mocks(mock_db, contact)

        mock_db.execute.return_value.fetchall.return_value = []

        resp = test_client.post(f"/crm/api/contacts/{contact.id}/enrich-title")
        assert resp.status_code == 200
        assert resp.json()["title"] is None

        mock_haiku.assert_not_called()

    @patch("src.api.routers.crm._enrich_with_haiku")
    def test_returns_null_when_haiku_finds_nothing(self, mock_haiku, test_client, mock_db):
        """When Haiku finds no title in signature, return null."""
        contact = _make_contact(title=None)
        company_mock = MagicMock()
        company_mock.name = "Acme Corp"
        contact.company = company_mock
        self._setup_enrich_mocks(mock_db, contact)

        sig_row = MagicMock()
        sig_row.sender_name = "Kasey Bevans"
        sig_row.sig_text = "Thanks!"
        mock_db.execute.return_value.fetchall.return_value = [sig_row]

        mock_haiku.return_value = {
            "kasey@acme.com": {"name": "Kasey Bevans", "title": None, "linkedin_url": None}
        }

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
