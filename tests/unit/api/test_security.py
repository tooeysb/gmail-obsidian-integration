"""
Security hardening tests for Phase 1.

Covers:
  - API key authentication on protected endpoints (401 without/wrong key)
  - Sort column injection prevention (400 on invalid column names)
  - Public endpoints that must remain unauthenticated
"""

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from src.api.main import app
from src.core.config import settings
from src.core.database import get_sync_db
from src.models.user import User

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _make_mock_db(mock_user: MagicMock | None = None) -> MagicMock:
    """
    Build a self-chaining query mock suitable for any endpoint test.

    When mock_user is provided it is returned by .first() so that
    get_current_user resolves successfully; otherwise .first() returns None.
    """
    from unittest.mock import MagicMock

    from sqlalchemy.orm import Session

    db = MagicMock(spec=Session)

    query_mock = MagicMock()
    query_mock.options.return_value = query_mock
    query_mock.outerjoin.return_value = query_mock
    query_mock.join.return_value = query_mock
    query_mock.filter.return_value = query_mock
    query_mock.order_by.return_value = query_mock
    query_mock.offset.return_value = query_mock
    query_mock.limit.return_value = query_mock
    query_mock.group_by.return_value = query_mock
    query_mock.subquery.return_value = query_mock
    query_mock.label.return_value = query_mock

    query_mock.count.return_value = 0
    query_mock.all.return_value = []
    query_mock.scalar.return_value = 0
    query_mock.first.return_value = mock_user  # None → 404 from get_current_user if reached

    db.query.return_value = query_mock
    return db


@pytest.fixture
def mock_user_obj() -> MagicMock:
    """Minimal mock User for get_current_user resolution."""
    from uuid import UUID

    user = MagicMock(spec=User)
    user.id = UUID("d4475ca3-0ddc-4ea0-ac89-95ae7fed1e31")
    user.email = "test@example.com"
    user.name = "Test User"
    return user


@pytest.fixture
def authed_client(mock_user_obj: MagicMock) -> TestClient:
    """
    TestClient authenticated with the real secret_key and a properly mocked DB.

    The mock DB's .first() returns mock_user_obj so that get_current_user
    succeeds without hitting a real database.
    """
    mock_db = _make_mock_db(mock_user=mock_user_obj)

    def override_get_sync_db():
        yield mock_db

    app.dependency_overrides[get_sync_db] = override_get_sync_db
    client = TestClient(app, headers={"X-API-Key": settings.secret_key})
    yield client
    app.dependency_overrides.clear()


@pytest.fixture
def unauthed_client() -> TestClient:
    """
    TestClient with NO API key header.

    The DB override is still registered to prevent accidental real DB calls,
    but auth should be rejected before the DB layer is reached.
    """
    mock_db = _make_mock_db(mock_user=None)

    def override_get_sync_db():
        yield mock_db

    app.dependency_overrides[get_sync_db] = override_get_sync_db
    client = TestClient(app, raise_server_exceptions=True)
    yield client
    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Authentication tests — missing API key
# ---------------------------------------------------------------------------


class TestMissingApiKey:
    """Requests without X-API-Key must receive 401."""

    def test_crm_contacts_no_api_key_returns_401(self, unauthed_client: TestClient):
        response = unauthed_client.get("/crm/api/contacts")
        assert response.status_code == 401

    def test_dashboard_stats_no_api_key_returns_401(self, unauthed_client: TestClient):
        response = unauthed_client.get("/dashboard/stats")
        assert response.status_code == 401

    def test_draft_compose_no_api_key_returns_401(self, unauthed_client: TestClient):
        response = unauthed_client.post(
            "/draft/compose",
            json={
                "recipient_email": "bob@example.com",
                "context": "Following up on the proposal",
            },
        )
        assert response.status_code == 401


# ---------------------------------------------------------------------------
# Authentication tests — wrong API key
# ---------------------------------------------------------------------------


class TestWrongApiKey:
    """Requests with an incorrect X-API-Key must receive 401."""

    def test_crm_contacts_wrong_api_key_returns_401(self):
        mock_db = _make_mock_db(mock_user=None)

        def override():
            yield mock_db

        app.dependency_overrides[get_sync_db] = override
        try:
            client = TestClient(app, headers={"X-API-Key": "definitely-wrong-key"})
            response = client.get("/crm/api/contacts")
            assert response.status_code == 401
        finally:
            app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Authentication tests — valid API key succeeds
# ---------------------------------------------------------------------------


class TestValidApiKey:
    """Requests with a valid API key should be served (not rejected)."""

    def test_crm_contacts_valid_api_key_succeeds(self, authed_client: TestClient):
        """GET /crm/api/contacts with correct key and empty DB returns 200."""
        response = authed_client.get("/crm/api/contacts")
        assert response.status_code == 200

        data = response.json()
        # Paginated envelope always present
        assert "items" in data
        assert "total" in data
        assert data["items"] == []
        assert data["total"] == 0


# ---------------------------------------------------------------------------
# Sort injection prevention — contacts
# ---------------------------------------------------------------------------


class TestContactSortInjection:
    """
    The /crm/api/contacts endpoint whitelists sort columns.
    Requests with unsupported column names must return 400.
    """

    def test_sort_injection_invalid_column_returns_400(self, authed_client: TestClient):
        """Passing a dangerous column name like __dict__ must be rejected."""
        response = authed_client.get("/crm/api/contacts?sort_by=__dict__")
        assert response.status_code == 400
        detail = response.json().get("detail", "")
        assert "sort" in detail.lower() or "invalid" in detail.lower()

    def test_sort_injection_valid_column_succeeds(self, authed_client: TestClient):
        """Known-safe sort column email_count must be accepted."""
        response = authed_client.get("/crm/api/contacts?sort_by=email_count")
        assert response.status_code == 200

    def test_sort_injection_name_column_succeeds(self, authed_client: TestClient):
        """Known-safe sort column name must be accepted."""
        response = authed_client.get("/crm/api/contacts?sort_by=name&sort_dir=asc")
        assert response.status_code == 200

    def test_sort_injection_sql_keyword_returns_400(self, authed_client: TestClient):
        """SQL keyword injection attempt must be rejected."""
        response = authed_client.get("/crm/api/contacts?sort_by=1%3BDROP%20TABLE")
        assert response.status_code == 400


# ---------------------------------------------------------------------------
# Sort injection prevention — companies
# ---------------------------------------------------------------------------


class TestCompanySortInjection:
    """
    The /crm/api/companies endpoint has its own column whitelist.
    Unsupported column names must return 400.
    """

    def test_company_sort_injection_returns_400(self, authed_client: TestClient):
        """Passing __dict__ as sort_by on the companies endpoint must be rejected."""
        response = authed_client.get("/crm/api/companies?sort_by=__dict__")
        assert response.status_code == 400
        detail = response.json().get("detail", "")
        assert "sort" in detail.lower() or "invalid" in detail.lower()

    def test_company_valid_sort_column_succeeds(self, authed_client: TestClient):
        """Known-safe sort column arr must be accepted."""
        response = authed_client.get("/crm/api/companies?sort_by=arr")
        assert response.status_code == 200


# ---------------------------------------------------------------------------
# Public endpoints (no API key required)
# ---------------------------------------------------------------------------


class TestPublicEndpoints:
    """Endpoints that must be reachable without any API key."""

    def test_health_check_is_public(self, unauthed_client: TestClient):
        """
        GET /health is a diagnostic endpoint.
        It may return 200 (healthy) or 503 (degraded) but never 401/403,
        since the mock DB and Redis will not be available in unit tests.
        """
        response = unauthed_client.get("/health")
        assert response.status_code in (
            200,
            503,
        ), f"Expected 200 or 503 from /health, got {response.status_code}"

    def test_root_is_public(self, unauthed_client: TestClient):
        """GET / redirects to /crm without any API key."""
        response = unauthed_client.get("/", follow_redirects=False)
        assert response.status_code == 307
        assert "/crm" in response.headers["location"]
