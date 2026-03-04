"""
Pytest configuration and shared fixtures.
"""

import asyncio
from collections.abc import Generator
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from src.api.main import app
from src.core.config import settings
from src.core.database import get_sync_db
from src.models.user import User


@pytest.fixture(scope="session")
def event_loop() -> Generator:
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def test_user_id() -> str:
    """Test user ID fixture."""
    return "test-user-123"


@pytest.fixture
def test_account_labels() -> list[str]:
    """Test account labels fixture."""
    return ["procore-main", "procore-private", "personal"]


@pytest.fixture
def mock_user() -> MagicMock:
    """
    Mock User object with realistic defaults for a single-user system.

    Attributes match the User SQLAlchemy model (UUIDMixin gives .id as UUID).
    """
    user = MagicMock(spec=User)
    user.id = UUID("d4475ca3-0ddc-4ea0-ac89-95ae7fed1e31")
    user.email = "test@example.com"
    user.name = "Test User"
    return user


@pytest.fixture
def mock_db(mock_user: MagicMock) -> MagicMock:
    """
    Mock synchronous SQLAlchemy Session.

    Configures the chained query pattern used by the CRM and dashboard routers:
      - db.query(...).options(...).outerjoin(...).filter(...).count()  -> 0
      - db.query(...).options(...).outerjoin(...).filter(...).offset(...).limit(...).all() -> []
      - db.query(User).first() -> mock_user  (used by get_current_user)
      - db.query(...).scalar() -> 0
      - db.query(...).first() -> None  (for non-User model queries)

    All chained calls return the same mock query object so that the count/all
    configuration is accessible regardless of how many intermediate calls are made.
    """
    db = MagicMock(spec=Session)

    # A single query mock whose chained methods all return itself, allowing
    # arbitrary call chains to terminate at .count(), .all(), .scalar(), or .first().
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

    # Terminal methods
    query_mock.count.return_value = 0
    query_mock.all.return_value = []
    query_mock.scalar.return_value = 0
    query_mock.first.return_value = mock_user  # get_current_user calls db.query(User).first()

    db.query.return_value = query_mock

    return db


@pytest.fixture
def test_client(mock_db: MagicMock) -> Generator[TestClient, None, None]:
    """
    FastAPI TestClient with:
      - get_sync_db dependency overridden to inject mock_db (no real DB needed)
      - X-API-Key header pre-set to the configured secret_key

    The dependency override uses a generator to match FastAPI's expected signature
    for yield-based dependencies.
    """

    def override_get_sync_db():
        yield mock_db

    app.dependency_overrides[get_sync_db] = override_get_sync_db

    with TestClient(app, headers={"X-API-Key": settings.secret_key}) as client:
        yield client

    # Clean up overrides after each test to prevent state leakage between test modules
    app.dependency_overrides.clear()
