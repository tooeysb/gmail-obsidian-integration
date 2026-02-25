"""
Pytest configuration and shared fixtures.
"""

import asyncio
from typing import Generator

import pytest


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


# TODO: Add more fixtures as needed:
# - test_client: FastAPI TestClient
# - test_db: Test database session
# - mock_gmail_client: Mocked Gmail API client
# - mock_claude_client: Mocked Claude API client
# - sample_contacts: Sample contact data
# - sample_emails: Sample email data
