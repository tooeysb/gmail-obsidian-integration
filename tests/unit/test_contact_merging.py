"""
Unit tests for contact merging logic.

Tests cover:
- Merging contacts with same email
- Handling different names for same email
- Handling missing data (names, phones)
- Aggregating email counts
- Combining account sources
- Database upsert behavior
"""

import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, Mock, patch

import pytest

from src.models.contact import Contact
from src.services.gmail.contact_merger import (
    _merge_contact_group,
    _resolve_last_contact,
    _resolve_name,
    _resolve_phone,
    merge_contacts_by_email,
)


@pytest.fixture
def user_id() -> uuid.UUID:
    """Fixture for a test user ID."""
    return uuid.uuid4()


@pytest.fixture
def mock_db_session() -> Mock:
    """Fixture for a mocked database session."""
    session = MagicMock()
    return session


class TestMergeContactsSameEmail:
    """Test merging contacts with the same email address."""

    def test_merge_two_contacts_same_email_different_accounts(
        self, user_id: uuid.UUID, mock_db_session: Mock
    ) -> None:
        """Test merging two contacts with same email from different accounts."""
        contacts = [
            {
                "email": "john.doe@example.com",
                "name": "John Doe",
                "phone": "+1234567890",
                "account_source": "procore-main",
                "email_count": 10,
                "last_contact_at": datetime(2024, 1, 15, tzinfo=timezone.utc),
            },
            {
                "email": "john.doe@example.com",
                "name": "John Doe",
                "phone": "+1234567890",
                "account_source": "personal",
                "email_count": 5,
                "last_contact_at": datetime(2024, 1, 20, tzinfo=timezone.utc),
            },
        ]

        # Mock the database query result
        mock_contact = Contact(
            id=uuid.uuid4(),
            user_id=user_id,
            email="john.doe@example.com",
            name="John Doe",
            phone="+1234567890",
            account_sources=["procore-main", "personal"],
            email_count=15,
            last_contact_at=datetime(2024, 1, 20, tzinfo=timezone.utc),
        )
        mock_db_session.execute.return_value.scalars.return_value.all.return_value = [
            mock_contact
        ]

        result = merge_contacts_by_email(contacts, user_id, mock_db_session)

        # Verify database operations
        assert mock_db_session.execute.called
        assert mock_db_session.commit.called

        # Verify result
        assert len(result) == 1
        assert result[0].email == "john.doe@example.com"
        assert result[0].email_count == 15
        assert set(result[0].account_sources) == {"procore-main", "personal"}

    def test_merge_contacts_aggregates_email_counts(
        self, user_id: uuid.UUID, mock_db_session: Mock
    ) -> None:
        """Test that email counts are aggregated correctly across accounts."""
        contacts = [
            {
                "email": "jane@example.com",
                "name": "Jane Smith",
                "account_source": "procore-main",
                "email_count": 25,
            },
            {
                "email": "jane@example.com",
                "name": "Jane Smith",
                "account_source": "procore-private",
                "email_count": 15,
            },
            {
                "email": "jane@example.com",
                "name": "Jane Smith",
                "account_source": "personal",
                "email_count": 10,
            },
        ]

        mock_contact = Contact(
            id=uuid.uuid4(),
            user_id=user_id,
            email="jane@example.com",
            name="Jane Smith",
            account_sources=["procore-main", "procore-private", "personal"],
            email_count=50,
        )
        mock_db_session.execute.return_value.scalars.return_value.all.return_value = [
            mock_contact
        ]

        result = merge_contacts_by_email(contacts, user_id, mock_db_session)

        assert len(result) == 1
        assert result[0].email_count == 50

    def test_merge_contacts_deduplicates_account_sources(
        self, user_id: uuid.UUID, mock_db_session: Mock
    ) -> None:
        """Test that duplicate account sources are removed."""
        contacts = [
            {
                "email": "bob@example.com",
                "name": "Bob",
                "account_source": "procore-main",
                "email_count": 5,
            },
            {
                "email": "bob@example.com",
                "name": "Bob",
                "account_source": "procore-main",  # Duplicate
                "email_count": 3,
            },
        ]

        mock_contact = Contact(
            id=uuid.uuid4(),
            user_id=user_id,
            email="bob@example.com",
            name="Bob",
            account_sources=["procore-main"],  # Only one entry
            email_count=8,
        )
        mock_db_session.execute.return_value.scalars.return_value.all.return_value = [
            mock_contact
        ]

        result = merge_contacts_by_email(contacts, user_id, mock_db_session)

        assert len(result) == 1
        assert result[0].account_sources == ["procore-main"]


class TestMergeContactsDifferentNames:
    """Test handling of name conflicts when merging contacts."""

    def test_merge_contacts_prefers_most_recent_name(
        self, user_id: uuid.UUID, mock_db_session: Mock
    ) -> None:
        """Test that most recent name is used when names differ."""
        contacts = [
            {
                "email": "john@example.com",
                "name": "John D",
                "account_source": "procore-main",
                "last_contact_at": datetime(2024, 1, 10, tzinfo=timezone.utc),
            },
            {
                "email": "john@example.com",
                "name": "John Doe",  # More recent, should be used
                "account_source": "personal",
                "last_contact_at": datetime(2024, 1, 20, tzinfo=timezone.utc),
            },
        ]

        mock_contact = Contact(
            id=uuid.uuid4(),
            user_id=user_id,
            email="john@example.com",
            name="John Doe",
            account_sources=["procore-main", "personal"],
        )
        mock_db_session.execute.return_value.scalars.return_value.all.return_value = [
            mock_contact
        ]

        result = merge_contacts_by_email(contacts, user_id, mock_db_session)

        assert len(result) == 1
        assert result[0].name == "John Doe"

    def test_merge_contacts_handles_empty_names(
        self, user_id: uuid.UUID, mock_db_session: Mock
    ) -> None:
        """Test that empty names are ignored in favor of non-empty names."""
        contacts = [
            {
                "email": "jane@example.com",
                "name": "",  # Empty
                "account_source": "procore-main",
                "last_contact_at": datetime(2024, 1, 20, tzinfo=timezone.utc),
            },
            {
                "email": "jane@example.com",
                "name": "Jane Smith",  # Should be used
                "account_source": "personal",
                "last_contact_at": datetime(2024, 1, 10, tzinfo=timezone.utc),
            },
        ]

        mock_contact = Contact(
            id=uuid.uuid4(),
            user_id=user_id,
            email="jane@example.com",
            name="Jane Smith",
            account_sources=["procore-main", "personal"],
        )
        mock_db_session.execute.return_value.scalars.return_value.all.return_value = [
            mock_contact
        ]

        result = merge_contacts_by_email(contacts, user_id, mock_db_session)

        assert len(result) == 1
        assert result[0].name == "Jane Smith"


class TestMergeContactsMissingData:
    """Test handling of missing or incomplete contact data."""

    def test_merge_contacts_with_missing_names(
        self, user_id: uuid.UUID, mock_db_session: Mock
    ) -> None:
        """Test merging contacts when all names are missing."""
        contacts = [
            {
                "email": "noreply@example.com",
                "account_source": "procore-main",
                "email_count": 10,
            },
            {
                "email": "noreply@example.com",
                "account_source": "personal",
                "email_count": 5,
            },
        ]

        mock_contact = Contact(
            id=uuid.uuid4(),
            user_id=user_id,
            email="noreply@example.com",
            name=None,
            account_sources=["procore-main", "personal"],
            email_count=15,
        )
        mock_db_session.execute.return_value.scalars.return_value.all.return_value = [
            mock_contact
        ]

        result = merge_contacts_by_email(contacts, user_id, mock_db_session)

        assert len(result) == 1
        assert result[0].email == "noreply@example.com"
        assert result[0].name is None

    def test_merge_contacts_with_missing_phone(
        self, user_id: uuid.UUID, mock_db_session: Mock
    ) -> None:
        """Test merging contacts when phone numbers are missing."""
        contacts = [
            {
                "email": "alice@example.com",
                "name": "Alice",
                "account_source": "procore-main",
            },
            {
                "email": "alice@example.com",
                "name": "Alice",
                "account_source": "personal",
            },
        ]

        mock_contact = Contact(
            id=uuid.uuid4(),
            user_id=user_id,
            email="alice@example.com",
            name="Alice",
            phone=None,
            account_sources=["procore-main", "personal"],
        )
        mock_db_session.execute.return_value.scalars.return_value.all.return_value = [
            mock_contact
        ]

        result = merge_contacts_by_email(contacts, user_id, mock_db_session)

        assert len(result) == 1
        assert result[0].phone is None

    def test_merge_contacts_uses_first_non_empty_phone(
        self, user_id: uuid.UUID, mock_db_session: Mock
    ) -> None:
        """Test that first non-empty phone number is used."""
        contacts = [
            {
                "email": "bob@example.com",
                "name": "Bob",
                "phone": "",  # Empty
                "account_source": "procore-main",
            },
            {
                "email": "bob@example.com",
                "name": "Bob",
                "phone": "+1234567890",  # Should be used
                "account_source": "personal",
            },
        ]

        mock_contact = Contact(
            id=uuid.uuid4(),
            user_id=user_id,
            email="bob@example.com",
            name="Bob",
            phone="+1234567890",
            account_sources=["procore-main", "personal"],
        )
        mock_db_session.execute.return_value.scalars.return_value.all.return_value = [
            mock_contact
        ]

        result = merge_contacts_by_email(contacts, user_id, mock_db_session)

        assert len(result) == 1
        assert result[0].phone == "+1234567890"

    def test_merge_contacts_with_missing_email_count(
        self, user_id: uuid.UUID, mock_db_session: Mock
    ) -> None:
        """Test that missing email_count is treated as 0."""
        contacts = [
            {
                "email": "test@example.com",
                "name": "Test",
                "account_source": "procore-main",
                "email_count": 10,
            },
            {
                "email": "test@example.com",
                "name": "Test",
                "account_source": "personal",
                # email_count missing
            },
        ]

        mock_contact = Contact(
            id=uuid.uuid4(),
            user_id=user_id,
            email="test@example.com",
            name="Test",
            account_sources=["procore-main", "personal"],
            email_count=10,  # Only counts the first one
        )
        mock_db_session.execute.return_value.scalars.return_value.all.return_value = [
            mock_contact
        ]

        result = merge_contacts_by_email(contacts, user_id, mock_db_session)

        assert len(result) == 1
        assert result[0].email_count == 10


class TestMergeContactsEdgeCases:
    """Test edge cases and error conditions."""

    def test_merge_empty_contact_list(self, user_id: uuid.UUID, mock_db_session: Mock) -> None:
        """Test that empty contact list returns empty result."""
        result = merge_contacts_by_email([], user_id, mock_db_session)

        assert result == []
        assert not mock_db_session.execute.called
        assert not mock_db_session.commit.called

    def test_merge_contacts_with_empty_email(
        self, user_id: uuid.UUID, mock_db_session: Mock
    ) -> None:
        """Test that contacts with empty emails are skipped."""
        contacts = [
            {
                "email": "",  # Empty email
                "name": "John",
                "account_source": "procore-main",
            },
            {
                "email": "valid@example.com",
                "name": "Jane",
                "account_source": "personal",
            },
        ]

        mock_contact = Contact(
            id=uuid.uuid4(),
            user_id=user_id,
            email="valid@example.com",
            name="Jane",
            account_sources=["personal"],
        )
        mock_db_session.execute.return_value.scalars.return_value.all.return_value = [
            mock_contact
        ]

        result = merge_contacts_by_email(contacts, user_id, mock_db_session)

        # Only valid contact should be processed
        assert len(result) == 1
        assert result[0].email == "valid@example.com"

    def test_merge_contacts_email_case_insensitive(
        self, user_id: uuid.UUID, mock_db_session: Mock
    ) -> None:
        """Test that email merging is case-insensitive."""
        contacts = [
            {
                "email": "John.Doe@EXAMPLE.COM",
                "name": "John",
                "account_source": "procore-main",
            },
            {
                "email": "john.doe@example.com",
                "name": "John",
                "account_source": "personal",
            },
        ]

        mock_contact = Contact(
            id=uuid.uuid4(),
            user_id=user_id,
            email="john.doe@example.com",
            name="John",
            account_sources=["procore-main", "personal"],
        )
        mock_db_session.execute.return_value.scalars.return_value.all.return_value = [
            mock_contact
        ]

        result = merge_contacts_by_email(contacts, user_id, mock_db_session)

        # Should be merged into one contact with lowercase email
        assert len(result) == 1
        assert result[0].email.lower() == "john.doe@example.com"

    def test_merge_multiple_distinct_emails(
        self, user_id: uuid.UUID, mock_db_session: Mock
    ) -> None:
        """Test merging list with multiple distinct email addresses."""
        contacts = [
            {
                "email": "john@example.com",
                "name": "John",
                "account_source": "procore-main",
            },
            {
                "email": "jane@example.com",
                "name": "Jane",
                "account_source": "personal",
            },
            {
                "email": "john@example.com",  # Duplicate of first
                "name": "John Doe",
                "account_source": "procore-private",
            },
        ]

        mock_contact_1 = Contact(
            id=uuid.uuid4(),
            user_id=user_id,
            email="john@example.com",
            name="John Doe",
            account_sources=["procore-main", "procore-private"],
        )
        mock_contact_2 = Contact(
            id=uuid.uuid4(),
            user_id=user_id,
            email="jane@example.com",
            name="Jane",
            account_sources=["personal"],
        )
        mock_db_session.execute.return_value.scalars.return_value.all.return_value = [
            mock_contact_1,
            mock_contact_2,
        ]

        result = merge_contacts_by_email(contacts, user_id, mock_db_session)

        # Should result in 2 contacts (John and Jane)
        assert len(result) == 2
        emails = {c.email for c in result}
        assert emails == {"john@example.com", "jane@example.com"}


class TestHelperFunctions:
    """Test internal helper functions."""

    def test_resolve_name_prefers_most_recent(self) -> None:
        """Test _resolve_name prefers most recent non-empty name."""
        contacts = [
            {
                "name": "Old Name",
                "last_contact_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
            },
            {
                "name": "New Name",
                "last_contact_at": datetime(2024, 1, 15, tzinfo=timezone.utc),
            },
        ]

        result = _resolve_name(contacts)
        assert result == "New Name"

    def test_resolve_name_handles_none_timestamps(self) -> None:
        """Test _resolve_name handles missing timestamps."""
        contacts = [
            {"name": "Name 1"},  # No timestamp
            {"name": "Name 2", "last_contact_at": datetime(2024, 1, 1, tzinfo=timezone.utc)},
        ]

        result = _resolve_name(contacts)
        # Should pick Name 2 since it has a timestamp
        assert result == "Name 2"

    def test_resolve_phone_uses_first_non_empty(self) -> None:
        """Test _resolve_phone uses first non-empty phone."""
        contacts = [
            {"phone": ""},
            {"phone": "+1234567890"},
            {"phone": "+0987654321"},
        ]

        result = _resolve_phone(contacts)
        assert result == "+1234567890"

    def test_resolve_last_contact_finds_max(self) -> None:
        """Test _resolve_last_contact finds most recent timestamp."""
        time1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        time2 = datetime(2024, 1, 15, tzinfo=timezone.utc)
        time3 = datetime(2024, 1, 10, tzinfo=timezone.utc)

        contacts = [
            {"last_contact_at": time1},
            {"last_contact_at": time2},
            {"last_contact_at": time3},
        ]

        result = _resolve_last_contact(contacts)
        assert result == time2

    def test_merge_contact_group_integration(self) -> None:
        """Test _merge_contact_group with complete contact data."""
        now = datetime.now(timezone.utc)
        contacts = [
            {
                "name": "John Doe",
                "phone": "+1234567890",
                "account_source": "procore-main",
                "email_count": 10,
                "last_contact_at": now,
            },
            {
                "name": "John D",
                "phone": "+0987654321",
                "account_source": "personal",
                "email_count": 5,
                "last_contact_at": now - timedelta(days=1),
            },
        ]

        result = _merge_contact_group(contacts)

        assert result["name"] == "John Doe"  # Most recent
        assert result["phone"] == "+1234567890"  # First non-empty
        assert result["email_count"] == 15
        assert set(result["account_sources"]) == {"procore-main", "personal"}
        assert result["last_contact_at"] == now
