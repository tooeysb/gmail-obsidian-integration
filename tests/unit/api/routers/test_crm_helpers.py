"""
Unit tests for CRM router helper functions.
Tests serialization and utility functions without requiring a database.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from src.api.routers.crm import _serialize_contact, _serialize_dt


class TestSerializeDt:
    """Test datetime serialization helper."""

    def test_none(self):
        assert _serialize_dt(None) is None

    def test_naive_datetime(self):
        dt = datetime(2024, 1, 15, 10, 30, 0)
        result = _serialize_dt(dt)
        assert result == "2024-01-15T10:30:00"

    def test_aware_datetime(self):
        dt = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        result = _serialize_dt(dt)
        assert "2024-01-15" in result
        assert "10:30:00" in result


class TestSerializeContact:
    """Test contact serialization."""

    def _make_contact(self, **overrides):
        """Create a mock Contact with sensible defaults."""
        contact = MagicMock()
        defaults = {
            "id": uuid4(),
            "name": "John Doe",
            "email": "john@example.com",
            "phone": "+1234567890",
            "title": "Engineer",
            "contact_type": "Customer",
            "is_vip": False,
            "email_count": 42,
            "tags": ["vip", "construction"],
            "relationship_context": "Key account contact",
            "company_id": uuid4(),
            "last_contact_at": datetime(2024, 1, 15, tzinfo=timezone.utc),
            "notes": "Test notes",
            "personal_email": "john.personal@gmail.com",
            "account_sources": ["procore-main"],
            "salesforce_id": "SF001",
            "address": "123 Main St",
            "created_at": datetime(2023, 1, 1, tzinfo=timezone.utc),
            "updated_at": datetime(2024, 1, 15, tzinfo=timezone.utc),
        }
        defaults.update(overrides)
        for k, v in defaults.items():
            setattr(contact, k, v)
        return contact

    def test_basic_fields(self):
        contact = self._make_contact()
        result = _serialize_contact(contact, "Acme Corp")

        assert result["name"] == "John Doe"
        assert result["email"] == "john@example.com"
        assert result["phone"] == "+1234567890"
        assert result["title"] == "Engineer"
        assert result["company_name"] == "Acme Corp"
        assert result["email_count"] == 42

    def test_no_company(self):
        contact = self._make_contact(company_id=None)
        result = _serialize_contact(contact, None)

        assert result["company_name"] is None
        assert result["company_id"] is None
        assert result["company"] is None

    def test_with_company(self):
        cid = uuid4()
        contact = self._make_contact(company_id=cid)
        result = _serialize_contact(contact, "Acme Corp")

        assert result["company"]["name"] == "Acme Corp"
        assert result["company"]["id"] == str(cid)

    def test_tags_default_to_empty(self):
        contact = self._make_contact(tags=None)
        result = _serialize_contact(contact, None)
        assert result["tags"] == []

    def test_account_sources_default_to_empty(self):
        contact = self._make_contact(account_sources=None)
        result = _serialize_contact(contact, None)
        assert result["account_sources"] == []

    def test_datetime_serialized(self):
        dt = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        contact = self._make_contact(last_contact_at=dt)
        result = _serialize_contact(contact, None)
        assert "2024-06-15" in result["last_contact_at"]

    def test_none_datetimes(self):
        contact = self._make_contact(
            last_contact_at=None, created_at=None, updated_at=None
        )
        result = _serialize_contact(contact, None)
        assert result["last_contact_at"] is None
        assert result["created_at"] is None
        assert result["updated_at"] is None
