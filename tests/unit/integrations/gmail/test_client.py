"""
Unit tests for Gmail API client.
"""

from datetime import datetime
from unittest.mock import MagicMock, Mock, call, patch

import pytest
from googleapiclient.errors import HttpError

from src.integrations.gmail.client import GmailClient, GmailClientError


@pytest.fixture
def mock_credentials():
    """Mock OAuth2 credentials."""
    return {
        "access_token": "test_access_token",
        "refresh_token": "test_refresh_token",
        "token_uri": "https://oauth2.googleapis.com/token",
        "client_id": "test_client_id",
        "client_secret": "test_client_secret",
        "scopes": ["https://www.googleapis.com/auth/gmail.readonly"],
    }


@pytest.fixture
def mock_rate_limiter():
    """Mock rate limiter."""
    limiter = MagicMock()
    limiter.wait_for_token = MagicMock()
    return limiter


@pytest.fixture
def mock_gmail_service():
    """Mock Gmail API service."""
    with patch("src.integrations.gmail.client.build") as mock_build:
        gmail_mock = MagicMock()
        people_mock = MagicMock()

        def build_side_effect(service_name, version, credentials):
            if service_name == "gmail":
                return gmail_mock
            elif service_name == "people":
                return people_mock

        mock_build.side_effect = build_side_effect

        yield {"gmail": gmail_mock, "people": people_mock}


@pytest.fixture
def client(mock_credentials, mock_rate_limiter, mock_gmail_service):
    """Create Gmail client with mocked dependencies."""
    with patch("src.integrations.gmail.client.Credentials"):
        client = GmailClient(
            credentials=mock_credentials,
            rate_limiter=mock_rate_limiter,
        )
        client.gmail_service = mock_gmail_service["gmail"]
        client.people_service = mock_gmail_service["people"]
        return client


class TestGmailClientInit:
    """Test suite for GmailClient initialization."""

    def test_init_with_credentials(self, mock_credentials, mock_rate_limiter):
        """Test client initialization with credentials."""
        with patch("src.integrations.gmail.client.build"), \
             patch("src.integrations.gmail.client.Credentials") as mock_creds_class:
            mock_creds = MagicMock()
            mock_creds.expired = False
            mock_creds_class.return_value = mock_creds

            client = GmailClient(
                credentials=mock_credentials,
                rate_limiter=mock_rate_limiter,
            )

            assert client.rate_limiter == mock_rate_limiter
            assert client.gmail_service is not None
            assert client.people_service is not None

    def test_init_without_rate_limiter(self, mock_credentials):
        """Test client creates default rate limiter if not provided."""
        with patch("src.integrations.gmail.client.build"), \
             patch("src.integrations.gmail.client.Credentials"), \
             patch("src.integrations.gmail.client.GmailRateLimiter") as mock_limiter_class:
            mock_limiter = MagicMock()
            mock_limiter_class.return_value = mock_limiter

            client = GmailClient(credentials=mock_credentials)

            assert client.rate_limiter is not None


class TestFetchContacts:
    """Test suite for fetch_contacts method."""

    def test_fetch_contacts_success(self, client, mock_rate_limiter):
        """Test successful contact fetching."""
        # Mock People API response
        mock_response = {
            "connections": [
                {
                    "names": [{"displayName": "John Doe"}],
                    "emailAddresses": [{"value": "john@example.com"}],
                    "phoneNumbers": [{"value": "+1234567890"}],
                },
                {
                    "names": [{"displayName": "Jane Smith"}],
                    "emailAddresses": [{"value": "jane@example.com"}],
                },
                {
                    # Contact without email (should be skipped)
                    "names": [{"displayName": "No Email"}],
                },
            ],
            "nextPageToken": "next_token_123",
        }

        client.people_service.people().connections().list().execute.return_value = mock_response

        contacts, next_token = client.fetch_contacts(page_size=100)

        assert len(contacts) == 2
        assert contacts[0] == {
            "email": "john@example.com",
            "name": "John Doe",
            "phone": "+1234567890",
        }
        assert contacts[1] == {
            "email": "jane@example.com",
            "name": "Jane Smith",
            "phone": None,
        }
        assert next_token == "next_token_123"
        mock_rate_limiter.wait_for_token.assert_called()

    def test_fetch_contacts_with_page_token(self, client):
        """Test contact fetching with page token."""
        mock_response = {
            "connections": [],
            "nextPageToken": None,
        }

        mock_list = client.people_service.people().connections().list
        mock_list.return_value.execute.return_value = mock_response

        client.fetch_contacts(page_size=500, page_token="existing_token")

        # Verify page token was passed
        mock_list.assert_called_with(
            resourceName="people/me",
            pageSize=500,
            pageToken="existing_token",
            personFields="names,emailAddresses,phoneNumbers",
        )

    def test_fetch_contacts_http_error(self, client, mock_rate_limiter):
        """Test contact fetching handles HTTP errors."""
        client.people_service.people().connections().list().execute.side_effect = HttpError(
            resp=Mock(status=500),
            content=b"Server error",
        )

        with pytest.raises(GmailClientError, match="Failed to fetch contacts"):
            client.fetch_contacts()


class TestFetchEmailsChunked:
    """Test suite for fetch_emails_chunked method."""

    def test_fetch_emails_chunked_success(self, client, mock_rate_limiter):
        """Test successful email ID fetching."""
        mock_response = {
            "messages": [
                {"id": "msg1"},
                {"id": "msg2"},
                {"id": "msg3"},
            ],
            "nextPageToken": "next_page_token",
        }

        client.gmail_service.users().messages().list().execute.return_value = mock_response

        message_ids, next_token = client.fetch_emails_chunked(batch_size=500)

        assert message_ids == ["msg1", "msg2", "msg3"]
        assert next_token == "next_page_token"
        mock_rate_limiter.wait_for_token.assert_called()

    def test_fetch_emails_chunked_with_query(self, client):
        """Test email fetching with search query."""
        mock_response = {
            "messages": [{"id": "msg1"}],
        }

        mock_list = client.gmail_service.users().messages().list
        mock_list.return_value.execute.return_value = mock_response

        client.fetch_emails_chunked(query="is:unread")

        # Verify query was passed
        call_kwargs = mock_list.call_args[1]
        assert call_kwargs["q"] == "is:unread"

    def test_fetch_emails_chunked_empty_response(self, client):
        """Test email fetching with empty response."""
        mock_response = {"messages": []}

        client.gmail_service.users().messages().list().execute.return_value = mock_response

        message_ids, next_token = client.fetch_emails_chunked()

        assert message_ids == []
        assert next_token is None

    def test_fetch_emails_chunked_http_error(self, client):
        """Test email fetching handles HTTP errors."""
        client.gmail_service.users().messages().list().execute.side_effect = HttpError(
            resp=Mock(status=403),
            content=b"Forbidden",
        )

        with pytest.raises(GmailClientError, match="Failed to fetch email IDs"):
            client.fetch_emails_chunked()


class TestFetchMessageBatch:
    """Test suite for fetch_message_batch method."""

    def test_fetch_message_batch_success(self, client, mock_rate_limiter):
        """Test successful batch message fetching."""
        message_ids = ["msg1", "msg2"]

        # Mock batch request
        mock_batch = MagicMock()
        client.gmail_service.new_batch_http_request.return_value = mock_batch

        # Mock message responses
        def execute_batch():
            # Simulate batch callback for each message
            for i, msg_id in enumerate(message_ids):
                mock_message = {
                    "id": msg_id,
                    "threadId": f"thread{i}",
                    "snippet": f"Email {i} preview",
                    "payload": {
                        "headers": [
                            {"name": "From", "value": f"sender{i}@example.com"},
                            {"name": "To", "value": f"recipient{i}@example.com"},
                            {"name": "Subject", "value": f"Subject {i}"},
                            {"name": "Date", "value": "Mon, 1 Jan 2024 12:00:00 +0000"},
                        ],
                    },
                }
                # Find the callback that was added to the batch
                callback = mock_batch.add.call_args_list[i][0][1]
                callback(msg_id, mock_message, None)

        mock_batch.execute.side_effect = execute_batch

        emails = client.fetch_message_batch(message_ids)

        assert len(emails) == 2
        assert emails[0]["gmail_message_id"] == "msg1"
        assert emails[0]["sender_email"] == "sender0@example.com"
        assert emails[1]["gmail_message_id"] == "msg2"
        mock_rate_limiter.wait_for_token.assert_called()

    def test_fetch_message_batch_empty_list(self, client):
        """Test batch fetching with empty message list."""
        emails = client.fetch_message_batch([])

        assert emails == []

    def test_fetch_message_batch_large_list(self, client, mock_rate_limiter):
        """Test batch fetching splits large lists into chunks."""
        # Create 250 message IDs (should be split into 3 chunks: 100, 100, 50)
        message_ids = [f"msg{i}" for i in range(250)]

        mock_batch = MagicMock()
        client.gmail_service.new_batch_http_request.return_value = mock_batch
        mock_batch.execute.return_value = None

        client.fetch_message_batch(message_ids)

        # Should call new_batch_http_request 3 times (3 chunks)
        assert client.gmail_service.new_batch_http_request.call_count == 3
        # Should call wait_for_token 3 times (once per chunk)
        assert mock_rate_limiter.wait_for_token.call_count == 3

    def test_fetch_message_batch_handles_errors(self, client):
        """Test batch fetching handles individual message errors."""
        message_ids = ["msg1", "msg2"]

        mock_batch = MagicMock()
        client.gmail_service.new_batch_http_request.return_value = mock_batch

        def execute_batch():
            # First message succeeds, second fails
            callback1 = mock_batch.add.call_args_list[0][0][1]
            callback1("msg1", {
                "id": "msg1",
                "threadId": "thread1",
                "payload": {"headers": [
                    {"name": "From", "value": "sender@example.com"},
                    {"name": "Subject", "value": "Test"},
                    {"name": "Date", "value": "Mon, 1 Jan 2024 12:00:00 +0000"},
                ]},
            }, None)

            callback2 = mock_batch.add.call_args_list[1][0][1]
            callback2("msg2", None, Exception("API Error"))

        mock_batch.execute.side_effect = execute_batch

        emails = client.fetch_message_batch(message_ids)

        # Should return only successful message
        assert len(emails) == 1
        assert emails[0]["gmail_message_id"] == "msg1"


class TestParseMessage:
    """Test suite for _parse_message method."""

    def test_parse_message_full(self, client):
        """Test parsing complete message."""
        message = {
            "id": "msg123",
            "threadId": "thread456",
            "snippet": "Email preview text",
            "payload": {
                "headers": [
                    {"name": "From", "value": "John Doe <john@example.com>"},
                    {"name": "To", "value": "jane@example.com"},
                    {"name": "Subject", "value": "Test Email"},
                    {"name": "Date", "value": "Mon, 1 Jan 2024 12:00:00 +0000"},
                ],
                "parts": [
                    {"filename": "attachment.pdf"},
                    {"filename": "image.png"},
                ],
            },
        }

        parsed = client._parse_message(message)

        assert parsed["gmail_message_id"] == "msg123"
        assert parsed["gmail_thread_id"] == "thread456"
        assert parsed["subject"] == "Test Email"
        assert parsed["sender_email"] == "john@example.com"
        assert parsed["sender_name"] == "John Doe"
        assert parsed["recipient_emails"] == "jane@example.com"
        assert parsed["has_attachments"] is True
        assert parsed["attachment_count"] == 2
        assert parsed["snippet"] == "Email preview text"
        assert isinstance(parsed["date"], datetime)

    def test_parse_message_minimal(self, client):
        """Test parsing message with minimal data."""
        message = {
            "id": "msg123",
            "threadId": "thread456",
        }

        parsed = client._parse_message(message)

        assert parsed["gmail_message_id"] == "msg123"
        assert parsed["gmail_thread_id"] == "thread456"
        assert parsed["subject"] is None
        assert parsed["sender_email"] == ""
        assert parsed["has_attachments"] is False
        assert parsed["attachment_count"] == 0

    def test_parse_message_no_sender_name(self, client):
        """Test parsing message without sender display name."""
        message = {
            "id": "msg123",
            "threadId": "thread456",
            "payload": {
                "headers": [
                    {"name": "From", "value": "sender@example.com"},
                ],
            },
        }

        parsed = client._parse_message(message)

        assert parsed["sender_email"] == "sender@example.com"
        assert parsed["sender_name"] is None

    def test_parse_message_invalid_date(self, client):
        """Test parsing message with invalid date header."""
        message = {
            "id": "msg123",
            "threadId": "thread456",
            "internalDate": "1704110400000",  # Unix timestamp in milliseconds
            "payload": {
                "headers": [
                    {"name": "Date", "value": "Invalid Date Format"},
                ],
            },
        }

        parsed = client._parse_message(message)

        # Should fallback to internalDate
        assert parsed["date"] is not None
        assert isinstance(parsed["date"], datetime)


class TestGetMessageBody:
    """Test suite for get_message_body method."""

    def test_get_message_body_simple(self, client, mock_rate_limiter):
        """Test fetching message body from simple structure."""
        mock_message = {
            "id": "msg123",
            "payload": {
                "body": {
                    "data": "VGVzdCBtZXNzYWdlIGJvZHk=",  # "Test message body" in base64
                },
            },
        }

        client.gmail_service.users().messages().get().execute.return_value = mock_message

        body = client.get_message_body("msg123")

        assert body == "Test message body"
        mock_rate_limiter.wait_for_token.assert_called()

    def test_get_message_body_multipart(self, client):
        """Test fetching body from multipart message."""
        mock_message = {
            "id": "msg123",
            "payload": {
                "parts": [
                    {
                        "mimeType": "text/plain",
                        "body": {
                            "data": "UGxhaW4gdGV4dCBib2R5",  # "Plain text body"
                        },
                    },
                    {
                        "mimeType": "text/html",
                        "body": {
                            "data": "PGh0bWw+Ym9keTwvaHRtbD4=",
                        },
                    },
                ],
            },
        }

        client.gmail_service.users().messages().get().execute.return_value = mock_message

        body = client.get_message_body("msg123")

        assert body == "Plain text body"

    def test_get_message_body_not_found(self, client):
        """Test fetching body when no body exists."""
        mock_message = {
            "id": "msg123",
            "payload": {},
        }

        client.gmail_service.users().messages().get().execute.return_value = mock_message

        body = client.get_message_body("msg123")

        assert body is None

    def test_get_message_body_http_error(self, client):
        """Test get_message_body handles HTTP errors."""
        client.gmail_service.users().messages().get().execute.side_effect = HttpError(
            resp=Mock(status=404),
            content=b"Not found",
        )

        with pytest.raises(GmailClientError, match="Failed to fetch message body"):
            client.get_message_body("msg123")


class TestClientClose:
    """Test suite for client cleanup."""

    def test_close(self, client, mock_rate_limiter):
        """Test closing client and rate limiter."""
        client.close()

        mock_rate_limiter.close.assert_called_once()
