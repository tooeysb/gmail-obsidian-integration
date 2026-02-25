"""
Gmail API client with rate limiting and batching support.
Handles email and contact fetching with proper pagination and error handling.
"""

import base64
import email
from datetime import datetime
from email.utils import parseaddr, parsedate_to_datetime
from typing import Any

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from src.core.config import settings
from src.integrations.gmail.rate_limiter import GmailRateLimiter, with_retry


class GmailClientError(Exception):
    """Base exception for Gmail client errors."""

    pass


class GmailClient:
    """
    Gmail API client with rate limiting and batch operations.

    Provides methods to:
    - Fetch contacts from People API
    - Fetch emails with pagination
    - Batch fetch multiple messages efficiently
    - Parse email metadata
    """

    def __init__(
        self,
        credentials: dict,
        rate_limiter: GmailRateLimiter | None = None,
    ):
        """
        Initialize Gmail client with OAuth2 credentials.

        Args:
            credentials: OAuth2 credentials dict with access_token, refresh_token, etc.
            rate_limiter: Optional rate limiter instance (creates default if not provided)
        """
        self.credentials = self._build_credentials(credentials)
        self.rate_limiter = rate_limiter or GmailRateLimiter()

        # Build Gmail and People API services
        self.gmail_service = build("gmail", "v1", credentials=self.credentials)
        self.people_service = build("people", "v1", credentials=self.credentials)

    def _build_credentials(self, creds_dict: dict) -> Credentials:
        """
        Build Google OAuth2 Credentials object from dict.

        Args:
            creds_dict: Dictionary containing OAuth2 credentials

        Returns:
            Credentials object for Google API
        """
        creds = Credentials(
            token=creds_dict.get("access_token"),
            refresh_token=creds_dict.get("refresh_token"),
            token_uri=creds_dict.get("token_uri"),
            client_id=creds_dict.get("client_id"),
            client_secret=creds_dict.get("client_secret"),
            scopes=creds_dict.get("scopes", []),
        )

        # Refresh token if expired
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())

        return creds

    @with_retry
    def fetch_contacts(
        self,
        page_size: int = 1000,
        page_token: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """
        Fetch contacts from People API with pagination.

        Args:
            page_size: Number of contacts per page (max 1000)
            page_token: Token for next page (from previous call)

        Returns:
            Tuple of (contacts_list, next_page_token)

        Example:
            contacts, next_token = client.fetch_contacts(page_size=500)
            while next_token:
                more_contacts, next_token = client.fetch_contacts(
                    page_size=500,
                    page_token=next_token
                )
                contacts.extend(more_contacts)
        """
        self.rate_limiter.wait_for_token()

        try:
            request = self.people_service.people().connections().list(
                resourceName="people/me",
                pageSize=min(page_size, 1000),
                pageToken=page_token,
                personFields="names,emailAddresses,phoneNumbers",
            )
            response = request.execute()

            connections = response.get("connections", [])
            contacts = []

            for person in connections:
                # Extract primary email
                email_addresses = person.get("emailAddresses", [])
                if not email_addresses:
                    continue

                primary_email = email_addresses[0].get("value")
                if not primary_email:
                    continue

                # Extract name
                names = person.get("names", [])
                name = names[0].get("displayName") if names else None

                # Extract phone
                phone_numbers = person.get("phoneNumbers", [])
                phone = phone_numbers[0].get("value") if phone_numbers else None

                contacts.append({
                    "email": primary_email,
                    "name": name,
                    "phone": phone,
                })

            next_page_token = response.get("nextPageToken")
            return contacts, next_page_token

        except HttpError as e:
            raise GmailClientError(f"Failed to fetch contacts: {e}")

    @with_retry
    def fetch_emails_chunked(
        self,
        batch_size: int | None = None,
        page_token: str | None = None,
        query: str | None = None,
    ) -> tuple[list[str], str | None]:
        """
        Fetch email message IDs with pagination.

        This fetches only message IDs, not full messages.
        Use fetch_message_batch() to get full message details.

        Args:
            batch_size: Number of message IDs per page (default: settings.gmail_batch_size)
            page_token: Token for next page (from previous call)
            query: Gmail search query (e.g., "is:unread", "from:example@example.com")

        Returns:
            Tuple of (message_ids, next_page_token)

        Example:
            message_ids, next_token = client.fetch_emails_chunked(batch_size=500)
            messages = client.fetch_message_batch(message_ids)
        """
        batch_size = batch_size or settings.gmail_batch_size
        self.rate_limiter.wait_for_token()

        try:
            request = self.gmail_service.users().messages().list(
                userId="me",
                maxResults=min(batch_size, 500),
                pageToken=page_token,
                q=query,
            )
            response = request.execute()

            messages = response.get("messages", [])
            message_ids = [msg["id"] for msg in messages]
            next_page_token = response.get("nextPageToken")

            return message_ids, next_page_token

        except HttpError as e:
            raise GmailClientError(f"Failed to fetch email IDs: {e}")

    @with_retry
    def fetch_message_batch(
        self,
        message_ids: list[str],
        format: str = "metadata",
    ) -> list[dict[str, Any]]:
        """
        Batch fetch multiple messages efficiently.

        Uses Gmail batch API to fetch multiple messages in a single request,
        avoiding N+1 query problems.

        Args:
            message_ids: List of Gmail message IDs to fetch
            format: Message format ("metadata", "full", or "minimal")

        Returns:
            List of parsed email dictionaries

        Example:
            message_ids = ["msg1", "msg2", "msg3"]
            emails = client.fetch_message_batch(message_ids)
            for email in emails:
                print(email["subject"], email["sender_email"])
        """
        if not message_ids:
            return []

        emails = []

        # Gmail batch API has a limit of 100 requests per batch
        # Split into chunks of 50 to balance throughput with token bucket capacity
        chunk_size = 50
        for i in range(0, len(message_ids), chunk_size):
            chunk = message_ids[i:i + chunk_size]
            batch_emails = self._fetch_batch_chunk(chunk, format)
            emails.extend(batch_emails)

        return emails

    def _fetch_batch_chunk(
        self,
        message_ids: list[str],
        format: str = "metadata",
    ) -> list[dict[str, Any]]:
        """
        Fetch a single batch chunk of up to 100 messages.

        Args:
            message_ids: List of up to 100 message IDs
            format: Message format

        Returns:
            List of parsed email dictionaries
        """
        self.rate_limiter.wait_for_token(tokens=len(message_ids))

        try:
            batch = self.gmail_service.new_batch_http_request()
            emails = []

            def callback(request_id: str, response: dict, exception: Exception | None) -> None:
                """Callback for batch request."""
                if exception:
                    # Log error but don't fail entire batch
                    print(f"Error fetching message {request_id}: {exception}")
                    return

                try:
                    parsed_email = self._parse_message(response)
                    emails.append(parsed_email)
                except Exception as e:
                    print(f"Error parsing message {request_id}: {e}")

            # Add each message to batch
            for msg_id in message_ids:
                request = self.gmail_service.users().messages().get(
                    userId="me",
                    id=msg_id,
                    format=format,
                    metadataHeaders=["From", "To", "Subject", "Date"] if format == "metadata" else None,
                )
                batch.add(request, callback=callback)

            # Execute batch request
            batch.execute()

            return emails

        except HttpError as e:
            raise GmailClientError(f"Failed to fetch message batch: {e}")

    def _parse_message(self, message: dict) -> dict[str, Any]:
        """
        Parse Gmail API message response into structured dict.

        Args:
            message: Gmail API message response

        Returns:
            Parsed email dictionary with metadata

        Example output:
            {
                "gmail_message_id": "abc123",
                "gmail_thread_id": "thread123",
                "subject": "Hello World",
                "sender_email": "sender@example.com",
                "sender_name": "John Doe",
                "recipient_emails": "recipient@example.com",
                "date": datetime(2024, 1, 1, 12, 0, 0),
                "has_attachments": False,
                "attachment_count": 0,
                "snippet": "Email preview text...",
            }
        """
        msg_id = message.get("id")
        thread_id = message.get("threadId")
        snippet = message.get("snippet", "")

        # Parse headers
        headers = {}
        if "payload" in message and "headers" in message["payload"]:
            for header in message["payload"]["headers"]:
                headers[header["name"].lower()] = header["value"]

        # Parse sender
        from_header = headers.get("from", "")
        sender_name, sender_email = parseaddr(from_header)
        if not sender_email:
            sender_email = from_header

        # Parse recipients
        to_header = headers.get("to", "")
        recipient_emails = to_header

        # Parse subject
        subject = headers.get("subject")

        # Parse date
        date_header = headers.get("date")
        date = None
        if date_header:
            try:
                date = parsedate_to_datetime(date_header)
            except Exception:
                # Fallback to internalDate if date header parsing fails
                internal_date = message.get("internalDate")
                if internal_date:
                    date = datetime.fromtimestamp(int(internal_date) / 1000)

        # Check for attachments
        has_attachments = False
        attachment_count = 0
        if "payload" in message:
            parts = message["payload"].get("parts", [])
            for part in parts:
                if part.get("filename"):
                    has_attachments = True
                    attachment_count += 1

        return {
            "gmail_message_id": msg_id,
            "gmail_thread_id": thread_id,
            "subject": subject,
            "sender_email": sender_email,
            "sender_name": sender_name or None,
            "recipient_emails": recipient_emails,
            "date": date,
            "has_attachments": has_attachments,
            "attachment_count": attachment_count,
            "snippet": snippet,
        }

    def get_message_body(self, message_id: str) -> str | None:
        """
        Fetch full message body for a single message.

        Note: This is a separate method because fetching full bodies
        is more expensive than metadata. Use sparingly.

        Args:
            message_id: Gmail message ID

        Returns:
            Plain text body or None if not found
        """
        self.rate_limiter.wait_for_token()

        try:
            message = self.gmail_service.users().messages().get(
                userId="me",
                id=message_id,
                format="full",
            ).execute()

            # Extract body from payload
            payload = message.get("payload", {})
            body = self._extract_body_from_payload(payload)

            return body

        except HttpError as e:
            raise GmailClientError(f"Failed to fetch message body: {e}")

    def _extract_body_from_payload(self, payload: dict) -> str | None:
        """
        Extract plain text body from message payload.

        Args:
            payload: Message payload from Gmail API

        Returns:
            Plain text body or None
        """
        # Check if body is in main payload
        if "body" in payload and "data" in payload["body"]:
            body_data = payload["body"]["data"]
            return base64.urlsafe_b64decode(body_data).decode("utf-8", errors="ignore")

        # Check parts for text/plain
        if "parts" in payload:
            for part in payload["parts"]:
                if part.get("mimeType") == "text/plain":
                    if "data" in part.get("body", {}):
                        body_data = part["body"]["data"]
                        return base64.urlsafe_b64decode(body_data).decode("utf-8", errors="ignore")

                # Recursively check nested parts
                if "parts" in part:
                    nested_body = self._extract_body_from_payload(part)
                    if nested_body:
                        return nested_body

        return None

    def close(self) -> None:
        """Close rate limiter connection."""
        self.rate_limiter.close()
