"""
Example usage of Gmail API client.

This script demonstrates how to use the GmailClient to fetch contacts and emails.
"""

import json
from datetime import datetime

from src.core.config import settings
from src.integrations.gmail.client import GmailClient
from src.integrations.gmail.rate_limiter import GmailRateLimiter


def example_fetch_contacts(client: GmailClient) -> list[dict]:
    """
    Example: Fetch all contacts with pagination.

    Returns:
        List of all contacts
    """
    print("Fetching contacts...")
    all_contacts = []
    next_token = None

    while True:
        contacts, next_token = client.fetch_contacts(
            page_size=1000,
            page_token=next_token,
        )

        all_contacts.extend(contacts)
        print(f"  Fetched {len(contacts)} contacts (total: {len(all_contacts)})")

        if not next_token:
            break

    print(f"Total contacts fetched: {len(all_contacts)}")
    return all_contacts


def example_fetch_emails(client: GmailClient, max_emails: int = 100) -> list[dict]:
    """
    Example: Fetch recent emails with batch operations.

    Args:
        max_emails: Maximum number of emails to fetch

    Returns:
        List of parsed email dictionaries
    """
    print(f"Fetching up to {max_emails} recent emails...")

    # Step 1: Fetch message IDs
    message_ids, next_token = client.fetch_emails_chunked(
        batch_size=max_emails,
    )

    print(f"  Found {len(message_ids)} message IDs")

    # Step 2: Batch fetch full message details (avoids N+1)
    emails = client.fetch_message_batch(message_ids)

    print(f"  Fetched {len(emails)} full email details")
    return emails


def example_fetch_emails_with_query(client: GmailClient, query: str = "is:unread") -> list[dict]:
    """
    Example: Fetch emails matching a Gmail search query.

    Args:
        query: Gmail search query (e.g., "is:unread", "from:example@example.com")

    Returns:
        List of parsed email dictionaries
    """
    print(f"Fetching emails matching query: {query}")

    # Fetch message IDs with query
    message_ids, next_token = client.fetch_emails_chunked(
        batch_size=100,
        query=query,
    )

    print(f"  Found {len(message_ids)} matching emails")

    # Batch fetch details
    emails = client.fetch_message_batch(message_ids)
    return emails


def example_checkpoint_resume(client: GmailClient) -> None:
    """
    Example: Demonstrate checkpoint support for resumable operations.
    """
    print("Demonstrating checkpoint support...")

    # Fetch first batch and save checkpoint
    batch_1_ids, checkpoint = client.fetch_emails_chunked(batch_size=50)
    print(f"  Batch 1: Fetched {len(batch_1_ids)} IDs")
    print(f"  Checkpoint token: {checkpoint[:20]}..." if checkpoint else "  No more batches")

    # Simulate saving checkpoint to database
    # save_to_db({"checkpoint_token": checkpoint, "batch_num": 1})

    # Resume from checkpoint
    if checkpoint:
        batch_2_ids, next_checkpoint = client.fetch_emails_chunked(
            batch_size=50,
            page_token=checkpoint,
        )
        print(f"  Batch 2 (resumed): Fetched {len(batch_2_ids)} IDs")


def example_rate_limiter_stats(client: GmailClient) -> None:
    """
    Example: Monitor rate limiter token count.
    """
    print("\nRate limiter stats:")
    token_count = client.rate_limiter.get_token_count()
    print(f"  Current tokens: {token_count:.2f}")
    print(f"  Max tokens: {client.rate_limiter.max_tokens}")
    print(f"  Refill rate: {client.rate_limiter.refill_rate} tokens/sec")


def main():
    """
    Main example script.

    Usage:
        1. Ensure you have valid OAuth2 credentials in the database
        2. Set up environment variables (.env file)
        3. Run: python -m src.integrations.gmail.example
    """
    # Example credentials structure (replace with actual credentials from database)
    credentials = {
        "access_token": "YOUR_ACCESS_TOKEN",
        "refresh_token": "YOUR_REFRESH_TOKEN",
        "token_uri": "https://oauth2.googleapis.com/token",
        "client_id": settings.google_client_id,
        "client_secret": settings.google_client_secret,
        "scopes": [
            "https://www.googleapis.com/auth/gmail.readonly",
            "https://www.googleapis.com/auth/contacts.readonly",
        ],
    }

    # Create rate limiter
    rate_limiter = GmailRateLimiter(
        redis_url=settings.redis_url,
        max_tokens=settings.gmail_rate_limit_qps,
        refill_rate=float(settings.gmail_rate_limit_qps),
    )

    # Create Gmail client
    client = GmailClient(
        credentials=credentials,
        rate_limiter=rate_limiter,
    )

    try:
        # Example 1: Fetch contacts
        print("\n" + "=" * 60)
        print("Example 1: Fetch Contacts")
        print("=" * 60)
        contacts = example_fetch_contacts(client)
        if contacts:
            print("\nFirst contact:")
            print(json.dumps(contacts[0], indent=2))

        # Example 2: Fetch recent emails
        print("\n" + "=" * 60)
        print("Example 2: Fetch Recent Emails")
        print("=" * 60)
        emails = example_fetch_emails(client, max_emails=10)
        if emails:
            print("\nFirst email:")
            email = emails[0]
            print(f"  ID: {email['gmail_message_id']}")
            print(f"  From: {email['sender_name']} <{email['sender_email']}>")
            print(f"  Subject: {email['subject']}")
            print(f"  Date: {email['date']}")
            print(f"  Attachments: {email['attachment_count']}")

        # Example 3: Search emails
        print("\n" + "=" * 60)
        print("Example 3: Search Unread Emails")
        print("=" * 60)
        unread_emails = example_fetch_emails_with_query(client, query="is:unread")
        print(f"Found {len(unread_emails)} unread emails")

        # Example 4: Checkpoint support
        print("\n" + "=" * 60)
        print("Example 4: Checkpoint Support")
        print("=" * 60)
        example_checkpoint_resume(client)

        # Example 5: Rate limiter stats
        example_rate_limiter_stats(client)

    finally:
        # Clean up
        client.close()
        print("\nClient closed successfully")


if __name__ == "__main__":
    main()
