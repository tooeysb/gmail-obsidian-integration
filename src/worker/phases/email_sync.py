"""
Phase 2: Email sync from Gmail accounts.

Fetches message IDs then full messages in batches, inserting with ON CONFLICT DO NOTHING.
Closes/reopens DB sessions around long Gmail fetches to avoid Supabase connection timeouts.
"""

import uuid
from collections.abc import Callable
from datetime import datetime
from typing import Any

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from src.core.config import settings
from src.core.logging import get_logger
from src.integrations.gmail.client import GmailClient
from src.models import Email, GmailAccount, SyncJob

logger = get_logger(__name__)

# Progress percentage range for this phase
PROGRESS_MIN = 15
PROGRESS_MAX = 40
SUMMARY_MAX_LENGTH = 500


def sync_emails_for_accounts(
    db_factory: Callable[[], Session],
    db: Session,
    job: SyncJob,
    accounts: list[GmailAccount],
    user_id: str,
    correlation_id: str,
    progress_callback: Callable[[str, int, int, str], None],
    get_credentials: Callable[[GmailAccount], dict],
    get_last_date: Callable[[Session, Any], datetime | None],
    get_oldest_date: Callable[[Session, Any], datetime | None],
    get_existing_count: Callable[[Session, Any], int],
) -> tuple[Session, SyncJob, list[Email]]:
    """
    Sync emails from all Gmail accounts.

    Returns the (potentially new) db session, re-merged job, and all Email objects.
    The caller must use the returned db/job since sessions may be closed and reopened.
    """
    all_emails: list[Email] = []
    total_emails_fetched = 0

    for i, account in enumerate(accounts):
        existing_count = get_existing_count(db, account.id)
        oldest_email_date = get_oldest_date(db, account.id)
        newest_email_date = get_last_date(db, account.id)

        logger.info(
            "[%s] Account %s (%s): %d emails already in database",
            correlation_id,
            account.account_label,
            account.account_email,
            existing_count,
        )

        credentials = get_credentials(account)
        gmail_client = GmailClient(credentials)

        queries_to_run = _build_sync_queries(oldest_email_date, newest_email_date)

        logger.info(
            "[%s] Running %d sync strategies for %s",
            correlation_id,
            len(queries_to_run),
            account.account_label,
        )

        for strategy in queries_to_run:
            gmail_query = strategy["query"]
            description = strategy["description"]

            logger.info(
                "[%s] Starting %s for %s", correlation_id, description, account.account_label
            )

            next_page_token = None
            strategy_fetch_count = 0

            while True:
                message_ids, next_page_token = gmail_client.fetch_emails_chunked(
                    batch_size=settings.gmail_batch_size,
                    page_token=next_page_token,
                    query=gmail_query,
                )

                if not message_ids:
                    break

                logger.info(
                    "[%s] Fetched %d message IDs (%s)",
                    correlation_id,
                    len(message_ids),
                    description,
                )

                # Close DB session before long Gmail fetch to prevent connection timeout
                db.close()

                email_dicts = gmail_client.fetch_message_batch(message_ids, format="full")
                logger.info(
                    "[%s] Fetched %d full messages (%s)",
                    correlation_id,
                    len(email_dicts),
                    description,
                )

                # Reopen DB session and re-merge detached ORM objects
                db = db_factory()
                job = db.merge(job)

                batch_emails = _create_email_objects(email_dicts, user_id, account.id)
                all_emails.extend(batch_emails)
                total_emails_fetched += len(email_dicts)
                strategy_fetch_count += len(email_dicts)

                if email_dicts:
                    _insert_email_batch(db, batch_emails, correlation_id)

                # Update progress
                progress = int(
                    PROGRESS_MIN
                    + (i / len(accounts)) * (PROGRESS_MAX - PROGRESS_MIN)
                    + (total_emails_fetched / 10000) * 5
                )
                progress = min(progress, PROGRESS_MAX)
                progress_callback(
                    "emails",
                    progress,
                    total_emails_fetched,
                    f"Fetched {total_emails_fetched} emails",
                )
                job.progress_pct = progress
                job.emails_processed = total_emails_fetched
                db.commit()

                if not next_page_token:
                    break

            logger.info(
                "[%s] Completed %s: fetched %d emails",
                correlation_id,
                description,
                strategy_fetch_count,
            )

    logger.info("[%s] Total emails fetched: %d", correlation_id, len(all_emails))
    job.emails_total = len(all_emails)
    db.commit()

    return db, job, all_emails


def _build_sync_queries(
    oldest_email_date: datetime | None, newest_email_date: datetime | None
) -> list[dict[str, str]]:
    """Build the list of Gmail queries for dual sync strategy."""
    queries: list[dict[str, str]] = []

    if oldest_email_date and newest_email_date:
        for category in ["PROMOTIONS", "SOCIAL", "FORUMS", "UPDATES"]:
            queries.append(
                {
                    "query": f"label:CATEGORY_{category}",
                    "description": f"gap-fill scan (CATEGORY_{category} emails missed in initial backfill)",
                }
            )

    if newest_email_date:
        after_date_str = newest_email_date.strftime("%Y/%m/%d")
        queries.append(
            {
                "query": f"in:anywhere after:{after_date_str}",
                "description": f"forward sync (new emails after {after_date_str})",
            }
        )

    if oldest_email_date:
        before_date_str = oldest_email_date.strftime("%Y/%m/%d")
        queries.append(
            {
                "query": f"in:anywhere before:{before_date_str}",
                "description": f"historical backfill (old emails before {before_date_str})",
            }
        )

    if not queries:
        queries.append(
            {
                "query": "in:anywhere",
                "description": "initial scan (all emails from all labels)",
            }
        )

    return queries


def _create_email_objects(email_dicts: list[dict], user_id: str, account_id: Any) -> list[Email]:
    """Create Email ORM objects from Gmail API dicts."""
    emails = []
    for d in email_dicts:
        emails.append(
            Email(
                id=uuid.uuid4(),
                user_id=uuid.UUID(user_id),
                account_id=account_id,
                gmail_message_id=d["gmail_message_id"],
                gmail_thread_id=d.get("gmail_thread_id"),
                subject=d.get("subject", ""),
                sender_email=d.get("sender_email", ""),
                sender_name=d.get("sender_name"),
                recipient_emails=d.get("recipient_emails", ""),
                date=d.get("date", datetime.utcnow()),
                summary=d.get("snippet", "")[:SUMMARY_MAX_LENGTH],
                body=d.get("body"),
                has_attachments=d.get("has_attachments", False),
                attachment_count=d.get("attachment_count", 0),
            )
        )
    return emails


def _insert_email_batch(db: Session, emails: list[Email], correlation_id: str) -> None:
    """INSERT ... ON CONFLICT DO NOTHING for a batch of emails."""
    try:
        email_dicts = [
            {
                "id": e.id,
                "user_id": e.user_id,
                "account_id": e.account_id,
                "gmail_message_id": e.gmail_message_id,
                "gmail_thread_id": e.gmail_thread_id,
                "subject": e.subject,
                "sender_email": e.sender_email,
                "sender_name": e.sender_name,
                "recipient_emails": e.recipient_emails,
                "date": e.date,
                "summary": e.summary,
                "body": e.body,
                "has_attachments": e.has_attachments,
                "attachment_count": e.attachment_count,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
            }
            for e in emails
        ]
        stmt = insert(Email).on_conflict_do_nothing(
            index_elements=["account_id", "gmail_message_id"]
        )
        db.execute(stmt, email_dicts)
        db.commit()
        logger.info(
            "[%s] Inserted %d emails (duplicates automatically skipped)",
            correlation_id,
            len(email_dicts),
        )
    except Exception as e:
        logger.error("[%s] Error inserting emails: %s", correlation_id, e, exc_info=True)
        db.rollback()
        logger.warning("[%s] Continuing after insert error...", correlation_id)
