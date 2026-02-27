"""
ID-First email fetching tasks.

This module implements a two-phase email sync strategy:
Phase 1: Fetch all message IDs (fast, 1 quota unit per call)
Phase 2: Workers claim batches and fetch full messages (5 quota units per message)

Benefits over pagination approach:
- 5x faster initial scan (messages.list vs messages.get)
- Perfect parallelization (multiple workers, no coordination needed)
- Easy resume on failure (just fetch unclaimed IDs)
- No wasted time on empty pagination
"""
import time
import uuid
from datetime import datetime, timedelta

from celery import chain, group
from sqlalchemy import create_engine, func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker

from src.core.config import settings
from src.core.logging import get_logger
from src.integrations.gmail.client import GmailClient
from src.models import Email, EmailQueue, GmailAccount
from src.worker.celery_app import celery_app

# Database setup
engine = create_engine(settings.database_url)
SessionLocal = sessionmaker(bind=engine)

logger = get_logger(__name__)

BATCH_SIZE = 100  # IDs to claim and fetch per worker task


@celery_app.task(name="fetch_all_message_ids")
def fetch_all_message_ids(user_id: str, account_label: str = None):
    """
    Phase 1: Fetch all message IDs for an account and queue them.

    This is FAST - messages.list() uses only 1 quota unit vs 5 for full messages.
    Can fetch 15,000 IDs per minute vs 3,000 full messages.

    Args:
        user_id: User UUID
        account_label: Optional account label filter
    """
    task_id = str(uuid.uuid4())[:8]
    logger.info(f"[{task_id}] Starting ID fetch for user {user_id}")

    db = SessionLocal()
    try:
        # Get accounts to scan
        query = db.query(GmailAccount).filter(
            GmailAccount.user_id == user_id,
            GmailAccount.is_active == True,
        )

        if account_label:
            query = query.filter(GmailAccount.account_label == account_label)

        accounts = query.all()

        if not accounts:
            logger.warning(f"[{task_id}] No active accounts found")
            return

        for account in accounts:
            logger.info(
                f"[{task_id}] Fetching IDs for {account.account_email} ({account.account_label})"
            )

            # Create Gmail client
            creds = account.get_credentials_dict()
            gmail_client = GmailClient(creds)

            # Fetch ALL message IDs (fast!)
            all_ids = []
            next_page_token = None
            page_count = 0

            while True:
                try:
                    # Fetch a page of IDs (500 at a time)
                    message_ids, next_page_token = gmail_client.fetch_emails_chunked(
                        batch_size=500,
                        next_page_token=next_page_token,
                        query="in:anywhere",  # All emails
                    )

                    if message_ids:
                        all_ids.extend(message_ids)
                        page_count += 1

                        logger.info(
                            f"[{task_id}] [{account.account_email}] Fetched page {page_count}: "
                            f"{len(message_ids)} IDs (total: {len(all_ids)})"
                        )

                    if not next_page_token:
                        break

                    # Small delay between pages
                    time.sleep(0.5)

                except Exception as e:
                    logger.error(f"[{task_id}] Error fetching IDs: {e}")
                    break

            logger.info(
                f"[{task_id}] [{account.account_email}] Fetched {len(all_ids)} total message IDs"
            )

            # Check which IDs already exist in Email table
            existing_ids = set()
            if all_ids:
                existing = (
                    db.query(Email.gmail_message_id)
                    .filter(
                        Email.account_id == account.id,
                        Email.gmail_message_id.in_(all_ids),
                    )
                    .all()
                )
                existing_ids = {row[0] for row in existing}

            new_ids = [msg_id for msg_id in all_ids if msg_id not in existing_ids]

            logger.info(
                f"[{task_id}] [{account.account_email}] "
                f"{len(existing_ids)} already fetched, {len(new_ids)} new IDs to queue"
            )

            # Insert new IDs into EmailQueue
            if new_ids:
                queue_records = [
                    {
                        "id": uuid.uuid4(),
                        "account_id": account.id,
                        "gmail_message_id": msg_id,
                        "created_at": datetime.utcnow(),
                    }
                    for msg_id in new_ids
                ]

                # Batch insert in chunks of 1000
                for i in range(0, len(queue_records), 1000):
                    chunk = queue_records[i : i + 1000]
                    stmt = insert(EmailQueue).on_conflict_do_nothing(
                        index_elements=["account_id", "gmail_message_id"]
                    )
                    db.execute(stmt, chunk)
                    db.commit()

                logger.info(
                    f"[{task_id}] [{account.account_email}] Queued {len(new_ids)} message IDs"
                )

            # Trigger worker tasks to fetch full messages
            pending_count = (
                db.query(func.count(EmailQueue.id))
                .filter(
                    EmailQueue.account_id == account.id,
                    EmailQueue.claimed_by == None,
                )
                .scalar()
            )

            logger.info(
                f"[{task_id}] [{account.account_email}] {pending_count} pending IDs in queue"
            )

            # Spawn worker tasks (one per batch)
            num_batches = (pending_count + BATCH_SIZE - 1) // BATCH_SIZE
            if num_batches > 0:
                logger.info(
                    f"[{task_id}] [{account.account_email}] Spawning {num_batches} worker tasks"
                )

                # Use Celery group to run tasks in parallel
                tasks = [
                    fetch_message_batch.s(str(account.id)) for _ in range(min(num_batches, 10))
                ]
                job = group(tasks)
                job.apply_async()
    finally:
        db.close()


@celery_app.task(name="fetch_message_batch")
def fetch_message_batch(account_id: str):
    """
    Phase 2: Claim a batch of queued IDs and fetch full messages.

    Multiple workers can run this task in parallel, each claiming different batches.
    """
    task_id = str(uuid.uuid4())[:8]
    worker_id = f"{celery_app.current_task.request.id}"

    logger.info(f"[{task_id}] Worker {worker_id} claiming batch for account {account_id}")

    db = SessionLocal()
    try:
        # Get account
        account = db.query(GmailAccount).filter(GmailAccount.id == account_id).first()

        if not account:
            logger.error(f"[{task_id}] Account {account_id} not found")
            return

        # Claim a batch of unclaimed IDs (atomically)
        claimed_at = datetime.utcnow()
        claim_result = (
            db.query(EmailQueue)
            .filter(
                EmailQueue.account_id == account_id,
                EmailQueue.claimed_by == None,
            )
            .limit(BATCH_SIZE)
            .with_for_update(skip_locked=True)  # Skip rows locked by other workers
            .all()
        )

        if not claim_result:
            logger.info(f"[{task_id}] No unclaimed IDs available")
            return

        # Mark as claimed
        claimed_ids = [record.gmail_message_id for record in claim_result]
        db.query(EmailQueue).filter(EmailQueue.id.in_([r.id for r in claim_result])).update(
            {"claimed_by": worker_id, "claimed_at": claimed_at}, synchronize_session=False
        )
        db.commit()

        logger.info(f"[{task_id}] Claimed {len(claimed_ids)} IDs")

        # Fetch full messages
        creds = account.get_credentials_dict()
        gmail_client = GmailClient(creds)

        try:
            email_dicts = gmail_client.fetch_message_batch(claimed_ids)
            logger.info(f"[{task_id}] Fetched {len(email_dicts)} full messages")

            # Insert into Email table
            emails_to_insert = []
            for email_dict in email_dicts:
                email_data = {
                    "id": uuid.uuid4(),
                    "user_id": account.user_id,
                    "account_id": account.id,
                    "gmail_message_id": email_dict["gmail_message_id"],
                    "gmail_thread_id": email_dict.get("gmail_thread_id"),
                    "subject": email_dict.get("subject", ""),
                    "sender_email": email_dict.get("sender_email", ""),
                    "sender_name": email_dict.get("sender_name"),
                    "recipient_emails": email_dict.get("recipient_emails", ""),
                    "date": email_dict.get("date", datetime.utcnow()),
                    "summary": email_dict.get("snippet", "")[:500],
                    "has_attachments": email_dict.get("has_attachments", False),
                    "attachment_count": email_dict.get("attachment_count", 0),
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                }
                emails_to_insert.append(email_data)

            if emails_to_insert:
                stmt = insert(Email).on_conflict_do_nothing(
                    index_elements=["account_id", "gmail_message_id"]
                )
                db.execute(stmt, emails_to_insert)
                db.commit()

                logger.info(f"[{task_id}] Inserted {len(emails_to_insert)} emails")

            # Remove from queue
            db.query(EmailQueue).filter(EmailQueue.id.in_([r.id for r in claim_result])).delete(
                synchronize_session=False
            )
            db.commit()

            logger.info(f"[{task_id}] Removed {len(claim_result)} IDs from queue")

            # Check if more work remains
            remaining = (
                db.query(func.count(EmailQueue.id))
                .filter(
                    EmailQueue.account_id == account_id,
                    EmailQueue.claimed_by == None,
                )
                .scalar()
            )

            if remaining > 0:
                logger.info(f"[{task_id}] {remaining} IDs remaining, spawning another task")
                fetch_message_batch.delay(account_id)

        except Exception as e:
            logger.error(f"[{task_id}] Error fetching messages: {e}")
            # Unclaim on error so another worker can retry
            db.query(EmailQueue).filter(EmailQueue.id.in_([r.id for r in claim_result])).update(
                {"claimed_by": None, "claimed_at": None}, synchronize_session=False
            )
            db.commit()
            raise
    finally:
        db.close()
