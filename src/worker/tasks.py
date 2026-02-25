"""
Celery tasks for Gmail-to-Obsidian integration.
Main orchestration task that coordinates all components.
"""

import json
import logging
import time
import uuid
from collections import defaultdict
from datetime import datetime
from typing import Any

from celery import Task
from sqlalchemy import create_engine, func, insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from src.core.config import settings
from src.core.logging import get_logger
from src.integrations.claude.batch_processor import ThemeBatchProcessor
from src.integrations.gmail.client import GmailClient
from src.models import Contact, Email, EmailTag, GmailAccount, SyncJob, User
from src.services.gmail.contact_merger import merge_contacts_by_email
from src.services.obsidian.note_generator import NoteGenerator
from src.services.obsidian.vault_manager import ObsidianVaultManager
from src.services.theme_detection.prompt_template import generate_tags
from src.worker.celery_app import celery_app

logger = get_logger(__name__)

# Database session factory
engine = create_engine(settings.database_url)
SessionLocal = sessionmaker(bind=engine)


def get_last_processed_email_date(db: Session, account_id: uuid.UUID) -> datetime | None:
    """
    Get the date of the most recent email we've already processed for an account.
    This allows us to resume from where we left off.

    Args:
        db: Database session
        account_id: Gmail account ID

    Returns:
        Most recent email date, or None if no emails exist
    """
    result = db.query(func.max(Email.date)).filter(Email.account_id == account_id).scalar()
    return result


def get_existing_email_count(db: Session, account_id: uuid.UUID) -> int:
    """
    Count how many emails we've already fetched for an account.

    Args:
        db: Database session
        account_id: Gmail account ID

    Returns:
        Number of emails already in database
    """
    return db.query(func.count(Email.id)).filter(Email.account_id == account_id).scalar() or 0


class CallbackTask(Task):
    """Base task with progress callback support."""

    def update_progress(
        self, phase: str, progress: int, emails_processed: int = 0, message: str = ""
    ) -> None:
        """Update task progress."""
        self.update_state(
            state="PROGRESS",
            meta={
                "phase": phase,
                "progress": progress,
                "emails_processed": emails_processed,
                "message": message,
            },
        )


@celery_app.task(bind=True, base=CallbackTask, name="scan_gmail_task")
def scan_gmail_task(
    self, user_id: str, account_labels: list[str] | None = None
) -> dict[str, Any]:
    """
    Main orchestration task for multi-account Gmail scan.

    Phases:
    1. Sync contacts from all accounts (0-10%)
    2. Merge contacts by email (10-15%)
    3. Sync emails from all accounts (15-40%)
    4. Detect themes with Claude Batch API (40-70%)
    5. Generate unified Obsidian vault (70-100%)

    Args:
        user_id: User ID
        account_labels: Optional list of account labels to scan (defaults to all 3)

    Returns:
        dict with status, progress, and metrics
    """
    correlation_id = str(uuid.uuid4())
    logger.info(
        f"[{correlation_id}] Starting Gmail scan for user {user_id}, "
        f"accounts: {account_labels}"
    )

    if account_labels is None:
        # Process in order: personal → procore-private → procore-main
        # This processes tooey@hth-corp.com first, then 2e@procore.com, then tooey@procore.com
        account_labels = ["personal", "procore-private", "procore-main"]

    db = SessionLocal()
    job = None

    try:
        # Create sync job record
        job = SyncJob(
            id=uuid.uuid4(),
            user_id=uuid.UUID(user_id),
            status="running",
            phase="contacts",
            progress_pct=0,
            celery_task_id=self.request.id,
            started_at=datetime.utcnow(),
        )
        db.add(job)
        db.commit()
        db.refresh(job)

        # Get user and accounts
        user = db.query(User).filter(User.id == uuid.UUID(user_id)).first()
        if not user:
            raise ValueError(f"User {user_id} not found")

        accounts = (
            db.query(GmailAccount)
            .filter(
                GmailAccount.user_id == uuid.UUID(user_id),
                GmailAccount.account_label.in_(account_labels),
                GmailAccount.is_active == True,
            )
            .all()
        )

        if not accounts:
            raise ValueError(f"No active accounts found for labels: {account_labels}")

        logger.info(f"[{correlation_id}] Found {len(accounts)} accounts to scan")

        # Initialize services
        theme_processor = ThemeBatchProcessor()
        vault_manager = ObsidianVaultManager(settings.obsidian_vault_path)
        note_generator = NoteGenerator()

        # Helper function to get credentials dict from account
        def get_credentials_from_account(account: GmailAccount) -> dict:
            """Extract credentials dict from GmailAccount for GmailClient."""
            creds = json.loads(account.credentials)
            # Map stored keys to what GmailClient expects
            return {
                "access_token": creds.get("token"),  # GmailClient expects "access_token"
                "refresh_token": creds.get("refresh_token"),
                "token_uri": creds.get("token_uri"),
                "client_id": creds.get("client_id"),
                "client_secret": creds.get("client_secret"),
                "scopes": creds.get("scopes", []),
            }

        # ========================================
        # PHASE 1: Skip Contacts (no scope available)
        # ========================================
        # TODO: Add contacts scope and re-enable contacts sync
        logger.info(f"[{correlation_id}] Skipping contacts sync (no contacts scope)")

        # ========================================
        # PHASE 2: Sync Emails (0-40%)
        # ========================================
        self.update_progress("emails", 0, message="Fetching emails from Gmail accounts")
        job.phase = "emails"
        job.progress_pct = 15
        db.commit()

        all_emails = []
        total_emails_fetched = 0
        total_emails_skipped = 0  # Track duplicates/already processed

        for i, account in enumerate(accounts):
            # ========================================
            # Check for existing emails (Resume Logic)
            # ========================================
            existing_count = get_existing_email_count(db, account.id)
            last_email_date = get_last_processed_email_date(db, account.id)

            logger.info(
                f"[{correlation_id}] Account {account.account_label} ({account.account_email}): "
                f"{existing_count} emails already in database"
            )

            # Build Gmail query to resume from last processed email
            # Note: Gmail API returns emails in REVERSE CHRONOLOGICAL order (newest first)
            # So we fetch emails newer than our last processed email, and they come newest→oldest
            gmail_query = None
            if last_email_date:
                # Format: after:YYYY/MM/DD
                # This fetches emails AFTER the newest one we have (i.e., even newer emails)
                date_str = last_email_date.strftime("%Y/%m/%d")
                gmail_query = f"after:{date_str}"
                logger.info(
                    f"[{correlation_id}] Resuming from {date_str} - fetching newer emails "
                    f"(Gmail returns newest first)"
                )
            else:
                logger.info(
                    f"[{correlation_id}] Starting fresh scan - fetching all emails "
                    f"(newest → oldest)"
                )

            credentials = get_credentials_from_account(account)
            gmail_client = GmailClient(credentials)

            # Fetch emails with pagination
            next_page_token = None
            while True:
                # Fetch message IDs (with optional date filter for resume)
                message_ids, next_page_token = gmail_client.fetch_emails_chunked(
                    batch_size=settings.gmail_batch_size,
                    page_token=next_page_token,
                    query=gmail_query,  # Resume from last date
                )

                if not message_ids:
                    break

                logger.info(
                    f"[{correlation_id}] Fetched {len(message_ids)} message IDs from "
                    f"{account.account_label}"
                )

                # Fetch full message details
                email_dicts = gmail_client.fetch_message_batch(message_ids)
                logger.info(
                    f"[{correlation_id}] Fetched {len(email_dicts)} full messages from "
                    f"{account.account_label}"
                )

                # Create Email objects
                for email_dict in email_dicts:
                    email = Email(
                        id=uuid.uuid4(),
                        user_id=uuid.UUID(user_id),
                        account_id=account.id,
                        gmail_message_id=email_dict["gmail_message_id"],
                        gmail_thread_id=email_dict.get("gmail_thread_id"),
                        subject=email_dict.get("subject", ""),
                        sender_email=email_dict.get("sender_email", ""),
                        sender_name=email_dict.get("sender_name"),
                        recipient_emails=email_dict.get("recipient_emails", ""),
                        date=email_dict.get("date", datetime.utcnow()),
                        summary=email_dict.get("snippet", "")[:500],  # 500-char summary
                        has_attachments=email_dict.get("has_attachments", False),
                        attachment_count=email_dict.get("attachment_count", 0),
                    )
                    all_emails.append(email)

                total_emails_fetched += len(email_dicts)

                # Update progress (0-40%)
                progress = int((i / len(accounts)) * 30 + (total_emails_fetched / 10000) * 10)
                progress = min(progress, 40)
                self.update_progress(
                    "emails",
                    progress,
                    emails_processed=total_emails_fetched,
                    message=f"Fetched {total_emails_fetched} emails",
                )
                job.progress_pct = progress
                job.emails_processed = total_emails_fetched
                db.commit()

                # Save emails to database with upsert logic (skip duplicates)
                # Use INSERT ... ON CONFLICT DO NOTHING for crash recovery
                new_emails = all_emails[-len(email_dicts) :]
                if new_emails:
                    try:
                        # Convert Email objects to dicts for insert
                        email_dicts_for_insert = [
                            {
                                "id": email.id,
                                "user_id": email.user_id,
                                "account_id": email.account_id,
                                "gmail_message_id": email.gmail_message_id,
                                "gmail_thread_id": email.gmail_thread_id,
                                "subject": email.subject,
                                "sender_email": email.sender_email,
                                "sender_name": email.sender_name,
                                "recipient_emails": email.recipient_emails,
                                "date": email.date,
                                "summary": email.summary,
                                "has_attachments": email.has_attachments,
                                "attachment_count": email.attachment_count,
                                "created_at": datetime.utcnow(),
                                "updated_at": datetime.utcnow(),
                            }
                            for email in new_emails
                        ]

                        # INSERT ... ON CONFLICT DO NOTHING (skip duplicates)
                        stmt = insert(Email).on_conflict_do_nothing(
                            index_elements=["account_id", "gmail_message_id"]
                        )
                        db.execute(stmt, email_dicts_for_insert)
                        db.commit()

                        logger.info(
                            f"[{correlation_id}] Inserted {len(email_dicts_for_insert)} emails "
                            f"(duplicates automatically skipped)"
                        )

                    except Exception as e:
                        logger.error(
                            f"[{correlation_id}] Error inserting emails: {e}", exc_info=True
                        )
                        db.rollback()
                        # Continue processing - don't crash on insert errors
                        logger.warning(f"[{correlation_id}] Continuing after insert error...")

                # Add delay between batches to respect rate limits
                if next_page_token:
                    time.sleep(2)  # 2 second pause between batches
                    logger.info(f"[{correlation_id}] Rate limit pause - continuing...")
                else:
                    break

        logger.info(f"[{correlation_id}] Total emails fetched: {len(all_emails)}")
        job.emails_total = len(all_emails)
        db.commit()

        # ========================================
        # PHASE 4: Theme Detection (40-70%)
        # ========================================
        self.update_progress("themes", 40, message="Detecting themes with Claude AI")
        job.phase = "themes"
        job.progress_pct = 40
        db.commit()

        # Process emails in batches for theme detection
        batch_size = settings.claude_batch_size
        email_batches = [
            all_emails[i : i + batch_size] for i in range(0, len(all_emails), batch_size)
        ]

        for batch_idx, email_batch in enumerate(email_batches):
            logger.info(
                f"[{correlation_id}] Processing theme detection batch {batch_idx + 1}/"
                f"{len(email_batches)} ({len(email_batch)} emails)"
            )

            # Submit batch to Claude
            batch_results = theme_processor.process_emails_sync(email_batch)

            # Generate tags from themes
            for email, themes in zip(email_batch, batch_results):
                if not themes:
                    logger.warning(
                        f"[{correlation_id}] No themes detected for email {email.id}"
                    )
                    continue

                # Get account label for this email
                account = next(
                    (acc for acc in accounts if acc.id == email.account_id), None
                )
                account_label = account.account_label if account else "unknown"

                # Generate tags
                tags = generate_tags(themes, account_label)

                # Create EmailTag records
                for tag_dict in tags:
                    email_tag = EmailTag(
                        id=uuid.uuid4(),
                        email_id=email.id,
                        tag=tag_dict["tag"],
                        tag_category=tag_dict["category"],
                        confidence=tag_dict.get("confidence"),
                    )
                    db.add(email_tag)

            db.commit()

            # Update progress
            progress = int(40 + ((batch_idx + 1) / len(email_batches)) * 30)
            self.update_progress(
                "themes",
                progress,
                emails_processed=len(all_emails),
                message=f"Processed {(batch_idx + 1) * batch_size} emails for themes",
            )
            job.progress_pct = progress
            db.commit()

        logger.info(f"[{correlation_id}] Theme detection complete for all emails")

        # ========================================
        # PHASE 5: Generate Vault (70-100%)
        # ========================================
        self.update_progress("vault", 70, message="Generating Obsidian vault")
        job.phase = "vault"
        job.progress_pct = 70
        db.commit()

        # Initialize vault
        vault_manager.initialize_vault()
        logger.info(f"[{correlation_id}] Vault initialized at {settings.obsidian_vault_path}")

        # Group emails by contact
        emails_by_contact = defaultdict(list)
        for email in all_emails:
            emails_by_contact[email.sender_email].append(email)

        # Generate contact notes
        self.update_progress("vault", 75, message="Generating contact notes")
        for contact_idx, contact in enumerate(merged_contacts):
            contact_emails = emails_by_contact.get(contact.email, [])

            # Fetch tags for contact emails
            email_ids = [e.id for e in contact_emails]
            tags_query = db.query(EmailTag).filter(EmailTag.email_id.in_(email_ids)).all()

            # Generate contact note
            contact_note = note_generator.generate_contact_note(contact, contact_emails)

            # Write to vault
            contact_path = vault_manager.get_contact_path(contact.name or contact.email)
            contact_path.write_text(contact_note)

            # Update progress
            if contact_idx % 10 == 0:
                progress = int(75 + (contact_idx / len(merged_contacts)) * 10)
                self.update_progress(
                    "vault", progress, message=f"Generated {contact_idx} contact notes"
                )
                job.progress_pct = progress
                db.commit()

        logger.info(f"[{correlation_id}] Generated {len(merged_contacts)} contact notes")

        # Generate email notes
        self.update_progress("vault", 85, message="Generating email notes")
        for email_idx, email in enumerate(all_emails):
            # Fetch tags for this email
            tags = db.query(EmailTag).filter(EmailTag.email_id == email.id).all()
            tag_strings = [f"{t.tag_category}/{t.tag}" for t in tags]

            # Generate email note
            email_note = note_generator.generate_email_note(email, tag_strings)

            # Write to vault
            email_path = vault_manager.get_email_path(email.date, email.subject or "Untitled")
            vault_manager.ensure_email_directory(email.date)
            email_path.write_text(email_note)

            # Update progress
            if email_idx % 100 == 0:
                progress = int(85 + (email_idx / len(all_emails)) * 15)
                self.update_progress(
                    "vault", progress, message=f"Generated {email_idx} email notes"
                )
                job.progress_pct = progress
                db.commit()

        logger.info(f"[{correlation_id}] Generated {len(all_emails)} email notes")

        # ========================================
        # COMPLETE
        # ========================================
        self.update_progress("completed", 100, message="Scan complete!")
        job.status = "completed"
        job.phase = "completed"
        job.progress_pct = 100
        job.completed_at = datetime.utcnow()
        db.commit()

        logger.info(
            f"[{correlation_id}] Gmail scan complete for user {user_id}. "
            f"Processed {len(merged_contacts)} contacts and {len(all_emails)} emails."
        )

        return {
            "status": "completed",
            "progress": 100,
            "contacts_processed": len(merged_contacts),
            "emails_processed": len(all_emails),
            "vault_path": str(settings.obsidian_vault_path),
            "correlation_id": correlation_id,
        }

    except Exception as e:
        logger.error(f"[{correlation_id}] Error during Gmail scan: {str(e)}", exc_info=True)

        if job:
            job.status = "failed"
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            db.commit()

        raise

    finally:
        db.close()
