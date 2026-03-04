"""
Celery tasks for CRM-HTH email processing pipeline.
Main orchestration task that coordinates phase modules.
"""

import json
import uuid
from datetime import datetime
from typing import Any

from celery import Task
from sqlalchemy import func
from sqlalchemy.orm import Session

from src.core.config import settings

# Database session factory (shared worker engine with connection recycling)
from src.core.database import WorkerSessionLocal as SessionLocal
from src.core.logging import get_logger
from src.integrations.claude.batch_processor import ThemeBatchProcessor
from src.models import Email, GmailAccount, SyncJob, User
from src.services.obsidian.note_generator import NoteGenerator
from src.services.obsidian.vault_manager import ObsidianVaultManager
from src.worker.celery_app import celery_app
from src.worker.phases.email_sync import sync_emails_for_accounts
from src.worker.phases.theme_detection import detect_themes
from src.worker.phases.vault_generation import generate_vault

logger = get_logger(__name__)


def get_last_processed_email_date(db: Session, account_id: uuid.UUID) -> datetime | None:
    """Get the date of the most recent processed email for an account."""
    return db.query(func.max(Email.date)).filter(Email.account_id == account_id).scalar()


def get_oldest_processed_email_date(db: Session, account_id: uuid.UUID) -> datetime | None:
    """Get the date of the oldest processed email for an account."""
    return db.query(func.min(Email.date)).filter(Email.account_id == account_id).scalar()


def get_existing_email_count(db: Session, account_id: uuid.UUID) -> int:
    """Count emails already fetched for an account."""
    return db.query(func.count(Email.id)).filter(Email.account_id == account_id).scalar() or 0


def _get_credentials_from_account(account: GmailAccount) -> dict:
    """Extract credentials dict from GmailAccount for GmailClient."""
    creds = json.loads(account.credentials)
    return {
        "access_token": creds.get("token"),
        "refresh_token": creds.get("refresh_token"),
        "token_uri": creds.get("token_uri"),
        "client_id": creds.get("client_id"),
        "client_secret": creds.get("client_secret"),
        "scopes": creds.get("scopes", []),
    }


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
def scan_gmail_task(self, user_id: str, account_labels: list[str] | None = None) -> dict[str, Any]:
    """
    Main orchestration task for multi-account Gmail scan.

    Delegates to phase modules:
      Phase 2: email_sync — fetch emails from Gmail (15-40%)
      Phase 4: theme_detection — detect themes with Claude (40-70%)
      Phase 5: vault_generation — generate Obsidian vault (70-100%, dev only)
    """
    correlation_id = str(uuid.uuid4())
    logger.info(
        "[%s] Starting Gmail scan for user %s, accounts: %s",
        correlation_id,
        user_id,
        account_labels,
    )

    if account_labels is None:
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
            raise ValueError("User %s not found" % user_id)

        accounts = (
            db.query(GmailAccount)
            .filter(
                GmailAccount.user_id == uuid.UUID(user_id),
                GmailAccount.account_label.in_(account_labels),
                GmailAccount.is_active is True,
            )
            .all()
        )

        if not accounts:
            raise ValueError("No active accounts found for labels: %s" % account_labels)

        logger.info("[%s] Found %d accounts to scan", correlation_id, len(accounts))

        # Phase 1: Contacts sync (skipped — no scope available)
        logger.info("[%s] Skipping contacts sync (no contacts scope)", correlation_id)
        merged_contacts = []

        # Phase 2: Sync emails
        self.update_progress("emails", 0, message="Fetching emails from Gmail accounts")
        job.phase = "emails"
        job.progress_pct = 15
        db.commit()

        db, job, all_emails = sync_emails_for_accounts(
            db_factory=SessionLocal,
            db=db,
            job=job,
            accounts=accounts,
            user_id=user_id,
            correlation_id=correlation_id,
            progress_callback=self.update_progress,
            get_credentials=_get_credentials_from_account,
            get_last_date=get_last_processed_email_date,
            get_oldest_date=get_oldest_processed_email_date,
            get_existing_count=get_existing_email_count,
        )

        # Phase 4: Theme detection
        self.update_progress("themes", 40, message="Detecting themes with Claude AI")
        job.phase = "themes"
        job.progress_pct = 40
        db.commit()

        theme_processor = ThemeBatchProcessor()
        detect_themes(
            db=db,
            job=job,
            all_emails=all_emails,
            accounts=accounts,
            theme_processor=theme_processor,
            correlation_id=correlation_id,
            progress_callback=self.update_progress,
        )

        # Phase 5: Vault generation (dev only)
        vault_manager = ObsidianVaultManager(settings.obsidian_vault_path)
        note_generator = NoteGenerator()

        self.update_progress("vault", 70, message="Generating Obsidian vault")
        job.phase = "vault"
        job.progress_pct = 70
        db.commit()

        generate_vault(
            db=db,
            job=job,
            all_emails=all_emails,
            merged_contacts=merged_contacts,
            vault_manager=vault_manager,
            note_generator=note_generator,
            correlation_id=correlation_id,
            progress_callback=self.update_progress,
        )

        # Complete
        self.update_progress("completed", 100, message="Scan complete!")
        job.status = "completed"
        job.phase = "completed"
        job.progress_pct = 100
        job.completed_at = datetime.utcnow()
        db.commit()

        logger.info(
            "[%s] Gmail scan complete for user %s. Processed %d contacts and %d emails.",
            correlation_id,
            user_id,
            len(merged_contacts),
            len(all_emails),
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
        logger.error("[%s] Error during Gmail scan: %s", correlation_id, e, exc_info=True)

        if job:
            job.status = "failed"
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            db.commit()

        raise

    finally:
        db.close()
