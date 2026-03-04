"""
Phase 4: Theme detection with Claude Batch API.

Processes emails in batches through Claude for theme extraction, then generates tags.
"""

import uuid
from collections.abc import Callable

from sqlalchemy.orm import Session

from src.core.config import settings
from src.core.logging import get_logger
from src.integrations.claude.batch_processor import ThemeBatchProcessor
from src.models import Email, EmailTag, GmailAccount, SyncJob
from src.services.theme_detection.prompt_template import generate_tags

logger = get_logger(__name__)

PROGRESS_MIN = 40
PROGRESS_MAX = 70


def detect_themes(
    db: Session,
    job: SyncJob,
    all_emails: list[Email],
    accounts: list[GmailAccount],
    theme_processor: ThemeBatchProcessor,
    correlation_id: str,
    progress_callback: Callable[[str, int, int, str], None],
) -> None:
    """Run theme detection on all emails using Claude Batch API."""
    batch_size = settings.claude_batch_size
    email_batches = [all_emails[i : i + batch_size] for i in range(0, len(all_emails), batch_size)]

    for batch_idx, email_batch in enumerate(email_batches):
        logger.info(
            "[%s] Processing theme detection batch %d/%d (%d emails)",
            correlation_id,
            batch_idx + 1,
            len(email_batches),
            len(email_batch),
        )

        batch_results = theme_processor.process_emails_sync(email_batch)

        for email in email_batch:
            email_id = str(email.id)
            themes = batch_results.get(email_id)

            if not themes:
                logger.warning("[%s] No themes detected for email %s", correlation_id, email.id)
                continue

            email_exists = db.query(Email.id).filter(Email.id == email.id).first()
            if not email_exists:
                logger.debug("[%s] Skipping tags for duplicate email %s", correlation_id, email.id)
                continue

            account = next((acc for acc in accounts if acc.id == email.account_id), None)
            account_label = account.account_label if account else "unknown"

            tags = generate_tags(themes, account_label)

            for tag_dict in tags:
                email_tag = EmailTag(
                    id=uuid.uuid4(),
                    email_id=email.id,
                    tag=tag_dict["tag"],
                    tag_category=tag_dict["tag_category"],
                    confidence=tag_dict.get("confidence"),
                )
                db.add(email_tag)

        db.commit()

        progress = int(
            PROGRESS_MIN + ((batch_idx + 1) / len(email_batches)) * (PROGRESS_MAX - PROGRESS_MIN)
        )
        progress_callback(
            "themes",
            progress,
            len(all_emails),
            f"Processed {(batch_idx + 1) * batch_size} emails for themes",
        )
        job.progress_pct = progress
        db.commit()

    logger.info("[%s] Theme detection complete for all emails", correlation_id)
