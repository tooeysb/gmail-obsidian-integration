"""
Phase 5: Obsidian vault generation (local development only).

Generates contact and email notes in the Obsidian vault directory.
Skipped in production (Heroku has read-only filesystem).
"""

from collections import defaultdict
from collections.abc import Callable

from sqlalchemy.orm import Session

from src.core.config import settings
from src.core.logging import get_logger
from src.models import Contact, Email, EmailTag, SyncJob
from src.services.obsidian.note_generator import NoteGenerator
from src.services.obsidian.vault_manager import ObsidianVaultManager

logger = get_logger(__name__)

PROGRESS_MIN = 70
PROGRESS_MAX = 100
CONTACT_LOG_INTERVAL = 10
EMAIL_LOG_INTERVAL = 100


def generate_vault(
    db: Session,
    job: SyncJob,
    all_emails: list[Email],
    merged_contacts: list[Contact],
    vault_manager: ObsidianVaultManager,
    note_generator: NoteGenerator,
    correlation_id: str,
    progress_callback: Callable[[str, int, int, str], None],
) -> None:
    """Generate Obsidian vault notes from processed emails and contacts."""
    if not settings.is_development:
        logger.info("[%s] Skipping vault generation (non-development environment)", correlation_id)
        return

    vault_manager.initialize_vault()
    logger.info("[%s] Vault initialized at %s", correlation_id, settings.obsidian_vault_path)

    # Group emails by sender
    emails_by_contact: dict[str, list[Email]] = defaultdict(list)
    for email in all_emails:
        emails_by_contact[email.sender_email].append(email)

    # Generate contact notes
    progress_callback("vault", 75, 0, "Generating contact notes")
    for contact_idx, contact in enumerate(merged_contacts):
        contact_emails = emails_by_contact.get(contact.email, [])

        email_ids = [e.id for e in contact_emails]
        db.query(EmailTag).filter(EmailTag.email_id.in_(email_ids)).all()

        contact_note = note_generator.generate_contact_note(contact, contact_emails)
        contact_path = vault_manager.get_contact_path(contact.name or contact.email)
        contact_path.write_text(contact_note)

        if contact_idx % CONTACT_LOG_INTERVAL == 0:
            progress = int(75 + (contact_idx / max(len(merged_contacts), 1)) * 10)
            progress_callback("vault", progress, 0, f"Generated {contact_idx} contact notes")
            job.progress_pct = progress
            db.commit()

    logger.info("[%s] Generated %d contact notes", correlation_id, len(merged_contacts))

    # Generate email notes
    progress_callback("vault", 85, 0, "Generating email notes")
    for email_idx, email in enumerate(all_emails):
        tags = db.query(EmailTag).filter(EmailTag.email_id == email.id).all()
        tag_strings = [f"{t.tag_category}/{t.tag}" for t in tags]

        email_note = note_generator.generate_email_note(email, tag_strings)
        email_path = vault_manager.get_email_path(email.date, email.subject or "Untitled")
        vault_manager.ensure_email_directory(email.date)
        email_path.write_text(email_note)

        if email_idx % EMAIL_LOG_INTERVAL == 0:
            progress = int(85 + (email_idx / max(len(all_emails), 1)) * 15)
            progress_callback("vault", progress, 0, f"Generated {email_idx} email notes")
            job.progress_pct = progress
            db.commit()

    logger.info("[%s] Generated %d email notes", correlation_id, len(all_emails))
