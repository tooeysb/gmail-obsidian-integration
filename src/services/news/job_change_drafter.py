"""
Job change draft generator.

When LinkedIn monitoring detects a job change (company mismatch),
this service generates a congratulatory outreach draft using the
user's voice profile.
"""

import uuid
from datetime import UTC, datetime

from sqlalchemy.orm import Session, joinedload

from src.core.logging import get_logger
from src.models.contact import Contact
from src.models.draft_suggestion import DraftSuggestion
from src.services.voice.draft_service import EmailDraftService

logger = get_logger(__name__)


class JobChangeDraftService:
    """Generates voice-matched congratulatory drafts for job changes."""

    def __init__(self, db: Session):
        self.db = db
        self.draft_service = EmailDraftService(db)

    def _build_context(self, contact: Contact) -> str:
        """Build context string for EmailDraftService."""
        name = contact.name or contact.email
        old_company = contact.company.name if contact.company else "their previous company"
        new_company = contact.linkedin_company_raw or "a new company"

        parts = [
            f"{name} has moved from {old_company} to {new_company}.",
        ]
        if contact.title:
            parts.append(f"Previous role: {contact.title}.")
        parts.append("This is a congratulatory note about their career move.")
        parts.append("Keep it warm, brief, and personal.")

        return " ".join(parts)

    def generate_for_contact(self, contact: Contact, user_id: str) -> DraftSuggestion | None:
        """Generate a congratulatory draft for a single contact's job change."""
        # Check no pending job change draft already exists
        existing = (
            self.db.query(DraftSuggestion)
            .filter(
                DraftSuggestion.contact_id == contact.id,
                DraftSuggestion.trigger_type == "job_change",
                DraftSuggestion.status == "pending",
            )
            .first()
        )
        if existing:
            logger.debug("Pending job change draft already exists for %s", contact.email)
            return None

        context = self._build_context(contact)

        try:
            result = self.draft_service.draft_email(
                user_id=user_id,
                recipient_email=contact.email,
                context=context,
                tone="warm",
            )
        except Exception:
            logger.exception("Voice draft generation failed for %s", contact.email)
            raise

        suggestion = DraftSuggestion(
            user_id=uuid.UUID(user_id),
            news_item_id=None,
            contact_id=contact.id,
            trigger_type="job_change",
            match_confidence="full_name",
            subject=result.subject,
            body=result.body,
            context_used=context,
            tone="warm",
            status="pending",
            generated_at=datetime.now(UTC),
            model_used=result.model,
        )
        self.db.add(suggestion)
        contact.job_change_draft_generated_at = datetime.now(UTC)
        self.db.commit()

        logger.info("Generated job change draft for %s", contact.email)
        return suggestion

    def generate_all_pending(self, user_id: str) -> dict:
        """
        Generate drafts for all contacts with detected job changes
        that haven't had a draft generated yet.

        Returns stats: {contacts_found, drafts_generated, errors}
        """
        contacts = (
            self.db.query(Contact)
            .options(joinedload(Contact.company))
            .filter(
                Contact.user_id == user_id,
                Contact.job_change_detected_at.isnot(None),
                Contact.job_change_draft_generated_at.is_(None),
                Contact.deleted_at.is_(None),
            )
            .all()
        )

        stats = {"contacts_found": len(contacts), "drafts_generated": 0, "errors": 0}

        for contact in contacts:
            try:
                result = self.generate_for_contact(contact, user_id)
                if result:
                    stats["drafts_generated"] += 1
            except Exception:
                logger.exception("Job change draft failed for %s", contact.email)
                stats["errors"] += 1

        logger.info("Job change draft generation complete: %s", stats)
        return stats
