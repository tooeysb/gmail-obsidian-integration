"""
Draft suggestion generator.

Matches high-relevance news items to CRM contacts and generates
personalized email drafts using the existing voice profile system.
"""

from datetime import UTC, datetime

from sqlalchemy.orm import Session, joinedload

from src.core.config import settings
from src.core.logging import get_logger
from src.models.company_news import CompanyNewsItem
from src.models.contact import Contact
from src.models.draft_suggestion import DraftSuggestion
from src.services.voice.draft_service import EmailDraftService

logger = get_logger(__name__)

CATEGORY_LABELS = {
    "project_win": "just won a major project",
    "project_completion": "just completed a major project",
    "executive_hire": "has a new executive joining the team",
    "expansion": "is expanding operations",
    "partnership": "announced a new partnership",
    "award": "received a notable industry award",
    "financial_results": "released financial results",
}


class NewsDraftGeneratorService:
    """Generates voice-matched draft emails from news events."""

    def __init__(self, db: Session):
        self.db = db
        self.draft_service = EmailDraftService(db)

    def _build_context(self, item: CompanyNewsItem, contact: Contact) -> str:
        """Build the context string for EmailDraftService."""
        analysis = item.analysis or {}
        category = analysis.get("category", "other")
        category_desc = CATEGORY_LABELS.get(category, "has recent news")
        company_name = item.company.name if item.company else "Their company"

        parts = [
            f"{company_name} {category_desc}.",
            f"Article: '{item.title}'.",
        ]

        outreach_angle = analysis.get("outreach_angle", "")
        if outreach_angle:
            parts.append(outreach_angle)

        contact_desc = contact.name or contact.email
        if contact.title:
            contact_desc += f", {contact.title}"
        parts.append(f"This email is to {contact_desc} at {company_name}.")

        return " ".join(parts)

    def generate_for_news_item(
        self, item: CompanyNewsItem, max_contacts: int = 3
    ) -> list[DraftSuggestion]:
        """
        Generate draft suggestions for contacts at the news item's company.
        Prioritizes VIPs, then by email_count.
        """
        contacts = (
            self.db.query(Contact)
            .filter(
                Contact.company_id == item.company_id,
                Contact.user_id == item.user_id,
            )
            .order_by(Contact.is_vip.desc(), Contact.email_count.desc())
            .limit(max_contacts)
            .all()
        )

        if not contacts:
            logger.debug("No contacts found for company %s", item.company_id)
            return []

        suggestions = []
        for contact in contacts:
            # Check if draft already exists for this news+contact
            existing = (
                self.db.query(DraftSuggestion)
                .filter(
                    DraftSuggestion.news_item_id == item.id,
                    DraftSuggestion.contact_id == contact.id,
                )
                .first()
            )
            if existing:
                continue

            context = self._build_context(item, contact)

            try:
                result = self.draft_service.draft_email(
                    user_id=str(item.user_id),
                    recipient_email=contact.email,
                    context=context,
                    tone="professional",
                )

                suggestion = DraftSuggestion(
                    user_id=item.user_id,
                    news_item_id=item.id,
                    contact_id=contact.id,
                    subject=result.subject,
                    body=result.body,
                    context_used=context,
                    tone="professional",
                    status="pending",
                    generated_at=datetime.now(UTC),
                    model_used=result.model,
                )
                self.db.add(suggestion)
                suggestions.append(suggestion)

                logger.info(
                    "Generated draft for %s re: %s",
                    contact.email,
                    item.title[:60],
                )

            except Exception:
                logger.exception("Draft generation failed for %s", contact.email)

        if suggestions:
            item.status = "actioned"
            self.db.commit()

        return suggestions

    def generate_all_pending(self, user_id: str) -> dict:
        """
        Generate drafts for all analyzed news items above the relevance threshold
        that haven't been actioned yet.

        Returns stats: {items_processed, drafts_generated, errors}
        """
        threshold = settings.news_relevance_threshold
        max_contacts = settings.news_max_drafts_per_item

        # Find analyzed items that haven't been actioned
        items = (
            self.db.query(CompanyNewsItem)
            .options(joinedload(CompanyNewsItem.company))
            .filter(
                CompanyNewsItem.user_id == user_id,
                CompanyNewsItem.status == "analyzed",
            )
            .all()
        )

        # Filter by relevance score in Python (JSON field)
        relevant_items = [
            item
            for item in items
            if item.analysis and item.analysis.get("relevance_score", 0) >= threshold
        ]

        stats = {"items_processed": 0, "drafts_generated": 0, "errors": 0}

        for item in relevant_items:
            try:
                suggestions = self.generate_for_news_item(item, max_contacts=max_contacts)
                stats["items_processed"] += 1
                stats["drafts_generated"] += len(suggestions)
            except Exception:
                logger.exception("Error generating drafts for news item %s", item.id)
                stats["errors"] += 1

        logger.info("Draft generation complete: %s", stats)
        return stats
