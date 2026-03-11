"""
Draft suggestion generator.

Matches high-relevance news items to CRM contacts mentioned by name
in the article, and generates personalized email drafts using the
existing voice profile system.
"""

import re
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
        parts.append(
            f"This email is to {contact_desc} at {company_name} "
            f"because they were mentioned in this article."
        )

        return " ".join(parts)

    def _find_mentioned_contacts(self, item: CompanyNewsItem) -> list[tuple[Contact, str]]:
        """Find contacts at this company whose name appears in the article text.

        Returns list of (Contact, match_confidence) tuples where
        match_confidence is 'full_name' or 'last_name'.
        """
        article_text = f"{item.title or ''} {item.summary or ''}".lower()
        if not article_text.strip():
            return []

        # Company name words for false-positive filtering
        company_name_words = set()
        if item.company and item.company.name:
            company_name_words = {w.lower() for w in item.company.name.split()}

        contacts = (
            self.db.query(Contact)
            .filter(
                Contact.company_id == item.company_id,
                Contact.user_id == item.user_id,
                Contact.is_active.is_(True),
                Contact.deleted_at.is_(None),
                Contact.name.isnot(None),
            )
            .all()
        )

        mentioned = []
        for contact in contacts:
            name = contact.name.strip()
            if not name:
                continue

            # Full name match (word boundaries)
            pattern = r"\b" + re.escape(name.lower()) + r"\b"
            if re.search(pattern, article_text):
                mentioned.append((contact, "full_name"))
                continue

            # Last name fallback (>= 4 chars to avoid false positives)
            parts = name.split()
            if len(parts) >= 2:
                last = parts[-1]
                if len(last) >= 4:
                    # Skip if last name is a word in the company name
                    # (e.g., "Yates" in "Yates Construction")
                    if last.lower() in company_name_words:
                        continue
                    pattern = r"\b" + re.escape(last.lower()) + r"\b"
                    if re.search(pattern, article_text):
                        mentioned.append((contact, "last_name"))

        return mentioned

    def generate_for_news_item(self, item: CompanyNewsItem) -> list[DraftSuggestion]:
        """
        Generate draft suggestions for contacts mentioned by name
        in the news article's title or summary.
        """
        results = self._find_mentioned_contacts(item)

        if not results:
            logger.debug(
                "No contacts mentioned in article '%s' for company %s",
                item.title[:60],
                item.company_id,
            )
            return []

        suggestions = []
        for contact, confidence in results:
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
                    trigger_type="news_mention",
                    match_confidence=confidence,
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
                    "Generated %s-confidence draft for %s (mentioned in '%s')",
                    confidence,
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
        that haven't been actioned yet. Only generates drafts for contacts
        mentioned by name in the article.

        Returns stats: {items_processed, drafts_generated, errors}
        """
        threshold = settings.news_relevance_threshold

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
                suggestions = self.generate_for_news_item(item)
                stats["items_processed"] += 1
                stats["drafts_generated"] += len(suggestions)
            except Exception:
                logger.exception("Error generating drafts for news item %s", item.id)
                stats["errors"] += 1

        logger.info("Draft generation complete: %s", stats)
        return stats
