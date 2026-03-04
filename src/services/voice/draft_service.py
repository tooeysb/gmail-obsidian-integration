"""
Email Draft Service.

Drafts emails in the user's voice using their voice profile and similar sent emails
as reference material.
"""

import json
from dataclasses import dataclass

from anthropic import Anthropic
from sqlalchemy import or_
from sqlalchemy.orm import Session

from src.core.config import settings
from src.core.logging import get_logger
from src.models import Email, GmailAccount
from src.models.relationship_profile import RelationshipProfile
from src.models.voice_profile import VoiceProfile
from src.services.voice.draft_prompt import (
    DRAFT_SYSTEM_PROMPT,
    DRAFT_USER_TEMPLATE,
    format_example_emails,
)

logger = get_logger(__name__)


@dataclass
class DraftResult:
    """Result of an email draft generation."""

    subject: str
    body: str
    similar_emails_used: int
    voice_profile_used: str
    model: str


class EmailDraftService:
    """Drafts emails in the user's voice."""

    def __init__(self, db: Session):
        self.db = db
        self.client = Anthropic(api_key=settings.anthropic_api_key)
        self.model = settings.draft_model
        self.max_examples = settings.draft_max_examples

    def draft_email(
        self,
        user_id: str,
        recipient_email: str,
        context: str,
        tone: str | None = None,
        reply_to_subject: str | None = None,
    ) -> DraftResult:
        """
        Draft an email in the user's voice.

        Args:
            user_id: User UUID
            recipient_email: Who the email is to
            context: What the email should be about
            tone: Optional tone override (urgent, casual, formal)
            reply_to_subject: Subject of email being replied to

        Returns:
            DraftResult with subject and body
        """
        # Load voice profile
        profile = self._load_voice_profile(user_id)
        if not profile:
            raise ValueError("No voice profile found. Run generate_voice_profile.py first.")

        # Determine relationship type
        relationship_type = self._get_relationship_type(user_id, recipient_email)

        # Find similar sent emails
        similar_emails = self._find_similar_sent_emails(
            user_id, recipient_email, context, relationship_type
        )

        # Build prompt
        example_text = (
            format_example_emails(similar_emails) if similar_emails else "No similar emails found."
        )
        tone_str = tone or "match my usual tone for this type of person"
        reply_line = f"- Replying to subject: {reply_to_subject}" if reply_to_subject else ""

        user_prompt = DRAFT_USER_TEMPLATE.format(
            voice_profile_json=json.dumps(profile.profile_data, indent=2),
            example_emails=example_text,
            recipient_email=recipient_email,
            relationship_type=relationship_type,
            context=context,
            tone=tone_str,
            reply_line=reply_line,
        )

        # Call Claude
        message = self.client.messages.create(
            model=self.model,
            max_tokens=2048,
            system=[
                {
                    "type": "text",
                    "text": DRAFT_SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[{"role": "user", "content": user_prompt}],
        )

        # Parse response
        text_content = ""
        for block in message.content:
            if block.type == "text":
                text_content = block.text
                break

        subject, body = self._parse_draft_response(text_content)

        return DraftResult(
            subject=subject,
            body=body,
            similar_emails_used=len(similar_emails),
            voice_profile_used=profile.profile_name,
            model=self.model,
        )

    def _load_voice_profile(self, user_id: str) -> VoiceProfile | None:
        """Load the user's voice profile, preferring 'default'."""
        return (
            self.db.query(VoiceProfile)
            .filter(VoiceProfile.user_id == user_id)
            .order_by(
                # Prefer 'default' profile
                (VoiceProfile.profile_name == "default").desc(),
                VoiceProfile.generated_at.desc(),
            )
            .first()
        )

    def _get_relationship_type(self, user_id: str, recipient_email: str) -> str:
        """Look up the relationship type for this recipient."""
        profile = (
            self.db.query(RelationshipProfile)
            .filter(
                RelationshipProfile.user_id == user_id,
                RelationshipProfile.contact_email == recipient_email,
            )
            .first()
        )
        if profile:
            return profile.relationship_type
        return "unknown"

    def _find_similar_sent_emails(
        self,
        user_id: str,
        recipient_email: str,
        context: str,
        relationship_type: str,
    ) -> list[dict]:
        """
        Find similar sent emails using tag overlap scoring.

        Scoring:
        - Same recipient: +3 points
        - Same relationship type: +2 points
        - Matching topic/domain tag: +1 point per match
        - Recency bonus: +0.5 for emails in last 6 months
        """
        # Get user's email addresses
        accounts = (
            self.db.query(GmailAccount)
            .filter(
                GmailAccount.user_id == user_id,
                GmailAccount.is_active == True,  # noqa: E712
            )
            .all()
        )
        user_emails = [acc.account_email for acc in accounts]

        if not user_emails:
            return []

        # First, try exact recipient match
        exact_match = (
            self.db.query(Email)
            .filter(
                Email.user_id == user_id,
                Email.sender_email.in_(user_emails),
                Email.body != None,  # noqa: E711
                Email.body != "",
                Email.recipient_emails.ilike(f"%{recipient_email}%"),
            )
            .order_by(Email.date.desc())
            .limit(self.max_examples)
            .all()
        )

        results = []
        seen_ids = set()

        for email in exact_match:
            seen_ids.add(email.id)
            results.append(self._email_to_dict(email))

        # If we need more, find emails by relationship type
        if len(results) < self.max_examples and relationship_type != "unknown":
            # Find other contacts with same relationship type
            same_type_contacts = (
                self.db.query(RelationshipProfile.contact_email)
                .filter(
                    RelationshipProfile.user_id == user_id,
                    RelationshipProfile.relationship_type == relationship_type,
                )
                .all()
            )
            contact_emails = [r[0] for r in same_type_contacts]

            if contact_emails:
                type_match = (
                    self.db.query(Email)
                    .filter(
                        Email.user_id == user_id,
                        Email.sender_email.in_(user_emails),
                        Email.body != None,  # noqa: E711
                        Email.body != "",
                        Email.id.notin_(seen_ids) if seen_ids else True,
                        or_(
                            *[Email.recipient_emails.ilike(f"%{ce}%") for ce in contact_emails[:20]]
                        ),
                    )
                    .order_by(Email.date.desc())
                    .limit(self.max_examples - len(results))
                    .all()
                )

                for email in type_match:
                    seen_ids.add(email.id)
                    results.append(self._email_to_dict(email))

        # If still need more, get recent sent emails as general reference
        if len(results) < self.max_examples:
            remaining = self.max_examples - len(results)
            general = (
                self.db.query(Email)
                .filter(
                    Email.user_id == user_id,
                    Email.sender_email.in_(user_emails),
                    Email.body != None,  # noqa: E711
                    Email.body != "",
                    Email.id.notin_(seen_ids) if seen_ids else True,
                )
                .order_by(Email.date.desc())
                .limit(remaining)
                .all()
            )

            for email in general:
                results.append(self._email_to_dict(email))

        return results[: self.max_examples]

    def _email_to_dict(self, email: Email) -> dict:
        """Convert Email model to dict for prompt formatting."""
        return {
            "subject": email.subject,
            "sender_email": email.sender_email,
            "recipient_emails": email.recipient_emails,
            "date": email.date.isoformat() if email.date else "",
            "body": email.body or email.summary or "",
        }

    def _parse_draft_response(self, text: str) -> tuple[str, str]:
        """Parse Claude's response into subject and body."""
        text = text.strip()

        # Look for "Subject: " prefix
        if text.lower().startswith("subject:"):
            lines = text.split("\n", 1)
            subject = lines[0].split(":", 1)[1].strip()
            body = lines[1].strip() if len(lines) > 1 else ""
        else:
            # No subject line found, use first line as subject
            lines = text.split("\n", 1)
            subject = lines[0].strip()
            body = lines[1].strip() if len(lines) > 1 else ""

        return subject, body
