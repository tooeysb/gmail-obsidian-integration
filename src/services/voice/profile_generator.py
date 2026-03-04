"""
Voice Profile Generator.

Analyzes sent emails to build a writing voice profile using Claude Sonnet 4.5.
"""

import json
import uuid
from datetime import datetime

from anthropic import Anthropic
from sqlalchemy import func
from sqlalchemy.orm import Session

from src.core.config import settings
from src.core.logging import get_logger
from src.core.utils import strip_markdown_codeblocks
from src.models import Email, GmailAccount
from src.models.voice_profile import VoiceProfile
from src.services.voice.draft_prompt import (
    VOICE_ANALYSIS_SYSTEM_PROMPT,
    VOICE_ANALYSIS_USER_TEMPLATE,
    format_emails_for_analysis,
)

logger = get_logger(__name__)


class VoiceProfileGenerator:
    """Generates voice profiles by analyzing sent emails with Claude."""

    def __init__(self, db: Session):
        self.db = db
        self.client = Anthropic(api_key=settings.anthropic_api_key)
        self.model = settings.draft_model

    def generate_profile(
        self,
        user_id: str,
        profile_name: str = "default",
        sample_size: int = 1000,
    ) -> VoiceProfile:
        """
        Generate a voice profile by analyzing the user's sent emails.

        Args:
            user_id: User UUID
            profile_name: Name for this profile
            sample_size: Max number of sent emails to analyze

        Returns:
            Created or updated VoiceProfile
        """
        logger.info("Generating voice profile '%s' for user %s", profile_name, user_id)

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
            raise ValueError(f"No active accounts found for user {user_id}")

        logger.info("User email addresses: %s", user_emails)

        # Fetch sent emails with body content
        sent_emails = self._fetch_sent_emails(user_id, user_emails, sample_size)

        if not sent_emails:
            raise ValueError("No sent emails with body content found. Run body backfill first.")

        logger.info("Found %s sent emails for analysis", len(sent_emails))

        # Analyze in batches (Claude has context limits)
        profile_data = self._analyze_emails(sent_emails)

        # Save or update profile
        profile = self._save_profile(user_id, profile_name, profile_data, len(sent_emails))

        logger.info("Voice profile '%s' generated with %s samples", profile_name, len(sent_emails))

        return profile

    def _fetch_sent_emails(
        self,
        user_id: str,
        user_emails: list[str],
        sample_size: int,
    ) -> list[dict]:
        """
        Fetch a diverse sample of sent emails with body content.

        Samples strategically across recipients, time periods, and topics.
        """
        # Get sent emails with body using probabilistic sampling.
        # Using random() < threshold in WHERE is O(n) scan, much faster than
        # ORDER BY random() which is O(n log n) sort on the full result set.
        total = (
            self.db.query(func.count(Email.id))
            .filter(
                Email.user_id == user_id,
                Email.sender_email.in_(user_emails),
                Email.body != None,  # noqa: E711
                Email.body != "",
            )
            .scalar()
            or 0
        )

        base_filter = [
            Email.user_id == user_id,
            Email.sender_email.in_(user_emails),
            Email.body != None,  # noqa: E711
            Email.body != "",
        ]

        if total <= sample_size:
            emails = self.db.query(Email).filter(*base_filter).all()
        else:
            # Over-sample by 2x then truncate to handle variance in random()
            ratio = min(1.0, (sample_size * 2) / total)
            emails = (
                self.db.query(Email)
                .filter(*base_filter, func.random() < ratio)
                .limit(sample_size)
                .all()
            )

        return [
            {
                "id": str(email.id),
                "subject": email.subject,
                "sender_email": email.sender_email,
                "recipient_emails": email.recipient_emails,
                "date": email.date.isoformat() if email.date else "",
                "body": email.body,
                "summary": email.summary,
            }
            for email in emails
        ]

    def _analyze_emails(self, emails: list[dict]) -> dict:
        """
        Send emails to Claude for voice analysis.

        Processes in batches if there are many emails, then merges results.
        """
        # Claude Sonnet can handle ~100k tokens. With ~2k chars per email,
        # we can fit ~40 emails per batch comfortably.
        batch_size = 40
        batch_profiles = []

        for i in range(0, len(emails), batch_size):
            batch = emails[i : i + batch_size]
            emails_text = format_emails_for_analysis(batch)

            user_prompt = VOICE_ANALYSIS_USER_TEMPLATE.format(
                count=len(batch),
                emails_text=emails_text,
            )

            logger.info(
                "Analyzing batch %s (%s emails, ~%s chars)",
                i // batch_size + 1,
                len(batch),
                len(user_prompt),
            )

            message = self.client.messages.create(
                model=self.model,
                max_tokens=4096,
                system=[
                    {
                        "type": "text",
                        "text": VOICE_ANALYSIS_SYSTEM_PROMPT,
                        "cache_control": {"type": "ephemeral"},
                    }
                ],
                messages=[{"role": "user", "content": user_prompt}],
            )

            # Parse JSON response
            text_content = ""
            for block in message.content:
                if block.type == "text":
                    text_content = block.text
                    break

            text_content = strip_markdown_codeblocks(text_content)

            profile_data = json.loads(text_content)
            batch_profiles.append(profile_data)

        # If only one batch, return it directly
        if len(batch_profiles) == 1:
            return batch_profiles[0]

        # Merge multiple batch profiles using Claude
        return self._merge_profiles(batch_profiles)

    def _merge_profiles(self, profiles: list[dict]) -> dict:
        """Merge multiple batch voice profiles into one using Claude."""
        merge_prompt = (
            "Merge these voice profiles from different batches of the same person's emails "
            "into a single comprehensive profile. Combine patterns, deduplicate, and synthesize "
            "the most accurate representation.\n\n"
        )
        for i, profile in enumerate(profiles, 1):
            merge_prompt += f"### Profile {i}\n```json\n{json.dumps(profile, indent=2)}\n```\n\n"

        merge_prompt += (
            "Return a single merged JSON profile with the same structure. "
            "Return ONLY the JSON, no other text."
        )

        message = self.client.messages.create(
            model=self.model,
            max_tokens=4096,
            system=[
                {
                    "type": "text",
                    "text": VOICE_ANALYSIS_SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[{"role": "user", "content": merge_prompt}],
        )

        text_content = ""
        for block in message.content:
            if block.type == "text":
                text_content = block.text
                break

        text_content = strip_markdown_codeblocks(text_content)

        return json.loads(text_content)

    def _save_profile(
        self,
        user_id: str,
        profile_name: str,
        profile_data: dict,
        sample_count: int,
    ) -> VoiceProfile:
        """Save or update the voice profile in the database."""
        existing = (
            self.db.query(VoiceProfile)
            .filter(
                VoiceProfile.user_id == user_id,
                VoiceProfile.profile_name == profile_name,
            )
            .first()
        )

        now = datetime.utcnow()

        if existing:
            existing.profile_data = profile_data
            existing.sample_count = sample_count
            existing.generated_at = now
            existing.updated_at = now
            self.db.commit()
            self.db.refresh(existing)
            return existing

        profile = VoiceProfile(
            id=uuid.uuid4(),
            user_id=user_id,
            profile_name=profile_name,
            profile_data=profile_data,
            sample_count=sample_count,
            generated_at=now,
        )
        self.db.add(profile)
        self.db.commit()
        self.db.refresh(profile)
        return profile
