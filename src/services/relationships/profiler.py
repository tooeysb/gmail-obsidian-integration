"""
Relationship profiler using Claude Haiku.

Generates AI-powered relationship summaries from sampled emails.
Follows the same API pattern as ThemeBatchProcessor.
"""

import json
import logging
from datetime import datetime
from typing import Any
from uuid import UUID

from anthropic import Anthropic
from sqlalchemy.orm import Session

from src.core.config import settings
from src.models.email import Email
from src.models.relationship_profile import RelationshipProfile
from src.services.relationships.email_sampler import sample_emails

logger = logging.getLogger(__name__)

RELATIONSHIP_SYSTEM_PROMPT = """You are analyzing email communication between a user and one of their contacts.
Given a set of representative emails spanning their communication history, generate a detailed relationship profile.

Return a JSON object with these fields:

1. "relationship_summary" (string): 2-3 sentences describing the nature and dynamics of this relationship.

2. "perceived_opinion" (string): 1-2 sentences about what this person likely thinks of the user, based on tone, responsiveness, and content patterns.

3. "primary_topics" (array of strings): Top 5-10 recurring discussion topics across all emails.

4. "communication_style" (string): Description of how they communicate - formal/casual/mixed, verbose/terse, responsive/slow, emoji usage, etc.

5. "notable_events" (array of strings): Key moments in the relationship - projects, celebrations, conflicts, decisions, milestones. Include approximate dates when possible.

6. "conflicts" (array of objects): Each with:
   - "description" (string): What the conflict/tension was about
   - "approximate_date" (string): When it occurred
   - "resolution_status" (string): "resolved", "ongoing", or "unknown"
   Empty array if no conflicts detected.

7. "sentiment_trend" (string): One of "improving", "stable", "declining", or "mixed" - based on how the tone changes chronologically across the emails.

8. "key_quotes" (array of strings): 2-3 short, notable excerpts from email summaries that characterize the relationship. Keep under 20 words each.

Return ONLY a valid JSON object. No additional text or explanation."""


def profile_contact(
    contact_info: dict[str, Any],
    sampled_emails: list[Email],
) -> dict[str, Any]:
    """
    Generate a relationship profile for a single contact using Claude.

    Args:
        contact_info: Contact metadata dict from discover_contacts().
        sampled_emails: Representative emails from sample_emails().

    Returns:
        Parsed profile data dictionary.
    """
    client = Anthropic(api_key=settings.anthropic_api_key)

    # Format the user prompt with contact context and emails
    user_prompt = _format_user_prompt(contact_info, sampled_emails)

    try:
        message = client.messages.create(
            model=settings.claude_model,
            max_tokens=2048,
            system=[
                {
                    "type": "text",
                    "text": RELATIONSHIP_SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[{"role": "user", "content": user_prompt}],
        )

        # Parse response
        text_content = None
        for block in message.content:
            if block.type == "text":
                text_content = block.text
                break

        if not text_content:
            logger.error(f"No text in Claude response for {contact_info['contact_email']}")
            return _empty_profile()

        # Strip markdown code blocks
        text_content = text_content.strip()
        if text_content.startswith("```json"):
            text_content = text_content[7:]
            if text_content.endswith("```"):
                text_content = text_content[:-3]
            text_content = text_content.strip()
        elif text_content.startswith("```"):
            text_content = text_content[3:]
            if text_content.endswith("```"):
                text_content = text_content[:-3]
            text_content = text_content.strip()

        profile = json.loads(text_content)

        # Validate and fill defaults
        return _validate_profile(profile)

    except json.JSONDecodeError as e:
        logger.error(f"JSON parse error for {contact_info['contact_email']}: {e}")
        return _empty_profile()
    except Exception as e:
        logger.error(f"Claude API error for {contact_info['contact_email']}: {e}")
        return _empty_profile()


def profile_contacts_batch(
    contacts: list[dict[str, Any]],
    user_id: UUID,
    db: Session,
    batch_size: int = 10,
) -> int:
    """
    Profile multiple contacts in batches.

    For each contact:
    1. Sample representative emails
    2. Call Claude for relationship analysis
    3. Upsert result to relationship_profiles table

    Args:
        contacts: List of contact dicts from discover_contacts().
        user_id: User UUID.
        db: SQLAlchemy session.
        batch_size: Number of contacts per commit batch.

    Returns:
        Number of contacts successfully profiled.
    """
    total = len(contacts)
    profiled = 0
    skipped = 0

    logger.info(f"Starting relationship profiling for {total} contacts")

    for idx, contact_info in enumerate(contacts):
        contact_email = contact_info["contact_email"]

        # Check if already profiled
        existing = (
            db.query(RelationshipProfile)
            .filter(
                RelationshipProfile.user_id == user_id,
                RelationshipProfile.contact_email == contact_email,
            )
            .first()
        )

        if existing and existing.profiled_at:
            skipped += 1
            if (idx + 1) % 50 == 0:
                logger.info(f"Progress: {idx + 1}/{total} (skipped {skipped} already profiled)")
            continue

        # Sample emails
        emails = sample_emails(contact_email, user_id, db)
        if not emails:
            logger.warning(f"No emails sampled for {contact_email}, skipping")
            skipped += 1
            continue

        # Profile with Claude
        profile_data = profile_contact(contact_info, emails)

        # Upsert to database
        if existing:
            existing.contact_name = contact_info.get("contact_name")
            existing.relationship_type = contact_info["relationship_type"]
            existing.account_sources = contact_info["account_sources"]
            existing.total_email_count = contact_info["total_email_count"]
            existing.sent_count = contact_info["sent_count"]
            existing.received_count = contact_info["received_count"]
            existing.first_exchange_date = contact_info.get("first_exchange_date")
            existing.last_exchange_date = contact_info.get("last_exchange_date")
            existing.thread_count = contact_info.get("thread_count", 0)
            existing.profile_data = profile_data
            existing.profiled_at = datetime.utcnow()
        else:
            profile = RelationshipProfile(
                user_id=user_id,
                contact_email=contact_email,
                contact_name=contact_info.get("contact_name"),
                relationship_type=contact_info["relationship_type"],
                account_sources=contact_info["account_sources"],
                total_email_count=contact_info["total_email_count"],
                sent_count=contact_info["sent_count"],
                received_count=contact_info["received_count"],
                first_exchange_date=contact_info.get("first_exchange_date"),
                last_exchange_date=contact_info.get("last_exchange_date"),
                thread_count=contact_info.get("thread_count", 0),
                profile_data=profile_data,
                profiled_at=datetime.utcnow(),
            )
            db.add(profile)

        profiled += 1

        # Commit in batches
        if profiled % batch_size == 0:
            db.commit()
            logger.info(
                f"Progress: {idx + 1}/{total} contacts processed "
                f"({profiled} profiled, {skipped} skipped)"
            )

    # Final commit
    db.commit()
    logger.info(
        f"Profiling complete: {profiled} profiled, {skipped} skipped out of {total} contacts"
    )
    return profiled


def _format_user_prompt(contact_info: dict[str, Any], emails: list[Email]) -> str:
    """Format the user prompt with contact metadata and email samples."""
    lines = [
        "Analyze the following email communication and generate a relationship profile.",
        "",
        "## Contact Information",
        f"- **Name**: {contact_info.get('contact_name') or 'Unknown'}",
        f"- **Email**: {contact_info['contact_email']}",
        f"- **Relationship Type**: {contact_info['relationship_type']}",
        f"- **Total Emails**: {contact_info['total_email_count']}",
        f"- **Sent by user**: {contact_info['sent_count']}",
        f"- **Received from contact**: {contact_info['received_count']}",
        f"- **First Exchange**: {contact_info.get('first_exchange_date', 'Unknown')}",
        f"- **Last Exchange**: {contact_info.get('last_exchange_date', 'Unknown')}",
        f"- **Thread Count**: {contact_info.get('thread_count', 0)}",
        "",
        "## Representative Emails (chronological)",
        "",
    ]

    for i, email in enumerate(emails, 1):
        # Determine direction
        from src.services.relationships.contact_discovery import USER_EMAILS

        is_from_user = email.sender_email.lower().strip() in USER_EMAILS
        direction = "USER SENT" if is_from_user else "RECEIVED"

        tags_str = ""
        if email.tags:
            tag_list = [f"{t.tag_category}:{t.tag}" for t in email.tags]
            tags_str = f"\n  Tags: {', '.join(tag_list)}"

        lines.append(
            f"### Email {i} [{direction}]"
            f"\n  Date: {email.date.strftime('%Y-%m-%d %H:%M')}"
            f"\n  From: {email.sender_name or email.sender_email}"
            f"\n  Subject: {email.subject or '(no subject)'}"
            f"\n  Summary: {email.summary or '(no summary)'}"
            f"{tags_str}"
            f"\n"
        )

    lines.append("Generate the relationship profile as specified in the system prompt.")
    return "\n".join(lines)


def _validate_profile(profile: dict[str, Any]) -> dict[str, Any]:
    """Validate and fill defaults for profile fields."""
    defaults = {
        "relationship_summary": "",
        "perceived_opinion": "",
        "primary_topics": [],
        "communication_style": "",
        "notable_events": [],
        "conflicts": [],
        "sentiment_trend": "stable",
        "key_quotes": [],
    }

    for key, default in defaults.items():
        if key not in profile:
            profile[key] = default

    # Validate sentiment_trend
    valid_trends = {"improving", "stable", "declining", "mixed"}
    if profile["sentiment_trend"] not in valid_trends:
        profile["sentiment_trend"] = "stable"

    return profile


def _empty_profile() -> dict[str, Any]:
    """Return empty profile for failed processing."""
    return {
        "relationship_summary": "",
        "perceived_opinion": "",
        "primary_topics": [],
        "communication_style": "",
        "notable_events": [],
        "conflicts": [],
        "sentiment_trend": "stable",
        "key_quotes": [],
    }
