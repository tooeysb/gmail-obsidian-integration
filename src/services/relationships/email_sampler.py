"""
Email sampling for relationship profiling.

Selects representative emails across categories to give Claude a diverse view
of each contact relationship. Uses SQL queries on existing email data.
"""

import random
from uuid import UUID

from sqlalchemy import func
from sqlalchemy.orm import Session, selectinload

from src.core.logging import get_logger
from src.models.email import Email, EmailTag

logger = get_logger(__name__)


def sample_emails(
    contact_email: str,
    user_id: UUID,
    db: Session,
    max_samples: int = 25,
) -> list[Email]:
    """
    Select representative emails for a contact across 6 categories.

    Categories:
        1. First exchanges (3): Earliest emails by date
        2. Most recent (5): Latest emails by date
        3. Longest threads (4): Threads with most messages
        4. High-sentiment (4): Emails tagged positive or negative
        5. Conflict-flagged (4): Emails with urgent/negative/action tags
        6. Random (5): Random sample from remaining

    Args:
        contact_email: Contact email to sample for.
        user_id: User UUID.
        db: SQLAlchemy session.
        max_samples: Maximum total samples (default 25).

    Returns:
        List of Email model objects with tags eagerly loaded.
    """
    contact_email = contact_email.lower().strip()

    # Base query: all emails involving this contact
    base_q = (
        db.query(Email)
        .options(selectinload(Email.tags))
        .filter(
            Email.user_id == user_id,
            (func.lower(Email.sender_email) == contact_email)
            | (func.lower(Email.recipient_emails).contains(contact_email)),
        )
    )

    total_available = base_q.count()
    if total_available == 0:
        logger.warning("No emails found for contact %s", contact_email)
        return []

    # If total emails <= max_samples, return all
    if total_available <= max_samples:
        return base_q.order_by(Email.date.asc()).all()

    # Proportional allocation based on max_samples=25
    allocations = _compute_allocations(max_samples)
    sampled_ids: set[UUID] = set()
    sampled_emails: list[Email] = []

    def _add_emails(emails: list[Email], limit: int) -> None:
        """Add emails to sample, avoiding duplicates."""
        for email in emails:
            if email.id not in sampled_ids and len(sampled_emails) < max_samples:
                sampled_ids.add(email.id)
                sampled_emails.append(email)
                if len([e for e in sampled_emails if e.id in sampled_ids]) >= len(sampled_ids):
                    pass  # continue adding
            if len(sampled_ids) >= len(sampled_emails) + limit:
                break

    # Category 1: First exchanges (earliest emails)
    first_emails = base_q.order_by(Email.date.asc()).limit(allocations["first"]).all()
    _add_unique(sampled_ids, sampled_emails, first_emails, allocations["first"])

    # Category 2: Most recent emails
    recent_emails = base_q.order_by(Email.date.desc()).limit(allocations["recent"]).all()
    _add_unique(sampled_ids, sampled_emails, recent_emails, allocations["recent"])

    # Category 3: Longest threads (threads with most messages involving this contact)
    thread_subq = (
        db.query(
            Email.gmail_thread_id,
            func.count().label("msg_count"),
        )
        .filter(
            Email.user_id == user_id,
            Email.gmail_thread_id.isnot(None),
            (func.lower(Email.sender_email) == contact_email)
            | (func.lower(Email.recipient_emails).contains(contact_email)),
        )
        .group_by(Email.gmail_thread_id)
        .order_by(func.count().desc())
        .limit(allocations["threads"])
        .subquery()
    )

    thread_emails = (
        base_q.join(thread_subq, Email.gmail_thread_id == thread_subq.c.gmail_thread_id)
        .order_by(thread_subq.c.msg_count.desc(), Email.date.desc())
        .limit(allocations["threads"] * 2)  # Fetch extras, dedupe below
        .all()
    )
    _add_unique(sampled_ids, sampled_emails, thread_emails, allocations["threads"])

    # Category 4: High-sentiment emails (positive or negative)
    sentiment_emails = (
        base_q.join(EmailTag, EmailTag.email_id == Email.id)
        .filter(
            EmailTag.tag_category == "sentiment",
            EmailTag.tag.in_(["positive", "negative"]),
        )
        .order_by(Email.date.desc())
        .limit(allocations["sentiment"] * 2)
        .all()
    )
    _add_unique(sampled_ids, sampled_emails, sentiment_emails, allocations["sentiment"])

    # Category 5: Conflict-flagged (urgent sentiment, action items, negative tags)
    conflict_emails = (
        base_q.join(EmailTag, EmailTag.email_id == Email.id)
        .filter(
            ((EmailTag.tag_category == "sentiment") & (EmailTag.tag == "urgent"))
            | ((EmailTag.tag_category == "sentiment") & (EmailTag.tag == "negative"))
            | (EmailTag.tag_category == "action")
        )
        .order_by(Email.date.desc())
        .limit(allocations["conflict"] * 2)
        .all()
    )
    _add_unique(sampled_ids, sampled_emails, conflict_emails, allocations["conflict"])

    # Category 6: Random fill from remaining emails
    remaining_needed = max_samples - len(sampled_emails)
    if remaining_needed > 0:
        # Get IDs of all emails not yet sampled
        all_ids = [
            row[0]
            for row in db.query(Email.id)
            .filter(
                Email.user_id == user_id,
                (func.lower(Email.sender_email) == contact_email)
                | (func.lower(Email.recipient_emails).contains(contact_email)),
                Email.id.notin_(list(sampled_ids)) if sampled_ids else True,
            )
            .all()
        ]

        if all_ids:
            random_ids = random.sample(all_ids, min(remaining_needed, len(all_ids)))
            random_emails = (
                db.query(Email)
                .options(selectinload(Email.tags))
                .filter(Email.id.in_(random_ids))
                .all()
            )
            _add_unique(sampled_ids, sampled_emails, random_emails, remaining_needed)

    logger.info(
        f"Sampled {len(sampled_emails)} emails for {contact_email} "
        f"(from {total_available} total)"
    )

    # Sort chronologically for Claude
    sampled_emails.sort(key=lambda e: e.date)
    return sampled_emails


def _add_unique(
    seen_ids: set[UUID],
    result: list[Email],
    candidates: list[Email],
    limit: int,
) -> None:
    """Add up to `limit` unique emails from candidates to result."""
    added = 0
    for email in candidates:
        if added >= limit:
            break
        if email.id not in seen_ids:
            seen_ids.add(email.id)
            result.append(email)
            added += 1


def _compute_allocations(max_samples: int) -> dict[str, int]:
    """Compute proportional category allocations."""
    # Default for 25 samples
    if max_samples >= 25:
        return {
            "first": 3,
            "recent": 5,
            "threads": 4,
            "sentiment": 4,
            "conflict": 4,
            "random": 5,
        }

    # Scale proportionally for smaller samples
    total_fixed = 25
    ratios = {
        "first": 3 / total_fixed,
        "recent": 5 / total_fixed,
        "threads": 4 / total_fixed,
        "sentiment": 4 / total_fixed,
        "conflict": 4 / total_fixed,
        "random": 5 / total_fixed,
    }

    allocations = {}
    remaining = max_samples
    for key, ratio in ratios.items():
        alloc = max(1, round(ratio * max_samples))
        allocations[key] = min(alloc, remaining)
        remaining -= allocations[key]
        if remaining <= 0:
            break

    # Fill any unset categories
    for key in ratios:
        if key not in allocations:
            allocations[key] = 0

    return allocations
