"""
Contact discovery from existing email data.

Uses SQL queries to find bidirectional contacts and compute communication metrics.
No external API calls needed - works entirely from the emails table.
"""

import logging
import re
from typing import Any
from uuid import UUID

from sqlalchemy import func, text
from sqlalchemy.orm import Session

from src.models.account import GmailAccount
from src.models.email import Email, EmailTag

logger = logging.getLogger(__name__)

# Automated/noreply addresses to exclude
NOREPLY_PATTERNS = [
    r"^no[-_.]?reply@",
    r"^noreply@",
    r"^do[-_.]?not[-_.]?reply@",
    r"^notifications?@",
    r"^mailer[-_.]?daemon@",
    r"^postmaster@",
    r"^bounce[s]?@",
    r"^auto[-_.]?reply@",
    r"^daemon@",
    r"^newsletter@",
    r"^info@.*\.noreply\.",
    r"^.*@noreply\.",
    r"^.*noreply.*@",
    r"^alerts?@",
    r"^digest@",
    r"^unsubscribe@",
    r"^feedback@.*\.google\.com$",
    r"^calendar[-_.]?notify@",
]

_NOREPLY_COMPILED = [re.compile(p, re.IGNORECASE) for p in NOREPLY_PATTERNS]

# Known user email addresses (these are the sender for outgoing emails)
USER_EMAILS = {
    "tooey@procore.com",
    "2e@procore.com",
    "tooey@hth-corp.com",
}


def _is_noreply(email: str) -> bool:
    """Check if an email address matches automated/noreply patterns."""
    email = email.strip().lower()
    return any(p.match(email) for p in _NOREPLY_COMPILED)


def _is_user_email(email: str) -> bool:
    """Check if an email belongs to the user's own accounts."""
    return email.strip().lower() in USER_EMAILS


def _classify_relationship(email_addr: str, tags: dict[str, int]) -> str:
    """
    Classify relationship type from domain heuristics and existing tags.

    Args:
        email_addr: Contact email address.
        tags: Counter of relationship tags from email_tags for this contact.

    Returns:
        Relationship type string.
    """
    # Check existing AI-generated relationship tags first
    if tags:
        most_common = max(tags, key=tags.get)
        if tags[most_common] >= 2:  # At least 2 emails tagged with this type
            return most_common

    # Domain-based heuristics
    domain = email_addr.split("@")[-1].lower()

    if domain == "procore.com":
        return "coworker"

    personal_domains = {
        "gmail.com",
        "yahoo.com",
        "hotmail.com",
        "outlook.com",
        "icloud.com",
        "aol.com",
        "protonmail.com",
        "me.com",
        "live.com",
        "msn.com",
        "comcast.net",
        "att.net",
        "verizon.net",
        "sbcglobal.net",
        "cox.net",
        "charter.net",
    }
    if domain in personal_domains:
        return "personal"

    return "unknown"


def discover_contacts(user_id: UUID, db: Session) -> list[dict[str, Any]]:
    """
    Discover contacts with bidirectional communication.

    Uses bulk SQL queries to avoid N+1 problems. Total: ~5 queries regardless
    of contact count.

    Args:
        user_id: User UUID to discover contacts for.
        db: SQLAlchemy session.

    Returns:
        List of contact dictionaries with metrics.
    """
    logger.info(f"Discovering contacts for user {user_id}")

    # Step 1: Find all unique sender emails (people who emailed the user)
    received_query = (
        db.query(
            func.lower(func.trim(Email.sender_email)).label("email"),
            func.count().label("received_count"),
            func.min(Email.date).label("first_received"),
            func.max(Email.date).label("last_received"),
        )
        .filter(Email.user_id == user_id)
        .group_by(func.lower(func.trim(Email.sender_email)))
    )
    received_map: dict[str, dict] = {}
    for row in received_query.all():
        email = row.email
        if not _is_user_email(email) and not _is_noreply(email):
            received_map[email] = {
                "received_count": row.received_count,
                "first_received": row.first_received,
                "last_received": row.last_received,
            }

    logger.info(f"Found {len(received_map)} unique sender addresses")

    # Step 2: Find all addresses the user sent emails to
    # recipient_emails is a comma-separated field, parse in Python
    sent_emails_raw = (
        db.query(Email.recipient_emails)
        .filter(Email.user_id == user_id)
        .filter(Email.sender_email.in_(list(USER_EMAILS)))
        .all()
    )

    sent_map: dict[str, int] = {}
    for (recipients,) in sent_emails_raw:
        if not recipients:
            continue
        for addr in recipients.split(","):
            addr = addr.strip().lower()
            if "<" in addr and ">" in addr:
                addr = addr[addr.index("<") + 1 : addr.index(">")].strip()
            if addr and not _is_user_email(addr) and not _is_noreply(addr):
                sent_map[addr] = sent_map.get(addr, 0) + 1

    logger.info(f"Found {len(sent_map)} unique recipient addresses")

    # Step 3: Find bidirectional contacts (in both sets)
    bidirectional = set(received_map.keys()) & set(sent_map.keys())

    # Also include addresses user sent to 2+ times (replied-to unknowns)
    replied_to = {addr for addr, count in sent_map.items() if count >= 2}
    contacts_to_profile = bidirectional | replied_to

    logger.info(
        f"Bidirectional contacts: {len(bidirectional)}, "
        f"replied-to additions: {len(replied_to - bidirectional)}, "
        f"total: {len(contacts_to_profile)}"
    )

    # Step 4: Bulk fetch relationship tags
    relationship_tags = _get_relationship_tags(user_id, contacts_to_profile, db)

    # Step 5: Bulk fetch display names (most common sender_name per email)
    logger.info("Bulk fetching display names...")
    name_map = _bulk_get_names(user_id, contacts_to_profile, db)

    # Step 6: Bulk fetch thread counts + date ranges using sender_email only
    # (Checking recipient_emails with LIKE per-contact is too slow for bulk)
    logger.info("Bulk fetching thread stats from sender emails...")
    thread_stats_map = _bulk_get_thread_stats(user_id, contacts_to_profile, db)

    # Step 7: Bulk fetch account sources
    logger.info("Bulk fetching account sources...")
    account_map = _bulk_get_accounts(user_id, contacts_to_profile, db)

    # Step 8: Assemble contacts
    logger.info("Assembling contact records...")
    contacts = []
    for email_addr in contacts_to_profile:
        recv = received_map.get(email_addr, {})
        sent_count = sent_map.get(email_addr, 0)
        received_count = recv.get("received_count", 0)

        ts = thread_stats_map.get(email_addr, {})
        tags_for_contact = relationship_tags.get(email_addr, {})

        # Use received dates if thread stats don't have them (sender-only query)
        first_date = ts.get("first_date") or recv.get("first_received")
        last_date = ts.get("last_date") or recv.get("last_received")

        contact = {
            "contact_email": email_addr,
            "contact_name": name_map.get(email_addr),
            "relationship_type": _classify_relationship(email_addr, tags_for_contact),
            "account_sources": sorted(account_map.get(email_addr, [])),
            "total_email_count": sent_count + received_count,
            "sent_count": sent_count,
            "received_count": received_count,
            "first_exchange_date": first_date,
            "last_exchange_date": last_date,
            "thread_count": ts.get("thread_count", 0),
        }
        contacts.append(contact)

    # Sort by total email count descending
    contacts.sort(key=lambda c: c["total_email_count"], reverse=True)

    logger.info(f"Contact discovery complete: {len(contacts)} contacts found")
    return contacts


def _bulk_get_names(
    user_id: UUID, contact_emails: set[str], db: Session
) -> dict[str, str]:
    """Bulk fetch most common display name per contact. Single query."""
    if not contact_emails:
        return {}

    # Get all (email, name, count) rows in one query, then pick best per email in Python
    results = (
        db.query(
            func.lower(func.trim(Email.sender_email)).label("email"),
            Email.sender_name,
            func.count().label("cnt"),
        )
        .filter(
            Email.user_id == user_id,
            func.lower(func.trim(Email.sender_email)).in_(contact_emails),
            Email.sender_name.isnot(None),
            Email.sender_name != "",
        )
        .group_by(func.lower(func.trim(Email.sender_email)), Email.sender_name)
        .all()
    )

    # Pick name with highest count per email
    best: dict[str, tuple[str, int]] = {}
    for row in results:
        email = row.email
        if email not in best or row.cnt > best[email][1]:
            best[email] = (row.sender_name, row.cnt)

    return {email: name for email, (name, _) in best.items()}


def _bulk_get_thread_stats(
    user_id: UUID, contact_emails: set[str], db: Session
) -> dict[str, dict]:
    """
    Bulk fetch thread count and date range per contact.

    Uses sender_email only (not recipient LIKE) for performance.
    This means it only counts threads where the contact was the sender,
    but it runs as a single query across all contacts.
    """
    if not contact_emails:
        return {}

    results = (
        db.query(
            func.lower(func.trim(Email.sender_email)).label("email"),
            func.count(func.distinct(Email.gmail_thread_id)).label("thread_count"),
            func.min(Email.date).label("first_date"),
            func.max(Email.date).label("last_date"),
        )
        .filter(
            Email.user_id == user_id,
            func.lower(func.trim(Email.sender_email)).in_(contact_emails),
        )
        .group_by(func.lower(func.trim(Email.sender_email)))
        .all()
    )

    return {
        row.email: {
            "thread_count": row.thread_count,
            "first_date": row.first_date,
            "last_date": row.last_date,
        }
        for row in results
    }


def _bulk_get_accounts(
    user_id: UUID, contact_emails: set[str], db: Session
) -> dict[str, list[str]]:
    """Bulk fetch account labels per contact. Two queries total."""
    if not contact_emails:
        return {}

    # Get distinct (email, account_id) pairs
    results = (
        db.query(
            func.lower(func.trim(Email.sender_email)).label("email"),
            Email.account_id,
        )
        .filter(
            Email.user_id == user_id,
            func.lower(func.trim(Email.sender_email)).in_(contact_emails),
        )
        .distinct()
        .all()
    )

    # Collect unique account IDs
    email_to_account_ids: dict[str, set] = {}
    all_account_ids: set = set()
    for row in results:
        email_to_account_ids.setdefault(row.email, set()).add(row.account_id)
        all_account_ids.add(row.account_id)

    # Map account IDs to labels in one query
    if not all_account_ids:
        return {}

    label_results = (
        db.query(GmailAccount.id, GmailAccount.account_label)
        .filter(GmailAccount.id.in_(list(all_account_ids)))
        .all()
    )
    id_to_label = {row.id: row.account_label for row in label_results}

    return {
        email: [id_to_label[aid] for aid in aids if aid in id_to_label]
        for email, aids in email_to_account_ids.items()
    }


def _get_relationship_tags(
    user_id: UUID, contact_emails: set[str], db: Session
) -> dict[str, dict[str, int]]:
    """
    Get relationship tag counts per contact email from email_tags. Single query.

    Returns:
        Dict mapping contact_email -> {tag_value: count}
    """
    if not contact_emails:
        return {}

    results = (
        db.query(
            func.lower(func.trim(Email.sender_email)).label("email"),
            EmailTag.tag,
            func.count().label("cnt"),
        )
        .join(EmailTag, EmailTag.email_id == Email.id)
        .filter(
            Email.user_id == user_id,
            EmailTag.tag_category == "relationship",
            func.lower(func.trim(Email.sender_email)).in_(contact_emails),
        )
        .group_by(func.lower(func.trim(Email.sender_email)), EmailTag.tag)
        .all()
    )

    tags: dict[str, dict[str, int]] = {}
    for row in results:
        if row.email not in tags:
            tags[row.email] = {}
        tags[row.email][row.tag] = row.cnt

    return tags
