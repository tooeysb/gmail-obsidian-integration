"""
Contact merging service for consolidating contacts across multiple Gmail accounts.

This service handles:
- Merging contacts by email address
- Combining account_sources arrays
- Aggregating email counts per account
- Resolving conflicts in names and phone numbers
"""

from collections import defaultdict
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from src.models.contact import Contact


def merge_contacts_by_email(
    contacts: list[dict[str, Any]], user_id: UUID, db_session: Session
) -> list[Contact]:
    """
    Merge contacts by email address and save to database.

    Groups contacts by email address, combines account sources, aggregates email counts,
    and handles conflicts in names and phone numbers.

    Args:
        contacts: List of contact dictionaries with keys:
            - email (str, required): Contact email address
            - name (str, optional): Contact display name
            - phone (str, optional): Contact phone number
            - account_source (str, required): Account label (e.g., 'procore-main')
            - email_count (int, optional): Number of emails from this contact in this account
            - last_contact_at (datetime, optional): Most recent email timestamp
        user_id: UUID of the user who owns these contacts
        db_session: SQLAlchemy database session

    Returns:
        List of merged Contact objects saved to database

    Edge cases handled:
        - Same email with different names: Uses most recent name
        - Missing names: Uses first non-empty name found
        - Phone number conflicts: Uses first non-empty phone found
        - Duplicate account sources: Deduplicates automatically
    """
    if not contacts:
        return []

    # Group contacts by email address
    contact_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for contact in contacts:
        email = contact.get("email", "").strip().lower()
        if not email:
            continue  # Skip contacts without email
        contact_groups[email].append(contact)

    # Merge each group
    merged_contacts = []
    for email, contact_list in contact_groups.items():
        merged = _merge_contact_group(contact_list)
        merged["email"] = email
        merged["user_id"] = user_id
        merged_contacts.append(merged)

    # Upsert contacts to database using PostgreSQL INSERT ... ON CONFLICT
    if not merged_contacts:
        return []

    # Build insert statement with ON CONFLICT UPDATE
    stmt = insert(Contact).values(merged_contacts)
    stmt = stmt.on_conflict_do_update(
        index_elements=["user_id", "email"],
        set_={
            "name": stmt.excluded.name,
            "phone": stmt.excluded.phone,
            "account_sources": stmt.excluded.account_sources,
            "email_count": stmt.excluded.email_count,
            "last_contact_at": stmt.excluded.last_contact_at,
            "updated_at": stmt.excluded.updated_at,
        },
    )

    # Execute upsert
    db_session.execute(stmt)
    db_session.commit()

    # Fetch and return the saved contacts
    result = db_session.execute(
        select(Contact).where(
            Contact.user_id == user_id,
            Contact.email.in_([c["email"] for c in merged_contacts]),
        )
    )
    return list(result.scalars().all())


def _merge_contact_group(contact_list: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Merge a list of contacts with the same email address.

    Args:
        contact_list: List of contact dictionaries for the same email

    Returns:
        Single merged contact dictionary
    """
    # Collect all unique account sources
    account_sources = []
    seen_sources = set()
    for contact in contact_list:
        source = contact.get("account_source")
        if source and source not in seen_sources:
            account_sources.append(source)
            seen_sources.add(source)

    # Aggregate email counts per account
    total_email_count = sum(contact.get("email_count", 0) for contact in contact_list)

    # Find most recent name (prefer non-empty, most recent contact)
    name = _resolve_name(contact_list)

    # Find first non-empty phone number
    phone = _resolve_phone(contact_list)

    # Find most recent last_contact_at timestamp
    last_contact_at = _resolve_last_contact(contact_list)

    return {
        "name": name,
        "phone": phone,
        "account_sources": account_sources,
        "email_count": total_email_count,
        "last_contact_at": last_contact_at,
        "updated_at": datetime.utcnow(),
    }


def _resolve_name(contact_list: list[dict[str, Any]]) -> str | None:
    """
    Resolve name conflicts by preferring most recent non-empty name.

    Args:
        contact_list: List of contacts with the same email

    Returns:
        Best name to use, or None if all names are empty
    """
    # Sort by last_contact_at descending (most recent first)
    sorted_contacts = sorted(
        contact_list,
        key=lambda c: c.get("last_contact_at") or datetime.min,
        reverse=True,
    )

    # Return first non-empty name
    for contact in sorted_contacts:
        name = contact.get("name")
        if name and name.strip():
            return name.strip()

    return None


def _resolve_phone(contact_list: list[dict[str, Any]]) -> str | None:
    """
    Resolve phone number conflicts by using first non-empty phone.

    Args:
        contact_list: List of contacts with the same email

    Returns:
        First non-empty phone number, or None if all are empty
    """
    for contact in contact_list:
        phone = contact.get("phone")
        if phone and phone.strip():
            return phone.strip()

    return None


def _resolve_last_contact(contact_list: list[dict[str, Any]]) -> datetime | None:
    """
    Find the most recent last_contact_at timestamp.

    Args:
        contact_list: List of contacts with the same email

    Returns:
        Most recent timestamp, or None if all are None
    """
    timestamps = [c.get("last_contact_at") for c in contact_list if c.get("last_contact_at")]

    if not timestamps:
        return None

    return max(timestamps)
