"""
CRM API routes for contact and company management.
"""

import re
import uuid
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, or_, text
from sqlalchemy.orm import Session, selectinload

from src.api.middleware.auth import get_current_user
from src.core.database import get_sync_db
from src.core.utils import serialize_dt
from src.models.company import Company
from src.models.contact import Contact
from src.models.email import Email
from src.models.email_participant import EmailParticipant
from src.models.relationship_profile import RelationshipProfile
from src.models.user import User

router = APIRouter()

# Allowed sort columns to prevent attribute enumeration via getattr
CONTACT_SORTABLE_COLUMNS: frozenset[str] = frozenset(
    {
        "email_count",
        "name",
        "email",
        "last_contact_at",
        "created_at",
        "updated_at",
        "title",
        "contact_type",
        "is_vip",
    }
)

COMPANY_SORTABLE_COLUMNS: frozenset[str] = frozenset(
    {
        "arr",
        "name",
        "domain",
        "created_at",
        "updated_at",
        "account_tier",
        "industry",
        "company_type",
        "revenue_segment",
    }
)


# ---------------------------------------------------------------------------
# Pydantic response schemas
# ---------------------------------------------------------------------------


class ContactSummary(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: Optional[str] = None
    email: str
    phone: Optional[str] = None
    title: Optional[str] = None
    contact_type: Optional[str] = None
    is_vip: bool = False
    email_count: int = 0
    tags: list[str] = []
    relationship_context: Optional[str] = None
    company_id: Optional[str] = None
    company_name: Optional[str] = None
    last_contact_at: Optional[str] = None
    created_at: Optional[str] = None


class CompanySummary(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    domain: Optional[str] = None
    industry: Optional[str] = None
    company_type: Optional[str] = None
    account_tier: Optional[str] = None
    arr: Optional[float] = None
    revenue_segment: Optional[str] = None
    billing_state: Optional[str] = None
    contact_count: int = 0
    created_at: Optional[str] = None


class EmailSummary(BaseModel):
    id: str
    subject: Optional[str] = None
    date: str
    sender_name: Optional[str] = None
    sender_email: str
    summary: Optional[str] = None
    has_attachments: bool = False
    direction: Optional[str] = None


class RelationshipProfileSummary(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    relationship_type: str
    total_email_count: int = 0
    sent_count: int = 0
    received_count: int = 0
    first_exchange_date: Optional[str] = None
    last_exchange_date: Optional[str] = None
    thread_count: int = 0
    avg_response_time_hours: Optional[float] = None
    profile_data: Optional[dict] = None
    profiled_at: Optional[str] = None


class ContactUpdateRequest(BaseModel):
    name: Optional[str] = None
    title: Optional[str] = None
    phone: Optional[str] = None
    contact_type: Optional[str] = None
    is_vip: Optional[bool] = None
    tags: Optional[list[str]] = None
    notes: Optional[str] = None
    relationship_context: Optional[str] = None
    company_id: Optional[str] = None
    personal_email: Optional[str] = None


class CompanyUpdateRequest(BaseModel):
    notes: Optional[str] = None
    company_type: Optional[str] = None
    account_tier: Optional[str] = None
    industry: Optional[str] = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


# _get_user kept for backward compat with tests; prefer Depends(get_current_user)
def _get_user(db: Session) -> User:
    user = db.query(User).first()
    if not user:
        raise HTTPException(status_code=404, detail="No user found")
    return user


# Re-export for test compatibility
_serialize_dt = serialize_dt


def _serialize_email_dict(email: Email, direction: str | None = None) -> dict:
    """Serialize an Email model to a response dict."""
    result = {
        "id": str(email.id),
        "subject": email.subject,
        "date": serialize_dt(email.date),
        "sender_name": email.sender_name,
        "sender_email": email.sender_email,
        "summary": email.summary,
        "has_attachments": email.has_attachments,
        "body": email.body,
    }
    if direction is not None:
        result["direction"] = direction
    return result


def _paginated_response(total: int, page: int, page_size: int, items: list) -> dict:
    """Build a standard paginated response envelope."""
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size,
        "items": items,
    }


def _serialize_contact(contact: Contact, company_name: str | None = None) -> dict:
    return {
        "id": str(contact.id),
        "name": contact.name,
        "email": contact.email,
        "phone": contact.phone,
        "title": contact.title,
        "contact_type": contact.contact_type,
        "is_vip": contact.is_vip,
        "email_count": contact.email_count,
        "tags": contact.tags or [],
        "relationship_context": contact.relationship_context,
        "company_id": str(contact.company_id) if contact.company_id else None,
        "company_name": company_name,
        "company": (
            {"id": str(contact.company_id), "name": company_name}
            if contact.company_id and company_name
            else None
        ),
        "last_contact_at": serialize_dt(contact.last_contact_at),
        "notes": contact.notes,
        "personal_email": contact.personal_email,
        "account_sources": contact.account_sources or [],
        "salesforce_id": contact.salesforce_id,
        "address": contact.address,
        "created_at": serialize_dt(contact.created_at),
        "updated_at": serialize_dt(contact.updated_at),
    }


def _serialize_company(company: Company, contact_count: int = 0) -> dict:
    return {
        "id": str(company.id),
        "name": company.name,
        "domain": company.domain,
        "aliases": company.aliases,
        "industry": company.industry,
        "company_type": company.company_type,
        "billing_state": company.billing_state,
        "arr": float(company.arr) if company.arr is not None else None,
        "revenue_segment": company.revenue_segment,
        "account_tier": company.account_tier,
        "salesforce_id": company.salesforce_id,
        "renewal_date": company.renewal_date.isoformat() if company.renewal_date else None,
        "account_owner": company.account_owner,
        "csm": company.csm,
        "notes": company.notes,
        "contact_count": contact_count,
        "source_data": company.source_data,
        "created_at": serialize_dt(company.created_at),
        "updated_at": serialize_dt(company.updated_at),
    }


# ---------------------------------------------------------------------------
# GET /dashboard
# ---------------------------------------------------------------------------


@router.get("/dashboard")
def crm_dashboard(user: User = Depends(get_current_user), db: Session = Depends(get_sync_db)):
    """CRM dashboard with aggregate statistics."""
    uid = user.id

    total_contacts = db.query(func.count(Contact.id)).filter(Contact.user_id == uid).scalar() or 0
    total_companies = db.query(func.count(Company.id)).filter(Company.user_id == uid).scalar() or 0
    total_emails = db.query(func.count(Email.id)).filter(Email.user_id == uid).scalar() or 0
    vip_count = (
        db.query(func.count(Contact.id))
        .filter(Contact.user_id == uid, Contact.is_vip == True)
        .scalar()
        or 0
    )

    # Top 10 contacts by email_count
    top_contacts_q = (
        db.query(Contact)
        .options(selectinload(Contact.company))
        .filter(Contact.user_id == uid)
        .order_by(Contact.email_count.desc())
        .limit(10)
        .all()
    )
    top_contacts = [
        _serialize_contact(c, c.company.name if c.company else None) for c in top_contacts_q
    ]

    # Recent 20 emails
    recent_emails_q = (
        db.query(Email).filter(Email.user_id == uid).order_by(Email.date.desc()).limit(20).all()
    )
    recent_emails = [_serialize_email_dict(e) for e in recent_emails_q]

    # Email volume by month (last 12 months)
    volume_q = (
        db.query(
            func.date_trunc("month", Email.date).label("month"),
            func.count(Email.id).label("count"),
        )
        .filter(Email.user_id == uid)
        .group_by(func.date_trunc("month", Email.date))
        .order_by(func.date_trunc("month", Email.date).desc())
        .limit(12)
        .all()
    )
    email_volume_by_month = [
        {"month": row.month.isoformat() if row.month else None, "count": row.count}
        for row in volume_q
    ]

    return {
        "total_contacts": total_contacts,
        "total_companies": total_companies,
        "total_emails": total_emails,
        "vip_count": vip_count,
        "top_contacts": top_contacts,
        "recent_emails": recent_emails,
        "email_volume_by_month": email_volume_by_month,
    }


# ---------------------------------------------------------------------------
# GET /contacts
# ---------------------------------------------------------------------------


@router.get("/contacts")
def list_contacts(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    search: Optional[str] = Query(None),
    sort_by: str = Query("email_count"),
    sort_dir: str = Query("desc"),
    is_vip: Optional[bool] = Query(None),
    contact_type: Optional[str] = Query(None),
    tags: Optional[str] = Query(None, description="Comma-separated tags"),
    company_id: Optional[str] = Query(None),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_sync_db),
):
    """Paginated contact list with search and filtering."""
    uid = user.id

    query = (
        db.query(Contact)
        .options(selectinload(Contact.company))
        .outerjoin(Company, Contact.company_id == Company.id)
        .filter(Contact.user_id == uid)
    )

    # Filters
    if search:
        pattern = f"%{search}%"
        query = query.filter(
            or_(
                Contact.name.ilike(pattern),
                Contact.email.ilike(pattern),
                Company.name.ilike(pattern),
            )
        )

    if is_vip is not None:
        query = query.filter(Contact.is_vip == is_vip)

    if contact_type:
        query = query.filter(Contact.contact_type == contact_type)

    if tags:
        tag_list = [t.strip() for t in tags.split(",") if t.strip()]
        for tag in tag_list:
            query = query.filter(Contact.tags.any(tag))

    if company_id:
        query = query.filter(Contact.company_id == company_id)

    # Count before pagination
    total = query.count()

    # Sorting (whitelist to prevent attribute enumeration)
    if sort_by not in CONTACT_SORTABLE_COLUMNS:
        raise HTTPException(status_code=400, detail=f"Invalid sort column: {sort_by}")
    sort_column = getattr(Contact, sort_by)
    if sort_dir.lower() == "asc":
        query = query.order_by(sort_column.asc())
    else:
        query = query.order_by(sort_column.desc())

    # Pagination
    offset = (page - 1) * page_size
    contacts = query.offset(offset).limit(page_size).all()

    return _paginated_response(
        total,
        page,
        page_size,
        [_serialize_contact(c, c.company.name if c.company else None) for c in contacts],
    )


# ---------------------------------------------------------------------------
# GET /contacts/{id}
# ---------------------------------------------------------------------------


@router.get("/contacts/{contact_id}")
def get_contact(
    contact_id: str,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_sync_db),
):
    """Get full contact detail with relationship profile and recent emails."""
    uid = user.id

    contact = (
        db.query(Contact)
        .options(selectinload(Contact.company))
        .filter(Contact.id == contact_id, Contact.user_id == uid)
        .first()
    )
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")

    # Relationship profile
    rel_profile = (
        db.query(RelationshipProfile)
        .filter(
            RelationshipProfile.user_id == uid,
            RelationshipProfile.contact_email == contact.email,
        )
        .first()
    )

    rel_data = None
    if rel_profile:
        rel_data = {
            "id": str(rel_profile.id),
            "relationship_type": rel_profile.relationship_type,
            "total_email_count": rel_profile.total_email_count,
            "sent_count": rel_profile.sent_count,
            "received_count": rel_profile.received_count,
            "first_exchange_date": _serialize_dt(rel_profile.first_exchange_date),
            "last_exchange_date": _serialize_dt(rel_profile.last_exchange_date),
            "thread_count": rel_profile.thread_count,
            "avg_response_time_hours": rel_profile.avg_response_time_hours,
            "profile_data": rel_profile.profile_data,
            "profiled_at": _serialize_dt(rel_profile.profiled_at),
        }

    # Recent emails via EmailParticipant (last 10)
    recent_participants = (
        db.query(EmailParticipant)
        .options(selectinload(EmailParticipant.email))
        .filter(EmailParticipant.contact_id == contact.id)
        .order_by(EmailParticipant.created_at.desc())
        .limit(10)
        .all()
    )

    recent_emails = []
    for ep in recent_participants:
        e = ep.email
        if e is None:
            continue
        direction = "received" if ep.role == "sender" else "sent"
        recent_emails.append(_serialize_email_dict(e, direction=direction))

    # Sort recent emails by date descending
    recent_emails.sort(key=lambda x: x["date"] or "", reverse=True)
    recent_emails = recent_emails[:10]

    # Email stats
    total_emails_for_contact = (
        db.query(func.count(EmailParticipant.id))
        .filter(EmailParticipant.contact_id == contact.id)
        .scalar()
        or 0
    )
    sent_count = (
        db.query(func.count(EmailParticipant.id))
        .filter(EmailParticipant.contact_id == contact.id, EmailParticipant.role == "sender")
        .scalar()
        or 0
    )
    received_count = total_emails_for_contact - sent_count

    # Email count by year
    by_year_q = (
        db.query(
            func.extract("year", Email.date).label("year"),
            func.count(Email.id).label("count"),
        )
        .join(EmailParticipant, EmailParticipant.email_id == Email.id)
        .filter(EmailParticipant.contact_id == contact.id)
        .group_by(func.extract("year", Email.date))
        .order_by(func.extract("year", Email.date).desc())
        .all()
    )
    by_year = {str(int(row.year)): row.count for row in by_year_q if row.year}

    email_stats = {
        "total": total_emails_for_contact,
        "sent": sent_count,
        "received": received_count,
        "by_year": by_year,
    }

    contact_data = _serialize_contact(contact, contact.company.name if contact.company else None)

    return {
        "contact": contact_data,
        "relationship_profile": rel_data,
        "recent_emails": recent_emails,
        "email_stats": email_stats,
    }


# ---------------------------------------------------------------------------
# PATCH /contacts/{id}
# ---------------------------------------------------------------------------


@router.patch("/contacts/{contact_id}")
def update_contact(
    contact_id: str,
    body: ContactUpdateRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_sync_db),
):
    """Update editable contact fields."""
    uid = user.id

    contact = (
        db.query(Contact)
        .options(selectinload(Contact.company))
        .filter(Contact.id == contact_id, Contact.user_id == uid)
        .first()
    )
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")

    update_data = body.model_dump(exclude_unset=True)

    # Convert company_id string to UUID if provided
    if "company_id" in update_data:
        val = update_data["company_id"]
        if val is not None:
            # Verify company exists and belongs to user
            company = db.query(Company).filter(Company.id == val, Company.user_id == uid).first()
            if not company:
                raise HTTPException(status_code=404, detail="Company not found")

    allowed_fields = set(ContactUpdateRequest.model_fields.keys())
    for field, value in update_data.items():
        if field not in allowed_fields:
            raise HTTPException(status_code=400, detail=f"Field '{field}' is not updatable")
        setattr(contact, field, value)

    db.commit()
    db.refresh(contact)

    return _serialize_contact(contact, contact.company.name if contact.company else None)


# ---------------------------------------------------------------------------
# GET /contacts/{id}/emails
# ---------------------------------------------------------------------------


@router.get("/contacts/{contact_id}/emails")
def list_contact_emails(
    contact_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_sync_db),
):
    """Paginated emails for a specific contact via EmailParticipant junction."""
    uid = user.id

    contact = db.query(Contact).filter(Contact.id == contact_id, Contact.user_id == uid).first()
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")

    base_query = (
        db.query(EmailParticipant)
        .options(selectinload(EmailParticipant.email))
        .join(Email, EmailParticipant.email_id == Email.id)
        .filter(EmailParticipant.contact_id == contact.id)
    )

    total = base_query.count()
    offset = (page - 1) * page_size

    participants = base_query.order_by(Email.date.desc()).offset(offset).limit(page_size).all()

    items = []
    for ep in participants:
        e = ep.email
        if e is None:
            continue
        direction = "received" if ep.role == "sender" else "sent"
        items.append(_serialize_email_dict(e, direction=direction))

    return _paginated_response(total, page, page_size, items)


# ---------------------------------------------------------------------------
# GET /companies
# ---------------------------------------------------------------------------


@router.get("/companies")
def list_companies(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    search: Optional[str] = Query(None),
    sort_by: str = Query("arr"),
    sort_dir: str = Query("desc"),
    company_type: Optional[str] = Query(None),
    account_tier: Optional[str] = Query(None),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_sync_db),
):
    """Paginated company list with search and filtering."""
    uid = user.id

    # Subquery for contact_count
    contact_count_sq = (
        db.query(
            Contact.company_id,
            func.count(Contact.id).label("contact_count"),
        )
        .filter(Contact.user_id == uid, Contact.company_id.isnot(None))
        .group_by(Contact.company_id)
        .subquery()
    )

    query = (
        db.query(Company, func.coalesce(contact_count_sq.c.contact_count, 0).label("contact_count"))
        .outerjoin(contact_count_sq, Company.id == contact_count_sq.c.company_id)
        .filter(Company.user_id == uid)
    )

    if search:
        pattern = f"%{search}%"
        query = query.filter(
            or_(
                Company.name.ilike(pattern),
                Company.domain.ilike(pattern),
            )
        )

    if company_type:
        query = query.filter(Company.company_type == company_type)

    if account_tier:
        query = query.filter(Company.account_tier == account_tier)

    total = query.count()

    # Sorting (whitelist to prevent attribute enumeration)
    if sort_by == "contact_count":
        sort_col = func.coalesce(contact_count_sq.c.contact_count, 0)
    elif sort_by not in COMPANY_SORTABLE_COLUMNS:
        raise HTTPException(status_code=400, detail=f"Invalid sort column: {sort_by}")
    else:
        sort_col = getattr(Company, sort_by)
    if sort_dir.lower() == "asc":
        query = query.order_by(sort_col.asc().nullslast())
    else:
        query = query.order_by(sort_col.desc().nullslast())

    offset = (page - 1) * page_size
    results = query.offset(offset).limit(page_size).all()

    items = [_serialize_company(company, cc) for company, cc in results]

    return _paginated_response(total, page, page_size, items)


# ---------------------------------------------------------------------------
# GET /companies/{id}
# ---------------------------------------------------------------------------


@router.get("/companies/{company_id}")
def get_company(
    company_id: str,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_sync_db),
):
    """Get full company detail with contacts and email summary."""
    uid = user.id

    company = db.query(Company).filter(Company.id == company_id, Company.user_id == uid).first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    # Contacts at this company
    contacts = (
        db.query(Contact)
        .filter(Contact.user_id == uid, Contact.company_id == company.id)
        .order_by(Contact.email_count.desc())
        .all()
    )

    contacts_data = [_serialize_contact(c, company.name) for c in contacts]

    # Email summary across all contacts at company
    contact_ids = [c.id for c in contacts]
    if contact_ids:
        total_emails = (
            db.query(func.count(func.distinct(EmailParticipant.email_id)))
            .filter(EmailParticipant.contact_id.in_(contact_ids))
            .scalar()
            or 0
        )

        date_range = (
            db.query(func.min(Email.date), func.max(Email.date))
            .join(EmailParticipant, EmailParticipant.email_id == Email.id)
            .filter(EmailParticipant.contact_id.in_(contact_ids))
            .first()
        )

        first_email = _serialize_dt(date_range[0]) if date_range and date_range[0] else None
        last_email = _serialize_dt(date_range[1]) if date_range and date_range[1] else None
    else:
        total_emails = 0
        first_email = None
        last_email = None

    email_summary = {
        "total_emails": total_emails,
        "unique_contacts": len(contacts),
        "first_email_date": first_email,
        "last_email_date": last_email,
    }

    company_data = _serialize_company(company, len(contacts))

    return {
        "company": company_data,
        "contacts": contacts_data,
        "email_summary": email_summary,
    }


# ---------------------------------------------------------------------------
# GET /companies/{id}/discovered-contacts
# ---------------------------------------------------------------------------


def _enrich_with_haiku(
    signatures: dict[str, tuple[str, str]], company_name: str
) -> dict[str, dict]:
    """Use Claude Haiku to extract names, job titles, and LinkedIn URLs from email signatures.

    Args:
        signatures: {email: (sender_name, body_text)} for each person
        company_name: the company name for context

    Returns:
        {email: {"name": str, "title": str, "linkedin_url": str}} — values are null if not found
    """
    import json
    import logging

    from anthropic import Anthropic

    from src.core.config import settings

    if not signatures:
        return {}

    logger = logging.getLogger(__name__)

    # Build the prompt with all signatures (first 30 lines of each body)
    sig_entries = []
    for email, (name, body) in signatures.items():
        lines = body.strip().split("\n")
        snippet = "\n".join(lines[:30])
        sig_entries.append(f"[{email}] Sender header name: {name}\n{snippet}")

    combined = "\n---\n".join(sig_entries)

    try:
        client = Anthropic(api_key=settings.anthropic_api_key)
        message = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=2048,
            system=[
                {
                    "type": "text",
                    "text": (
                        "Extract information from email signature blocks. "
                        f"Company: {company_name}.\n\n"
                        "For each person, return:\n"
                        '- "name": their proper display name in "First Last" format '
                        "(the sender header may be in Last, First format — normalize it)\n"
                        '- "title": their job title from the signature (null if not found)\n'
                        '- "linkedin_url": their personal LinkedIn profile URL if present '
                        "in the signature (null if not found — do NOT guess)\n\n"
                        "Return ONLY valid JSON:\n"
                        '{"email@example.com": {"name": "...", "title": "...", '
                        '"linkedin_url": "..."}, ...}\n\n'
                        "Only extract what is explicitly stated. Do not fabricate titles "
                        "or LinkedIn URLs."
                    ),
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[{"role": "user", "content": combined}],
        )

        raw = message.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
            if raw.endswith("```"):
                raw = raw[:-3]
            raw = raw.strip()

        result = json.loads(raw)
        return {k.lower(): v for k, v in result.items() if isinstance(v, dict)}
    except Exception:
        logger.warning("Haiku enrichment failed, skipping", exc_info=True)
        return {}


def _build_linkedin_url(name: str | None, company_name: str) -> str | None:
    """Build a LinkedIn search URL for a person at a company."""
    if not name:
        return None
    from urllib.parse import quote

    # Clean name: strip "Last, First - (Region)" format
    clean = name.split(" - ")[0].strip()
    # Handle "Last, First" format
    if "," in clean:
        parts = clean.split(",", 1)
        clean = parts[1].strip() + " " + parts[0].strip()
    # Strip middle initials (single letter followed by optional period)
    clean = re.sub(r"\b[A-Z]\.\s*", "", clean).strip()
    # Clean company name
    co = company_name.split(" - ")[0].strip()
    for suffix in [" Inc.", " Inc", " LLC", " Ltd", " Corp.", " Corp", " Co."]:
        if co.endswith(suffix):
            co = co[: -len(suffix)].strip()
    query = quote(f"{clean} {co}")
    return f"https://www.linkedin.com/search/results/people/?keywords={query}"


@router.get("/companies/{company_id}/discovered-contacts")
def get_discovered_contacts(
    company_id: str,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_sync_db),
):
    """
    Find email addresses matching a company's domain from all emails,
    excluding people already in the CRM as contacts.
    Searches sender_email and recipient_emails fields.
    """
    uid = user.id

    company = db.query(Company).filter(Company.id == company_id, Company.user_id == uid).first()
    if not company or not company.domain:
        return {"discovered": [], "domain": None}

    domain = company.domain.lower().strip()

    # Get existing contact emails at this company to exclude
    existing_emails = set()
    existing_contacts = (
        db.query(Contact.email)
        .filter(Contact.user_id == uid, Contact.company_id == company.id)
        .all()
    )
    for (email,) in existing_contacts:
        existing_emails.add(email.lower())

    # Also get all contacts with this domain (might be assigned to different company)
    all_domain_contacts = (
        db.query(Contact.email)
        .filter(Contact.user_id == uid, Contact.email.ilike(f"%@{domain}"))
        .all()
    )
    for (email,) in all_domain_contacts:
        existing_emails.add(email.lower())

    # Search sender_email for this domain
    sender_rows = db.execute(
        text(
            """
                SELECT sender_email, sender_name,
                       COUNT(*) as email_count,
                       MAX(date) as last_email,
                       MIN(date) as first_email
                FROM emails
                WHERE user_id = :uid
                  AND LOWER(sender_email) LIKE :domain_pattern
                GROUP BY sender_email, sender_name
                ORDER BY COUNT(*) DESC
            """
        ),
        {"uid": str(uid), "domain_pattern": f"%@{domain}"},
    ).fetchall()

    # Search recipient_emails for this domain (comma-separated field)
    recipient_rows = db.execute(
        text(
            """
                SELECT recipient_emails, COUNT(*) as email_count
                FROM emails
                WHERE user_id = :uid
                  AND LOWER(recipient_emails) LIKE :domain_pattern
                GROUP BY recipient_emails
            """
        ),
        {"uid": str(uid), "domain_pattern": f"%@{domain}%"},
    ).fetchall()

    # Aggregate discovered people
    people: dict[str, dict] = {}  # email -> {name, email_count, last_email, first_email}

    # From sender_email
    for row in sender_rows:
        email = row.sender_email.lower().strip()
        if email in existing_emails:
            continue
        if email not in people:
            people[email] = {
                "email": row.sender_email.strip(),
                "name": row.sender_name,
                "email_count": 0,
                "last_email": None,
                "first_email": None,
            }
        people[email]["email_count"] += row.email_count
        if row.last_email:
            cur = people[email]["last_email"]
            if not cur or row.last_email > cur:
                people[email]["last_email"] = row.last_email
        if row.first_email:
            cur = people[email]["first_email"]
            if not cur or row.first_email < cur:
                people[email]["first_email"] = row.first_email

    # From recipient_emails (parse comma-separated)
    email_pattern = re.compile(r"[\w.+-]+@[\w.-]+\.\w+")
    name_email_pattern = re.compile(r'"?([^"<]*)"?\s*<([^>]+)>')

    for row in recipient_rows:
        raw = row.recipient_emails or ""
        # Extract emails with optional names: "Name <email>" or just email
        for match in name_email_pattern.finditer(raw):
            raw_name = match.group(1).strip().strip(",").strip()
            name = raw_name if raw_name else None
            email = match.group(2).strip().lower()
            if f"@{domain}" not in email:
                continue
            if email in existing_emails:
                continue
            if email not in people:
                people[email] = {
                    "email": match.group(2).strip(),
                    "name": name,
                    "email_count": 0,
                    "last_email": None,
                    "first_email": None,
                }
            people[email]["email_count"] += row.email_count
            if not people[email]["name"] and name:
                people[email]["name"] = name

        # Also catch bare emails not already captured by name_email_pattern
        for email_match in email_pattern.finditer(raw):
            email = email_match.group(0).lower()
            if f"@{domain}" not in email:
                continue
            if email in existing_emails:
                continue
            if email not in people:
                people[email] = {
                    "email": email_match.group(0),
                    "name": None,
                    "email_count": 0,
                    "last_email": None,
                    "first_email": None,
                }
                people[email]["email_count"] += row.email_count

    # Sort by email count descending
    discovered = sorted(people.values(), key=lambda p: -p["email_count"])

    # Enrich with job titles from signature blocks via Claude Haiku
    sender_emails = [p["email"].lower() for p in discovered if p["name"]]
    signatures: dict[str, tuple[str, str]] = {}
    if sender_emails:
        sig_rows = db.execute(
            text(
                """
                SELECT DISTINCT ON (LOWER(sender_email))
                       sender_email, body
                FROM emails
                WHERE user_id = :uid
                  AND LOWER(sender_email) = ANY(:emails)
                  AND body IS NOT NULL
                  AND body != ''
                ORDER BY LOWER(sender_email), date DESC
            """
            ),
            {"uid": str(uid), "emails": sender_emails},
        ).fetchall()

        for row in sig_rows:
            email_key = row.sender_email.lower().strip()
            if email_key in people and people[email_key]["name"]:
                signatures[email_key] = (people[email_key]["name"], row.body)

    enriched = _enrich_with_haiku(signatures, company.name) if signatures else {}

    # Serialize dates, apply Haiku enrichment, add LinkedIn search URLs as fallback
    for p in discovered:
        p["last_email"] = serialize_dt(p["last_email"]) if p["last_email"] else None
        p["first_email"] = serialize_dt(p["first_email"]) if p["first_email"] else None

        info = enriched.get(p["email"].lower(), {})
        # Use Haiku-extracted name if available (normalizes "Last, First" to "First Last")
        if info.get("name"):
            p["name"] = info["name"]
        p["title"] = info.get("title")
        # Use LinkedIn URL from signature if found, otherwise build a search URL
        p["linkedin_url"] = info.get("linkedin_url") or _build_linkedin_url(p["name"], company.name)

    return {"discovered": discovered, "domain": domain, "total": len(discovered)}


# ---------------------------------------------------------------------------
# POST /companies/{id}/contacts  — add a discovered person as a CRM contact
# ---------------------------------------------------------------------------


class AddContactRequest(BaseModel):
    email: str
    name: Optional[str] = None
    title: Optional[str] = None

    model_config = ConfigDict(extra="forbid")


@router.post("/companies/{company_id}/contacts")
def add_contact_to_company(
    company_id: str,
    body: AddContactRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_sync_db),
):
    """Create a new contact linked to this company from a discovered email."""
    uid = user.id

    company = db.query(Company).filter(Company.id == company_id, Company.user_id == uid).first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    # Check for existing contact with this email
    existing = (
        db.query(Contact)
        .filter(Contact.user_id == uid, Contact.email == body.email.lower().strip())
        .first()
    )
    if existing:
        # If contact exists but at a different company, optionally reassign
        if existing.company_id != company.id:
            existing.company_id = company.id
            db.commit()
            db.refresh(existing)
        return {
            "contact": {
                "id": str(existing.id),
                "email": existing.email,
                "name": existing.name,
                "company_id": str(existing.company_id) if existing.company_id else None,
            },
            "created": False,
        }

    # Count emails for this person
    email_lower = body.email.lower().strip()
    email_count_result = (
        db.query(func.count(Email.id))
        .filter(
            Email.user_id == uid,
            or_(
                func.lower(Email.sender_email) == email_lower,
                func.lower(Email.recipient_emails).contains(email_lower),
            ),
        )
        .scalar()
    )

    contact = Contact(
        id=uuid.uuid4(),
        user_id=uid,
        company_id=company.id,
        email=email_lower,
        name=body.name,
        title=body.title,
        email_count=email_count_result or 0,
        account_sources=[],
    )
    db.add(contact)
    db.commit()
    db.refresh(contact)

    return {
        "contact": {
            "id": str(contact.id),
            "email": contact.email,
            "name": contact.name,
            "company_id": str(contact.company_id),
            "email_count": contact.email_count,
        },
        "created": True,
    }


# ---------------------------------------------------------------------------
# PATCH /companies/{id}
# ---------------------------------------------------------------------------


@router.patch("/companies/{company_id}")
def update_company(
    company_id: str,
    body: CompanyUpdateRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_sync_db),
):
    """Update editable company fields."""
    uid = user.id

    company = db.query(Company).filter(Company.id == company_id, Company.user_id == uid).first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    update_data = body.model_dump(exclude_unset=True)
    allowed_fields = set(CompanyUpdateRequest.model_fields.keys())
    for field, value in update_data.items():
        if field not in allowed_fields:
            raise HTTPException(status_code=400, detail=f"Field '{field}' is not updatable")
        setattr(company, field, value)

    db.commit()
    db.refresh(company)

    contact_count = (
        db.query(func.count(Contact.id))
        .filter(Contact.company_id == company.id, Contact.user_id == uid)
        .scalar()
        or 0
    )

    return _serialize_company(company, contact_count)


# ---------------------------------------------------------------------------
# GET /search
# ---------------------------------------------------------------------------


@router.get("/search")
def global_search(
    q: str = Query(..., min_length=1),
    limit: int = Query(10, ge=1, le=50),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_sync_db),
):
    """Global search across contacts and companies."""
    uid = user.id
    pattern = f"%{q}%"

    contacts = (
        db.query(Contact)
        .options(selectinload(Contact.company))
        .filter(
            Contact.user_id == uid,
            or_(Contact.name.ilike(pattern), Contact.email.ilike(pattern)),
        )
        .order_by(Contact.email_count.desc())
        .limit(limit)
        .all()
    )

    companies = (
        db.query(Company)
        .filter(
            Company.user_id == uid,
            or_(Company.name.ilike(pattern), Company.domain.ilike(pattern)),
        )
        .limit(limit)
        .all()
    )

    return {
        "contacts": [
            _serialize_contact(c, c.company.name if c.company else None) for c in contacts
        ],
        "companies": [_serialize_company(c, 0) for c in companies],
    }
