"""
CRM API routes for contact and company management.
"""

import re
import uuid
from urllib.parse import quote_plus

import httpx
from bs4 import BeautifulSoup
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict
from sqlalchemy import case, func, or_, text
from sqlalchemy.orm import Session, selectinload

from src.api.middleware.auth import get_current_user
from src.core.database import get_sync_db
from src.core.logging import get_logger
from src.core.utils import serialize_dt
from src.models.company import Company
from src.models.contact import Contact
from src.models.discovered_contact import DiscoveredContact
from src.models.email import Email
from src.models.email_participant import EmailParticipant
from src.models.relationship_profile import RelationshipProfile
from src.models.user import User
from src.services.enrichment.email_participant_builder import EmailParticipantBuilder
from src.services.news.company_names import SKIP_NAMES, clean_company_name

_logger = get_logger(__name__)

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
        "account_owner",
        "renewal_date",
    }
)


# ---------------------------------------------------------------------------
# Pydantic response schemas
# ---------------------------------------------------------------------------


class ContactSummary(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str | None = None
    email: str
    phone: str | None = None
    title: str | None = None
    contact_type: str | None = None
    is_vip: bool = False
    email_count: int = 0
    tags: list[str] = []
    relationship_context: str | None = None
    company_id: str | None = None
    company_name: str | None = None
    last_contact_at: str | None = None
    created_at: str | None = None


class CompanySummary(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    domain: str | None = None
    industry: str | None = None
    company_type: str | None = None
    account_tier: str | None = None
    arr: float | None = None
    revenue_segment: str | None = None
    billing_state: str | None = None
    contact_count: int = 0
    created_at: str | None = None


class EmailSummary(BaseModel):
    id: str
    subject: str | None = None
    date: str
    sender_name: str | None = None
    sender_email: str
    summary: str | None = None
    has_attachments: bool = False
    direction: str | None = None


class RelationshipProfileSummary(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    relationship_type: str
    total_email_count: int = 0
    sent_count: int = 0
    received_count: int = 0
    first_exchange_date: str | None = None
    last_exchange_date: str | None = None
    thread_count: int = 0
    avg_response_time_hours: float | None = None
    profile_data: dict | None = None
    profiled_at: str | None = None


class ContactUpdateRequest(BaseModel):
    name: str | None = None
    title: str | None = None
    phone: str | None = None
    contact_type: str | None = None
    is_vip: bool | None = None
    tags: list[str] | None = None
    notes: str | None = None
    relationship_context: str | None = None
    company_id: str | None = None
    personal_email: str | None = None
    linkedin_url: str | None = None
    enrichment_status: str | None = None
    enrichment_notes: str | None = None


class CompanyUpdateRequest(BaseModel):
    name: str | None = None
    notes: str | None = None
    company_type: str | None = None
    work_type: str | None = None
    account_tier: str | None = None
    industry: str | None = None
    news_search_override: str | None = None
    linkedin_url: str | None = None


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
        "linkedin_url": contact.linkedin_url,
        "enrichment_status": contact.enrichment_status,
        "enrichment_notes": contact.enrichment_notes,
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
        "work_type": company.work_type,
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
        "news_search_override": company.news_search_override,
        "linkedin_url": company.linkedin_url,
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
        .filter(Contact.user_id == uid, Contact.is_vip is True)
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

    # Recent 20 emails involving CRM contacts (EXISTS avoids costly DISTINCT on join)
    contact_link = (
        db.query(EmailParticipant.email_id)
        .filter(
            EmailParticipant.email_id == Email.id,
            EmailParticipant.contact_id.isnot(None),
        )
        .exists()
    )
    recent_emails_q = (
        db.query(Email)
        .filter(Email.user_id == uid, contact_link)
        .order_by(Email.date.desc())
        .limit(20)
        .all()
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
    search: str | None = Query(None),
    sort_by: str = Query("email_count"),
    sort_dir: str = Query("desc"),
    is_vip: bool | None = Query(None),
    contact_type: str | None = Query(None),
    tags: str | None = Query(None, description="Comma-separated tags"),
    company_id: str | None = Query(None),
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
        query = query.order_by(sort_column.asc().nullslast())
    else:
        query = query.order_by(sort_column.desc().nullslast())

    # Pagination
    offset = (page - 1) * page_size
    contacts = query.offset(offset).limit(page_size).all()

    # Batch query: last email received from / sent to each contact on this page
    email_dates: dict[str, dict] = {}
    if contacts:
        contact_ids = [c.id for c in contacts]
        rows = (
            db.query(
                EmailParticipant.contact_id,
                func.max(case((EmailParticipant.role == "sender", Email.date))).label(
                    "last_received"
                ),
                func.max(case((EmailParticipant.role.in_(["to", "cc", "bcc"]), Email.date))).label(
                    "last_sent"
                ),
            )
            .join(Email, Email.id == EmailParticipant.email_id)
            .filter(EmailParticipant.contact_id.in_(contact_ids))
            .group_by(EmailParticipant.contact_id)
            .all()
        )
        for row in rows:
            email_dates[str(row.contact_id)] = {
                "last_email_received": serialize_dt(row.last_received),
                "last_email_sent": serialize_dt(row.last_sent),
            }

    def _build(c):
        d = _serialize_contact(c, c.company.name if c.company else None)
        dates = email_dates.get(str(c.id), {})
        d["last_email_received"] = dates.get("last_email_received")
        d["last_email_sent"] = dates.get("last_email_sent")
        return d

    return _paginated_response(
        total,
        page,
        page_size,
        [_build(c) for c in contacts],
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

    # Auto-link emails if contact has known emails but no participant records yet
    participant_exists = (
        db.query(EmailParticipant.id)
        .filter(EmailParticipant.contact_id == contact.id)
        .limit(1)
        .first()
    )
    if not participant_exists and contact.email_count and contact.email_count > 0:
        builder = EmailParticipantBuilder(user_id=uid, db=db)
        builder.build_for_contact(contact.id, contact.email)

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
# POST /contacts/{id}/enrich-title
# ---------------------------------------------------------------------------


@router.post("/contacts/{contact_id}/enrich-title")
def enrich_contact_title(
    contact_id: str,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_sync_db),
):
    """Extract job title from email signatures via Haiku. Returns immediately if title exists."""
    uid = user.id

    contact = (
        db.query(Contact)
        .options(selectinload(Contact.company))
        .filter(Contact.id == contact_id, Contact.user_id == uid)
        .first()
    )
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")

    # Already has a title — return it without calling Haiku
    if contact.title:
        return {"title": contact.title}

    # Fetch several recent emails, pick longest body (most likely to have a signature)
    sig_rows = db.execute(
        text(
            """
            SELECT sender_name, COALESCE(body, summary) AS sig_text
            FROM emails
            WHERE user_id = :uid
              AND LOWER(sender_email) = :contact_email
              AND LENGTH(COALESCE(body, summary)) > 100
            ORDER BY date DESC
            LIMIT 5
            """
        ),
        {"uid": str(uid), "contact_email": contact.email.lower()},
    ).fetchall()

    company_name = contact.company.name if contact.company else ""
    company_aliases = contact.company.aliases if contact.company else None
    company_domain = contact.company.domain if contact.company else None
    title = None

    # Step 1: If we have a stored LinkedIn URL, scrape title from it directly
    if contact.linkedin_url:
        title = _scrape_linkedin_title(contact.linkedin_url, contact.name)

    # Step 2: Try LinkedIn search (fast — ~2-5s)
    if not title:
        title = _search_linkedin_title(contact.name, company_name, company_aliases, company_domain)

    # Step 3: Fall back to email signature enrichment via Haiku (slow — ~15-25s)
    if not title and sig_rows:
        _logger.info("No LinkedIn title for %s, trying email signatures…", contact.name)
        best = max(sig_rows, key=lambda r: len(r.sig_text or ""))
        signatures = {
            contact.email.lower(): (
                best.sender_name or contact.name or "",
                best.sig_text,
            )
        }
        enriched = _enrich_with_haiku(signatures, company_name)
        info = enriched.get(contact.email.lower(), {})
        title = info.get("title")

    if title:
        contact.title = title
        db.commit()
        _logger.info("Auto-enriched title for contact %s: %s", contact.id, title)

    # Try to discover company LinkedIn URL if we don't have one yet
    if contact.company and not contact.company.linkedin_url:
        co_search = _best_company_name(company_name, company_aliases, company_domain)
        company_li = _search_company_linkedin(co_search)
        if company_li:
            contact.company.linkedin_url = company_li
            db.commit()
            _logger.info("Discovered company LinkedIn for %s: %s", company_name, company_li)

    return {"title": title}


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
# POST /contacts/{id}/link-emails
# ---------------------------------------------------------------------------


@router.post("/contacts/{contact_id}/link-emails")
def link_contact_emails(
    contact_id: str,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_sync_db),
):
    """Build EmailParticipant records linking this contact to their existing emails."""
    uid = user.id

    contact = db.query(Contact).filter(Contact.id == contact_id, Contact.user_id == uid).first()
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")

    builder = EmailParticipantBuilder(user_id=uid, db=db)
    count = builder.build_for_contact(contact.id, contact.email)

    return {"linked": count, "contact_id": str(contact.id), "email": contact.email}


# ---------------------------------------------------------------------------
# GET /companies
# ---------------------------------------------------------------------------


@router.get("/companies")
def list_companies(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    search: str | None = Query(None),
    sort_by: str = Query("arr"),
    sort_dir: str = Query("desc"),
    company_type: str | None = Query(None),
    account_tier: str | None = Query(None),
    no_contact: bool = Query(False),
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

    # Subquery for discovered_count
    discovered_count_sq = (
        db.query(
            DiscoveredContact.company_id,
            func.count(DiscoveredContact.id).label("discovered_count"),
        )
        .filter(DiscoveredContact.user_id == uid)
        .group_by(DiscoveredContact.company_id)
        .subquery()
    )

    cc_col = func.coalesce(contact_count_sq.c.contact_count, 0)
    dc_col = func.coalesce(discovered_count_sq.c.discovered_count, 0)

    query = (
        db.query(Company, cc_col.label("contact_count"), dc_col.label("discovered_count"))
        .outerjoin(contact_count_sq, Company.id == contact_count_sq.c.company_id)
        .outerjoin(discovered_count_sq, Company.id == discovered_count_sq.c.company_id)
        .filter(Company.user_id == uid)
    )

    # Filter by email activity
    if no_contact:
        # Only companies with zero contacts AND zero discovered contacts
        query = query.filter(cc_col == 0, dc_col == 0)
    else:
        # Only companies with at least some email activity
        query = query.filter(or_(cc_col > 0, dc_col > 0))

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

    items = [_serialize_company(company, cc) for company, cc, dc in results]

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


# Common nickname → formal name mappings for LinkedIn search
_NICKNAME_MAP = {
    "dave": "David",
    "mike": "Michael",
    "bob": "Robert",
    "rob": "Robert",
    "bill": "William",
    "will": "William",
    "jim": "James",
    "jimmy": "James",
    "tom": "Thomas",
    "tommy": "Thomas",
    "dan": "Daniel",
    "danny": "Daniel",
    "joe": "Joseph",
    "tony": "Anthony",
    "rick": "Richard",
    "dick": "Richard",
    "rich": "Richard",
    "pat": "Patrick",
    "matt": "Matthew",
    "chris": "Christopher",
    "nick": "Nicholas",
    "ed": "Edward",
    "ted": "Edward",
    "al": "Albert",
    "steve": "Steven",
    "andy": "Andrew",
    "drew": "Andrew",
    "greg": "Gregory",
    "jeff": "Jeffrey",
    "jerry": "Gerald",
    "larry": "Lawrence",
    "charlie": "Charles",
    "chuck": "Charles",
    "hank": "Henry",
    "harry": "Harold",
    "jack": "John",
    "jon": "Jonathan",
    "ken": "Kenneth",
    "liz": "Elizabeth",
    "beth": "Elizabeth",
    "kate": "Katherine",
    "kathy": "Katherine",
    "sue": "Susan",
    "peggy": "Margaret",
    "maggie": "Margaret",
    "jen": "Jennifer",
    "sam": "Samuel",
    "ben": "Benjamin",
    "tim": "Timothy",
    "wes": "Wesley",
    "doug": "Douglas",
    "pete": "Peter",
    "phil": "Philip",
    "ray": "Raymond",
    "ron": "Ronald",
    "walt": "Walter",
}


def _expand_nickname(name: str) -> str | None:
    """If the first name is a common nickname, return the name with the formal version."""
    parts = name.split()
    if not parts:
        return None
    first = parts[0].lower()
    formal = _NICKNAME_MAP.get(first)
    if formal:
        return formal + " " + " ".join(parts[1:])
    return None


def _parse_linkedin_title_parts(raw_title: str, name: str | None) -> str | None:
    """Extract job title from a LinkedIn-style title string like 'Name - Title - Company | LinkedIn'."""
    raw_title = raw_title.split(" | ")[0].strip()
    parts = [p.strip() for p in raw_title.split(" - ")]

    if len(parts) >= 3:
        title = " - ".join(parts[1:-1])
        if title and title.lower() not in ("linkedin", ""):
            _logger.info("LinkedIn found title for %s: %s", name, title)
            return title
    elif len(parts) == 2:
        candidate = parts[1]
        if candidate and candidate.lower() not in ("linkedin", ""):
            _logger.info("LinkedIn found title for %s: %s", name, candidate)
            return candidate
    return None


def _scrape_linkedin_title(linkedin_url: str, name: str | None) -> str | None:
    """Fetch a LinkedIn profile directly and extract the job title from page metadata.

    LinkedIn serves <title> and og:title tags even to bots, formatted as:
      "Name - Title - Company | LinkedIn"
    Falls back to DuckDuckGo search if direct fetch fails.
    """
    # Normalize URL
    li_url = linkedin_url.strip().rstrip("/")
    if not li_url.startswith("http"):
        li_url = "https://" + li_url

    # Step 1: Fetch the LinkedIn page directly — they serve metadata to bots
    try:
        resp = httpx.get(
            li_url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
                ),
                "Accept": "text/html",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=8,
            follow_redirects=True,
        )
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "lxml")
            # Try <title> tag first
            title_tag = soup.find("title")
            if title_tag:
                result = _parse_linkedin_title_parts(title_tag.get_text(), name)
                if result:
                    return result
            # Try og:title meta tag
            og_title = soup.find("meta", property="og:title")
            if og_title and og_title.get("content"):
                result = _parse_linkedin_title_parts(og_title["content"], name)
                if result:
                    return result
        else:
            _logger.info("LinkedIn direct fetch returned %s for %s", resp.status_code, li_url)
    except Exception:
        _logger.warning("LinkedIn direct fetch failed for %s", li_url, exc_info=True)

    # Step 2: Fall back to DuckDuckGo search
    slug = li_url.rstrip("/").split("/")[-1]
    query = f"site:linkedin.com/in/{slug}"
    ddg_url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
    try:
        resp = httpx.get(
            ddg_url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; CRM-HTH/1.0)"},
            timeout=6,
            follow_redirects=True,
        )
        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, "lxml")
        for result in soup.select(".result"):
            title_el = result.select_one(".result__a")
            link_el = result.select_one(".result__url")
            if not title_el:
                continue
            link_text = link_el.get_text().strip() if link_el else ""
            if "linkedin.com/in/" not in link_text:
                continue
            return _parse_linkedin_title_parts(title_el.get_text().strip(), name)

        return None
    except Exception:
        _logger.warning("LinkedIn DuckDuckGo fallback failed for %s", li_url, exc_info=True)
        return None


def _search_company_linkedin(company_name: str) -> str | None:
    """Search DuckDuckGo for a company's LinkedIn page URL.

    Returns the linkedin.com/company/... URL if found, else None.
    """
    if not company_name:
        return None

    query = f'"{company_name}" site:linkedin.com/company'
    url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
    try:
        resp = httpx.get(
            url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; CRM-HTH/1.0)"},
            timeout=6,
            follow_redirects=True,
        )
        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, "lxml")
        for result in soup.select(".result"):
            link_el = result.select_one(".result__url")
            if not link_el:
                continue
            link_text = link_el.get_text().strip()
            if "linkedin.com/company/" not in link_text:
                continue

            # Extract the clean URL
            # DuckDuckGo shows URLs like "https://www.linkedin.com/company/manhattan-construction"
            if link_text.startswith("http"):
                li_url = link_text
            else:
                li_url = "https://" + link_text
            # Normalize: strip trailing slashes and query params
            li_url = li_url.split("?")[0].rstrip("/")
            _logger.info("Found company LinkedIn for %s: %s", company_name, li_url)
            return li_url

        return None
    except Exception:
        _logger.warning("Company LinkedIn search failed for %s", company_name, exc_info=True)
        return None


def _best_company_name(company_name: str, aliases: list[str] | None, domain: str | None) -> str:
    """Pick the most descriptive company name for search queries.

    Always prefers aliases when available (they are curated by the user).
    Falls back to domain-derived names for single-word company names.
    """
    co = company_name.split(" - ")[0].strip() if company_name else ""
    co = re.sub(r",?\s*(Inc\.?|LLC|Ltd\.?|Corp\.?|Co\.?)$", "", co).strip()

    # Always prefer aliases — they are explicitly curated and more accurate
    if aliases:
        for alias in aliases:
            clean_alias = alias.split(" - ")[0].strip()
            clean_alias = re.sub(r",?\s*(Inc\.?|LLC|Ltd\.?|Corp\.?|Co\.?)$", "", clean_alias)
            clean_alias = clean_alias.strip()
            if clean_alias and clean_alias.lower() != co.lower():
                _logger.info("Using alias %r instead of %r for search", clean_alias, co)
                return clean_alias

    # For single-word names, try deriving from domain
    if len(co.split()) < 2 and domain:
        # Remove common TLD and split camelCase/concatenated words
        slug = domain.split(".")[0]
        # Insert spaces before uppercase letters (camelCase)
        spaced = re.sub(r"([a-z])([A-Z])", r"\1 \2", slug)
        # If still one word, try splitting known company-name patterns
        if " " not in spaced and len(spaced) > len(co) + 3:
            # Try to find the primary name as prefix and split there
            lower_slug = spaced.lower()
            lower_co = co.lower()
            if lower_slug.startswith(lower_co) and len(lower_slug) > len(lower_co):
                remainder = spaced[len(co) :]
                derived = co + " " + remainder.capitalize()
                _logger.info("Derived company name %r from domain %r", derived, domain)
                return derived

    return co


def _search_linkedin_title(
    name: str | None,
    company_name: str,
    aliases: list[str] | None = None,
    domain: str | None = None,
) -> str | None:
    """Search DuckDuckGo for a person's LinkedIn profile and extract their job title.

    LinkedIn results typically have titles like:
      "John Smith - VP of Operations - Acme Corp | LinkedIn"
    We parse the title to extract the middle portion (the job title).
    """
    if not name:
        return None

    clean_name = name.split(" - ")[0].strip()
    # Pick the best company name from primary name, aliases, and domain
    co = _best_company_name(company_name, aliases, domain)
    # Short company name = first word only (e.g. "McCarthy" from "McCarthy Holdings")
    co_short = co.split()[0] if co else ""

    # Build name variants (handles Dave/David, Mike/Michael, etc.)
    name_variants = [clean_name]
    alt = _expand_nickname(clean_name)
    if alt and alt != clean_name:
        name_variants.append(alt)

    # Build queries: exact match first, then relaxed, then nickname variant
    # Cap at 3 queries (6s each = 18s max) to stay within Heroku's 30s timeout
    queries = []
    if co:
        queries.append(f'"{clean_name}" "{co}" site:linkedin.com/in')
        queries.append(f"{clean_name} {co_short} site:linkedin.com/in")
        if alt:
            queries.append(f"{alt} {co_short} site:linkedin.com/in")
    else:
        queries.append(f'"{clean_name}" site:linkedin.com/in')
        if alt:
            queries.append(f'"{alt}" site:linkedin.com/in')

    for query in queries[:3]:
        result = _search_linkedin_query(clean_name, query)
        if result:
            return result

    _logger.info("LinkedIn search found no title for %s at %s", name, company_name)
    return None


def _search_linkedin_query(name: str, query: str) -> str | None:
    """Execute a single DuckDuckGo search query and parse LinkedIn titles from results."""
    url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
    try:
        resp = httpx.get(
            url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; CRM-HTH/1.0)"},
            timeout=6,
            follow_redirects=True,
        )
        if resp.status_code != 200:
            _logger.warning("LinkedIn search returned %s", resp.status_code)
            return None

        soup = BeautifulSoup(resp.text, "lxml")

        for result in soup.select(".result"):
            title_el = result.select_one(".result__a")
            link_el = result.select_one(".result__url")
            if not title_el:
                continue
            link_text = link_el.get_text().strip() if link_el else ""
            if "linkedin.com/in/" not in link_text:
                continue

            # Parse: "Name - Title - Company | LinkedIn"
            raw_title = title_el.get_text().strip()
            raw_title = raw_title.split(" | ")[0].strip()  # Remove "| LinkedIn"
            parts = [p.strip() for p in raw_title.split(" - ")]

            if len(parts) >= 3:
                title = " - ".join(parts[1:-1])
                if title and title.lower() not in ("linkedin", ""):
                    _logger.info("LinkedIn found title for %s: %s", name, title)
                    return title
            elif len(parts) == 2:
                candidate = parts[1]
                if candidate and candidate.lower() not in ("linkedin", ""):
                    _logger.info("LinkedIn found title for %s: %s", name, candidate)
                    return candidate

        return None
    except Exception:
        _logger.warning("LinkedIn query failed for %s", name, exc_info=True)
        return None


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
    Return pre-discovered people matching this company's domain.
    Reads from the discovered_contacts cache table (populated by daily cron job).
    Haiku enrichment for job titles is applied on-demand.
    """
    from src.models.discovered_contact import DiscoveredContact

    uid = user.id

    company = db.query(Company).filter(Company.id == company_id, Company.user_id == uid).first()
    if not company or not company.domain:
        return {"discovered": [], "domain": None}

    domain = company.domain.lower().strip()

    # Exclude people who have since been added as CRM contacts
    existing_emails = set()
    for (email,) in (
        db.query(Contact.email)
        .filter(Contact.user_id == uid, Contact.email.ilike(f"%@{domain}"))
        .all()
    ):
        existing_emails.add(email.lower())

    # Read from cache table
    rows = (
        db.query(DiscoveredContact)
        .filter(
            DiscoveredContact.company_id == company.id,
            DiscoveredContact.user_id == uid,
        )
        .order_by(DiscoveredContact.email_count.desc())
        .all()
    )

    # Filter out people who have been added as contacts since last discovery run
    people = []
    for dc in rows:
        if dc.email.lower() in existing_emails:
            continue
        people.append(
            {
                "email": dc.email,
                "name": dc.name,
                "email_count": dc.email_count,
                "last_email": serialize_dt(dc.last_email_at),
                "first_email": serialize_dt(dc.first_email_at),
            }
        )

    # Enrich with job titles from signature blocks via Claude Haiku
    sender_emails = [p["email"].lower() for p in people if p["name"]]
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

        people_lookup = {p["email"].lower(): p for p in people}
        for row in sig_rows:
            email_key = row.sender_email.lower().strip()
            p = people_lookup.get(email_key)
            if p and p["name"]:
                signatures[email_key] = (p["name"], row.body)

    enriched = _enrich_with_haiku(signatures, company.name) if signatures else {}

    for p in people:
        info = enriched.get(p["email"].lower(), {})
        if info.get("name"):
            p["name"] = info["name"]
        p["title"] = info.get("title")
        p["linkedin_url"] = info.get("linkedin_url") or _build_linkedin_url(p["name"], company.name)

    return {"discovered": people, "domain": domain, "total": len(people)}


# ---------------------------------------------------------------------------
# POST /companies/{id}/contacts  — add a discovered person as a CRM contact
# ---------------------------------------------------------------------------


class AddContactRequest(BaseModel):
    email: str
    name: str | None = None
    title: str | None = None

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

    # Link existing emails to this new contact via EmailParticipant
    builder = EmailParticipantBuilder(user_id=uid, db=db)
    participant_count = builder.build_for_contact(contact.id, contact.email)
    _logger.info(
        "Linked %d emails to new contact %s (%s)",
        participant_count,
        contact.name,
        contact.email,
    )

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

    # Enforce: ENR-ranked companies are always General Contractor
    if company.source_data and company.source_data.get("enr", {}).get("rank_2024"):
        company.company_type = "General Contractor"

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


# ---------------------------------------------------------------------------
# GET /reports/challenging-names
# ---------------------------------------------------------------------------


@router.get("/reports/challenging-names")
def report_challenging_names(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_sync_db),
):
    """Companies whose names are too short or generic for automated news search."""
    uid = user.id
    companies = (
        db.query(Company)
        .filter(
            Company.user_id == uid,
            Company.news_search_override.is_(None),
        )
        .order_by(Company.name)
        .all()
    )

    results = []
    for c in companies:
        clean = clean_company_name(c.name)
        reason = None
        if len(clean) <= 3:
            reason = "too_short"
        elif clean.lower() in SKIP_NAMES:
            reason = "generic_name"

        if reason:
            results.append(
                {
                    "id": str(c.id),
                    "name": c.name,
                    "clean_name": clean,
                    "reason": reason,
                }
            )

    return {"items": results, "total": len(results)}


# ---------------------------------------------------------------------------
# GET /reports/companies-without-people
# ---------------------------------------------------------------------------


@router.get("/reports/companies-without-people")
def report_companies_without_people(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_sync_db),
):
    """Companies that have zero linked contacts/people."""
    uid = user.id

    # Subquery: company IDs that have at least one contact
    has_contacts_sq = (
        db.query(Contact.company_id).filter(Contact.company_id.isnot(None)).distinct().subquery()
    )

    companies = (
        db.query(Company)
        .filter(
            Company.user_id == uid,
            ~Company.id.in_(db.query(has_contacts_sq.c.company_id)),
        )
        .order_by(Company.name)
        .all()
    )

    results = [
        {
            "id": str(c.id),
            "name": c.name,
            "domain": c.domain,
            "industry": c.industry,
            "company_type": c.company_type,
            "account_tier": c.account_tier,
        }
        for c in companies
    ]

    return {"items": results, "total": len(results)}


# ---------------------------------------------------------------------------
# GET /reports/needs-linkedin-url
# ---------------------------------------------------------------------------


@router.get("/reports/needs-linkedin-url")
def report_needs_linkedin_url(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_sync_db),
):
    """Contacts without a title where LinkedIn search failed and no LinkedIn URL is stored."""
    uid = user.id

    contacts = (
        db.query(Contact)
        .options(selectinload(Contact.company))
        .filter(
            Contact.user_id == uid,
            Contact.title.is_(None),
            Contact.linkedin_url.is_(None),
        )
        .order_by(Contact.email_count.desc())
        .limit(100)
        .all()
    )

    results = [
        {
            "id": str(c.id),
            "name": c.name,
            "email": c.email,
            "company_name": c.company.name if c.company else None,
            "email_count": c.email_count,
        }
        for c in contacts
    ]

    return {"items": results, "total": len(results)}


# ---------------------------------------------------------------------------
# GET /reports/needs-browser-enrich
# ---------------------------------------------------------------------------


@router.get("/reports/needs-browser-enrich")
def report_needs_browser_enrich(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_sync_db),
):
    """Contacts that have a LinkedIn URL but no title — ready for browser-based enrichment."""
    uid = user.id

    contacts = (
        db.query(Contact)
        .options(selectinload(Contact.company))
        .filter(
            Contact.user_id == uid,
            Contact.title.is_(None),
            Contact.linkedin_url.isnot(None),
        )
        .order_by(Contact.email_count.desc())
        .limit(100)
        .all()
    )

    results = [
        {
            "id": str(c.id),
            "name": c.name,
            "email": c.email,
            "company_name": c.company.name if c.company else None,
            "linkedin_url": c.linkedin_url,
            "email_count": c.email_count,
        }
        for c in contacts
    ]

    return {"items": results, "total": len(results)}


# ---------------------------------------------------------------------------
# GET /reports/needs-human-research
# ---------------------------------------------------------------------------


@router.get("/reports/needs-human-research")
def report_needs_human_research(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_sync_db),
):
    """Contacts flagged by enrichment automation as needing human research."""
    uid = user.id

    contacts = (
        db.query(Contact)
        .options(selectinload(Contact.company))
        .filter(
            Contact.user_id == uid,
            Contact.enrichment_status == "needs_review",
        )
        .order_by(Contact.email_count.desc())
        .limit(200)
        .all()
    )

    results = [
        {
            "id": str(c.id),
            "name": c.name,
            "email": c.email,
            "company_name": c.company.name if c.company else None,
            "linkedin_url": c.linkedin_url,
            "enrichment_notes": c.enrichment_notes,
            "email_count": c.email_count,
        }
        for c in contacts
    ]

    return {"items": results, "total": len(results)}


# ---------------------------------------------------------------------------
# POST /companies/{company_id}/scan-emails
# ---------------------------------------------------------------------------


@router.post("/companies/{company_id}/scan-emails")
def scan_company_emails(
    company_id: str,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_sync_db),
):
    """Trigger an email sync for the user's accounts (forward-sync only)."""
    from src.models.job import SyncJob
    from src.worker.tasks import scan_gmail_task

    uid = user.id

    # Verify company exists
    company = (
        db.query(Company)
        .filter(Company.id == uuid.UUID(company_id), Company.user_id == uid)
        .first()
    )
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    # Check if a scan is already running
    existing = (
        db.query(SyncJob)
        .filter(
            SyncJob.user_id == uid,
            SyncJob.status.in_(["queued", "running"]),
        )
        .first()
    )
    if existing:
        raise HTTPException(
            status_code=409,
            detail="An email scan is already running.",
        )

    task = scan_gmail_task.delay(str(uid))
    _logger.info("Triggered email scan from company %s, task %s", company_id, task.id)

    return {"status": "queued", "message": "Email scan started", "job_id": task.id}


# ---------------------------------------------------------------------------
# POST /companies/{target_id}/merge
# ---------------------------------------------------------------------------


class MergeCompanyRequest(BaseModel):
    source_id: str


@router.post("/companies/{target_id}/merge")
def merge_company(
    target_id: str,
    body: MergeCompanyRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_sync_db),
):
    """Merge source company into target company. Target survives."""
    from src.models.company_news import CompanyNewsItem
    from src.models.contact_enrichment import ContactEnrichment

    uid = user.id
    target_uuid = uuid.UUID(target_id)
    source_uuid = uuid.UUID(body.source_id)

    if target_uuid == source_uuid:
        raise HTTPException(status_code=400, detail="Cannot merge a company into itself")

    target = db.query(Company).filter(Company.id == target_uuid, Company.user_id == uid).first()
    if not target:
        raise HTTPException(status_code=404, detail="Target company not found")

    source = db.query(Company).filter(Company.id == source_uuid, Company.user_id == uid).first()
    if not source:
        raise HTTPException(status_code=404, detail="Source company not found")

    # 1. Move contacts
    contacts_moved = (
        db.query(Contact)
        .filter(Contact.company_id == source_uuid)
        .update({"company_id": target_uuid}, synchronize_session="fetch")
    )

    # 2. Move news items — handle unique constraint (company_id, source_url)
    # Get URLs already on target
    target_urls = {
        row[0]
        for row in db.query(CompanyNewsItem.source_url)
        .filter(CompanyNewsItem.company_id == target_uuid)
        .all()
    }

    source_news = db.query(CompanyNewsItem).filter(CompanyNewsItem.company_id == source_uuid).all()

    news_moved = 0
    news_dupes = 0
    for item in source_news:
        if item.source_url in target_urls:
            db.delete(item)
            news_dupes += 1
        else:
            item.company_id = target_uuid
            news_moved += 1

    # 3. Move enrichments
    enrichments_moved = (
        db.query(ContactEnrichment)
        .filter(ContactEnrichment.company_id == source_uuid)
        .update({"company_id": target_uuid}, synchronize_session="fetch")
    )

    # 3b. Move discovered contacts (delete dupes by email)
    target_discovered_emails = {
        row[0]
        for row in db.query(DiscoveredContact.email)
        .filter(DiscoveredContact.company_id == target_uuid)
        .all()
    }
    source_discovered = (
        db.query(DiscoveredContact).filter(DiscoveredContact.company_id == source_uuid).all()
    )
    for dc in source_discovered:
        if dc.email in target_discovered_emails:
            db.delete(dc)
        else:
            dc.company_id = target_uuid

    # 4. Delete source company (cascades remaining news items + draft suggestions)
    db.delete(source)
    db.commit()

    _logger.info(
        "Merged company %s into %s: %d contacts, %d news (%d dupes), %d enrichments",
        source_uuid,
        target_uuid,
        contacts_moved,
        news_moved,
        news_dupes,
        enrichments_moved,
    )

    # Re-query target with contact count
    contact_count = (
        db.query(func.count(Contact.id)).filter(Contact.company_id == target_uuid).scalar()
    )

    return {
        "status": "merged",
        "target": _serialize_company(target, contact_count),
        "records_moved": {
            "contacts": contacts_moved,
            "news_items": news_moved,
            "news_duplicates_removed": news_dupes,
            "enrichments": enrichments_moved,
        },
    }
