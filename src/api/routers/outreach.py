"""
Outreach API routes for news intelligence and draft suggestions.
"""

from datetime import UTC, datetime, timedelta
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import func
from sqlalchemy.orm import Session, joinedload

from src.api.middleware.auth import get_current_user
from src.core.database import get_sync_db
from src.core.utils import serialize_dt
from src.models.company_news import CompanyNewsItem
from src.models.contact import Contact
from src.models.draft_suggestion import DraftSuggestion
from src.models.user import User

router = APIRouter()

NEWS_SORTABLE_COLUMNS = frozenset({"published_at", "created_at", "status", "title"})
SUGGESTION_SORTABLE_COLUMNS = frozenset({"created_at", "status", "generated_at"})


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------


class NewsItemResponse(BaseModel):
    id: str
    company_id: str
    company_name: str | None = None
    title: str
    summary: str | None = None
    source_url: str
    source_type: str
    published_at: str | None = None
    analysis: dict | None = None
    status: str
    created_at: str | None = None
    draft_count: int = 0


class DraftSuggestionResponse(BaseModel):
    id: str
    news_item_id: str | None = None
    contact_id: str
    contact_name: str | None = None
    contact_email: str
    contact_title: str | None = None
    company_name: str | None = None
    trigger_type: str = "news_mention"
    match_confidence: str = "full_name"
    news_title: str | None = None
    news_category: str | None = None
    news_url: str | None = None
    subject: str
    body: str
    status: str
    generated_at: str | None = None
    created_at: str | None = None


class DraftUpdateRequest(BaseModel):
    status: str | None = None
    subject: str | None = None
    body: str | None = None
    snoozed_until: str | None = None
    match_confidence: str | None = None


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _serialize_news_item(item: CompanyNewsItem, draft_count: int = 0) -> dict:
    return {
        "id": str(item.id),
        "company_id": str(item.company_id),
        "company_name": item.company.name if item.company else None,
        "title": item.title,
        "summary": item.summary,
        "source_url": item.source_url,
        "source_type": item.source_type,
        "published_at": serialize_dt(item.published_at),
        "analysis": item.analysis,
        "status": item.status,
        "created_at": serialize_dt(item.created_at),
        "draft_count": draft_count,
    }


def _serialize_suggestion(s: DraftSuggestion) -> dict:
    news_item = s.news_item
    contact = s.contact
    analysis = news_item.analysis if news_item else {}

    # For job change drafts, company comes from the contact; for news, from the article
    if news_item and news_item.company:
        company_name = news_item.company.name
    elif contact and contact.company:
        company_name = contact.company.name
    else:
        company_name = None

    return {
        "id": str(s.id),
        "news_item_id": str(s.news_item_id) if s.news_item_id else None,
        "contact_id": str(s.contact_id),
        "contact_name": contact.name if contact else None,
        "contact_email": contact.email if contact else "",
        "contact_title": contact.title if contact else None,
        "company_name": company_name,
        "trigger_type": s.trigger_type or "news_mention",
        "match_confidence": s.match_confidence or "full_name",
        "news_title": news_item.title if news_item else None,
        "news_category": analysis.get("category") if analysis else None,
        "news_url": news_item.source_url if news_item else None,
        "subject": s.subject,
        "body": s.body,
        "status": s.status,
        "generated_at": serialize_dt(s.generated_at),
        "created_at": serialize_dt(s.created_at),
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/dashboard")
def outreach_dashboard(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_sync_db),
):
    """Summary stats for the Outreach tab."""
    user_id = user.id

    pending_drafts = (
        db.query(func.count(DraftSuggestion.id))
        .filter(DraftSuggestion.user_id == user_id, DraftSuggestion.status == "pending")
        .scalar()
    )

    # Only count articles that mention a contact (have linked draft suggestions)
    actioned_ids = (
        db.query(DraftSuggestion.news_item_id)
        .filter(DraftSuggestion.user_id == user_id, DraftSuggestion.news_item_id.isnot(None))
        .distinct()
        .subquery()
    )

    cutoff_24h = datetime.now(UTC) - timedelta(hours=24)
    news_today = (
        db.query(func.count(CompanyNewsItem.id))
        .filter(
            CompanyNewsItem.user_id == user_id,
            CompanyNewsItem.published_at >= cutoff_24h,
            CompanyNewsItem.id.in_(db.query(actioned_ids.c.news_item_id)),
        )
        .scalar()
    )

    total_news = (
        db.query(func.count(CompanyNewsItem.id))
        .filter(
            CompanyNewsItem.user_id == user_id,
            CompanyNewsItem.id.in_(db.query(actioned_ids.c.news_item_id)),
        )
        .scalar()
    )

    total_analyzed = (
        db.query(func.count(CompanyNewsItem.id))
        .filter(CompanyNewsItem.user_id == user_id, CompanyNewsItem.status != "new")
        .scalar()
    )

    drafts_sent = (
        db.query(func.count(DraftSuggestion.id))
        .filter(DraftSuggestion.user_id == user_id, DraftSuggestion.status == "sent")
        .scalar()
    )

    review_drafts = (
        db.query(func.count(DraftSuggestion.id))
        .filter(
            DraftSuggestion.user_id == user_id,
            DraftSuggestion.status == "pending",
            DraftSuggestion.match_confidence == "last_name",
        )
        .scalar()
    )

    return {
        "pending_drafts": pending_drafts or 0,
        "news_today": news_today or 0,
        "total_news": total_news or 0,
        "total_analyzed": total_analyzed or 0,
        "drafts_sent": drafts_sent or 0,
        "review_drafts": review_drafts or 0,
    }


@router.get("/news")
def list_news_items(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    status: str | None = None,
    category: str | None = None,
    company_id: str | None = None,
    published_within: str | None = Query(
        None, description="Filter by published_at recency: '24h', '7d', '30d'"
    ),
    sort_by: str = "created_at",
    sort_dir: str = "desc",
    user: User = Depends(get_current_user),
    db: Session = Depends(get_sync_db),
):
    """Paginated news items with filtering."""
    if sort_by not in NEWS_SORTABLE_COLUMNS:
        raise HTTPException(400, f"Invalid sort column: {sort_by}")

    # Only show articles that have at least one draft suggestion (contact mentioned)
    has_draft = (
        db.query(DraftSuggestion.news_item_id)
        .filter(DraftSuggestion.news_item_id.isnot(None))
        .distinct()
        .subquery()
    )

    query = (
        db.query(CompanyNewsItem)
        .options(joinedload(CompanyNewsItem.company))
        .filter(
            CompanyNewsItem.user_id == user.id,
            CompanyNewsItem.id.in_(db.query(has_draft.c.news_item_id)),
        )
    )

    if company_id:
        query = query.filter(CompanyNewsItem.company_id == company_id)

    if status:
        query = query.filter(CompanyNewsItem.status == status)

    if published_within:
        hours_map = {"24h": 24, "7d": 7 * 24, "30d": 30 * 24}
        hours = hours_map.get(published_within)
        if hours:
            cutoff = datetime.now(UTC) - timedelta(hours=hours)
            query = query.filter(CompanyNewsItem.published_at >= cutoff)

    # Sort
    col = getattr(CompanyNewsItem, sort_by)
    query = query.order_by(col.desc() if sort_dir == "desc" else col.asc())

    total = query.count()
    items = query.offset((page - 1) * page_size).limit(page_size).all()

    # Get draft counts
    draft_counts = {}
    if items:
        item_ids = [i.id for i in items]
        counts = (
            db.query(DraftSuggestion.news_item_id, func.count(DraftSuggestion.id))
            .filter(DraftSuggestion.news_item_id.in_(item_ids))
            .group_by(DraftSuggestion.news_item_id)
            .all()
        )
        draft_counts = dict(counts)

    return {
        "items": [_serialize_news_item(i, draft_counts.get(i.id, 0)) for i in items],
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size,
    }


@router.get("/suggestions")
def list_suggestions(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    status: str = "pending",
    sort_by: str = "created_at",
    sort_dir: str = "desc",
    user: User = Depends(get_current_user),
    db: Session = Depends(get_sync_db),
):
    """Paginated draft suggestions."""
    if sort_by not in SUGGESTION_SORTABLE_COLUMNS:
        raise HTTPException(400, f"Invalid sort column: {sort_by}")

    query = (
        db.query(DraftSuggestion)
        .options(
            joinedload(DraftSuggestion.news_item).joinedload(CompanyNewsItem.company),
            joinedload(DraftSuggestion.contact).joinedload(Contact.company),
        )
        .filter(DraftSuggestion.user_id == user.id)
    )

    if status != "all":
        query = query.filter(DraftSuggestion.status == status)

    col = getattr(DraftSuggestion, sort_by)
    query = query.order_by(col.desc() if sort_dir == "desc" else col.asc())

    total = (
        db.query(func.count(DraftSuggestion.id))
        .filter(DraftSuggestion.user_id == user.id)
        .filter(DraftSuggestion.status == status if status != "all" else True)
        .scalar()
    )
    items = query.offset((page - 1) * page_size).limit(page_size).all()

    return {
        "items": [_serialize_suggestion(s) for s in items],
        "total": total or 0,
        "page": page,
        "page_size": page_size,
        "total_pages": ((total or 0) + page_size - 1) // page_size,
    }


@router.get("/suggestions/{suggestion_id}")
def get_suggestion(
    suggestion_id: UUID,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_sync_db),
):
    """Get a single draft suggestion with full context."""
    s = (
        db.query(DraftSuggestion)
        .options(
            joinedload(DraftSuggestion.news_item).joinedload(CompanyNewsItem.company),
            joinedload(DraftSuggestion.contact).joinedload(Contact.company),
        )
        .filter(DraftSuggestion.id == suggestion_id, DraftSuggestion.user_id == user.id)
        .first()
    )
    if not s:
        raise HTTPException(404, "Suggestion not found")
    return _serialize_suggestion(s)


@router.patch("/suggestions/{suggestion_id}")
def update_suggestion(
    suggestion_id: UUID,
    body: DraftUpdateRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_sync_db),
):
    """Update draft status or content."""
    ALLOWED_STATUSES = {"pending", "edited", "sent", "dismissed", "snoozed"}

    s = (
        db.query(DraftSuggestion)
        .filter(DraftSuggestion.id == suggestion_id, DraftSuggestion.user_id == user.id)
        .first()
    )
    if not s:
        raise HTTPException(404, "Suggestion not found")

    if body.status:
        if body.status not in ALLOWED_STATUSES:
            raise HTTPException(400, f"Invalid status: {body.status}")
        s.status = body.status

    if body.subject is not None:
        s.subject = body.subject
        if not body.status:
            s.status = "edited"

    if body.body is not None:
        s.body = body.body
        if not body.status:
            s.status = "edited"

    if body.match_confidence is not None:
        if body.match_confidence not in ("full_name", "last_name"):
            raise HTTPException(400, f"Invalid match_confidence: {body.match_confidence}")
        s.match_confidence = body.match_confidence

    if body.snoozed_until:
        from dateutil import parser as dateutil_parser

        s.snoozed_until = dateutil_parser.parse(body.snoozed_until)

    db.commit()
    return {"status": "updated", "id": str(s.id)}


@router.post("/suggestions/{suggestion_id}/regenerate")
def regenerate_suggestion(
    suggestion_id: UUID,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_sync_db),
):
    """Regenerate a draft with fresh voice profile."""
    s = (
        db.query(DraftSuggestion)
        .options(
            joinedload(DraftSuggestion.news_item).joinedload(CompanyNewsItem.company),
            joinedload(DraftSuggestion.contact).joinedload(Contact.company),
        )
        .filter(DraftSuggestion.id == suggestion_id, DraftSuggestion.user_id == user.id)
        .first()
    )
    if not s:
        raise HTTPException(404, "Suggestion not found")

    from src.services.voice.draft_service import EmailDraftService

    draft_service = EmailDraftService(db)
    result = draft_service.draft_email(
        user_id=str(user.id),
        recipient_email=s.contact.email,
        context=s.context_used,
        tone=s.tone,
    )

    s.subject = result.subject
    s.body = result.body
    s.model_used = result.model
    s.status = "pending"
    from datetime import datetime

    s.generated_at = datetime.now(UTC)

    db.commit()
    return _serialize_suggestion(s)


@router.post("/trigger")
def trigger_pipeline(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_sync_db),
):
    """Manually trigger the news intelligence pipeline."""
    from src.worker.news_tasks import run_news_pipeline

    task = run_news_pipeline.delay(str(user.id))
    return {"task_id": task.id, "status": "queued"}
