"""
Dashboard API routes for monitoring email processing stats.
"""

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from fastapi import APIRouter, Depends
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from sqlalchemy import func
from sqlalchemy.orm import Session

from src.api.middleware.auth import require_api_key
from src.core.database import get_sync_db
from src.core.logging import get_logger
from src.models import Email, GmailAccount, GuardianEvent, SyncJob

logger = get_logger(__name__)

router = APIRouter()


def _load_gmail_totals() -> dict[str, int]:
    """Load Gmail totals from GMAIL_TOTALS_JSON env var (queried offline to avoid rate limits)."""
    raw = os.environ.get("GMAIL_TOTALS_JSON", "{}")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


GMAIL_TOTALS = _load_gmail_totals()


class AccountStats(BaseModel):
    """Per-account email statistics."""

    account_label: str
    account_email: str
    gmail_total: int | None
    db_count: int
    progress_pct: float
    oldest_email: str | None
    newest_email: str | None
    emails_per_minute: float | None
    estimated_completion: str | None


class MonitorEvent(BaseModel):
    """Monitor activity event."""

    event_type: str
    description: str
    account_email: str | None
    created_at: str
    time_ago: str  # Human-readable time ago (e.g., "2 minutes ago")


class EmailStatsResponse(BaseModel):
    """Email processing statistics."""

    total_emails: int
    minutes_since_last_email: int | None
    last_email_time: str | None
    active_scans: int
    current_job_id: str | None
    current_job_progress: int | None
    accounts: list[AccountStats]
    monitor_events: list[MonitorEvent]


@router.get("/stats", response_model=EmailStatsResponse, dependencies=[Depends(require_api_key)])
async def get_email_stats(db: Session = Depends(get_sync_db)) -> EmailStatsResponse:
    """
    Get current email processing statistics.

    Returns:
        Email count, time since last email, active scan info, and per-account stats
    """
    # Get total email count
    total_emails = db.query(func.count(Email.id)).scalar() or 0

    # Get last email time
    last_email_time = db.query(func.max(Email.created_at)).scalar()
    minutes_since_last = None
    last_email_str = None

    if last_email_time:
        now = datetime.now(last_email_time.tzinfo if last_email_time.tzinfo else None)
        minutes_since_last = int((now - last_email_time).total_seconds() / 60)
        last_email_str = last_email_time.isoformat()

    # Get active scan info
    active_job = (
        db.query(SyncJob)
        .filter(SyncJob.status == "running")
        .order_by(SyncJob.started_at.desc())
        .first()
    )

    active_scans = 1 if active_job else 0
    current_job_id = str(active_job.id) if active_job else None
    current_job_progress = active_job.progress_pct if active_job else None

    # Get per-account stats
    accounts_list = []
    gmail_accounts = db.query(GmailAccount).filter(GmailAccount.is_active is True).all()

    # Bulk query: count, min(date), max(date) per account in ONE query instead of 3 * N
    account_stats_q = (
        db.query(
            Email.account_id,
            func.count(Email.id).label("cnt"),
            func.min(Email.date).label("oldest"),
            func.max(Email.date).label("newest"),
        )
        .group_by(Email.account_id)
        .all()
    )
    stats_by_account = {
        row.account_id: {"cnt": row.cnt, "oldest": row.oldest, "newest": row.newest}
        for row in account_stats_q
    }

    # Bulk query: recent email count per account (last 15 min) in ONE query
    fifteen_min_ago = datetime.utcnow() - timedelta(minutes=15)
    recent_stats_q = (
        db.query(
            Email.account_id,
            func.count(Email.id).label("cnt"),
        )
        .filter(Email.created_at >= fifteen_min_ago)
        .group_by(Email.account_id)
        .all()
    )
    recent_by_account = {row.account_id: row.cnt for row in recent_stats_q}

    for account in gmail_accounts:
        acct_stats = stats_by_account.get(account.id, {})
        db_count = acct_stats.get("cnt", 0)
        oldest = acct_stats.get("oldest")
        newest = acct_stats.get("newest")

        # Get Gmail total from config (avoids API rate limits)
        gmail_total = GMAIL_TOTALS.get(account.account_email, 0)

        # Calculate progress
        progress_pct = 0.0
        if gmail_total is not None and gmail_total > 0:
            progress_pct = (db_count / gmail_total) * 100

        # Calculate emails per minute (last 15 minutes)
        recent_count = recent_by_account.get(account.id, 0)
        emails_per_minute = round(recent_count / 15.0, 1) if recent_count > 0 else 0.0

        # Calculate estimated completion time
        estimated_completion = None
        if emails_per_minute > 0 and gmail_total:
            remaining = gmail_total - db_count
            if remaining > 0:
                minutes_remaining = remaining / emails_per_minute
                hours_remaining = minutes_remaining / 60

                if hours_remaining < 1:
                    estimated_completion = f"{int(minutes_remaining)}m"
                elif hours_remaining < 24:
                    estimated_completion = f"{int(hours_remaining)}h {int(minutes_remaining % 60)}m"
                else:
                    days = int(hours_remaining / 24)
                    hours = int(hours_remaining % 24)
                    estimated_completion = f"{days}d {hours}h"

        accounts_list.append(
            AccountStats(
                account_label=account.account_label,
                account_email=account.account_email,
                gmail_total=gmail_total,
                db_count=db_count,
                progress_pct=round(progress_pct, 1),
                oldest_email=oldest.isoformat() if oldest else None,
                newest_email=newest.isoformat() if newest else None,
                emails_per_minute=emails_per_minute,
                estimated_completion=estimated_completion,
            )
        )

    # Get recent monitor events (last 10)
    monitor_events_list = []
    recent_events = (
        db.query(GuardianEvent).order_by(GuardianEvent.created_at.desc()).limit(10).all()
    )

    for event in recent_events:
        # Calculate time ago
        now = datetime.now(event.created_at.tzinfo if event.created_at.tzinfo else None)
        delta = now - event.created_at

        if delta.total_seconds() < 60:
            time_ago = f"{int(delta.total_seconds())}s ago"
        elif delta.total_seconds() < 3600:
            time_ago = f"{int(delta.total_seconds() / 60)}m ago"
        elif delta.total_seconds() < 86400:
            time_ago = f"{int(delta.total_seconds() / 3600)}h ago"
        else:
            time_ago = f"{int(delta.total_seconds() / 86400)}d ago"

        # Extract account email from metadata if available
        account_email = None
        if event.event_metadata:
            account_email = event.event_metadata.get("account_email")

        monitor_events_list.append(
            MonitorEvent(
                event_type=event.event_type,
                description=event.description,
                account_email=account_email,
                created_at=event.created_at.isoformat(),
                time_ago=time_ago,
            )
        )

    return EmailStatsResponse(
        total_emails=total_emails,
        minutes_since_last_email=minutes_since_last,
        last_email_time=last_email_str,
        active_scans=active_scans,
        current_job_id=current_job_id,
        current_job_progress=current_job_progress,
        accounts=accounts_list,
        monitor_events=monitor_events_list,
    )


_WIDGET_HTML_PATH = (
    Path(__file__).resolve().parent.parent.parent / "static" / "dashboard" / "widget.html"
)


@router.get("/widget", response_class=HTMLResponse)
async def get_widget() -> str:
    """Get embeddable HTML widget for dashboard."""
    return _WIDGET_HTML_PATH.read_text()
