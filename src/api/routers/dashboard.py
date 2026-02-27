"""
Dashboard API routes for monitoring email processing stats.
"""

from datetime import datetime, timedelta

from fastapi import APIRouter, Depends
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from sqlalchemy import func
from sqlalchemy.orm import Session

from src.core.database import get_sync_db
from src.core.logging import get_logger
from src.models import Email, GmailAccount, SyncJob, GuardianEvent

logger = get_logger(__name__)

router = APIRouter()

# Gmail inbox totals (updated 2026-02-27)
# These are queried from Gmail API offline to avoid rate limits
GMAIL_TOTALS = {
    "tooey@procore.com": 1048991,
    "2e@procore.com": 87086,
    "tooey@hth-corp.com": 26266,
}


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


@router.get("/stats", response_model=EmailStatsResponse)
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
    gmail_accounts = db.query(GmailAccount).filter(GmailAccount.is_active == True).all()

    for account in gmail_accounts:
        # Get DB count for this account
        db_count = (
            db.query(func.count(Email.id)).filter(Email.account_id == account.id).scalar() or 0
        )

        # Get date range
        oldest = (
            db.query(func.min(Email.date)).filter(Email.account_id == account.id).scalar()
        )
        newest = (
            db.query(func.max(Email.date)).filter(Email.account_id == account.id).scalar()
        )

        # Get Gmail total from config (avoids API rate limits)
        gmail_total = GMAIL_TOTALS.get(account.account_email, 0)

        # Calculate progress
        progress_pct = 0.0
        if gmail_total is not None and gmail_total > 0:
            progress_pct = (db_count / gmail_total) * 100

        # Calculate emails per minute (last 15 minutes)
        fifteen_min_ago = datetime.utcnow() - timedelta(minutes=15)
        recent_count = (
            db.query(func.count(Email.id))
            .filter(Email.account_id == account.id, Email.created_at >= fifteen_min_ago)
            .scalar()
            or 0
        )
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
        db.query(GuardianEvent)
        .order_by(GuardianEvent.created_at.desc())
        .limit(10)
        .all()
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


@router.get("/widget", response_class=HTMLResponse)
async def get_widget() -> str:
    """
    Get embeddable HTML widget for dashboard.

    Returns:
        Self-contained HTML widget with auto-refresh and per-account stats
    """
    return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Email Processing Stats</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            display: flex;
            justify-content: center;
            align-items: flex-start;
            min-height: 100vh;
            padding: 20px;
        }

        .widget {
            background: white;
            border-radius: 20px;
            padding: 30px;
            box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
            max-width: 700px;
            width: 100%;
        }

        .title {
            font-size: 24px;
            font-weight: 700;
            color: #2d3748;
            margin-bottom: 20px;
            text-align: center;
        }

        .account-card {
            margin-bottom: 20px;
            padding: 20px;
            background: linear-gradient(135deg, #f6f8fb 0%, #eef1f5 100%);
            border-radius: 12px;
            border-left: 4px solid #667eea;
        }

        .account-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
        }

        .account-label {
            font-size: 16px;
            font-weight: 700;
            color: #2d3748;
        }

        .account-email {
            font-size: 12px;
            color: #718096;
        }

        .progress-bar-container {
            width: 100%;
            height: 8px;
            background: #e2e8f0;
            border-radius: 4px;
            overflow: hidden;
            margin-bottom: 10px;
        }

        .progress-bar {
            height: 100%;
            background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
            transition: width 0.3s ease;
        }

        .account-stats {
            display: grid;
            grid-template-columns: repeat(5, 1fr);
            gap: 15px;
            font-size: 13px;
        }

        .stat-item {
            text-align: center;
        }

        .stat-item-label {
            color: #718096;
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 4px;
        }

        .stat-item-value {
            color: #2d3748;
            font-size: 18px;
            font-weight: 700;
        }

        .total-card {
            padding: 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            border-radius: 12px;
            color: white;
            text-align: center;
            margin-bottom: 20px;
        }

        .total-label {
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 1px;
            opacity: 0.9;
            margin-bottom: 8px;
        }

        .total-value {
            font-size: 36px;
            font-weight: 800;
        }

        .status {
            display: flex;
            align-items: center;
            padding: 15px 20px;
            background: #f7fafc;
            border-radius: 10px;
        }

        .status-dot {
            width: 12px;
            height: 12px;
            border-radius: 50%;
            margin-right: 12px;
            animation: pulse 2s infinite;
        }

        .status-dot.active {
            background: #48bb78;
        }

        .status-dot.idle {
            background: #a0aec0;
        }

        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }

        .status-text {
            font-size: 14px;
            font-weight: 600;
            color: #4a5568;
        }

        .updated {
            text-align: center;
            font-size: 12px;
            color: #a0aec0;
            margin-top: 15px;
        }

        .error {
            color: #f56565;
            text-align: center;
            padding: 20px;
        }

        .monitor-section {
            margin-top: 20px;
            padding: 20px;
            background: #f7fafc;
            border-radius: 10px;
            border-top: 2px solid #e2e8f0;
        }

        .monitor-title {
            font-size: 14px;
            font-weight: 700;
            color: #2d3748;
            margin-bottom: 15px;
            display: flex;
            align-items: center;
        }

        .monitor-title::before {
            content: '🔍';
            margin-right: 8px;
        }

        .monitor-events {
            max-height: 200px;
            overflow-y: auto;
        }

        .monitor-event {
            padding: 10px;
            margin-bottom: 8px;
            background: white;
            border-radius: 6px;
            border-left: 3px solid #cbd5e0;
            font-size: 12px;
        }

        .monitor-event.monitor_started {
            border-left-color: #48bb78;
        }

        .monitor-event.stall_detected {
            border-left-color: #f56565;
        }

        .monitor-event.scan_restarted {
            border-left-color: #ed8936;
        }

        .monitor-event.slow_processing {
            border-left-color: #ecc94b;
        }

        .monitor-event.restart_failed {
            border-left-color: #e53e3e;
        }

        .event-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 5px;
        }

        .event-type {
            font-weight: 600;
            color: #4a5568;
            text-transform: capitalize;
        }

        .event-time {
            font-size: 11px;
            color: #a0aec0;
        }

        .event-description {
            color: #718096;
            line-height: 1.4;
        }

        .event-account {
            display: inline-block;
            margin-top: 5px;
            padding: 2px 8px;
            background: #edf2f7;
            border-radius: 4px;
            font-size: 10px;
            color: #4a5568;
            font-weight: 600;
        }
    </style>
</head>
<body>
    <div class="widget">
        <div class="title">📧 Email Processing Dashboard</div>

        <div id="content">
            <div class="total-card">
                <div class="total-label">Total Emails Processed</div>
                <div class="total-value" id="total">...</div>
            </div>

            <div id="accounts-container"></div>

            <div class="status">
                <div class="status-dot" id="status-dot"></div>
                <div class="status-text" id="status-text">Loading...</div>
            </div>

            <div class="monitor-section">
                <div class="monitor-title">Monitor Activity</div>
                <div class="monitor-events" id="monitor-events">
                    <div style="text-align: center; color: #a0aec0; padding: 20px;">
                        Loading monitor activity...
                    </div>
                </div>
            </div>

            <div class="updated" id="updated">Updated just now</div>
        </div>

        <div id="error" class="error" style="display: none;"></div>
    </div>

    <script>
        const API_URL = window.location.origin + '/dashboard/stats';

        function formatNumber(num) {
            return num ? num.toLocaleString() : '0';
        }

        function renderMonitorEvents(events) {
            const container = document.getElementById('monitor-events');

            if (!events || events.length === 0) {
                container.innerHTML = '<div style="text-align: center; color: #a0aec0; padding: 20px;">No recent monitor activity</div>';
                return;
            }

            container.innerHTML = events.map(event => {
                const accountBadge = event.account_email
                    ? `<div class="event-account">${event.account_email}</div>`
                    : '';

                return `
                    <div class="monitor-event ${event.event_type}">
                        <div class="event-header">
                            <div class="event-type">${event.event_type.replace(/_/g, ' ')}</div>
                            <div class="event-time">${event.time_ago}</div>
                        </div>
                        <div class="event-description">${event.description}</div>
                        ${accountBadge}
                    </div>
                `;
            }).join('');
        }

        function renderAccounts(accounts) {
            const container = document.getElementById('accounts-container');
            container.innerHTML = '';

            accounts.forEach(account => {
                const card = document.createElement('div');
                card.className = 'account-card';

                const gmailTotal = account.gmail_total || 0;
                const dbCount = account.db_count || 0;
                const progressPct = account.progress_pct || 0;
                const remaining = gmailTotal - dbCount;
                const emailsPerMin = account.emails_per_minute || 0;
                const estimatedCompletion = account.estimated_completion || 'N/A';

                card.innerHTML = `
                    <div class="account-header">
                        <div>
                            <div class="account-label">${account.account_label}</div>
                            <div class="account-email">${account.account_email}</div>
                        </div>
                        <div style="font-size: 18px; font-weight: 700; color: #667eea;">
                            ${progressPct.toFixed(1)}%
                        </div>
                    </div>
                    <div class="progress-bar-container">
                        <div class="progress-bar" style="width: ${progressPct}%"></div>
                    </div>
                    <div class="account-stats">
                        <div class="stat-item">
                            <div class="stat-item-label">Gmail Total</div>
                            <div class="stat-item-value">${formatNumber(gmailTotal)}</div>
                        </div>
                        <div class="stat-item">
                            <div class="stat-item-label">Processed</div>
                            <div class="stat-item-value">${formatNumber(dbCount)}</div>
                        </div>
                        <div class="stat-item">
                            <div class="stat-item-label">Remaining</div>
                            <div class="stat-item-value">${formatNumber(remaining)}</div>
                        </div>
                        <div class="stat-item">
                            <div class="stat-item-label">Emails/Min</div>
                            <div class="stat-item-value">${emailsPerMin.toFixed(1)}</div>
                        </div>
                        <div class="stat-item">
                            <div class="stat-item-label">ETA</div>
                            <div class="stat-item-value">${estimatedCompletion}</div>
                        </div>
                    </div>
                `;

                container.appendChild(card);
            });
        }

        async function updateStats() {
            try {
                const response = await fetch(API_URL);
                if (!response.ok) throw new Error('Failed to fetch stats');

                const data = await response.json();

                // Update total emails
                document.getElementById('total').textContent = formatNumber(data.total_emails);

                // Render account cards
                if (data.accounts && data.accounts.length > 0) {
                    renderAccounts(data.accounts);
                }

                // Render monitor events
                if (data.monitor_events) {
                    renderMonitorEvents(data.monitor_events);
                }

                // Update status
                const statusDot = document.getElementById('status-dot');
                const statusText = document.getElementById('status-text');

                if (data.active_scans > 0) {
                    statusDot.className = 'status-dot active';
                    statusText.textContent = `Active scan in progress (${data.current_job_progress}%)`;
                } else {
                    statusDot.className = 'status-dot idle';
                    statusText.textContent = 'No active scans';
                }

                // Update timestamp
                const now = new Date().toLocaleTimeString();
                document.getElementById('updated').textContent = `Updated at ${now}`;

                // Hide error, show content
                document.getElementById('error').style.display = 'none';
                document.getElementById('content').style.display = 'block';

            } catch (error) {
                console.error('Error fetching stats:', error);
                document.getElementById('error').textContent = 'Failed to load stats. Retrying...';
                document.getElementById('error').style.display = 'block';
            }
        }

        // Initial load
        updateStats();

        // Refresh every 10 seconds
        setInterval(updateStats, 10000);
    </script>
</body>
</html>
"""
