"""
Dashboard API routes for monitoring email processing stats.
"""

from datetime import datetime

from fastapi import APIRouter, Depends
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from sqlalchemy import func
from sqlalchemy.orm import Session

from src.core.database import get_sync_db
from src.core.logging import get_logger
from src.models import Email, SyncJob

logger = get_logger(__name__)

router = APIRouter()


class EmailStatsResponse(BaseModel):
    """Email processing statistics."""

    total_emails: int
    minutes_since_last_email: int | None
    last_email_time: str | None
    active_scans: int
    current_job_id: str | None
    current_job_progress: int | None


@router.get("/stats", response_model=EmailStatsResponse)
async def get_email_stats(db: Session = Depends(get_sync_db)) -> EmailStatsResponse:
    """
    Get current email processing statistics.

    Returns:
        Email count, time since last email, and active scan info
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

    return EmailStatsResponse(
        total_emails=total_emails,
        minutes_since_last_email=minutes_since_last,
        last_email_time=last_email_str,
        active_scans=active_scans,
        current_job_id=current_job_id,
        current_job_progress=current_job_progress,
    )


@router.get("/widget", response_class=HTMLResponse)
async def get_widget() -> str:
    """
    Get embeddable HTML widget for dashboard.

    Returns:
        Self-contained HTML widget with auto-refresh
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
            align-items: center;
            min-height: 100vh;
            padding: 20px;
        }

        .widget {
            background: white;
            border-radius: 20px;
            padding: 40px;
            box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
            max-width: 500px;
            width: 100%;
        }

        .title {
            font-size: 24px;
            font-weight: 700;
            color: #2d3748;
            margin-bottom: 30px;
            text-align: center;
        }

        .stat {
            margin-bottom: 25px;
            padding: 20px;
            background: linear-gradient(135deg, #f6f8fb 0%, #eef1f5 100%);
            border-radius: 12px;
            border-left: 4px solid #667eea;
        }

        .stat-label {
            font-size: 13px;
            font-weight: 600;
            color: #718096;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 8px;
        }

        .stat-value {
            font-size: 42px;
            font-weight: 800;
            color: #2d3748;
            line-height: 1;
        }

        .stat-value.good {
            color: #48bb78;
        }

        .stat-value.warning {
            color: #ed8936;
        }

        .stat-value.danger {
            color: #f56565;
        }

        .status {
            display: flex;
            align-items: center;
            padding: 15px 20px;
            background: #f7fafc;
            border-radius: 10px;
            margin-top: 20px;
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
            margin-top: 20px;
        }

        .error {
            color: #f56565;
            text-align: center;
            padding: 20px;
        }
    </style>
</head>
<body>
    <div class="widget">
        <div class="title">📧 Email Processing</div>

        <div id="content">
            <div class="stat">
                <div class="stat-label">Total Emails Processed</div>
                <div class="stat-value" id="total">...</div>
            </div>

            <div class="stat">
                <div class="stat-label">Minutes Since Last Email</div>
                <div class="stat-value" id="minutes">...</div>
            </div>

            <div class="status">
                <div class="status-dot" id="status-dot"></div>
                <div class="status-text" id="status-text">Loading...</div>
            </div>

            <div class="updated" id="updated">Updated just now</div>
        </div>

        <div id="error" class="error" style="display: none;"></div>
    </div>

    <script>
        const API_URL = window.location.origin + '/dashboard/stats';

        function formatNumber(num) {
            return num.toLocaleString();
        }

        function getColorClass(minutes) {
            if (minutes === null) return '';
            if (minutes < 5) return 'good';
            if (minutes < 15) return 'warning';
            return 'danger';
        }

        async function updateStats() {
            try {
                const response = await fetch(API_URL);
                if (!response.ok) throw new Error('Failed to fetch stats');

                const data = await response.json();

                // Update total emails
                document.getElementById('total').textContent = formatNumber(data.total_emails);

                // Update minutes since last email
                const minutesEl = document.getElementById('minutes');
                if (data.minutes_since_last_email !== null) {
                    minutesEl.textContent = data.minutes_since_last_email;
                    minutesEl.className = 'stat-value ' + getColorClass(data.minutes_since_last_email);
                } else {
                    minutesEl.textContent = '—';
                    minutesEl.className = 'stat-value';
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
