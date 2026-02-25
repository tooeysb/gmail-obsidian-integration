#!/bin/bash

# Monitor Gmail scan progress
# Shows real-time stats from database and job status

echo "📊 Gmail Scan Monitor"
echo "===================="
echo ""

# Load database URL
export DATABASE_URL=$(grep "^DATABASE_URL=" .env | cut -d= -f2- | tr -d '"')

# Activate virtual environment
source venv/bin/activate

# Run monitoring script
python3 << 'PYEOF'
import os
from datetime import datetime
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from src.models import Email, GmailAccount, SyncJob

engine = create_engine(os.environ['DATABASE_URL'])
SessionLocal = sessionmaker(bind=engine)
db = SessionLocal()

print("🔍 Current Status\n")
print("=" * 70)

# Account stats
accounts = db.query(GmailAccount).all()
print(f"\n📧 Email Counts by Account:\n")

total_emails = 0
for account in accounts:
    count = db.query(func.count(Email.id)).filter(Email.account_id == account.id).scalar()
    total_emails += count

    # Get date range
    first = db.query(func.min(Email.date)).filter(Email.account_id == account.id).scalar()
    last = db.query(func.max(Email.date)).filter(Email.account_id == account.id).scalar()

    print(f"   {account.account_label:20} ({account.account_email})")
    print(f"   └─ Emails: {count:,}")
    if first and last:
        print(f"   └─ Range: {first.strftime('%Y-%m-%d')} to {last.strftime('%Y-%m-%d')}")
    print()

print(f"📊 Total: {total_emails:,} emails across all accounts")

# Check when last email was processed
last_email_time = db.query(func.max(Email.created_at)).scalar()
if last_email_time:
    minutes_ago = int((datetime.now(last_email_time.tzinfo) - last_email_time).total_seconds() / 60)
    print(f"\n⏱️  Last Email Processed: {last_email_time.strftime('%Y-%m-%d %H:%M:%S')} ({minutes_ago} minutes ago)")

# Check for active jobs
jobs = db.query(SyncJob).order_by(SyncJob.started_at.desc()).limit(5).all()

if jobs:
    print(f"\n📋 Recent Scan Jobs:\n")
    for job in jobs:
        status_icon = {
            'running': '🔄',
            'completed': '✅',
            'failed': '❌',
            'queued': '⏳'
        }.get(job.status, '❓')

        print(f"   {status_icon} Job {str(job.id)[:8]}...")
        print(f"   └─ Status: {job.status}")
        print(f"   └─ Phase: {job.phase or 'Not started'}")
        print(f"   └─ Progress: {job.progress_pct}%")
        if job.emails_processed:
            print(f"   └─ Emails: {job.emails_processed:,}")
        if job.started_at:
            print(f"   └─ Started: {job.started_at.strftime('%Y-%m-%d %H:%M:%S')}")
        if job.updated_at:
            minutes_since_update = int((datetime.now(job.updated_at.tzinfo) - job.updated_at).total_seconds() / 60)
            print(f"   └─ Last Update: {job.updated_at.strftime('%Y-%m-%d %H:%M:%S')} ({minutes_since_update}m ago)")
        print()

print("=" * 70)

# Check for recent guardian activity
from src.models import GuardianEvent

recent_events = (
    db.query(GuardianEvent)
    .order_by(GuardianEvent.created_at.desc())
    .limit(5)
    .all()
)

if recent_events:
    print(f"\n🛡️  Recent Guardian Activity:\n")
    for event in recent_events:
        icon = {
            'stuck_detected': '⚠️ ',
            'job_killed': '🔪',
            'scan_restarted': '🔄',
            'error': '❌'
        }.get(event.event_type, '📝')

        minutes_ago = int((datetime.now(event.created_at.tzinfo if event.created_at.tzinfo else None) - event.created_at).total_seconds() / 60)

        print(f"   {icon} {event.event_type.replace('_', ' ').title()}")
        print(f"   └─ {event.description}")
        print(f"   └─ {minutes_ago} minutes ago")
        if event.metadata:
            print(f"   └─ Details: {event.metadata}")
        print()

print("=" * 70)

db.close()
PYEOF
