#!/usr/bin/env python3
"""
Email Sync Monitor - Watchdog for Gmail scanning process.

This script monitors email sync progress and automatically takes corrective action
when processing stalls or slows down unexpectedly.

Key functions:
- Monitor emails/min throughput per account every 60 seconds
- Detect stalls (0 emails/min for 5+ consecutive checks)
- Auto-restart hung tasks via Celery
- Alert on persistent issues
- Log all monitoring activity

Run as: python monitor_scan.py
Or as Heroku dyno: monitor: python monitor_scan.py
"""
import os
import time
from datetime import datetime, timedelta

import requests
from celery import Celery
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

from src.core.config import settings
from src.core.logging import get_logger
from src.models import Email, EmailQueue, GmailAccount, GuardianEvent

logger = get_logger(__name__)

# Dashboard API endpoint
DASHBOARD_URL = os.getenv("DASHBOARD_URL", "http://localhost:8000/dashboard/stats")

# Monitoring thresholds
STALL_THRESHOLD_MINUTES = 5  # Consider stalled if 0 emails/min for this long
CHECK_INTERVAL_SECONDS = 60  # Check every 60 seconds
MIN_EXPECTED_RATE = 10.0  # Minimum emails/min to consider "healthy"
MAX_PHASE2_WORKERS = int(os.getenv("MAX_PHASE2_WORKERS", "4"))

# Celery app for triggering tasks
celery_app = Celery("monitor")
celery_app.config_from_object("src.worker.celery_config")

# Database setup
engine = create_engine(settings.database_url)
SessionLocal = sessionmaker(bind=engine)


class AccountMonitor:
    """Tracks monitoring state for a single account."""

    def __init__(self, account_email: str):
        self.account_email = account_email
        self.consecutive_stalls = 0
        self.last_db_count = 0
        self.last_check_time = None
        self.restart_count = 0
        self.last_restart_time = None

    def update(self, current_db_count: int, emails_per_min: float) -> dict:
        """Update monitoring state and return action recommendation."""
        now = datetime.utcnow()

        # Calculate actual progress
        emails_added = 0
        if self.last_db_count > 0:
            emails_added = current_db_count - self.last_db_count

        # Update state
        self.last_db_count = current_db_count

        # Check for stall
        is_stalled = emails_per_min == 0.0
        is_slow = 0 < emails_per_min < MIN_EXPECTED_RATE

        if is_stalled:
            self.consecutive_stalls += 1
        else:
            self.consecutive_stalls = 0

        # Determine action
        action = None
        reason = None

        # Don't restart too frequently (minimum 10 minutes between restarts)
        can_restart = True
        if self.last_restart_time:
            minutes_since_restart = (now - self.last_restart_time).total_seconds() / 60
            can_restart = minutes_since_restart >= 10

        if self.consecutive_stalls >= STALL_THRESHOLD_MINUTES and can_restart:
            action = "restart"
            reason = f"Stalled for {self.consecutive_stalls} minutes (0 emails/min)"
        elif is_slow:
            action = "alert"
            reason = f"Processing slowly ({emails_per_min:.1f} emails/min, expected >{MIN_EXPECTED_RATE})"

        self.last_check_time = now

        return {
            "account": self.account_email,
            "status": "stalled" if is_stalled else ("slow" if is_slow else "healthy"),
            "emails_per_min": emails_per_min,
            "emails_added_last_check": emails_added,
            "consecutive_stalls": self.consecutive_stalls,
            "action": action,
            "reason": reason,
            "can_restart": can_restart,
        }

    def record_restart(self):
        """Record that we restarted this account's task."""
        self.restart_count += 1
        self.last_restart_time = datetime.utcnow()
        self.consecutive_stalls = 0
        logger.info(
            f"[{self.account_email}] Recorded restart #{self.restart_count} at {self.last_restart_time}"
        )


def get_account_stats() -> dict:
    """Fetch current stats from dashboard API."""
    try:
        response = requests.get(DASHBOARD_URL, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Failed to fetch dashboard stats: {e}")
        return None


def get_account_user_id(account_email: str) -> str | None:
    """Get user_id for an account."""
    db = SessionLocal()
    try:
        account = (
            db.query(GmailAccount)
            .filter(GmailAccount.account_email == account_email)
            .first()
        )
        return str(account.user_id) if account else None
    except Exception as e:
        logger.error(f"Error fetching user_id for {account_email}: {e}")
        return None
    finally:
        db.close()


def log_event(event_type: str, description: str, account_email: str = None, metadata: dict = None):
    """Log a monitoring event to the database."""
    db = SessionLocal()
    try:
        event_meta = metadata or {}
        if account_email:
            event_meta["account_email"] = account_email

        event = GuardianEvent(
            event_type=event_type,
            description=description,
            event_metadata=event_meta,
        )
        db.add(event)
        db.commit()
    except Exception as e:
        logger.error(f"Failed to log event: {e}")
        db.rollback()
    finally:
        db.close()


def check_queue_and_spawn_workers():
    """
    Check EmailQueue for unclaimed items and spawn Phase 2 workers directly.

    This is the primary self-healing mechanism: every 60 seconds, the monitor
    checks if there are queued emails waiting to be processed and ensures
    enough workers are running to handle them.

    Also recovers stale claims (items claimed >15 min ago that were never
    completed) to prevent deadlocks where all items are claimed but no
    workers are running.
    """
    db = SessionLocal()
    try:
        # Recover stale claims: unclaim items where workers crashed/timed out
        stale_threshold = datetime.utcnow() - timedelta(minutes=15)
        stale_count = (
            db.query(EmailQueue)
            .filter(
                EmailQueue.claimed_at < stale_threshold,
                EmailQueue.claimed_by != None,
            )
            .update({"claimed_by": None, "claimed_at": None}, synchronize_session=False)
        )
        if stale_count > 0:
            db.commit()
            logger.info(
                f"Recovered {stale_count} stale queue claims (claimed >15 min ago)"
            )
            log_event(
                "stale_claims_recovered",
                f"Recovered {stale_count} stale queue claims",
                metadata={"stale_count": stale_count},
            )

        # Get unclaimed counts per account
        queue_stats = (
            db.query(
                EmailQueue.account_id,
                func.count(EmailQueue.id).label("unclaimed"),
            )
            .filter(EmailQueue.claimed_by == None)
            .group_by(EmailQueue.account_id)
            .all()
        )

        if not queue_stats:
            return

        for account_id, unclaimed_count in queue_stats:
            # Estimate active workers: items claimed in the last 5 minutes
            active_cutoff = datetime.utcnow() - timedelta(minutes=5)
            active_workers = (
                db.query(func.count(func.distinct(EmailQueue.claimed_by)))
                .filter(
                    EmailQueue.account_id == account_id,
                    EmailQueue.claimed_by != None,
                    EmailQueue.claimed_at >= active_cutoff,
                )
                .scalar()
            ) or 0

            # Look up account email for logging
            account = db.query(GmailAccount).filter(GmailAccount.id == account_id).first()
            account_label = account.account_email if account else str(account_id)

            workers_needed = MAX_PHASE2_WORKERS - active_workers
            if unclaimed_count > 0 and workers_needed > 0:
                # Don't spawn more workers than needed for the remaining items
                batch_size = 200  # Matches BATCH_SIZE in id_first_tasks
                max_useful = (unclaimed_count + batch_size - 1) // batch_size
                workers_to_spawn = min(workers_needed, max_useful)

                logger.info(
                    f"[{account_label}] Queue: {unclaimed_count} unclaimed, "
                    f"{active_workers} active workers, spawning {workers_to_spawn} workers"
                )

                from src.worker.id_first_tasks import fetch_message_batch

                for _ in range(workers_to_spawn):
                    fetch_message_batch.delay(str(account_id))

                log_event(
                    "queue_workers_spawned",
                    f"Spawned {workers_to_spawn} Phase 2 workers for {account_label}",
                    account_email=account_label,
                    metadata={
                        "unclaimed": unclaimed_count,
                        "active_workers": active_workers,
                        "workers_spawned": workers_to_spawn,
                    },
                )
            elif unclaimed_count > 0:
                logger.info(
                    f"[{account_label}] Queue: {unclaimed_count} unclaimed, "
                    f"{active_workers} active workers (at capacity)"
                )

    except Exception as e:
        logger.error(f"Error checking queue: {e}", exc_info=True)
    finally:
        db.close()


def restart_account_sync(account_email: str, reason: str):
    """Restart email sync for a specific account via Celery.

    Queue-aware: if EmailQueue already has unclaimed items for this account,
    spawns Phase 2 workers directly instead of re-fetching IDs from Gmail.
    """
    logger.info(f"[{account_email}] Triggering restart: {reason}")

    # Get user_id for this account
    user_id = get_account_user_id(account_email)
    if not user_id:
        logger.error(f"[{account_email}] Cannot restart - user_id not found")
        return False

    try:
        # Check if there are already unclaimed items in the queue for this account
        db = SessionLocal()
        try:
            account = (
                db.query(GmailAccount)
                .filter(GmailAccount.account_email == account_email)
                .first()
            )
            if account:
                unclaimed = (
                    db.query(func.count(EmailQueue.id))
                    .filter(
                        EmailQueue.account_id == account.id,
                        EmailQueue.claimed_by == None,
                    )
                    .scalar()
                ) or 0

                if unclaimed > 0:
                    # Skip Phase 1 - spawn Phase 2 workers directly
                    logger.info(
                        f"[{account_email}] {unclaimed} unclaimed items in queue, "
                        f"spawning Phase 2 workers directly (skipping ID re-fetch)"
                    )
                    from src.worker.id_first_tasks import fetch_message_batch

                    workers = min(MAX_PHASE2_WORKERS, (unclaimed + 199) // 200)
                    for _ in range(workers):
                        fetch_message_batch.delay(str(account.id))
                    return True
        finally:
            db.close()

        # No queued items - fall through to Phase 1 (re-fetch IDs from Gmail)
        from src.worker.id_first_tasks import fetch_all_message_ids

        task = fetch_all_message_ids.delay(user_id=user_id)
        logger.info(f"[{account_email}] Started new ID-first scan task: {task.id}")
        return True

    except Exception as e:
        logger.error(f"[{account_email}] Failed to restart: {e}")
        return False


def monitor_loop():
    """Main monitoring loop."""
    logger.info("=" * 80)
    logger.info("EMAIL SYNC MONITOR STARTED")
    logger.info(f"Check interval: {CHECK_INTERVAL_SECONDS}s")
    logger.info(f"Stall threshold: {STALL_THRESHOLD_MINUTES} minutes")
    logger.info(f"Minimum expected rate: {MIN_EXPECTED_RATE} emails/min")
    logger.info("=" * 80)

    # Log monitor start
    log_event("monitor_started", "Email sync monitor started", metadata={
        "check_interval": CHECK_INTERVAL_SECONDS,
        "stall_threshold": STALL_THRESHOLD_MINUTES,
        "min_expected_rate": MIN_EXPECTED_RATE,
    })

    # Initialize monitors for each account
    monitors = {}

    iteration = 0

    while True:
        try:
            iteration += 1
            logger.info(f"\n[Check #{iteration}] {datetime.utcnow().isoformat()}")

            # Check queue and spawn workers if needed (primary self-healing)
            check_queue_and_spawn_workers()

            # Fetch current stats
            stats = get_account_stats()
            if not stats:
                logger.warning("Failed to fetch stats, retrying in 60s...")
                time.sleep(CHECK_INTERVAL_SECONDS)
                continue

            # Process each account
            accounts = stats.get("accounts", [])
            for account_data in accounts:
                account_email = account_data["account_email"]

                # Initialize monitor if needed
                if account_email not in monitors:
                    monitors[account_email] = AccountMonitor(account_email)

                monitor = monitors[account_email]

                # Update monitor state
                result = monitor.update(
                    current_db_count=account_data["db_count"],
                    emails_per_min=account_data.get("emails_per_minute", 0.0),
                )

                # Don't restart accounts that are complete (>= 99.5%)
                progress_pct = account_data.get("progress_pct", 0.0)
                if result["action"] == "restart" and progress_pct >= 99.5:
                    logger.info(
                        f"⏭️  [{account_email}] Skipping restart - account is {progress_pct:.1f}% complete"
                    )
                    result["action"] = None  # Clear the restart action

                # Log status
                status_emoji = {
                    "healthy": "✅",
                    "slow": "⚠️",
                    "stalled": "🔴",
                }
                emoji = status_emoji.get(result["status"], "❓")

                logger.info(
                    f"{emoji} [{account_email}] {result['status'].upper()} - "
                    f"{result['emails_per_min']:.1f} emails/min, "
                    f"+{result['emails_added_last_check']} since last check, "
                    f"stalls: {result['consecutive_stalls']}"
                )

                # Take action if needed
                if result["action"] == "restart":
                    logger.warning(f"🚨 [{account_email}] ACTION REQUIRED: {result['reason']}")

                    # Log stall detection
                    log_event(
                        "stall_detected",
                        f"Account stalled: {result['reason']}",
                        account_email=account_email,
                        metadata={
                            "emails_per_min": result['emails_per_min'],
                            "consecutive_stalls": result['consecutive_stalls'],
                        }
                    )

                    if restart_account_sync(account_email, result["reason"]):
                        monitor.record_restart()
                        logger.info(f"✅ [{account_email}] Restart initiated successfully")

                        # Log successful restart
                        log_event(
                            "scan_restarted",
                            f"Successfully restarted sync for {account_email}",
                            account_email=account_email,
                            metadata={"restart_count": monitor.restart_count}
                        )
                    else:
                        logger.error(f"❌ [{account_email}] Restart failed")

                        # Log restart failure
                        log_event(
                            "restart_failed",
                            f"Failed to restart sync for {account_email}",
                            account_email=account_email,
                        )

                elif result["action"] == "alert":
                    logger.warning(f"⚠️  [{account_email}] {result['reason']}")

                    # Log slow processing alert
                    log_event(
                        "slow_processing",
                        f"Account processing slowly: {result['reason']}",
                        account_email=account_email,
                        metadata={"emails_per_min": result['emails_per_min']}
                    )

            # Wait before next check
            time.sleep(CHECK_INTERVAL_SECONDS)

        except KeyboardInterrupt:
            logger.info("\n🛑 Monitor stopped by user")
            break
        except Exception as e:
            logger.error(f"Monitor error: {e}", exc_info=True)
            logger.info("Continuing monitoring after error...")
            time.sleep(CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    monitor_loop()
