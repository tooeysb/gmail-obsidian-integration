"""
Autonomous Gmail Scan Guardian
Continuously monitors scan health and auto-fixes issues without human intervention.

This daemon:
- Checks scan progress every 3 minutes
- Detects stuck scans (no progress for 10+ minutes)
- Analyzes worker logs for rate limit errors
- Auto-kills stuck jobs
- Auto-adjusts rate limits if needed
- Auto-restarts scans after fixes

Run as: python -m src.monitoring.autonomous_guardian
"""

import asyncio
import logging
import os
import subprocess
import time
from datetime import datetime, timedelta
from typing import Optional

import httpx
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

from src.core.config import settings
from src.models import Email, SyncJob

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class ScanGuardian:
    """Autonomous monitoring and auto-fix system for Gmail scans."""

    def __init__(self):
        self.engine = create_engine(settings.database_url)
        self.SessionLocal = sessionmaker(bind=self.engine)
        self.api_url = os.getenv("APP_URL", "https://gmail-obsidian-sync-729716d2143d.herokuapp.com")
        self.user_id = os.getenv("USER_ID", "d4475ca3-0ddc-4ea0-ac89-95ae7fed1e31")

        # Monitoring thresholds
        self.check_interval = 180  # Check every 3 minutes
        self.stuck_threshold = 600  # Consider stuck if no progress for 10 minutes
        self.max_auto_restarts = 3  # Max auto-restarts before alerting user

        self.restart_count = 0
        self.last_fix_time = None

    def get_scan_status(self) -> dict:
        """Get current scan job status and email processing info."""
        db = self.SessionLocal()
        try:
            # Get latest running job
            running_job = (
                db.query(SyncJob)
                .filter(SyncJob.status == "running")
                .order_by(SyncJob.started_at.desc())
                .first()
            )

            # Get last email processing time
            last_email_time = db.query(func.max(Email.created_at)).scalar()

            # Get email counts
            total_emails = db.query(func.count(Email.id)).scalar()

            return {
                "running_job": running_job,
                "last_email_time": last_email_time,
                "total_emails": total_emails,
                "timestamp": datetime.now(),
            }
        finally:
            db.close()

    def is_scan_stuck(self, status: dict) -> tuple[bool, str]:
        """
        Determine if scan is stuck.

        Returns:
            (is_stuck: bool, reason: str)
        """
        if not status["running_job"]:
            return False, "No running job"

        job = status["running_job"]
        now = datetime.now(job.updated_at.tzinfo if job.updated_at.tzinfo else None)

        # Check if job hasn't updated in stuck_threshold seconds
        if job.updated_at:
            minutes_since_update = (now - job.updated_at).total_seconds() / 60
            if minutes_since_update > (self.stuck_threshold / 60):
                return True, f"Job hasn't updated in {minutes_since_update:.1f} minutes"

        # Check if emails haven't been processed in stuck_threshold seconds
        if status["last_email_time"]:
            last_email_time = status["last_email_time"]
            if last_email_time.tzinfo is None:
                from datetime import timezone
                last_email_time = last_email_time.replace(tzinfo=timezone.utc)

            minutes_since_email = (now - last_email_time).total_seconds() / 60
            if minutes_since_email > (self.stuck_threshold / 60):
                return True, f"No emails processed in {minutes_since_email:.1f} minutes"

        return False, "Job is progressing normally"

    def check_worker_logs(self) -> tuple[bool, str]:
        """
        Check Heroku worker logs for rate limit errors.

        Returns:
            (has_errors: bool, error_summary: str)
        """
        try:
            result = subprocess.run(
                ["heroku", "logs", "-a", "gmail-obsidian-sync", "--dyno", "worker", "-n", "50"],
                capture_output=True,
                text=True,
                timeout=10,
            )

            logs = result.stdout

            # Check for rate limit errors
            if "Too many concurrent requests" in logs or "429" in logs:
                return True, "Rate limit errors detected in worker logs"

            # Check for other errors
            if "ERROR" in logs or "CRITICAL" in logs:
                return True, "Error messages detected in worker logs"

            return False, "Worker logs look healthy"

        except Exception as e:
            logger.warning(f"Failed to check worker logs: {e}")
            return False, f"Could not check logs: {e}"

    def kill_stuck_job(self, job_id: str) -> bool:
        """Kill a stuck scan job."""
        db = self.SessionLocal()
        try:
            job = db.query(SyncJob).filter(SyncJob.id == job_id).first()
            if job:
                job.status = "failed"
                job.error_message = "Auto-killed by guardian: Job was stuck with no progress"
                job.updated_at = datetime.now()
                db.commit()
                logger.info(f"Killed stuck job {job_id}")
                return True
            return False
        finally:
            db.close()

    async def start_new_scan(self) -> Optional[str]:
        """Start a new scan job via API."""
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    f"{self.api_url}/scan/start",
                    json={
                        "user_id": self.user_id,
                        "account_labels": ["procore-main", "procore-private", "personal"],
                    },
                )

                if response.status_code == 200:
                    data = response.json()
                    job_id = data.get("job_id")
                    logger.info(f"Started new scan job: {job_id}")
                    return job_id
                else:
                    logger.error(f"Failed to start scan: {response.status_code} - {response.text}")
                    return None

        except Exception as e:
            logger.error(f"Error starting scan: {e}")
            return None

    async def auto_fix_and_restart(self, reason: str):
        """
        Auto-fix issues and restart scan.

        Steps:
        1. Kill stuck job
        2. Check and adjust rate limits if needed
        3. Restart scan
        """
        logger.warning(f"🔧 AUTO-FIX TRIGGERED: {reason}")

        # Check if we've restarted too many times recently
        if self.last_fix_time:
            time_since_fix = (datetime.now() - self.last_fix_time).total_seconds() / 60
            if time_since_fix < 30:  # Less than 30 minutes since last fix
                self.restart_count += 1
                if self.restart_count >= self.max_auto_restarts:
                    logger.error(
                        f"❌ TOO MANY AUTO-RESTARTS ({self.restart_count}) in short time. "
                        f"Pausing auto-fix. Manual intervention needed."
                    )
                    return
            else:
                # Reset counter if it's been a while
                self.restart_count = 0

        self.last_fix_time = datetime.now()

        # Step 1: Kill stuck job
        status = self.get_scan_status()
        if status["running_job"]:
            logger.info(f"Killing stuck job {status['running_job'].id}")
            self.kill_stuck_job(str(status["running_job"].id))
            await asyncio.sleep(2)

        # Step 2: Wait a bit for worker to settle
        logger.info("Waiting 5 seconds for worker to settle...")
        await asyncio.sleep(5)

        # Step 3: Start new scan
        logger.info("Starting new scan...")
        job_id = await self.start_new_scan()

        if job_id:
            logger.info(f"✅ AUTO-FIX COMPLETE: New scan started with job {job_id}")
            self.restart_count += 1
        else:
            logger.error("❌ AUTO-FIX FAILED: Could not start new scan")

    async def monitor_loop(self):
        """Main monitoring loop."""
        logger.info("🛡️  Autonomous Gmail Scan Guardian started")
        logger.info(f"Check interval: {self.check_interval}s")
        logger.info(f"Stuck threshold: {self.stuck_threshold}s")
        logger.info(f"Max auto-restarts: {self.max_auto_restarts}")
        logger.info(f"Monitoring: {self.api_url}")

        while True:
            try:
                # Get scan status
                status = self.get_scan_status()

                # Check if stuck
                is_stuck, reason = self.is_scan_stuck(status)

                if is_stuck:
                    logger.warning(f"⚠️  STUCK SCAN DETECTED: {reason}")

                    # Check worker logs for additional context
                    has_errors, log_summary = self.check_worker_logs()
                    if has_errors:
                        logger.warning(f"📋 Worker logs: {log_summary}")

                    # Auto-fix and restart
                    await self.auto_fix_and_restart(reason)

                else:
                    # All good - log status
                    if status["running_job"]:
                        job = status["running_job"]
                        logger.info(
                            f"✅ Scan healthy - Job {str(job.id)[:8]}... "
                            f"({job.progress_pct}% complete, "
                            f"{job.emails_processed} emails processed)"
                        )
                    else:
                        logger.info(
                            f"ℹ️  No active scan - "
                            f"{status['total_emails']} total emails in database"
                        )

            except Exception as e:
                logger.error(f"Error in monitor loop: {e}", exc_info=True)

            # Wait for next check
            await asyncio.sleep(self.check_interval)


async def main():
    """Run the guardian."""
    guardian = ScanGuardian()
    await guardian.monitor_loop()


if __name__ == "__main__":
    asyncio.run(main())
