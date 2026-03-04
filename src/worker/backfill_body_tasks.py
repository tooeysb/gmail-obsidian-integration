"""
Backfill email bodies for existing emails that were fetched with format="metadata".

Architecture:
- Bypasses EmailQueue entirely for speed
- Queries emails WHERE body IS NULL directly
- Uses UPDATE ... WHERE to atomically claim rows (no lock contention)
- Multiple parallel workers per account, each self-sustaining

Tasks:
- start_body_backfill: Entry point — spawns N parallel workers per account
- backfill_worker: Claims a batch directly from emails table, fetches from Gmail, updates
"""

import json
import uuid
from datetime import datetime, timedelta

from celery import group
from sqlalchemy import func, text, update

from src.core.database import WorkerSessionLocal as SessionLocal
from src.core.logging import get_logger
from src.integrations.gmail.client import GmailClient
from src.integrations.gmail.rate_limiter import GmailRateLimiter
from src.models import Email, GmailAccount
from src.worker.celery_app import celery_app

logger = get_logger(__name__)

# Tuning knobs
BATCH_SIZE = 500  # Emails per worker cycle
WORKERS_PER_ACCOUNT = 3  # Default for non-workspace accounts

# Sentinel value written to body while fetch is in progress.
FETCHING_SENTINEL = "__fetching__"

# Account-specific overrides for high-limit accounts
PROCORE_MAIN_EMAIL = "tooey@procore.com"
HIGH_THROUGHPUT_WORKERS = 6  # Best measured: 5,222/min

# Cached rate limiters keyed by account_id (avoids creating new Redis pools per task)
_rate_limiters: dict[str, GmailRateLimiter] = {}


def _get_rate_limiter(account_id: str, account_email: str) -> GmailRateLimiter:
    """Get or create a cached rate limiter for an account."""
    if account_id not in _rate_limiters:
        if account_email == PROCORE_MAIN_EMAIL:
            max_tokens = 150
            refill_rate = 120.0
        elif "procore.com" in account_email or "hth-corp.com" not in account_email:
            max_tokens = 200
            refill_rate = 50.0
        else:
            # Personal Gmail: 250 units/sec = 50 msg/sec, use 40/sec (80% margin)
            max_tokens = 100
            refill_rate = 40.0
        rl = GmailRateLimiter(
            max_tokens=max_tokens,
            refill_rate=refill_rate,
        )
        rl.bucket_key = f"gmail:backfill:{account_id}:tokens"
        rl.timestamp_key = f"gmail:backfill:{account_id}:last_refill"
        _rate_limiters[account_id] = rl
    return _rate_limiters[account_id]


@celery_app.task(name="queue_body_backfill")
def queue_body_backfill(account_id: str, limit: int | None = None):
    """Backward-compatible entry point. Delegates to start_body_backfill."""
    start_body_backfill.delay(account_id, num_workers=WORKERS_PER_ACCOUNT)


@celery_app.task(name="start_body_backfill")
def start_body_backfill(account_id: str, num_workers: int | None = None):
    """
    Entry point: check how many emails need bodies, then spawn parallel workers.

    Args:
        account_id: Gmail account UUID
        num_workers: How many parallel workers to spawn (auto-detected if None)
    """
    task_id = str(uuid.uuid4())[:8]
    db = SessionLocal()
    try:
        account = db.query(GmailAccount).filter(GmailAccount.id == account_id).first()
        if not account:
            logger.error("[%s] Account %s not found", task_id, account_id)
            return

        # Use account-specific worker count
        if num_workers is None:
            if account.account_email == PROCORE_MAIN_EMAIL:
                num_workers = HIGH_THROUGHPUT_WORKERS
            else:
                num_workers = WORKERS_PER_ACCOUNT

        # Count emails needing bodies
        missing = (
            db.query(func.count(Email.id))
            .filter(Email.account_id == account_id, Email.body == None)  # noqa: E711
            .scalar()
        )

        if not missing:
            logger.info("[%s] [%s] All emails have bodies!", task_id, account.account_email)
            return

        # Cap workers to available work
        needed_workers = min(num_workers, (missing + BATCH_SIZE - 1) // BATCH_SIZE)

        logger.info(
            "[%s] [%s] %s emails need bodies. Spawning %s parallel workers (rate limit: %s)",
            task_id,
            account.account_email,
            missing,
            needed_workers,
            "15k/min" if account.account_email == PROCORE_MAIN_EMAIL else "standard",
        )

        tasks = [backfill_worker.s(account_id) for _ in range(needed_workers)]
        group(tasks).apply_async()

    finally:
        db.close()


@celery_app.task(name="fetch_body_batch")
def fetch_body_batch(account_id: str):
    """Backward-compatible alias."""
    backfill_worker(account_id)


@celery_app.task(name="backfill_worker")
def backfill_worker(account_id: str):
    """
    Claim a batch of emails without bodies, fetch from Gmail, update DB.

    Skips the EmailQueue entirely — queries the emails table directly.
    Uses LIMIT + FOR UPDATE SKIP LOCKED for safe parallel claiming.
    Self-sustains by spawning a replacement task when done.
    """
    task_id = str(uuid.uuid4())[:8]

    db = SessionLocal()
    try:
        account = db.query(GmailAccount).filter(GmailAccount.id == account_id).first()
        if not account:
            logger.error("[%s] Account %s not found", task_id, account_id)
            return

        # Claim a batch: find emails without body, lock them
        # Using raw SQL for atomic claim with a worker tag in updated_at
        emails_to_fetch = (
            db.query(Email.id, Email.gmail_message_id)
            .filter(
                Email.account_id == account_id,
                Email.body == None,  # noqa: E711
            )
            .limit(BATCH_SIZE)
            .with_for_update(skip_locked=True)
            .all()
        )

        if not emails_to_fetch:
            logger.info("[%s] [%s] No more emails need bodies", task_id, account.account_email)
            return

        email_ids = [row[0] for row in emails_to_fetch]
        gmail_ids = [row[1] for row in emails_to_fetch]

        # Mark as "in progress" with a sentinel body value to prevent re-claiming
        db.execute(
            update(Email)
            .where(Email.id.in_(email_ids))
            .values(body=FETCHING_SENTINEL, updated_at=datetime.utcnow())
        )
        db.commit()

        logger.info("[%s] [%s] Claimed %s emails", task_id, account.account_email, len(gmail_ids))

        # Save creds and close DB before long Gmail fetch
        creds = json.loads(account.credentials)
        account_id_uuid = account.id
        account_email = account.account_email
        db.close()

        # Cached per-account rate limiter (avoids creating new Redis pool per task)
        rate_limiter = _get_rate_limiter(str(account_id_uuid), account_email)
        gmail_client = GmailClient(creds, rate_limiter=rate_limiter)

        try:
            email_dicts = gmail_client.fetch_message_batch(gmail_ids, format="full")
            logger.info("[%s] [%s] Fetched %s from Gmail", task_id, account_email, len(email_dicts))

            # Reopen DB for writes
            db = SessionLocal()

            # Build a map of gmail_message_id → body
            body_map = {}
            for ed in email_dicts:
                body = ed.get("body")
                if body:
                    # Strip NUL bytes — PostgreSQL text columns reject \x00
                    body_map[ed["gmail_message_id"]] = body.replace("\x00", "")

            # Bulk UPDATE using VALUES join (single round-trip instead of N)
            updated = 0
            if body_map:
                values_list = list(body_map.items())
                # Use CAST() syntax (not ::text) to avoid SQLAlchemy param collision
                placeholders = ", ".join(
                    f"(CAST(:gid_{i} AS text), CAST(:body_{i} AS text))"
                    for i in range(len(values_list))
                )
                params = {"account_id": str(account_id_uuid)}
                for i, (gid, body_text) in enumerate(values_list):
                    params[f"gid_{i}"] = gid
                    params[f"body_{i}"] = body_text

                db.execute(
                    text(
                        f"""
                    UPDATE emails e
                    SET body = v.body, updated_at = NOW()
                    FROM (VALUES {placeholders}) AS v(gmail_message_id, body)
                    WHERE e.gmail_message_id = v.gmail_message_id
                      AND e.account_id = CAST(:account_id AS uuid)
                """
                    ),
                    params,
                )
                updated = len(values_list)

            # Distinguish between successfully fetched (no body) vs failed (429, etc.)
            fetched_gmail_ids = {ed["gmail_message_id"] for ed in email_dicts}

            # Emails that were fetched but had no body content (image-only, etc.)
            # Mark as empty string so they're not re-fetched
            no_body_ids = [
                eid
                for eid, gid in zip(email_ids, gmail_ids, strict=False)
                if gid in fetched_gmail_ids and gid not in body_map
            ]
            if no_body_ids:
                db.execute(
                    update(Email)
                    .where(Email.id.in_(no_body_ids))
                    .values(body="", updated_at=datetime.utcnow())
                )

            # Emails that FAILED to fetch (429, timeout, etc.)
            # Reset sentinel to NULL so they get retried
            failed_ids = [
                eid
                for eid, gid in zip(email_ids, gmail_ids, strict=False)
                if gid not in fetched_gmail_ids
            ]
            if failed_ids:
                db.execute(update(Email).where(Email.id.in_(failed_ids)).values(body=None))

            db.commit()

            logger.info(
                "[%s] [%s] Updated %s bodies (%s no text, %s failed→retry)",
                task_id,
                account_email,
                updated,
                len(no_body_ids),
                len(failed_ids),
            )

        except Exception as e:
            logger.error("[%s] [%s] Gmail fetch error: %s", task_id, account_email, e)
            # Clear sentinel so these get retried
            db = SessionLocal()
            db.execute(update(Email).where(Email.id.in_(email_ids)).values(body=None))
            db.commit()
            raise

        # Self-sustain: infer remaining work from batch size instead of extra query.
        # If this batch was full, there's almost certainly more work.
        try:
            has_more = len(emails_to_fetch) >= BATCH_SIZE

            if has_more:
                logger.info(
                    "[%s] [%s] More emails remaining, spawning replacement worker",
                    task_id,
                    account_email,
                )
                backfill_worker.delay(account_id)
            else:
                logger.info("[%s] [%s] Backfill complete!", task_id, account_email)

        except Exception as e:
            logger.error("[%s] Error checking remaining: %s", task_id, e)

    finally:
        db.close()


@celery_app.task(name="cleanup_stuck_fetching")
def cleanup_stuck_fetching():
    """Reset emails stuck in fetching state for more than 10 minutes."""
    db = SessionLocal()
    try:
        cutoff = datetime.utcnow() - timedelta(minutes=10)
        result = db.execute(
            update(Email)
            .where(
                Email.body == FETCHING_SENTINEL,
                Email.updated_at < cutoff,
            )
            .values(body=None)
        )
        if result.rowcount > 0:
            logger.warning("Reset %s stuck fetching emails", result.rowcount)
        db.commit()
    finally:
        db.close()
