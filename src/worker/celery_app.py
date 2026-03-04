"""
Celery application configuration.
"""

import ssl

from celery import Celery
from celery.schedules import crontab

from src.core.config import settings
from src.core.logging import get_logger

logger = get_logger(__name__)

# Create Celery app
celery_app = Celery(
    "crm_hth_worker",
    broker=settings.redis_url,
    backend=settings.redis_url,
    include=[
        "src.worker.tasks",
        "src.worker.id_first_tasks",
        "src.worker.backfill_body_tasks",
        "src.worker.news_tasks",
    ],
)

# Celery configuration
config = {
    "task_serializer": "json",
    "accept_content": ["json"],
    "result_serializer": "json",
    "timezone": "UTC",
    "enable_utc": True,
    "task_track_started": True,
    "task_time_limit": 3600 * 4,  # 4 hours max per task
    "task_soft_time_limit": 3600 * 3,  # 3 hours soft limit
    "worker_prefetch_multiplier": 1,
    "worker_max_tasks_per_child": 200,
    "task_acks_late": True,
    "task_reject_on_worker_lost": True,
    "result_expires": 3600 * 24,  # Results expire after 24 hours
}

# Heroku Redis uses self-signed certs and does not provide CA certs for
# verification. CERT_NONE is the documented Heroku Redis pattern.
if settings.redis_url.startswith("rediss://"):
    config["broker_use_ssl"] = {"ssl_cert_reqs": ssl.CERT_NONE}
    config["redis_backend_use_ssl"] = {"ssl_cert_reqs": ssl.CERT_NONE}

celery_app.conf.update(config)

DEFAULT_USER_ID = "d4475ca3-0ddc-4ea0-ac89-95ae7fed1e31"

# Scheduled tasks
celery_app.conf.beat_schedule = {
    # Daily news intelligence pipeline at 5 AM UTC (1 AM EST)
    # Includes daily digest email at the end
    "daily-news-pipeline": {
        "task": "run_news_pipeline",
        "schedule": crontab(hour=5, minute=0),
        "args": [DEFAULT_USER_ID],
    },
    # Weekly rollup email: Sunday 2 PM UTC (10 AM EST)
    "weekly-news-digest": {
        "task": "send_weekly_digest",
        "schedule": crontab(hour=14, minute=0, day_of_week=0),
        "args": [DEFAULT_USER_ID],
    },
    # Daily email sync at 3 AM UTC (11 PM EST) — fetches only new emails
    "daily-email-sync": {
        "task": "scan_gmail_task",
        "schedule": crontab(hour=3, minute=0),
        "args": [DEFAULT_USER_ID],
    },
}


@celery_app.task(bind=True)
def debug_task(self):
    """Debug task to test Celery is working."""
    logger.debug("Request: %r", self.request)
    return {"status": "ok", "message": "Celery is working!"}
