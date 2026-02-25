"""
Celery application configuration.
"""

import ssl
from celery import Celery

from src.core.config import settings

# Create Celery app
celery_app = Celery(
    "gmail_obsidian_worker",
    broker=settings.redis_url,
    backend=settings.redis_url,
    include=["src.worker.tasks"],
)

# Celery configuration
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_time_limit=3600 * 4,  # 4 hours max per task
    task_soft_time_limit=3600 * 3,  # 3 hours soft limit
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=10,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    result_expires=3600 * 24,  # Results expire after 24 hours
    # SSL configuration for Heroku Redis (rediss://)
    broker_use_ssl={
        "ssl_cert_reqs": ssl.CERT_NONE,  # Don't verify certificate (Heroku manages this)
    },
    redis_backend_use_ssl={
        "ssl_cert_reqs": ssl.CERT_NONE,  # Don't verify certificate (Heroku manages this)
    },
)


@celery_app.task(bind=True)
def debug_task(self):
    """Debug task to test Celery is working."""
    print(f"Request: {self.request!r}")
    return {"status": "ok", "message": "Celery is working!"}
