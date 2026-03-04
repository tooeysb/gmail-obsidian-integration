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
    include=["src.worker.tasks", "src.worker.id_first_tasks", "src.worker.backfill_body_tasks"],
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

# Only add SSL config if using rediss:// (production/Heroku)
if settings.redis_url.startswith("rediss://"):
    config["broker_use_ssl"] = {"ssl_cert_reqs": ssl.CERT_NONE}
    config["redis_backend_use_ssl"] = {"ssl_cert_reqs": ssl.CERT_NONE}

celery_app.conf.update(config)


@celery_app.task(bind=True)
def debug_task(self):
    """Debug task to test Celery is working."""
    print(f"Request: {self.request!r}")
    return {"status": "ok", "message": "Celery is working!"}
