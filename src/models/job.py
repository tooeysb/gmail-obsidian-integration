"""
Sync Job model for tracking scan progress.
"""

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.models.base import Base, TimestampMixin, UUIDMixin

if TYPE_CHECKING:
    from src.models.user import User


class SyncJob(Base, UUIDMixin, TimestampMixin):
    """
    Sync Job model.
    Tracks the progress of Gmail scanning and vault generation jobs.
    """

    __tablename__ = "sync_jobs"

    # Foreign Keys
    user_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )

    # Job Status
    status: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default="queued",
        comment="Job status: queued, running, completed, failed, cancelled",
    )

    phase: Mapped[str | None] = mapped_column(
        String(50),
        nullable=True,
        comment="Current phase: contacts, emails, themes, vault",
    )

    progress_pct: Mapped[int] = mapped_column(
        Integer, default=0, nullable=False, comment="Progress percentage (0-100)"
    )

    # Metrics
    emails_processed: Mapped[int] = mapped_column(
        Integer, default=0, nullable=False, comment="Number of emails processed"
    )

    emails_total: Mapped[int | None] = mapped_column(
        Integer, nullable=True, comment="Total emails to process (if known)"
    )

    contacts_processed: Mapped[int] = mapped_column(
        Integer, default=0, nullable=False, comment="Number of contacts processed"
    )

    # Timestamps
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="Job start timestamp"
    )

    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="Job completion timestamp"
    )

    # Error Handling
    error_message: Mapped[str | None] = mapped_column(
        Text, nullable=True, comment="Error message if job failed"
    )

    retry_count: Mapped[int] = mapped_column(
        Integer, default=0, nullable=False, comment="Number of retry attempts"
    )

    # Celery Task ID
    celery_task_id: Mapped[str | None] = mapped_column(
        String(255), nullable=True, index=True, comment="Celery task ID for tracking"
    )

    # Relationships
    user: Mapped["User"] = relationship("User", back_populates="sync_jobs")

    def __repr__(self) -> str:
        return f"<SyncJob(id={self.id}, status={self.status}, progress={self.progress_pct}%)>"

    @property
    def is_running(self) -> bool:
        """Check if job is currently running."""
        return self.status in ("queued", "running")

    @property
    def is_complete(self) -> bool:
        """Check if job has completed (success or failure)."""
        return self.status in ("completed", "failed", "cancelled")

    @property
    def duration_seconds(self) -> int | None:
        """Calculate job duration in seconds."""
        if self.started_at and self.completed_at:
            return int((self.completed_at - self.started_at).total_seconds())
        return None
