"""Guardian event model for tracking autonomous monitoring actions."""

import uuid
from datetime import datetime

from sqlalchemy import JSON, Column, DateTime, String, Text
from sqlalchemy.dialects.postgresql import UUID

from src.models.base import Base


class GuardianEvent(Base):
    """
    Guardian event for tracking autonomous monitoring and auto-fix actions.

    Tracks all guardian activities:
    - stuck_detected: Scan detected as stuck
    - job_killed: Stuck job automatically killed
    - scan_restarted: New scan automatically started
    - error: Guardian encountered an error
    """

    __tablename__ = "guardian_events"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    event_type = Column(String(50), nullable=False)  # stuck_detected, job_killed, scan_restarted, error
    description = Column(Text, nullable=False)
    job_id = Column(UUID(as_uuid=True), nullable=True)  # Related job if applicable
    metadata = Column(JSON, nullable=True)  # Additional context
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    resolved_at = Column(DateTime(timezone=True), nullable=True)  # When issue was resolved

    def __repr__(self):
        return f"<GuardianEvent {self.event_type} at {self.created_at}>"
