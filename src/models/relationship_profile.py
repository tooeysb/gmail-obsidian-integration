"""
Relationship Profile model for storing Claude-generated contact analysis.
"""

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, Float, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import ARRAY, JSON, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.models.base import Base, TimestampMixin, UUIDMixin

if TYPE_CHECKING:
    from src.models.user import User


class RelationshipProfile(Base, UUIDMixin, TimestampMixin):
    """
    Stores Claude-generated relationship analysis per contact.

    Combines SQL-computed metrics (email counts, dates) with
    AI-generated profiles (summaries, sentiment, conflict detection).
    """

    __tablename__ = "relationship_profiles"
    __table_args__ = (UniqueConstraint("user_id", "contact_email", name="uq_user_relationship"),)

    # Foreign Keys
    user_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="User who owns this relationship profile",
    )

    # Contact identification
    contact_email: Mapped[str] = mapped_column(
        String(255), nullable=False, index=True, comment="Contact email address"
    )

    contact_name: Mapped[str | None] = mapped_column(
        String(255), nullable=True, comment="Contact display name"
    )

    relationship_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        index=True,
        default="unknown",
        comment="coworker, client, personal, vendor, recruiter, unknown",
    )

    account_sources: Mapped[list[str]] = mapped_column(
        ARRAY(String),
        nullable=False,
        default=[],
        comment="Which Gmail accounts this contact appears in",
    )

    # Discovery metadata (computed from SQL queries)
    total_email_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, comment="Total emails with this contact"
    )

    sent_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, comment="Emails user sent TO this contact"
    )

    received_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, comment="Emails user received FROM this contact"
    )

    first_exchange_date: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="Date of earliest email"
    )

    last_exchange_date: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="Date of most recent email"
    )

    thread_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, comment="Distinct thread count"
    )

    avg_response_time_hours: Mapped[float | None] = mapped_column(
        Float, nullable=True, comment="Average response time in hours"
    )

    # Claude-generated profile (JSON for flexibility)
    profile_data: Mapped[dict | None] = mapped_column(
        JSON,
        nullable=True,
        comment="AI-generated profile: summary, opinion, topics, conflicts, etc.",
    )

    profiled_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="When Claude analysis was run"
    )

    # CRM Enrichment
    customer_data: Mapped[dict | None] = mapped_column(
        JSON, nullable=True, comment="Merged CRM enrichment data"
    )

    # Relationships
    user: Mapped["User"] = relationship("User", back_populates="relationship_profiles")

    def __repr__(self) -> str:
        return (
            f"<RelationshipProfile(id={self.id}, contact={self.contact_email}, "
            f"type={self.relationship_type}, emails={self.total_email_count})>"
        )
