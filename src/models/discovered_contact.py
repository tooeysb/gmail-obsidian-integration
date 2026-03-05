"""
Discovered contact model — cached results from domain-based email scanning.

Populated by the daily domain discovery cron job. Stores people found in emails
whose domain matches a CRM company, but who aren't yet CRM contacts.
"""

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.models.base import Base, TimestampMixin, UUIDMixin

if TYPE_CHECKING:
    from src.models.company import Company
    from src.models.user import User


class DiscoveredContact(Base, UUIDMixin, TimestampMixin):
    """
    Cached discovered contact from domain-based email scanning.
    Rebuilt daily by the domain discovery cron job.
    """

    __tablename__ = "discovered_contacts"
    __table_args__ = (
        UniqueConstraint("user_id", "email", name="uq_user_discovered_email"),
    )

    user_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    company_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("companies.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    email: Mapped[str] = mapped_column(
        String(255), nullable=False, index=True, comment="Discovered email address"
    )

    name: Mapped[str | None] = mapped_column(
        String(255), nullable=True, comment="Display name from email headers"
    )

    email_count: Mapped[int] = mapped_column(
        Integer, default=0, nullable=False, comment="Total emails involving this person"
    )

    last_email_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="Most recent email"
    )

    first_email_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="Earliest email"
    )

    # Relationships
    user: Mapped["User"] = relationship("User")
    company: Mapped["Company"] = relationship("Company")

    def __repr__(self) -> str:
        return f"<DiscoveredContact(email={self.email}, company_id={self.company_id})>"
