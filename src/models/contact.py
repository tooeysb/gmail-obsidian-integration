"""
Contact model with multi-account merging support.
"""

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import ARRAY, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.models.base import Base, TimestampMixin, UUIDMixin

if TYPE_CHECKING:
    from src.models.user import User


class Contact(Base, UUIDMixin, TimestampMixin):
    """
    Contact model.
    Contacts are merged across multiple Gmail accounts by email address.
    A single contact may appear in emails from multiple accounts.
    """

    __tablename__ = "contacts"
    __table_args__ = (UniqueConstraint("user_id", "email", name="uq_user_contact_email"),)

    # Foreign Keys
    user_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )

    # Contact Info
    email: Mapped[str] = mapped_column(
        String(255), nullable=False, index=True, comment="Contact email address (unique per user)"
    )

    name: Mapped[str | None] = mapped_column(
        String(255), nullable=True, comment="Contact display name"
    )

    phone: Mapped[str | None] = mapped_column(
        String(50), nullable=True, comment="Contact phone number"
    )

    # Multi-Account Tracking
    account_sources: Mapped[list[str]] = mapped_column(
        ARRAY(String),
        nullable=False,
        default=[],
        comment="Account labels where this contact appears (e.g., ['procore-main', 'personal'])",
    )

    # Email Count Tracking
    email_count: Mapped[int] = mapped_column(
        Integer, default=0, nullable=False, comment="Total email count across all accounts"
    )

    # Additional metadata (can be JSON if needed for flexibility)
    notes: Mapped[str | None] = mapped_column(
        Text, nullable=True, comment="User notes about this contact"
    )

    relationship_context: Mapped[str | None] = mapped_column(
        String(50),
        nullable=True,
        comment="Relationship type (colleague, client, friend, family)",
    )

    last_contact_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Timestamp of most recent email with this contact",
    )

    # Relationships
    user: Mapped["User"] = relationship("User", back_populates="contacts")

    def __repr__(self) -> str:
        return f"<Contact(id={self.id}, name={self.name}, email={self.email}, accounts={self.account_sources})>"
