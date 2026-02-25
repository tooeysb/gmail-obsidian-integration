"""
Email and EmailTag models.
"""

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.models.base import Base, TimestampMixin, UUIDMixin

if TYPE_CHECKING:
    from src.models.account import GmailAccount
    from src.models.user import User


class Email(Base, UUIDMixin, TimestampMixin):
    """
    Email model.
    Stores email metadata and summary (not full body for privacy).
    Each email is associated with a specific Gmail account.
    """

    __tablename__ = "emails"
    __table_args__ = (
        UniqueConstraint("account_id", "gmail_message_id", name="uq_account_message_id"),
        Index("ix_emails_date", "date"),
        Index("ix_emails_sender_email", "sender_email"),
    )

    # Foreign Keys
    user_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )

    account_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("gmail_accounts.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Gmail Identifiers
    gmail_message_id: Mapped[str] = mapped_column(
        String(255), nullable=False, comment="Gmail message ID"
    )

    gmail_thread_id: Mapped[str | None] = mapped_column(
        String(255), nullable=True, comment="Gmail thread ID"
    )

    # Email Content
    subject: Mapped[str | None] = mapped_column(Text, nullable=True, comment="Email subject")

    sender_email: Mapped[str] = mapped_column(
        String(255), nullable=False, comment="Sender email address"
    )

    sender_name: Mapped[str | None] = mapped_column(
        String(255), nullable=True, comment="Sender display name"
    )

    recipient_emails: Mapped[str] = mapped_column(
        Text, nullable=False, comment="Comma-separated recipient emails"
    )

    date: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True, comment="Email date"
    )

    # Summary (not full body for privacy)
    summary: Mapped[str | None] = mapped_column(
        Text, nullable=True, comment="500-char summary of email content"
    )

    # Metadata
    has_attachments: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False, comment="Whether email has attachments"
    )

    attachment_count: Mapped[int] = mapped_column(
        Integer, default=0, nullable=False, comment="Number of attachments"
    )

    # Relationships
    user: Mapped["User"] = relationship("User", back_populates="emails")

    account: Mapped["GmailAccount"] = relationship("GmailAccount", back_populates="emails")

    tags: Mapped[list["EmailTag"]] = relationship(
        "EmailTag", back_populates="email", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<Email(id={self.id}, subject={self.subject}, from={self.sender_email}, date={self.date})>"


class EmailTag(Base, UUIDMixin, TimestampMixin):
    """
    Email Tag model.
    Stores AI-generated tags for each email (topics, interests, relationships, etc.).
    """

    __tablename__ = "email_tags"
    __table_args__ = (
        Index("ix_email_tags_email_id", "email_id"),
        Index("ix_email_tags_tag", "tag"),
        Index("ix_email_tags_tag_category", "tag_category"),
    )

    # Foreign Keys
    email_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("emails.id", ondelete="CASCADE"), nullable=False
    )

    # Tag Info
    tag: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        comment="Tag value (e.g., 'project-alpha', 'scuba-diving', 'colleague')",
    )

    tag_category: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        comment="Tag category (topic, interest, relationship, sentiment, domain, account, action)",
    )

    # Optional: Confidence score from AI
    confidence: Mapped[float | None] = mapped_column(
        Float, nullable=True, comment="AI confidence score (0-1)"
    )

    # Relationships
    email: Mapped["Email"] = relationship("Email", back_populates="tags")

    def __repr__(self) -> str:
        return f"<EmailTag(id={self.id}, tag={self.tag}, category={self.tag_category})>"
