"""
Draft suggestion model for news-triggered email outreach.
"""

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, ForeignKey, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.models.base import Base, TimestampMixin, UUIDMixin

if TYPE_CHECKING:
    from src.models.company_news import CompanyNewsItem
    from src.models.contact import Contact
    from src.models.user import User


class DraftSuggestion(Base, UUIDMixin, TimestampMixin):
    """
    AI-generated email draft triggered by a company news event.

    Links a CompanyNewsItem to a Contact with a personalized email draft
    generated using the user's voice profile.
    """

    __tablename__ = "draft_suggestions"
    __table_args__ = (UniqueConstraint("news_item_id", "contact_id", name="uq_draft_news_contact"),)

    # Foreign Keys
    user_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    news_item_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("company_news_items.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    contact_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("contacts.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Draft content
    subject: Mapped[str] = mapped_column(String(500), nullable=False)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    context_used: Mapped[str] = mapped_column(
        Text, nullable=False, comment="Context string fed to EmailDraftService"
    )
    tone: Mapped[str | None] = mapped_column(String(50), nullable=True)

    # Status
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="pending",
        index=True,
        comment="pending, edited, sent, dismissed, snoozed",
    )

    snoozed_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Generation metadata
    generated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    model_used: Mapped[str | None] = mapped_column(
        String(100), nullable=True, comment="Claude model used for generation"
    )

    # Relationships
    user: Mapped["User"] = relationship("User")
    news_item: Mapped["CompanyNewsItem"] = relationship(
        "CompanyNewsItem", back_populates="draft_suggestions"
    )
    contact: Mapped["Contact"] = relationship("Contact")

    def __repr__(self) -> str:
        return (
            f"<DraftSuggestion(id={self.id}, contact_id={self.contact_id}, "
            f"status={self.status})>"
        )
