"""
Company news item model for tracking news from company websites and RSS feeds.
"""

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, ForeignKey, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSON, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.models.base import Base, TimestampMixin, UUIDMixin

if TYPE_CHECKING:
    from src.models.company import Company
    from src.models.draft_suggestion import DraftSuggestion
    from src.models.user import User


class CompanyNewsItem(Base, UUIDMixin, TimestampMixin):
    """
    News article or event scraped from a company website or RSS feed.

    The analysis JSON stores Claude Haiku classification:
    {
        "category": "project_win|project_completion|executive_hire|...",
        "relevance_score": 0.0-1.0,
        "entities": ["entity1", "entity2"],
        "outreach_angle": "suggested reason to reach out",
        "summary": "2-3 sentence summary"
    }
    """

    __tablename__ = "company_news_items"
    __table_args__ = (UniqueConstraint("company_id", "source_url", name="uq_company_news_source"),)

    # Foreign Keys
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

    # Source info
    source_url: Mapped[str] = mapped_column(String(2048), nullable=False, comment="Article URL")

    source_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default="company_website",
        comment="company_website, rss_enr, rss_construction_dive, rss_globenewswire, sec_edgar",
    )

    # Content
    title: Mapped[str] = mapped_column(String(500), nullable=False)

    summary: Mapped[str | None] = mapped_column(
        Text, nullable=True, comment="Short summary (scraped or AI-generated)"
    )

    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )

    raw_html_hash: Mapped[str | None] = mapped_column(
        String(64), nullable=True, comment="SHA256 of news page HTML for change detection"
    )

    # AI Analysis
    analysis: Mapped[dict | None] = mapped_column(
        JSON, nullable=True, comment="Claude Haiku classification results"
    )

    analyzed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Status
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="new",
        index=True,
        comment="new, analyzed, actioned, dismissed",
    )

    # Relationships
    user: Mapped["User"] = relationship("User")
    company: Mapped["Company"] = relationship("Company", back_populates="news_items")
    draft_suggestions: Mapped[list["DraftSuggestion"]] = relationship(
        "DraftSuggestion", back_populates="news_item", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return (
            f"<CompanyNewsItem(id={self.id}, company_id={self.company_id}, title={self.title!r})>"
        )
