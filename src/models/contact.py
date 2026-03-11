"""
Contact model with multi-account merging support.
"""

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import (
    ARRAY,
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSON, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.models.base import Base, TimestampMixin, UUIDMixin

if TYPE_CHECKING:
    from src.models.company import Company
    from src.models.contact_enrichment import ContactEnrichment
    from src.models.email_participant import EmailParticipant
    from src.models.linkedin_post import LinkedInPost
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

    company_id: Mapped[UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("companies.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
        comment="Company this contact belongs to",
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

    # CRM Enrichment Fields
    title: Mapped[str | None] = mapped_column(String(255), nullable=True, comment="Job title")

    personal_email: Mapped[str | None] = mapped_column(
        String(255), nullable=True, comment="Personal email address (separate from work email)"
    )

    contact_type: Mapped[str | None] = mapped_column(
        String(50), nullable=True, comment="Contact type (e.g., Champion, Decision Maker)"
    )

    is_vip: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False, comment="VIP flag for high-priority contacts"
    )

    tags: Mapped[list[str]] = mapped_column(
        ARRAY(String),
        default=[],
        server_default="{}",
        nullable=False,
        comment="Freeform tags for contact categorization",
    )

    salesforce_id: Mapped[str | None] = mapped_column(
        String(100), nullable=True, comment="Salesforce Contact ID"
    )

    address: Mapped[str | None] = mapped_column(Text, nullable=True, comment="Mailing address")

    linkedin_url: Mapped[str | None] = mapped_column(
        String(500), nullable=True, comment="LinkedIn profile URL"
    )

    enrichment_status: Mapped[str | None] = mapped_column(
        String(20),
        nullable=True,
        comment="Enrichment status: enriched, needs_review, skipped",
    )

    enrichment_notes: Mapped[str | None] = mapped_column(
        Text, nullable=True, comment="Notes from enrichment automation"
    )

    contact_source: Mapped[str | None] = mapped_column(
        String(20),
        nullable=True,
        server_default=text("'email'"),
        comment="How this contact was discovered: email, website, manual",
    )

    source_data: Mapped[dict | None] = mapped_column(
        JSON, nullable=True, comment="Raw imported CRM data for reference"
    )

    last_contact_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Timestamp of most recent email with this contact",
    )

    deleted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="Soft delete timestamp"
    )

    # Job Change Tracking
    is_active: Mapped[bool] = mapped_column(
        Boolean,
        default=True,
        server_default="true",
        nullable=False,
        comment="Inactive contacts have left their company; excluded from enrichment",
    )

    is_approved: Mapped[bool] = mapped_column(
        Boolean,
        default=False,
        server_default="false",
        nullable=False,
        comment="Manually approved by user as verified/complete",
    )

    linkedin_company_raw: Mapped[str | None] = mapped_column(
        String(500),
        nullable=True,
        comment="Company name as seen on LinkedIn during last check",
    )

    job_change_detected_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="When a company mismatch was detected on LinkedIn",
    )

    last_linkedin_check_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="When LinkedIn profile was last visited for re-check",
    )

    previous_company_id: Mapped[UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("companies.id", ondelete="SET NULL"),
        nullable=True,
        comment="Previous company before reassignment after job change",
    )

    job_change_draft_generated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="When a job change outreach draft was auto-generated",
    )

    # LinkedIn Monitoring
    monitoring_tier: Mapped[str | None] = mapped_column(
        String(1), nullable=True, index=True, comment="A/B/C monitoring tier"
    )

    tier_auto_suggested: Mapped[str | None] = mapped_column(
        String(1), nullable=True, comment="Auto-computed tier suggestion from email data"
    )

    tier_manually_set: Mapped[bool] = mapped_column(
        Boolean,
        default=False,
        server_default=text("false"),
        nullable=False,
        comment="Whether user overrode auto-suggested tier",
    )

    last_post_check_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="When LinkedIn posts were last scraped for this contact",
    )

    last_profile_check_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="When LinkedIn profile was last checked for job/title changes",
    )

    # Title Change Tracking
    linkedin_title_raw: Mapped[str | None] = mapped_column(
        String(500), nullable=True, comment="Job title as seen on LinkedIn during last check"
    )

    title_change_detected_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="When a title mismatch was detected on LinkedIn",
    )

    previous_title: Mapped[str | None] = mapped_column(
        String(255), nullable=True, comment="Job title before most recent change was detected"
    )

    # Relationships
    user: Mapped["User"] = relationship("User", back_populates="contacts")

    company: Mapped["Company | None"] = relationship(
        "Company", back_populates="contacts", foreign_keys=[company_id]
    )

    previous_company: Mapped["Company | None"] = relationship(
        "Company", foreign_keys=[previous_company_id]
    )

    enrichments: Mapped[list["ContactEnrichment"]] = relationship(
        "ContactEnrichment", back_populates="contact"
    )

    email_participants: Mapped[list["EmailParticipant"]] = relationship(
        "EmailParticipant", back_populates="contact"
    )

    linkedin_posts: Mapped[list["LinkedInPost"]] = relationship(
        "LinkedInPost", back_populates="contact"
    )

    def __repr__(self) -> str:
        return f"<Contact(id={self.id}, name={self.name}, email={self.email}, accounts={self.account_sources})>"
