"""
Company model for CRM enrichment data.
"""

from datetime import date, datetime
from typing import TYPE_CHECKING

from sqlalchemy import (
    ARRAY,
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSON, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.models.base import Base, TimestampMixin, UUIDMixin

if TYPE_CHECKING:
    from src.models.company_news import CompanyNewsItem
    from src.models.contact import Contact
    from src.models.user import User


class Company(Base, UUIDMixin, TimestampMixin):
    """
    Company model.
    Stores CRM company data imported from external sources (e.g., Salesforce exports).
    Contacts are linked to companies via company_id foreign key.
    """

    __tablename__ = "companies"
    __table_args__ = (UniqueConstraint("user_id", "name", name="uq_user_company_name"),)

    # Foreign Keys
    user_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="User who owns this company record",
    )

    # Company Identity
    name: Mapped[str] = mapped_column(
        String(255), nullable=False, index=True, comment="Company name"
    )

    domain: Mapped[str | None] = mapped_column(
        String(255), nullable=True, index=True, comment="Company website domain"
    )

    aliases: Mapped[list[str] | None] = mapped_column(
        ARRAY(String), nullable=True, comment="Alternative company names"
    )

    # Classification
    industry: Mapped[str | None] = mapped_column(
        String(100), nullable=True, comment="Industry vertical"
    )

    company_type: Mapped[str | None] = mapped_column(
        String(100),
        nullable=True,
        comment="Company type: General Contractor, Owner, Specialty Contractor",
    )

    work_type: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        comment="Type of work / market sectors (e.g., Corporate, Healthcare, Education)",
    )

    # Account Details
    billing_state: Mapped[str | None] = mapped_column(
        String(100), nullable=True, comment="Billing state/region"
    )

    arr: Mapped[float | None] = mapped_column(
        Numeric(15, 2), nullable=True, comment="Annual recurring revenue"
    )

    revenue_segment: Mapped[str | None] = mapped_column(
        String(50), nullable=True, comment="Revenue segment classification"
    )

    account_tier: Mapped[str | None] = mapped_column(
        String(50), nullable=True, comment="Account tier (e.g., Enterprise, SMB)"
    )

    # CRM References
    salesforce_id: Mapped[str | None] = mapped_column(
        String(100), nullable=True, comment="Salesforce Account ID"
    )

    renewal_date: Mapped[date | None] = mapped_column(
        Date, nullable=True, comment="Contract renewal date"
    )

    # Ownership
    account_owner: Mapped[str | None] = mapped_column(
        String(255), nullable=True, comment="Account owner name"
    )

    csm: Mapped[str | None] = mapped_column(
        String(255), nullable=True, comment="Customer success manager"
    )

    # Flexible storage
    notes: Mapped[str | None] = mapped_column(
        Text, nullable=True, comment="Notes about this company"
    )

    source_data: Mapped[dict | None] = mapped_column(
        JSON, nullable=True, comment="Raw imported data for reference"
    )

    # News intelligence
    news_page_url: Mapped[str | None] = mapped_column(
        String(2048), nullable=True, comment="Discovered news/insights page URL"
    )

    news_page_discovered_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="When news page was discovered"
    )

    news_scrape_enabled: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        server_default="true",
        comment="Whether to include in daily news scrape",
    )

    news_search_override: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        comment="Override search term for Google News (e.g. 'DPR Construction' for 'DPR')",
    )

    linkedin_url: Mapped[str | None] = mapped_column(
        String(500), nullable=True, comment="Company LinkedIn page URL"
    )

    # Relationships
    user: Mapped["User"] = relationship("User", back_populates="companies")

    contacts: Mapped[list["Contact"]] = relationship("Contact", back_populates="company")

    news_items: Mapped[list["CompanyNewsItem"]] = relationship(
        "CompanyNewsItem", back_populates="company", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<Company(id={self.id}, name={self.name}, domain={self.domain})>"
