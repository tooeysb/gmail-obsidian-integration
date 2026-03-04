"""
ContactEnrichment model for tracking CRM import audit trail.
"""

from typing import TYPE_CHECKING

from sqlalchemy import ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSON, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.models.base import Base, TimestampMixin, UUIDMixin

if TYPE_CHECKING:
    from src.models.company import Company
    from src.models.contact import Contact
    from src.models.user import User


class ContactEnrichment(Base, UUIDMixin, TimestampMixin):
    """
    Audit trail for CRM data imports.
    Tracks which source rows matched which contacts/companies,
    enabling re-import and debugging of enrichment pipelines.
    """

    __tablename__ = "contact_enrichments"
    __table_args__ = (
        UniqueConstraint("user_id", "source_tab", "match_email", name="uq_enrichment_source"),
    )

    # Foreign Keys
    user_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="User who owns this enrichment record",
    )

    # Match tracking
    match_email: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        index=True,
        comment="Email address used to match against contacts",
    )

    contact_id: Mapped[UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("contacts.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
        comment="Matched contact (null if no match found)",
    )

    company_id: Mapped[UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("companies.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
        comment="Matched or created company (null if no match)",
    )

    # Source tracking
    source_file: Mapped[str] = mapped_column(
        String(255), nullable=False, comment="Original import file name"
    )

    source_tab: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        index=True,
        comment="Sheet/tab within the source file",
    )

    source_row: Mapped[int | None] = mapped_column(
        Integer, nullable=True, comment="Row number in the source file"
    )

    raw_data: Mapped[dict | None] = mapped_column(
        JSON, nullable=True, comment="Raw row data from import source"
    )

    match_status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        comment="Match result: matched, unmatched, created, skipped",
    )

    # Relationships
    user: Mapped["User"] = relationship("User")

    contact: Mapped["Contact | None"] = relationship("Contact", back_populates="enrichments")

    company: Mapped["Company | None"] = relationship("Company")

    def __repr__(self) -> str:
        return (
            f"<ContactEnrichment(id={self.id}, email={self.match_email}, "
            f"status={self.match_status}, source={self.source_tab})>"
        )
