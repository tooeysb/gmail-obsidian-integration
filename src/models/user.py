"""
User model.
"""

from typing import TYPE_CHECKING

from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.models.base import Base, TimestampMixin, UUIDMixin

if TYPE_CHECKING:
    from src.models.account import GmailAccount
    from src.models.company import Company
    from src.models.contact import Contact
    from src.models.email import Email
    from src.models.job import SyncJob
    from src.models.relationship_profile import RelationshipProfile
    from src.models.voice_profile import VoiceProfile


class User(Base, UUIDMixin, TimestampMixin):
    """
    User model.
    A user can have multiple Gmail accounts authenticated.
    """

    __tablename__ = "users"

    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    name: Mapped[str | None] = mapped_column(String(255), nullable=True)

    # Relationships
    gmail_accounts: Mapped[list["GmailAccount"]] = relationship(
        "GmailAccount", back_populates="user", cascade="all, delete-orphan"
    )

    contacts: Mapped[list["Contact"]] = relationship(
        "Contact", back_populates="user", cascade="all, delete-orphan"
    )

    emails: Mapped[list["Email"]] = relationship(
        "Email", back_populates="user", cascade="all, delete-orphan"
    )

    sync_jobs: Mapped[list["SyncJob"]] = relationship(
        "SyncJob", back_populates="user", cascade="all, delete-orphan"
    )

    relationship_profiles: Mapped[list["RelationshipProfile"]] = relationship(
        "RelationshipProfile", back_populates="user", cascade="all, delete-orphan"
    )

    voice_profiles: Mapped[list["VoiceProfile"]] = relationship(
        "VoiceProfile", back_populates="user", cascade="all, delete-orphan"
    )

    companies: Mapped[list["Company"]] = relationship(
        "Company", back_populates="user", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<User(id={self.id}, email={self.email})>"
