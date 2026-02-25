"""
User model.
"""

from typing import TYPE_CHECKING

from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.models.base import Base, TimestampMixin, UUIDMixin

if TYPE_CHECKING:
    from src.models.account import GmailAccount
    from src.models.contact import Contact
    from src.models.email import Email
    from src.models.job import SyncJob


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

    def __repr__(self) -> str:
        return f"<User(id={self.id}, email={self.email})>"
