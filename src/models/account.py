"""
Gmail Account model for multi-account support.
"""

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import JSON, Boolean, DateTime, ForeignKey, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.models.base import Base, TimestampMixin, UUIDMixin

if TYPE_CHECKING:
    from src.models.email import Email
    from src.models.user import User


class GmailAccount(Base, UUIDMixin, TimestampMixin):
    """
    Gmail Account model.
    Stores OAuth2 credentials for each authenticated Gmail account.
    Supports multiple accounts per user (e.g., procore-main, procore-private, personal).
    """

    __tablename__ = "gmail_accounts"
    __table_args__ = (
        UniqueConstraint("user_id", "account_email", name="uq_user_account_email"),
    )

    # Foreign Keys
    user_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )

    # Account Info
    account_email: Mapped[str] = mapped_column(
        String(255), nullable=False, index=True, comment="Gmail address (e.g., tooey@procore.com)"
    )

    account_label: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        index=True,
        comment="Account identifier (procore-main, procore-private, personal)",
    )

    # OAuth2 Credentials (encrypted using pgcrypto)
    # Stored as JSON: {access_token, refresh_token, token_uri, client_id, client_secret, scopes, expiry}
    credentials: Mapped[dict | None] = mapped_column(
        JSON,
        nullable=True,
        comment="Encrypted OAuth2 credentials JSON",
    )

    # Status
    is_active: Mapped[bool] = mapped_column(
        Boolean, default=True, nullable=False, comment="Whether this account is currently active"
    )

    last_synced_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="Last successful sync timestamp"
    )

    # Relationships
    user: Mapped["User"] = relationship("User", back_populates="gmail_accounts")

    emails: Mapped[list["Email"]] = relationship(
        "Email", back_populates="account", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<GmailAccount(id={self.id}, label={self.account_label}, email={self.account_email})>"
