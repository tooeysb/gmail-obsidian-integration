"""
Voice Profile model for storing writing style analysis.
"""

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSON, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.models.base import Base, TimestampMixin, UUIDMixin

if TYPE_CHECKING:
    from src.models.user import User


class VoiceProfile(Base, UUIDMixin, TimestampMixin):
    """
    Stores Claude-generated writing voice analysis.

    Built by analyzing the user's sent emails to capture writing patterns,
    tone, vocabulary, and audience-specific adaptations.
    """

    __tablename__ = "voice_profiles"
    __table_args__ = (UniqueConstraint("user_id", "profile_name", name="uq_user_voice_profile"),)

    # Foreign Keys
    user_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="User who owns this voice profile",
    )

    profile_name: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        default="default",
        comment="Profile name (e.g., 'default', 'executive', 'casual')",
    )

    profile_data: Mapped[dict | None] = mapped_column(
        JSON,
        nullable=True,
        comment="Structured voice analysis: core traits, audience adaptations, anti-patterns",
    )

    sample_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, comment="Number of sent emails analyzed"
    )

    generated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="When voice analysis was last run"
    )

    # Relationships
    user: Mapped["User"] = relationship("User", back_populates="voice_profiles")

    def __repr__(self) -> str:
        return (
            f"<VoiceProfile(id={self.id}, user_id={self.user_id}, "
            f"name={self.profile_name}, samples={self.sample_count})>"
        )
