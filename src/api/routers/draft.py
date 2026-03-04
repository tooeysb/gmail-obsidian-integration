"""
Email drafting API router.

Provides endpoints for generating voice-matched email drafts.
"""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from src.api.middleware.auth import get_current_user
from src.core.database import get_sync_db
from src.core.logging import get_logger
from src.models.user import User
from src.services.voice.draft_service import EmailDraftService

logger = get_logger(__name__)

router = APIRouter()


class DraftRequest(BaseModel):
    """Request body for composing an email draft."""

    recipient_email: str
    context: str
    tone: str | None = None
    reply_to_subject: str | None = None


class DraftResponse(BaseModel):
    """Response body with the generated email draft."""

    subject: str
    body: str
    similar_emails_used: int
    voice_profile_used: str
    model: str


@router.post("/compose", response_model=DraftResponse)
def compose_draft(
    request: DraftRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_sync_db),
):
    """
    Generate a voice-matched email draft.

    Uses the user's voice profile and similar sent emails to draft
    an email that matches their natural writing style.
    """
    try:
        service = EmailDraftService(db)
        result = service.draft_email(
            user_id=str(user.id),
            recipient_email=request.recipient_email,
            context=request.context,
            tone=request.tone,
            reply_to_subject=request.reply_to_subject,
        )

        return DraftResponse(
            subject=result.subject,
            body=result.body,
            similar_emails_used=result.similar_emails_used,
            voice_profile_used=result.voice_profile_used,
            model=result.model,
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from None
    except Exception:
        logger.error("Draft generation failed", exc_info=True)
        raise HTTPException(status_code=500, detail="Draft generation failed") from None
