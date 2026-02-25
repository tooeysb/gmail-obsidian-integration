"""
Authentication routes for Gmail OAuth2 flow.
"""

import uuid
from typing import Any

import json
import secrets
import uuid
import warnings
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from google_auth_oauthlib.flow import Flow
from oauthlib.oauth2.rfc6749.errors import OAuth2Error
from pydantic import BaseModel
from sqlalchemy.orm import Session

from src.core.config import settings
from src.core.database import get_sync_db
from src.core.logging import get_logger
from src.integrations.gmail.auth import GmailAuthService
from src.models import GmailAccount, User

logger = get_logger(__name__)

router = APIRouter()

# OAuth2 scopes
GMAIL_SCOPES = [
    "openid",  # Required to get id_token with email
    "https://www.googleapis.com/auth/userinfo.email",  # Explicit email scope
    "https://www.googleapis.com/auth/gmail.readonly",
    # Note: Contacts scope removed for now - can add back later if needed
    # "https://www.googleapis.com/auth/contacts.readonly",
]


# Response models
class AuthUrlResponse(BaseModel):
    """Response for auth URL generation."""

    auth_url: str
    account_label: str
    message: str


class AuthCallbackResponse(BaseModel):
    """Response for OAuth callback."""

    status: str
    message: str
    account_label: str | None = None
    account_email: str | None = None


class AccountStatus(BaseModel):
    """Account authentication status."""

    label: str
    email: str
    is_active: bool
    last_synced_at: str | None


class AuthStatusResponse(BaseModel):
    """Response for authentication status check."""

    user_id: str
    authenticated_accounts: list[AccountStatus]
    total_accounts: int


@router.get("/login/{account_label}", response_model=AuthUrlResponse)
async def initiate_oauth(
    account_label: str,
    user_id: str = Query(..., description="User ID"),
    db: Session = Depends(get_sync_db),
) -> AuthUrlResponse:
    """
    Initiate OAuth2 flow for a specific Gmail account.

    Args:
        account_label: Account identifier (procore-main, procore-private, personal)
        user_id: User ID
        db: Database session

    Returns:
        Authorization URL for user to click
    """
    logger.info(f"Initiating OAuth for user {user_id}, account {account_label}")

    # Validate account label
    valid_labels = ["procore-main", "procore-private", "personal"]
    if account_label not in valid_labels:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid account label. Must be one of: {', '.join(valid_labels)}",
        )

    # Ensure user exists
    user = db.query(User).filter(User.id == uuid.UUID(user_id)).first()
    if not user:
        raise HTTPException(status_code=404, detail=f"User {user_id} not found")

    # Generate OAuth URL
    try:
        # Create OAuth2 flow
        flow = Flow.from_client_config(
            client_config={
                "web": {
                    "client_id": settings.google_client_id,
                    "client_secret": settings.google_client_secret,
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "redirect_uris": [settings.google_redirect_uri],
                }
            },
            scopes=GMAIL_SCOPES,
            redirect_uri=settings.google_redirect_uri,
        )

        # Generate state token with user_id and account_label
        state_data = f"{user_id}:{account_label}:{secrets.token_urlsafe(32)}"
        auth_url, _ = flow.authorization_url(
            access_type="offline",
            include_granted_scopes="true",
            state=state_data,
            prompt="consent",
        )

        logger.info(f"Generated auth URL for {account_label}")
    except Exception as e:
        logger.error(f"Error generating auth URL: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to generate authorization URL")

    return AuthUrlResponse(
        auth_url=auth_url,
        account_label=account_label,
        message=f"Click the URL to authorize {account_label} account",
    )


@router.get("/callback", response_model=AuthCallbackResponse)
async def oauth_callback(
    code: str = Query(..., description="Authorization code from Google"),
    state: str = Query(..., description="State token (contains user_id and account_label)"),
    db: Session = Depends(get_sync_db),
) -> AuthCallbackResponse:
    """
    Handle OAuth2 callback from Google.

    Args:
        code: Authorization code
        state: State token
        db: Database session

    Returns:
        Success/failure status
    """
    logger.info("Received OAuth callback")

    try:
        # Parse state token
        state_parts = state.split(":")
        if len(state_parts) < 2:
            raise ValueError("Invalid state token")

        user_id = state_parts[0]
        account_label = state_parts[1]

        logger.info(f"Processing callback for user {user_id}, account {account_label}")

        # Exchange code for credentials
        flow = Flow.from_client_config(
            client_config={
                "web": {
                    "client_id": settings.google_client_id,
                    "client_secret": settings.google_client_secret,
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "redirect_uris": [settings.google_redirect_uri],
                }
            },
            scopes=GMAIL_SCOPES,
            redirect_uri=settings.google_redirect_uri,
        )

        # Disable strict scope validation (Google may grant additional scopes)
        flow.oauth2session.scope = None

        # Fetch token
        flow.fetch_token(code=code)
        credentials = flow.credentials

        # Get user info to extract email
        # Try to get email from id_token first (most reliable)
        account_email = None

        if hasattr(credentials, 'id_token') and credentials.id_token:
            import jwt
            id_token_claims = jwt.decode(credentials.id_token, options={"verify_signature": False})
            account_email = id_token_claims.get("email")
            logger.info(f"Extracted email from id_token: {account_email}")

        # Fallback to userinfo endpoint if needed
        if not account_email:
            import requests as http_requests
            logger.info("Falling back to userinfo endpoint")
            userinfo_response = http_requests.get(
                "https://www.googleapis.com/oauth2/v2/userinfo",
                headers={"Authorization": f"Bearer {credentials.token}"}
            )
            userinfo = userinfo_response.json()
            logger.info(f"Userinfo response: {userinfo}")
            account_email = userinfo.get("email")

        if not account_email:
            logger.error("Could not retrieve email address from any source")
            raise ValueError("Could not retrieve email address from Google account")

        # Store credentials in database
        import json
        from datetime import datetime

        # Check if account already exists
        existing_account = db.query(GmailAccount).filter(
            GmailAccount.user_id == uuid.UUID(user_id),
            GmailAccount.account_label == account_label
        ).first()

        credentials_dict = {
            "token": credentials.token,
            "refresh_token": credentials.refresh_token,
            "token_uri": credentials.token_uri,
            "client_id": credentials.client_id,
            "client_secret": credentials.client_secret,
            "scopes": credentials.scopes,
        }

        if existing_account:
            # Update existing account
            existing_account.account_email = account_email
            existing_account.credentials = json.dumps(credentials_dict)
            existing_account.is_active = True
            existing_account.updated_at = datetime.utcnow()
        else:
            # Create new account
            new_account = GmailAccount(
                id=str(uuid.uuid4()),
                user_id=uuid.UUID(user_id),
                account_email=account_email,
                account_label=account_label,
                credentials=json.dumps(credentials_dict),
                is_active=True,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
            db.add(new_account)

        db.commit()

        logger.info(f"OAuth callback successful for {account_label} ({account_email})")

        return AuthCallbackResponse(
            status="success",
            message=f"Successfully authenticated {account_label} account",
            account_label=account_label,
            account_email=account_email,
        )

    except ValueError as e:
        logger.error(f"OAuth callback validation error: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

    except Exception as e:
        logger.error(f"OAuth callback error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Authentication failed")


@router.get("/status", response_model=AuthStatusResponse)
async def check_auth_status(
    user_id: str = Query(..., description="User ID"), db: Session = Depends(get_sync_db)
) -> AuthStatusResponse:
    """
    Check authentication status for all Gmail accounts.

    Args:
        user_id: User ID
        db: Database session

    Returns:
        List of authenticated accounts with status
    """
    logger.info(f"Checking auth status for user {user_id}")

    # Get user
    user = db.query(User).filter(User.id == uuid.UUID(user_id)).first()
    if not user:
        raise HTTPException(status_code=404, detail=f"User {user_id} not found")

    # Get all accounts for user
    accounts = (
        db.query(GmailAccount).filter(GmailAccount.user_id == uuid.UUID(user_id)).all()
    )

    account_statuses = [
        AccountStatus(
            label=account.account_label,
            email=account.account_email,
            is_active=account.is_active,
            last_synced_at=account.last_synced_at.isoformat() if account.last_synced_at else None,
        )
        for account in accounts
    ]

    logger.info(f"Found {len(accounts)} accounts for user {user_id}")

    return AuthStatusResponse(
        user_id=user_id,
        authenticated_accounts=account_statuses,
        total_accounts=len(accounts),
    )


@router.post("/revoke/{account_id}")
async def revoke_account(
    account_id: str,
    user_id: str = Query(..., description="User ID"),
    db: Session = Depends(get_sync_db),
) -> dict[str, Any]:
    """
    Revoke OAuth credentials for a Gmail account.

    Args:
        account_id: Account ID
        user_id: User ID
        db: Database session

    Returns:
        Success status
    """
    logger.info(f"Revoking credentials for account {account_id}")

    # Verify account belongs to user
    account = (
        db.query(GmailAccount)
        .filter(
            GmailAccount.id == uuid.UUID(account_id),
            GmailAccount.user_id == uuid.UUID(user_id),
        )
        .first()
    )

    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    # Revoke credentials
    auth_service = GmailAuthService()
    try:
        auth_service.revoke_credentials(account_id, db)
        logger.info(f"Successfully revoked credentials for {account.account_label}")

        return {
            "status": "success",
            "message": f"Revoked credentials for {account.account_label}",
        }

    except Exception as e:
        logger.error(f"Error revoking credentials: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to revoke credentials")
