"""
Gmail OAuth2 authentication service.
Handles OAuth2 flow, credential storage/retrieval with encryption, and token refresh.
"""

import json
import secrets
from datetime import datetime, timedelta
from typing import Any

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.config import settings
from src.core.logging import get_logger
from src.models.account import GmailAccount
from src.models.user import User

logger = get_logger(__name__)


class GmailAuthService:
    """Service for Gmail OAuth2 authentication and credential management."""

    # OAuth2 scopes required for Gmail and Contacts access
    SCOPES = [
        "https://www.googleapis.com/auth/gmail.readonly",
        "https://www.googleapis.com/auth/contacts.readonly",
    ]

    # Valid account labels
    VALID_LABELS = ["procore-main", "procore-private", "personal"]

    def __init__(self, db_session: AsyncSession):
        """
        Initialize Gmail auth service.

        Args:
            db_session: SQLAlchemy async database session
        """
        self.db = db_session
        self._validate_config()

    def _validate_config(self) -> None:
        """Validate required OAuth2 configuration is present."""
        if not settings.google_client_id:
            raise ValueError("GOOGLE_CLIENT_ID not configured")
        if not settings.google_client_secret:
            raise ValueError("GOOGLE_CLIENT_SECRET not configured")
        if not settings.google_redirect_uri:
            raise ValueError("GOOGLE_REDIRECT_URI not configured")

    def get_auth_url(self, user_id: str, account_label: str) -> tuple[str, str]:
        """
        Generate OAuth2 authorization URL for user to authenticate Gmail account.

        Args:
            user_id: UUID of user initiating authentication
            account_label: Label for account (procore-main, procore-private, personal)

        Returns:
            Tuple of (authorization_url, state_token)
            - authorization_url: URL to redirect user to for authentication
            - state_token: CSRF token to validate callback (store in session)

        Raises:
            ValueError: If account_label is invalid
        """
        logger.info(f"Generating OAuth URL for user_id={user_id}, account_label={account_label}")

        if account_label not in self.VALID_LABELS:
            logger.error(f"Invalid account_label: {account_label}")
            raise ValueError(
                f"Invalid account_label '{account_label}'. "
                f"Must be one of: {', '.join(self.VALID_LABELS)}"
            )

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
            scopes=self.SCOPES,
            redirect_uri=settings.google_redirect_uri,
        )

        # Generate state token with user_id and account_label embedded
        # Format: {random_token}.{user_id}.{account_label}
        state_token = f"{secrets.token_urlsafe(32)}.{user_id}.{account_label}"

        # Generate authorization URL with state
        auth_url, _ = flow.authorization_url(
            access_type="offline",  # Request refresh token
            prompt="consent",  # Force consent screen to ensure refresh token
            state=state_token,
            include_granted_scopes="true",
        )

        logger.info(f"Generated OAuth URL successfully for user_id={user_id}")
        return auth_url, state_token

    async def handle_callback(self, code: str, state: str) -> dict[str, Any]:
        """
        Handle OAuth2 callback, exchange code for credentials, and store in database.

        Args:
            code: Authorization code from OAuth2 callback
            state: State token from callback (must match state from get_auth_url)

        Returns:
            Dictionary with account info:
            {
                "account_id": str,
                "user_id": str,
                "account_email": str,
                "account_label": str,
            }

        Raises:
            ValueError: If state token is invalid or user not found
            Exception: If OAuth2 exchange fails
        """
        logger.info("Processing OAuth2 callback")

        # Parse state token to extract user_id and account_label
        try:
            parts = state.split(".")
            if len(parts) != 3:
                raise ValueError("Invalid state token format")
            _, user_id, account_label = parts
        except (ValueError, IndexError) as e:
            logger.error(f"Invalid state token format: {e}")
            raise ValueError(f"Invalid state token: {e}")

        # Validate account_label
        if account_label not in self.VALID_LABELS:
            raise ValueError(f"Invalid account_label in state: {account_label}")

        # Verify user exists
        stmt = select(User).where(User.id == user_id)
        result = await self.db.execute(stmt)
        user = result.scalar_one_or_none()
        if not user:
            raise ValueError(f"User not found: {user_id}")

        # Create OAuth2 flow and exchange code for credentials
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
            scopes=self.SCOPES,
            redirect_uri=settings.google_redirect_uri,
        )

        flow.fetch_token(code=code)
        credentials = flow.credentials

        # Get user's email from credentials (from ID token)
        account_email = self._get_email_from_credentials(credentials)

        # Store encrypted credentials in database
        account_id = await self._store_credentials(
            user_id=user_id,
            account_email=account_email,
            account_label=account_label,
            credentials=credentials,
        )

        logger.info(
            f"OAuth2 callback processed successfully - "
            f"account_id={account_id}, email={account_email}, label={account_label}"
        )

        return {
            "account_id": str(account_id),
            "user_id": user_id,
            "account_email": account_email,
            "account_label": account_label,
        }

    async def get_credentials(self, account_id: str) -> Credentials:
        """
        Retrieve and decrypt OAuth2 credentials for a Gmail account.
        Automatically refreshes expired tokens.

        Args:
            account_id: UUID of gmail_accounts record

        Returns:
            google.oauth2.credentials.Credentials object ready to use

        Raises:
            ValueError: If account not found or credentials missing
        """
        logger.info(f"Retrieving credentials for account_id={account_id}")

        # Retrieve account from database
        stmt = select(GmailAccount).where(GmailAccount.id == account_id)
        result = await self.db.execute(stmt)
        account = result.scalar_one_or_none()

        if not account:
            logger.error(f"Gmail account not found: {account_id}")
            raise ValueError(f"Gmail account not found: {account_id}")

        if not account.credentials:
            logger.error(f"No credentials stored for account: {account_id}")
            raise ValueError(f"No credentials stored for account: {account_id}")

        # Decrypt credentials
        decrypted_creds = await self._decrypt_credentials(account.credentials)

        # Reconstruct Credentials object
        credentials = Credentials(
            token=decrypted_creds.get("token"),
            refresh_token=decrypted_creds.get("refresh_token"),
            token_uri=decrypted_creds.get("token_uri"),
            client_id=decrypted_creds.get("client_id"),
            client_secret=decrypted_creds.get("client_secret"),
            scopes=decrypted_creds.get("scopes"),
        )

        # Check if token is expired and refresh if needed
        if credentials.expired and credentials.refresh_token:
            logger.info(f"Token expired for account_id={account_id}, refreshing...")
            credentials.refresh(Request())
            # Update stored credentials with new token
            await self._update_credentials(account_id, credentials)
            logger.info(f"Token refreshed successfully for account_id={account_id}")

        return credentials

    async def revoke_credentials(self, account_id: str) -> None:
        """
        Revoke OAuth2 credentials and mark account as inactive.

        Args:
            account_id: UUID of gmail_accounts record

        Raises:
            ValueError: If account not found
        """
        stmt = select(GmailAccount).where(GmailAccount.id == account_id)
        result = await self.db.execute(stmt)
        account = result.scalar_one_or_none()

        if not account:
            raise ValueError(f"Gmail account not found: {account_id}")

        # Mark account as inactive and clear credentials
        account.is_active = False
        account.credentials = None
        account.updated_at = datetime.utcnow()

        await self.db.commit()

    # Private helper methods

    def _get_email_from_credentials(self, credentials: Credentials) -> str:
        """
        Extract email address from OAuth2 credentials ID token.

        Args:
            credentials: Google OAuth2 credentials

        Returns:
            Email address from ID token

        Raises:
            ValueError: If email cannot be extracted
        """
        if not credentials.id_token:
            raise ValueError("No ID token in credentials")

        email = credentials.id_token.get("email")
        if not email:
            raise ValueError("No email in ID token")

        return email

    async def _store_credentials(
        self,
        user_id: str,
        account_email: str,
        account_label: str,
        credentials: Credentials,
    ) -> str:
        """
        Store encrypted OAuth2 credentials in database.
        Updates existing account if found, creates new otherwise.

        Args:
            user_id: UUID of user
            account_email: Gmail address
            account_label: Account label
            credentials: Google OAuth2 credentials

        Returns:
            UUID of created/updated gmail_accounts record
        """
        # Prepare credentials dict for encryption
        creds_dict = {
            "token": credentials.token,
            "refresh_token": credentials.refresh_token,
            "token_uri": credentials.token_uri,
            "client_id": credentials.client_id,
            "client_secret": credentials.client_secret,
            "scopes": credentials.scopes,
            "expiry": credentials.expiry.isoformat() if credentials.expiry else None,
        }

        # Encrypt credentials using pgcrypto
        encrypted_creds = await self._encrypt_credentials(creds_dict)

        # Check if account already exists
        stmt = select(GmailAccount).where(
            GmailAccount.user_id == user_id,
            GmailAccount.account_email == account_email,
        )
        result = await self.db.execute(stmt)
        account = result.scalar_one_or_none()

        if account:
            # Update existing account
            account.account_label = account_label
            account.credentials = encrypted_creds
            account.is_active = True
            account.updated_at = datetime.utcnow()
            account_id = account.id
        else:
            # Create new account
            from uuid import uuid4

            account = GmailAccount(
                id=uuid4(),
                user_id=user_id,
                account_email=account_email,
                account_label=account_label,
                credentials=encrypted_creds,
                is_active=True,
            )
            self.db.add(account)
            account_id = account.id

        await self.db.commit()
        await self.db.refresh(account)

        return str(account_id)

    async def _update_credentials(self, account_id: str, credentials: Credentials) -> None:
        """
        Update stored credentials after token refresh.

        Args:
            account_id: UUID of gmail_accounts record
            credentials: Refreshed credentials
        """
        creds_dict = {
            "token": credentials.token,
            "refresh_token": credentials.refresh_token,
            "token_uri": credentials.token_uri,
            "client_id": credentials.client_id,
            "client_secret": credentials.client_secret,
            "scopes": credentials.scopes,
            "expiry": credentials.expiry.isoformat() if credentials.expiry else None,
        }

        encrypted_creds = await self._encrypt_credentials(creds_dict)

        stmt = select(GmailAccount).where(GmailAccount.id == account_id)
        result = await self.db.execute(stmt)
        account = result.scalar_one_or_none()

        if account:
            account.credentials = encrypted_creds
            account.updated_at = datetime.utcnow()
            await self.db.commit()

    async def _encrypt_credentials(self, creds_dict: dict[str, Any]) -> dict[str, Any]:
        """
        Encrypt credentials using pgcrypto.

        Args:
            creds_dict: Credentials dictionary to encrypt

        Returns:
            Dictionary with encrypted credentials suitable for JSON storage
        """
        # Convert to JSON string
        creds_json = json.dumps(creds_dict)

        # Encrypt using pgcrypto's pgp_sym_encrypt with secret_key
        # Note: This stores the encrypted data as bytea, but we'll base64 encode for JSON
        query = text(
            """
            SELECT encode(
                pgp_sym_encrypt(:creds_json::text, :secret_key),
                'base64'
            ) as encrypted
        """
        )

        result = await self.db.execute(
            query,
            {
                "creds_json": creds_json,
                "secret_key": settings.secret_key,
            },
        )
        row = result.fetchone()

        if not row:
            raise ValueError("Failed to encrypt credentials")

        # Return as dict with encrypted field for JSON storage
        return {"encrypted": row[0]}

    async def _decrypt_credentials(self, encrypted_dict: dict[str, Any]) -> dict[str, Any]:
        """
        Decrypt credentials using pgcrypto.

        Args:
            encrypted_dict: Dictionary with encrypted credentials

        Returns:
            Decrypted credentials dictionary
        """
        if "encrypted" not in encrypted_dict:
            raise ValueError("Invalid encrypted credentials format")

        encrypted_data = encrypted_dict["encrypted"]

        # Decrypt using pgcrypto's pgp_sym_decrypt
        query = text(
            """
            SELECT pgp_sym_decrypt(
                decode(:encrypted_data, 'base64'),
                :secret_key
            ) as decrypted
        """
        )

        result = await self.db.execute(
            query,
            {
                "encrypted_data": encrypted_data,
                "secret_key": settings.secret_key,
            },
        )
        row = result.fetchone()

        if not row:
            raise ValueError("Failed to decrypt credentials")

        # Parse JSON string back to dict
        decrypted_json = row[0]
        return json.loads(decrypted_json)

    def __repr__(self) -> str:
        """String representation (never include sensitive data)."""
        return f"<GmailAuthService(scopes={len(self.SCOPES)})>"
