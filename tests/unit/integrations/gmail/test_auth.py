"""
Unit tests for Gmail OAuth2 authentication service.
Tests credential encryption, token refresh, and OAuth2 flow.
"""

import json
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, Mock, patch
from uuid import UUID, uuid4

import pytest
from google.oauth2.credentials import Credentials
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.integrations.gmail.auth import GmailAuthService
from src.models.account import GmailAccount
from src.models.user import User


@pytest.fixture
def mock_db_session():
    """Mock async database session."""
    session = AsyncMock(spec=AsyncSession)
    return session


@pytest.fixture
def auth_service(mock_db_session):
    """Create GmailAuthService instance with mocked dependencies."""
    with patch("src.integrations.gmail.auth.settings") as mock_settings:
        mock_settings.google_client_id = "test-client-id"
        mock_settings.google_client_secret = "test-client-secret"
        mock_settings.google_redirect_uri = "http://localhost:8000/auth/callback"
        mock_settings.secret_key = "test-secret-key-for-encryption"

        service = GmailAuthService(mock_db_session)
        return service


@pytest.fixture
def test_user():
    """Create test user."""
    return User(
        id=uuid4(),
        email="test@example.com",
        name="Test User",
    )


@pytest.fixture
def test_gmail_account(test_user):
    """Create test Gmail account."""
    return GmailAccount(
        id=uuid4(),
        user_id=test_user.id,
        account_email="test@gmail.com",
        account_label="personal",
        credentials={"encrypted": "base64_encrypted_data_here"},
        is_active=True,
    )


@pytest.fixture
def mock_credentials():
    """Create mock Google OAuth2 credentials."""
    return Credentials(
        token="test-access-token",
        refresh_token="test-refresh-token",
        token_uri="https://oauth2.googleapis.com/token",
        client_id="test-client-id",
        client_secret="test-client-secret",
        scopes=[
            "https://www.googleapis.com/auth/gmail.readonly",
            "https://www.googleapis.com/auth/contacts.readonly",
        ],
    )


class TestGmailAuthServiceInit:
    """Test GmailAuthService initialization."""

    def test_init_success(self, mock_db_session):
        """Test successful initialization with valid config."""
        with patch("src.integrations.gmail.auth.settings") as mock_settings:
            mock_settings.google_client_id = "test-client-id"
            mock_settings.google_client_secret = "test-client-secret"
            mock_settings.google_redirect_uri = "http://localhost:8000/callback"

            service = GmailAuthService(mock_db_session)
            assert service.db == mock_db_session
            assert service.SCOPES == [
                "https://www.googleapis.com/auth/gmail.readonly",
                "https://www.googleapis.com/auth/contacts.readonly",
            ]

    def test_init_missing_client_id(self, mock_db_session):
        """Test initialization fails with missing client ID."""
        with patch("src.integrations.gmail.auth.settings") as mock_settings:
            mock_settings.google_client_id = None
            mock_settings.google_client_secret = "test-secret"
            mock_settings.google_redirect_uri = "http://localhost:8000/callback"

            with pytest.raises(ValueError, match="GOOGLE_CLIENT_ID not configured"):
                GmailAuthService(mock_db_session)

    def test_init_missing_client_secret(self, mock_db_session):
        """Test initialization fails with missing client secret."""
        with patch("src.integrations.gmail.auth.settings") as mock_settings:
            mock_settings.google_client_id = "test-id"
            mock_settings.google_client_secret = None
            mock_settings.google_redirect_uri = "http://localhost:8000/callback"

            with pytest.raises(ValueError, match="GOOGLE_CLIENT_SECRET not configured"):
                GmailAuthService(mock_db_session)


class TestGetAuthUrl:
    """Test get_auth_url method."""

    def test_get_auth_url_success(self, auth_service):
        """Test successful auth URL generation."""
        user_id = str(uuid4())
        account_label = "procore-main"

        with patch("src.integrations.gmail.auth.Flow") as mock_flow_class:
            mock_flow = MagicMock()
            mock_flow.authorization_url.return_value = (
                "https://accounts.google.com/auth?state=abc123",
                None,
            )
            mock_flow_class.from_client_config.return_value = mock_flow

            auth_url, state = auth_service.get_auth_url(user_id, account_label)

            # Verify auth URL is returned
            assert auth_url.startswith("https://accounts.google.com/auth")

            # Verify state token format: {random}.{user_id}.{label}
            state_parts = state.split(".")
            assert len(state_parts) == 3
            assert state_parts[1] == user_id
            assert state_parts[2] == account_label

            # Verify flow was created with correct scopes
            mock_flow_class.from_client_config.assert_called_once()
            call_args = mock_flow_class.from_client_config.call_args
            assert call_args[1]["scopes"] == auth_service.SCOPES

            # Verify authorization_url was called with correct params
            mock_flow.authorization_url.assert_called_once()
            call_kwargs = mock_flow.authorization_url.call_args[1]
            assert call_kwargs["access_type"] == "offline"
            assert call_kwargs["prompt"] == "consent"

    def test_get_auth_url_invalid_label(self, auth_service):
        """Test auth URL generation with invalid account label."""
        user_id = str(uuid4())
        invalid_label = "invalid-label"

        with pytest.raises(ValueError, match="Invalid account_label"):
            auth_service.get_auth_url(user_id, invalid_label)

    def test_get_auth_url_all_valid_labels(self, auth_service):
        """Test auth URL generation with all valid labels."""
        user_id = str(uuid4())

        with patch("src.integrations.gmail.auth.Flow") as mock_flow_class:
            mock_flow = MagicMock()
            mock_flow.authorization_url.return_value = (
                "https://accounts.google.com/auth",
                None,
            )
            mock_flow_class.from_client_config.return_value = mock_flow

            for label in ["procore-main", "procore-private", "personal"]:
                auth_url, state = auth_service.get_auth_url(user_id, label)
                assert auth_url is not None
                assert label in state


class TestHandleCallback:
    """Test handle_callback method."""

    @pytest.mark.asyncio
    async def test_handle_callback_success(
        self, auth_service, mock_db_session, test_user, mock_credentials
    ):
        """Test successful OAuth2 callback handling."""
        state_token = f"random_token_123.{test_user.id}.procore-main"
        code = "test-authorization-code"

        # Mock database queries
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = test_user
        mock_db_session.execute.return_value = mock_result

        # Mock OAuth2 flow
        with patch("src.integrations.gmail.auth.Flow") as mock_flow_class:
            mock_flow = MagicMock()
            mock_flow.credentials = mock_credentials
            mock_flow.credentials.id_token = {"email": "test@procore.com"}
            mock_flow_class.from_client_config.return_value = mock_flow

            # Mock credential encryption
            auth_service._encrypt_credentials = AsyncMock(
                return_value={"encrypted": "base64_data"}
            )

            result = await auth_service.handle_callback(code, state_token)

            # Verify result structure
            assert "account_id" in result
            assert result["user_id"] == str(test_user.id)
            assert result["account_email"] == "test@procore.com"
            assert result["account_label"] == "procore-main"

            # Verify OAuth2 flow was executed
            mock_flow.fetch_token.assert_called_once_with(code=code)

    @pytest.mark.asyncio
    async def test_handle_callback_invalid_state(self, auth_service):
        """Test callback with invalid state token."""
        invalid_state = "invalid_format"
        code = "test-code"

        with pytest.raises(ValueError, match="Invalid state token"):
            await auth_service.handle_callback(code, invalid_state)

    @pytest.mark.asyncio
    async def test_handle_callback_user_not_found(
        self, auth_service, mock_db_session, test_user
    ):
        """Test callback when user doesn't exist."""
        state_token = f"random_token_123.{test_user.id}.procore-main"
        code = "test-authorization-code"

        # Mock database to return no user
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_db_session.execute.return_value = mock_result

        with pytest.raises(ValueError, match="User not found"):
            await auth_service.handle_callback(code, state_token)

    @pytest.mark.asyncio
    async def test_handle_callback_invalid_label_in_state(
        self, auth_service, mock_db_session, test_user
    ):
        """Test callback with invalid label in state token."""
        state_token = f"random_token_123.{test_user.id}.invalid-label"
        code = "test-authorization-code"

        with pytest.raises(ValueError, match="Invalid account_label in state"):
            await auth_service.handle_callback(code, state_token)


class TestGetCredentials:
    """Test get_credentials method."""

    @pytest.mark.asyncio
    async def test_get_credentials_success(
        self, auth_service, mock_db_session, test_gmail_account, mock_credentials
    ):
        """Test successful credential retrieval."""
        # Mock database query
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = test_gmail_account
        mock_db_session.execute.return_value = mock_result

        # Mock credential decryption
        decrypted_creds = {
            "token": "test-access-token",
            "refresh_token": "test-refresh-token",
            "token_uri": "https://oauth2.googleapis.com/token",
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "scopes": [
                "https://www.googleapis.com/auth/gmail.readonly",
                "https://www.googleapis.com/auth/contacts.readonly",
            ],
            "expiry": (datetime.utcnow() + timedelta(hours=1)).isoformat(),
        }
        auth_service._decrypt_credentials = AsyncMock(return_value=decrypted_creds)

        credentials = await auth_service.get_credentials(str(test_gmail_account.id))

        # Verify credentials object
        assert isinstance(credentials, Credentials)
        assert credentials.token == "test-access-token"
        assert credentials.refresh_token == "test-refresh-token"

    @pytest.mark.asyncio
    async def test_get_credentials_account_not_found(self, auth_service, mock_db_session):
        """Test credential retrieval when account doesn't exist."""
        account_id = str(uuid4())

        # Mock database to return no account
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_db_session.execute.return_value = mock_result

        with pytest.raises(ValueError, match="Gmail account not found"):
            await auth_service.get_credentials(account_id)

    @pytest.mark.asyncio
    async def test_get_credentials_no_credentials_stored(
        self, auth_service, mock_db_session, test_gmail_account
    ):
        """Test credential retrieval when no credentials are stored."""
        test_gmail_account.credentials = None

        # Mock database query
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = test_gmail_account
        mock_db_session.execute.return_value = mock_result

        with pytest.raises(ValueError, match="No credentials stored"):
            await auth_service.get_credentials(str(test_gmail_account.id))

    @pytest.mark.asyncio
    async def test_get_credentials_with_token_refresh(
        self, auth_service, mock_db_session, test_gmail_account
    ):
        """Test credential retrieval with expired token requiring refresh."""
        # Mock database query
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = test_gmail_account
        mock_db_session.execute.return_value = mock_result

        # Mock expired credentials
        decrypted_creds = {
            "token": "old-access-token",
            "refresh_token": "test-refresh-token",
            "token_uri": "https://oauth2.googleapis.com/token",
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "scopes": [
                "https://www.googleapis.com/auth/gmail.readonly",
            ],
            "expiry": (datetime.utcnow() - timedelta(hours=1)).isoformat(),  # Expired
        }
        auth_service._decrypt_credentials = AsyncMock(return_value=decrypted_creds)
        auth_service._update_credentials = AsyncMock()

        with patch.object(Credentials, "refresh") as mock_refresh:
            credentials = await auth_service.get_credentials(str(test_gmail_account.id))

            # Verify refresh was called
            mock_refresh.assert_called_once()

            # Verify credentials were updated in database
            auth_service._update_credentials.assert_called_once()


class TestRevokeCredentials:
    """Test revoke_credentials method."""

    @pytest.mark.asyncio
    async def test_revoke_credentials_success(
        self, auth_service, mock_db_session, test_gmail_account
    ):
        """Test successful credential revocation."""
        # Mock database query
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = test_gmail_account
        mock_db_session.execute.return_value = mock_result

        await auth_service.revoke_credentials(str(test_gmail_account.id))

        # Verify account was marked inactive and credentials cleared
        assert test_gmail_account.is_active is False
        assert test_gmail_account.credentials is None
        mock_db_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_revoke_credentials_account_not_found(
        self, auth_service, mock_db_session
    ):
        """Test revocation when account doesn't exist."""
        account_id = str(uuid4())

        # Mock database to return no account
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_db_session.execute.return_value = mock_result

        with pytest.raises(ValueError, match="Gmail account not found"):
            await auth_service.revoke_credentials(account_id)


class TestCredentialEncryption:
    """Test credential encryption and decryption."""

    @pytest.mark.asyncio
    async def test_encrypt_decrypt_credentials_roundtrip(
        self, auth_service, mock_db_session
    ):
        """Test encrypting and decrypting credentials produces original data."""
        original_creds = {
            "token": "test-access-token",
            "refresh_token": "test-refresh-token",
            "token_uri": "https://oauth2.googleapis.com/token",
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "scopes": ["https://www.googleapis.com/auth/gmail.readonly"],
            "expiry": datetime.utcnow().isoformat(),
        }

        # Mock encryption/decryption queries
        async def mock_execute(query, params):
            result = MagicMock()
            if "pgp_sym_encrypt" in str(query):
                # Simulate encryption
                result.fetchone.return_value = ("base64_encrypted_data",)
            elif "pgp_sym_decrypt" in str(query):
                # Simulate decryption
                result.fetchone.return_value = (json.dumps(original_creds),)
            return result

        mock_db_session.execute = mock_execute

        # Encrypt then decrypt
        encrypted = await auth_service._encrypt_credentials(original_creds)
        assert "encrypted" in encrypted

        decrypted = await auth_service._decrypt_credentials(encrypted)

        # Verify roundtrip produces original data
        assert decrypted == original_creds


class TestSecurityAndLogging:
    """Test security and logging features."""

    def test_no_sensitive_data_in_repr(self, auth_service):
        """Test that __repr__ doesn't expose sensitive data."""
        repr_str = repr(auth_service)
        assert "token" not in repr_str.lower()
        assert "secret" not in repr_str.lower()
        assert "credentials" not in repr_str.lower()

    def test_valid_labels_constant(self, auth_service):
        """Test VALID_LABELS constant matches expected values."""
        assert auth_service.VALID_LABELS == ["procore-main", "procore-private", "personal"]

    def test_required_scopes_constant(self, auth_service):
        """Test SCOPES constant includes required scopes."""
        assert "https://www.googleapis.com/auth/gmail.readonly" in auth_service.SCOPES
        assert "https://www.googleapis.com/auth/contacts.readonly" in auth_service.SCOPES
