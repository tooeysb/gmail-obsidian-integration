"""
SSO authentication middleware.

Requires visitors to authenticate at the HTH Corp Portal before accessing CRM.
Validates a `crm_session` JWT cookie on each request. If missing or invalid,
redirects to the Portal login page with a `next` parameter pointing back here.
"""

from urllib.parse import quote

import jwt
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import RedirectResponse, Response

from src.core.config import settings
from src.core.logging import get_logger

logger = get_logger(__name__)

# Routes that do not require SSO authentication.
PUBLIC_PREFIXES = (
    "/health",
    "/auth/sso",
    "/auth/login",
    "/auth/callback",
    "/auth/status",
    "/dashboard/stats",
    "/dashboard/widget",
    "/docs",
    "/openapi.json",
)


class SSOMiddleware(BaseHTTPMiddleware):
    """Redirect unauthenticated visitors to the HTH Corp Portal login."""

    async def dispatch(self, request: Request, call_next) -> Response:
        path = request.url.path

        # Allow public routes through without auth
        if any(path.startswith(prefix) for prefix in PUBLIC_PREFIXES):
            return await call_next(request)

        # Allow requests that carry a valid API key (existing machine-to-machine auth)
        api_key = request.headers.get("X-API-Key")
        if api_key and api_key == settings.secret_key:
            return await call_next(request)

        # Check for SSO session cookie
        token = request.cookies.get("crm_session")
        if token:
            try:
                jwt.decode(token, settings.sso_jwt_secret, algorithms=["HS256"])
                return await call_next(request)
            except jwt.ExpiredSignatureError:
                logger.info("SSO cookie expired, redirecting to login")
            except jwt.InvalidTokenError:
                logger.warning("Invalid SSO cookie, redirecting to login")

        # No valid session — redirect to Portal login
        current_url = str(request.url)
        login_url = f"{settings.portal_login_url}?next={quote(current_url)}"
        return RedirectResponse(url=login_url, status_code=302)
