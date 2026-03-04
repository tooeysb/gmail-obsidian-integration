"""
FastAPI application entry point.
"""

from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

import jwt
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from src.api.middleware.correlation import CorrelationIdMiddleware
from src.api.middleware.sso import SSOMiddleware
from src.core.config import settings
from src.core.logging import get_logger

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    # Startup
    logger.info("Starting CRM-HTH API")
    logger.info("Environment: %s", settings.app_env)
    logger.info("Database: %s", settings.supabase_url)

    yield

    # Shutdown
    logger.info("Shutting down API")


# Create FastAPI application
app = FastAPI(
    title="CRM-HTH",
    description="Autonomous email processing pipeline with relationship intelligence",
    version="1.0.0",
    lifespan=lifespan,
)

# Middleware (order matters: last added = first executed)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if settings.is_development else [settings.app_url],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(CorrelationIdMiddleware)
if settings.sso_jwt_secret:
    app.add_middleware(SSOMiddleware)


@app.get("/")
async def root():
    """Redirect root to CRM frontend."""
    return RedirectResponse(url="/crm")


@app.get("/health")
async def health_check():
    """Detailed health check with real DB and Redis probes."""
    health: dict = {"status": "healthy"}

    # DB check
    try:
        from sqlalchemy import text

        from src.core.database import sync_engine

        with sync_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        health["database"] = "connected"
    except Exception:
        health["database"] = "disconnected"
        health["status"] = "degraded"

    # Redis check
    try:
        import redis as redis_lib

        r = redis_lib.from_url(
            settings.redis_url,
            socket_connect_timeout=2,
            ssl_cert_reqs=None,  # Heroku Redis uses self-signed TLS certs
        )
        r.ping()
        r.close()
        health["redis"] = "connected"
    except Exception:
        health["redis"] = "disconnected"
        health["status"] = "degraded"

    from fastapi.responses import JSONResponse

    status_code = 200 if health["status"] == "healthy" else 503
    return JSONResponse(content=health, status_code=status_code)


@app.get("/auth/sso")
async def sso_callback(token: str = Query(..., description="SSO JWT from Portal")):
    """Validate Portal SSO token and set a long-lived session cookie."""
    if not settings.sso_jwt_secret:
        logger.error("SSO_JWT_SECRET not configured")
        return RedirectResponse(url="/health")

    try:
        payload = jwt.decode(token, settings.sso_jwt_secret, algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        logger.warning("SSO token expired")
        login_url = f"{settings.portal_login_url}?error=token_expired"
        return RedirectResponse(url=login_url, status_code=302)
    except jwt.InvalidTokenError:
        logger.warning("Invalid SSO token")
        login_url = f"{settings.portal_login_url}?error=invalid_token"
        return RedirectResponse(url=login_url, status_code=302)

    # Re-sign as a long-lived session cookie (24 hours)
    session_token = jwt.encode(
        {
            "user_id": payload["user_id"],
            "email": payload["email"],
            "exp": datetime.now(UTC) + timedelta(hours=24),
        },
        settings.sso_jwt_secret,
        algorithm="HS256",
    )

    response = RedirectResponse(url="/crm", status_code=302)
    response.set_cookie(
        key="crm_session",
        value=session_token,
        max_age=86400,
        httponly=True,
        secure=not settings.is_development,
        samesite="lax",
    )
    logger.info("SSO login successful for %s", payload.get("email"))
    return response


# Include routers
from src.api.routers import auth, crm, dashboard, draft, outreach, scan

app.include_router(auth.router, prefix="/auth", tags=["authentication"])
app.include_router(scan.router, prefix="/scan", tags=["scanning"])
app.include_router(dashboard.router, prefix="/dashboard", tags=["dashboard"])
app.include_router(draft.router, prefix="/draft", tags=["draft"])
app.include_router(crm.router, prefix="/crm/api", tags=["crm"])
app.include_router(outreach.router, prefix="/crm/api/outreach", tags=["outreach"])


@app.get("/crm")
async def crm_frontend():
    """Serve CRM frontend with API key injected."""
    html_path = Path("src/static/crm/index.html")
    html = html_path.read_text()
    # Inject API key before closing </head> so apiFetch can authenticate
    api_key_script = f'<script>window.CRM_API_KEY="{settings.secret_key}";</script></head>'
    html = html.replace("</head>", api_key_script)
    return HTMLResponse(content=html)


# Static file mounts (must be after all route registrations)
app.mount("/crm/static", StaticFiles(directory="src/static/crm"), name="crm-static")
