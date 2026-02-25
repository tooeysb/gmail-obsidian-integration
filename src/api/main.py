"""
FastAPI application entry point.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.core.config import settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    # Startup
    print(f"🚀 Starting Gmail-to-Obsidian Integration API")
    print(f"📧 Environment: {settings.app_env}")
    print(f"🗄️  Database: {settings.supabase_url}")

    yield

    # Shutdown
    print("👋 Shutting down API")


# Create FastAPI application
app = FastAPI(
    title="Gmail-to-Obsidian Integration",
    description="Automated system to process Gmail history and create Obsidian vault",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if settings.is_development else [settings.app_url],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "gmail-obsidian-integration",
        "version": "1.0.0",
        "environment": settings.app_env,
    }


@app.get("/health")
async def health_check():
    """Detailed health check."""
    return {
        "status": "healthy",
        "database": "connected",  # TODO: Add actual DB health check
        "redis": "connected",  # TODO: Add actual Redis health check
    }


# Include routers
from src.api.routers import auth, scan

app.include_router(auth.router, prefix="/auth", tags=["authentication"])
app.include_router(scan.router, prefix="/scan", tags=["scanning"])
