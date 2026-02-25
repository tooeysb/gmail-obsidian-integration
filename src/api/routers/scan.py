"""
Scan routes for starting and monitoring Gmail scans.
"""

import uuid
from datetime import datetime
from typing import Any

from celery.result import AsyncResult
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from src.core.config import settings
from src.core.database import get_sync_db
from src.core.logging import get_logger
from src.models import GmailAccount, SyncJob, User
from src.worker.celery_app import celery_app
from src.worker.tasks import scan_gmail_task

logger = get_logger(__name__)

router = APIRouter()


# Request/Response models
class StartScanRequest(BaseModel):
    """Request to start a Gmail scan."""

    user_id: str = Field(..., description="User ID")
    account_labels: list[str] | None = Field(
        default=None,
        description="Account labels to scan (defaults to all: procore-main, procore-private, personal)",
    )


class StartScanResponse(BaseModel):
    """Response for scan start."""

    job_id: str
    status: str
    accounts: list[str]
    status_url: str
    message: str


class JobStatusResponse(BaseModel):
    """Response for job status check."""

    job_id: str
    status: str
    phase: str | None
    progress: int
    emails_processed: int
    emails_total: int | None
    contacts_processed: int
    started_at: str | None
    completed_at: str | None
    error_message: str | None
    duration_seconds: int | None


@router.post("/start", response_model=StartScanResponse)
async def start_scan(
    request: StartScanRequest, db: Session = Depends(get_sync_db)
) -> StartScanResponse:
    """
    Start a multi-account Gmail scan.

    Args:
        request: Scan request with user_id and optional account_labels
        db: Database session

    Returns:
        Job ID and status URL for monitoring
    """
    logger.info(f"Starting scan for user {request.user_id}, accounts: {request.account_labels}")

    # Validate user exists
    user = db.query(User).filter(User.id == uuid.UUID(request.user_id)).first()
    if not user:
        raise HTTPException(status_code=404, detail=f"User {request.user_id} not found")

    # Default to all 3 accounts if not specified
    account_labels = request.account_labels or ["procore-main", "procore-private", "personal"]

    # Validate all accounts exist and are active
    accounts = (
        db.query(GmailAccount)
        .filter(
            GmailAccount.user_id == uuid.UUID(request.user_id),
            GmailAccount.account_label.in_(account_labels),
            GmailAccount.is_active == True,
        )
        .all()
    )

    if len(accounts) != len(account_labels):
        found_labels = [acc.account_label for acc in accounts]
        missing = set(account_labels) - set(found_labels)
        raise HTTPException(
            status_code=400,
            detail=f"Not all accounts are authenticated and active. Missing: {', '.join(missing)}",
        )

    # Check if there's already a running job for this user
    existing_job = (
        db.query(SyncJob)
        .filter(
            SyncJob.user_id == uuid.UUID(request.user_id),
            SyncJob.status.in_(["queued", "running"]),
        )
        .first()
    )

    if existing_job:
        logger.warning(f"Job already running for user {request.user_id}: {existing_job.id}")
        raise HTTPException(
            status_code=409,
            detail=f"A scan is already running for this user. Job ID: {existing_job.id}",
        )

    # Enqueue Celery task
    try:
        task = scan_gmail_task.delay(request.user_id, account_labels)
        logger.info(f"Enqueued scan task {task.id} for user {request.user_id}")

        return StartScanResponse(
            job_id=task.id,
            status="queued",
            accounts=account_labels,
            status_url=f"{settings.app_url}/scan/status/{task.id}",
            message=f"Scan started for {len(account_labels)} account(s)",
        )

    except Exception as e:
        logger.error(f"Error starting scan: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to start scan")


@router.get("/status/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str, db: Session = Depends(get_sync_db)) -> JobStatusResponse:
    """
    Get status of a scan job.

    Args:
        job_id: Celery task ID or SyncJob ID
        db: Database session

    Returns:
        Job status with progress and metrics
    """
    # Try to find job in database by celery_task_id or id
    job = (
        db.query(SyncJob)
        .filter(
            (SyncJob.celery_task_id == job_id) | (SyncJob.id == uuid.UUID(job_id))
        )
        .first()
    )

    if job:
        # Return database job status
        return JobStatusResponse(
            job_id=str(job.id),
            status=job.status,
            phase=job.phase,
            progress=job.progress_pct,
            emails_processed=job.emails_processed,
            emails_total=job.emails_total,
            contacts_processed=job.contacts_processed,
            started_at=job.started_at.isoformat() if job.started_at else None,
            completed_at=job.completed_at.isoformat() if job.completed_at else None,
            error_message=job.error_message,
            duration_seconds=job.duration_seconds,
        )

    # Fall back to Celery task status
    try:
        task = AsyncResult(job_id, app=celery_app)

        if task.state == "PENDING":
            return JobStatusResponse(
                job_id=job_id,
                status="queued",
                phase=None,
                progress=0,
                emails_processed=0,
                emails_total=None,
                contacts_processed=0,
                started_at=None,
                completed_at=None,
                error_message=None,
                duration_seconds=None,
            )

        elif task.state == "PROGRESS":
            info = task.info or {}
            return JobStatusResponse(
                job_id=job_id,
                status="running",
                phase=info.get("phase"),
                progress=info.get("progress", 0),
                emails_processed=info.get("emails_processed", 0),
                emails_total=None,
                contacts_processed=0,
                started_at=None,
                completed_at=None,
                error_message=None,
                duration_seconds=None,
            )

        elif task.state == "SUCCESS":
            result = task.result or {}
            return JobStatusResponse(
                job_id=job_id,
                status="completed",
                phase="completed",
                progress=100,
                emails_processed=result.get("emails_processed", 0),
                emails_total=result.get("emails_processed", 0),
                contacts_processed=result.get("contacts_processed", 0),
                started_at=None,
                completed_at=None,
                error_message=None,
                duration_seconds=None,
            )

        elif task.state == "FAILURE":
            return JobStatusResponse(
                job_id=job_id,
                status="failed",
                phase=None,
                progress=0,
                emails_processed=0,
                emails_total=None,
                contacts_processed=0,
                started_at=None,
                completed_at=None,
                error_message=str(task.info),
                duration_seconds=None,
            )

        else:
            return JobStatusResponse(
                job_id=job_id,
                status=task.state.lower(),
                phase=None,
                progress=0,
                emails_processed=0,
                emails_total=None,
                contacts_processed=0,
                started_at=None,
                completed_at=None,
                error_message=None,
                duration_seconds=None,
            )

    except Exception as e:
        logger.error(f"Error getting job status: {str(e)}")
        raise HTTPException(status_code=404, detail="Job not found")


@router.post("/cancel/{job_id}")
async def cancel_job(job_id: str, db: Session = Depends(get_sync_db)) -> dict[str, Any]:
    """
    Cancel a running scan job.

    Args:
        job_id: Celery task ID or SyncJob ID
        db: Database session

    Returns:
        Cancellation status
    """
    logger.info(f"Cancelling job {job_id}")

    # Find job in database
    job = (
        db.query(SyncJob)
        .filter(
            (SyncJob.celery_task_id == job_id) | (SyncJob.id == uuid.UUID(job_id))
        )
        .first()
    )

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status not in ["queued", "running"]:
        raise HTTPException(
            status_code=400, detail=f"Cannot cancel job with status: {job.status}"
        )

    # Revoke Celery task
    try:
        celery_task_id = job.celery_task_id or job_id
        celery_app.control.revoke(celery_task_id, terminate=True)

        # Update job status
        job.status = "cancelled"
        job.error_message = "Job cancelled by user"
        job.completed_at = datetime.utcnow()
        db.commit()

        logger.info(f"Cancelled job {job_id}")

        return {
            "status": "success",
            "message": f"Job {job_id} cancelled",
        }

    except Exception as e:
        logger.error(f"Error cancelling job: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to cancel job")


@router.get("/results/{job_id}")
async def get_job_results(job_id: str, db: Session = Depends(get_sync_db)) -> dict[str, Any]:
    """
    Get results of a completed scan job.

    Args:
        job_id: SyncJob ID
        db: Database session

    Returns:
        Job results with vault path and metrics
    """
    job = db.query(SyncJob).filter(SyncJob.id == uuid.UUID(job_id)).first()

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status != "completed":
        raise HTTPException(
            status_code=400,
            detail=f"Job has not completed yet. Current status: {job.status}",
        )

    return {
        "job_id": str(job.id),
        "status": job.status,
        "contacts_processed": job.contacts_processed,
        "emails_processed": job.emails_processed,
        "vault_path": str(settings.obsidian_vault_path),
        "duration_seconds": job.duration_seconds,
        "started_at": job.started_at.isoformat() if job.started_at else None,
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
    }
