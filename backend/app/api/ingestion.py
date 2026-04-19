"""Ingestion management API endpoints."""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Optional
import logging

from app.database import get_db
from app.crud import IngestionJobCRUD
from app.config import settings
from app.schemas import (IngestionJobResponse, IngestionJobListResponse,
                          IngestionStartRequest)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/ingestion", tags=["ingestion"])


def _get_celery():
    """
    Import and return the celery_app instance.

    IMPORTANT: We import celery_app here (not worker.tasks) because @shared_task
    binds to whichever Celery app is the "current" app at import time.
    In the FastAPI process, if tasks.py is imported WITHOUT celery_app being
    imported first, @shared_task uses a default app with broker=localhost:6379
    instead of the configured Redis URL → Connection refused.

    Using celery_app.send_task("task.name", ...) bypasses this entirely:
    the task is dispatched by name over the already-configured broker.
    """
    from worker.celery_app import celery_app
    return celery_app


# ── Jobs list ─────────────────────────────────────────────────────────────────

@router.get("/jobs", response_model=IngestionJobListResponse)
def list_jobs(
    status: Optional[str] = Query(None),
    region_key: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """List ingestion jobs with optional filtering."""
    skip = (page - 1) * page_size
    jobs, total = IngestionJobCRUD.get_all(
        db, skip=skip, limit=page_size,
        status=status, region_key=region_key)
    return IngestionJobListResponse(total=total, data=jobs)


@router.get("/jobs/{job_id}", response_model=IngestionJobResponse)
def get_job(job_id: int, db: Session = Depends(get_db)):
    """Get a specific ingestion job."""
    job = IngestionJobCRUD.get_by_id(db, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


# ── Start / retry ─────────────────────────────────────────────────────────────

@router.post("/start")
def start_ingestion(request: IngestionStartRequest):
    """
    Queue a historical ingestion task via Celery.
    Uses celery_app.send_task() by name to guarantee the correct broker
    is used regardless of import order in the FastAPI process.
    """
    for key in request.region_keys:
        if not settings.get_region(key):
            raise HTTPException(status_code=400,
                                detail=f"Unknown region key: '{key}'")
    try:
        celery_app = _get_celery()
        task = celery_app.send_task(
            "worker.tasks.ingest_historical_flights",
            queue="ingestion",
            kwargs={
                "begin_date":    request.begin_date,
                "end_date":      request.end_date,
                "region_keys":   request.region_keys,
                "force_reingest": request.force_reingest,
            },
        )
        logger.info(f"Queued historical ingestion task {task.id} "
                    f"({request.begin_date}→{request.end_date} "
                    f"regions={request.region_keys})")
        return {
            "status":      "queued",
            "task_id":     task.id,
            "begin_date":  request.begin_date,
            "end_date":    request.end_date,
            "region_keys": request.region_keys,
        }
    except Exception as e:
        logger.error(f"Failed to queue ingestion task: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to queue task: {e}. Is the Celery worker running?",
        )


@router.post("/jobs/{job_id}/retry")
def retry_job(job_id: int, db: Session = Depends(get_db)):
    """Retry a failed or pending ingestion job."""
    job = IngestionJobCRUD.get_by_id(db, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status not in ("failed", "pending"):
        raise HTTPException(
            status_code=400,
            detail=f"Cannot retry a job with status '{job.status}'")
    try:
        celery_app = _get_celery()
        task = celery_app.send_task(
            "worker.tasks.ingest_historical_flights",
            queue="ingestion",
            kwargs={
                "begin_date":    job.date_str,
                "end_date":      job.date_str,
                "region_keys":   [job.region_key],
                "force_reingest": True,
            },
        )
        logger.info(f"Re-queued job {job_id} as task {task.id}")
        return {"status": "queued", "task_id": task.id, "job_id": job_id}
    except Exception as e:
        logger.error(f"Failed to retry job {job_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/jobs/{job_id}")
def delete_job(job_id: int, db: Session = Depends(get_db)):
    """Delete an ingestion job record."""
    if not IngestionJobCRUD.delete(db, job_id):
        raise HTTPException(status_code=404, detail="Job not found")
    return {"deleted": True}
