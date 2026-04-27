"""
Enterprise Ingestion API Endpoints (v3.0 - Graceful Degradation)
"""
from fastapi import APIRouter

router = APIRouter(prefix="/ingestion", tags=["ingestion"])

@router.get("/jobs")
def list_jobs():
    """SRE Fallback: Return empty jobs list during Enterprise Migration."""
    return {"total": 0, "data": []}

@router.get("/jobs/{job_id}")
def get_job(job_id: int):
    return None