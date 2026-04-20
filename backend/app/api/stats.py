"""Statistics and health API endpoints."""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
import logging
from datetime import datetime

from app.database import get_db
from app.crud import FlightCRUD, AirlineCRUD
from app.schemas import FlightStatistics, HealthCheck

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/stats", tags=["statistics"])


@router.get("", response_model=FlightStatistics)
async def get_statistics(db: Session = Depends(get_db)):
    """Comprehensive flight statistics."""
    try:
        return FlightStatistics(**FlightCRUD.get_statistics(db))
    except Exception as e:
        logger.error(f"Statistics error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/airlines")
async def get_airline_statistics(limit: int = 10, db: Session = Depends(get_db)):
    """Most active airlines by flight count."""
    try:
        return {"data": AirlineCRUD.get_most_active(db, limit=limit)}
    except Exception as e:
        logger.error(f"Airline stats error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/health")
async def health_check(db: Session = Depends(get_db)):
    """API + database health check."""
    try:
        db.execute(text("SELECT 1"))
        db_status = "connected"
    except Exception as e:
        logger.error(f"DB health check failed: {e}")
        db_status = "disconnected"
    return HealthCheck(
        status="healthy" if db_status == "connected" else "unhealthy",
        timestamp=datetime.utcnow(),
        database=db_status,
    )


@router.get("/health/opensky")
async def opensky_health():
    """
    Full OpenSky API connectivity diagnostic.
    Tests curl, requests, and httpx backends independently.

    Use this to determine WHY ingestion is failing:
      - If curl succeeds but httpx fails → TLS/JA3 fingerprint issue → use OPENSKY_FORCE_BACKEND=curl
      - If all fail → IP-based block → run worker on non-cloud machine
      - If credentials missing → add OPENSKY_USERNAME + OPENSKY_PASSWORD
    """
    try:
        from worker.opensky_client import OpenSkyClient
        result = OpenSkyClient().test_connection()
        # Add actionable instructions
        result["env_instructions"] = {
            "force_curl":     "Set OPENSKY_FORCE_BACKEND=curl in Railway Variables",
            "force_requests": "Set OPENSKY_FORCE_BACKEND=requests in Railway Variables",
            "force_httpx":    "Set OPENSKY_FORCE_BACKEND=httpx in Railway Variables",
            "credentials":    "Set OPENSKY_USERNAME + OPENSKY_PASSWORD in Railway Variables",
        }
        return result
    except Exception as e:
        return {"any_reachable": False, "error": str(e)}
