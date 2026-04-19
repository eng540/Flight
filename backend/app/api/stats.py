"""Statistics API endpoints."""
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
        stats = FlightCRUD.get_statistics(db)
        return FlightStatistics(**stats)
    except Exception as e:
        logger.error(f"Error fetching statistics: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/airlines")
async def get_airline_statistics(limit: int = 10, db: Session = Depends(get_db)):
    """Most active airlines by flight count."""
    try:
        airlines = AirlineCRUD.get_most_active(db, limit=limit)
        return {"data": airlines}
    except Exception as e:
        logger.error(f"Error fetching airline statistics: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/health")
async def health_check(db: Session = Depends(get_db)):
    """Health check – tests DB connectivity."""
    try:
        db.execute(text("SELECT 1"))
        db_status = "connected"
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        db_status = "disconnected"

    return HealthCheck(
        status="healthy" if db_status == "connected" else "unhealthy",
        timestamp=datetime.utcnow(),
        database=db_status,
    )
