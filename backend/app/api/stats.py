"""
Enterprise Stats API Endpoints (v3.0 - Graceful Degradation)
"""
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
import logging
from datetime import datetime

from app.database import get_db
from app.schemas import FlightStatistics, HealthCheck

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/stats", tags=["statistics"])

@router.get("", response_model=FlightStatistics)
async def get_statistics(db: Session = Depends(get_db)):
    """
    SRE Fallback: Return safe zeros during the v3.0 Architecture Migration.
    Prevents UI crashes while the new Dimensional modeling aggregates data.
    """
    return FlightStatistics(
        total_flights=0,
        daily_stats=[],
        top_airlines=[],
        top_countries=[],
        flights_today=0,
        flights_this_week=0,
        flights_this_month=0
    )

@router.get("/airlines")
async def get_airline_statistics(limit: int = 10, db: Session = Depends(get_db)):
    return {"data": []}

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