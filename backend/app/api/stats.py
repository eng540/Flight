"""
Enterprise Stats API Endpoints (v4.0 - True Snowflake Analytics)
Queries the Fact and Dimension tables to generate high-performance metrics.
"""
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text, func, cast, Date
from datetime import datetime, timedelta, timezone
import logging

from app.database import get_db
from app import models
from app.schemas import FlightStatistics, HealthCheck, DailyFlightStats, AirlineActivityStats, CountryActivityStats

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/stats", tags=["statistics"])

@router.get("", response_model=FlightStatistics)
async def get_statistics(db: Session = Depends(get_db)):
    """Fetches high-level metrics directly from the Enterprise Data Warehouse."""
    now = datetime.now(timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    week_start = today_start - timedelta(days=7)
    month_start = today_start - timedelta(days=30)

    # 1. Core Metrics (Fact Table Aggregation)
    total_flights = db.query(models.FactFlightSession).count()
    
    flights_today = db.query(models.FactFlightSession).filter(
        models.FactFlightSession.first_seen_ts >= today_start
    ).count()
    
    flights_this_week = db.query(models.FactFlightSession).filter(
        models.FactFlightSession.first_seen_ts >= week_start
    ).count()
    
    flights_this_month = db.query(models.FactFlightSession).filter(
        models.FactFlightSession.first_seen_ts >= month_start
    ).count()

    # 2. Daily Trend (Group By Date on Fact Table)
    # This generates the Bar Chart data for the last 7 days
    daily_results = db.query(
        cast(models.FactFlightSession.first_seen_ts, Date).label('flight_date'),
        func.count(models.FactFlightSession.session_id).label('cnt')
    ).filter(
        models.FactFlightSession.first_seen_ts >= week_start
    ).group_by('flight_date').order_by('flight_date').all()
    
    # Fill missing days with 0
    daily_dict = {str(r.flight_date): r.cnt for r in daily_results}
    daily_stats = []
    for i in range(6, -1, -1):
        day_str = (today_start - timedelta(days=i)).strftime("%Y-%m-%d")
        daily_stats.append(DailyFlightStats(date=day_str, flight_count=daily_dict.get(day_str, 0)))

    # 3. Top Airlines (Join Fact with DimOperator)
    top_airlines_query = db.query(
        models.DimOperator.icao_code,
        models.DimOperator.name,
        func.count(models.FactFlightSession.session_id).label('cnt')
    ).join(
        models.FactFlightSession, models.DimOperator.id == models.FactFlightSession.operator_id
    ).group_by(
        models.DimOperator.id
    ).order_by(desc('cnt')).limit(5).all()
    
    top_airlines = [
        AirlineActivityStats(
            airline_icao24=r.icao_code or "UNK",
            airline_name=r.name,
            flight_count=r.cnt
        ) for r in top_airlines_query
    ]

    # 4. Top Countries (Join Fact with DimAircraft)
    top_countries_query = db.query(
        models.DimAircraft.country_code,
        func.count(models.FactFlightSession.session_id).label('cnt')
    ).join(
        models.FactFlightSession, models.DimAircraft.id == models.FactFlightSession.aircraft_id
    ).filter(
        models.DimAircraft.country_code.isnot(None)
    ).group_by(
        models.DimAircraft.country_code
    ).order_by(desc('cnt')).limit(5).all()
    
    top_countries = [
        CountryActivityStats(
            country_name=r.country_code or "Unknown",
            flight_count=r.cnt
        ) for r in top_countries_query
    ]

    return FlightStatistics(
        total_flights=total_flights,
        daily_stats=daily_stats,
        top_airlines=top_airlines,
        top_countries=top_countries,
        flights_today=flights_today,
        flights_this_week=flights_this_week,
        flights_this_month=flights_this_month
    )

@router.get("/airlines")
async def get_airline_statistics(limit: int = 100, db: Session = Depends(get_db)):
    """Returns list of operators for UI Dropdowns."""
    ops = db.query(models.DimOperator).limit(limit).all()
    return [{"id": o.id, "icao24": o.icao_code, "name": o.name} for o in ops]

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
        version="4.0.0-Enterprise"
    )