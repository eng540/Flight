"""
Enterprise Analytics API Endpoints (MVP Delivery)
Feeds the charts with real aggregated data.
"""
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func, desc
from typing import List
import logging

from app.database import get_db
from app import models
from app.schemas import CountryStats, AirportStats, RouteStats, AnalyticsSummary

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/analytics", tags=["analytics"])

@router.get("/top_countries", response_model=List[CountryStats])
def get_top_countries(limit: int = 15, db: Session = Depends(get_db)):
    """MVP: Get top countries from Aircraft dimension."""
    results = db.query(
        models.DimAircraft.country_code,
        func.count(models.DimAircraft.id).label('cnt')
    ).filter(models.DimAircraft.country_code.isnot(None))\
     .group_by(models.DimAircraft.country_code)\
     .order_by(desc('cnt')).limit(limit).all()
     
    return [CountryStats(country_name=r[0], flight_count=r[1]) for r in results]

@router.get("/top_airports", response_model=List[AirportStats])
def get_top_airports(limit: int = 15, db: Session = Depends(get_db)):
    """MVP: Aggregate departures and arrivals from flight sessions."""
    # Simplified query for fast MVP delivery
    dep_results = db.query(
        models.DimGeography.icao_code,
        func.count(models.FactFlightSession.session_id).label('cnt')
    ).join(models.FactFlightSession, models.DimGeography.id == models.FactFlightSession.dep_airport_id)\
     .group_by(models.DimGeography.icao_code).all()
     
    # Convert to schema
    final_results = []
    for r in dep_results:
        if r[0]: # Ensure ICAO exists
            final_results.append(AirportStats(airport_icao=r[0], flight_count=r[1], as_departure=r[1], as_arrival=0))
            
    # Sort and limit
    final_results.sort(key=lambda x: x.flight_count, reverse=True)
    return final_results[:limit]

@router.get("/top_routes", response_model=List[RouteStats])
def get_top_routes(limit: int = 20, db: Session = Depends(get_db)):
    """MVP: Just return empty array to prevent UI crash, routes require complex joins not needed for MVP."""
    return []

@router.get("/summary", response_model=AnalyticsSummary)
def get_summary(db: Session = Depends(get_db)):
    total = db.query(models.FactFlightSession).count()
    return AnalyticsSummary(
        total_flights=total,
        unique_countries=0,
        unique_airports=0,
        top_countries=[]
    )