"""
Enterprise Deep Analytics API Endpoints (v4.0)
Generates complex business intelligence insights from the Snowflake Schema.
"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, desc, extract
from typing import List
import logging

from app.database import get_db
from app import models
from app.schemas import CountryStats, DailyStats, HourlyStats, AirportStats, RouteStats, AnalyticsSummary
from sqlalchemy.orm import aliased

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/analytics", tags=["analytics"])

@router.get("/top_countries", response_model=List[CountryStats])
def get_top_countries(limit: int = 15, db: Session = Depends(get_db)):
    """BI: Most active aircraft registration countries."""
    results = db.query(
        models.DimAircraft.country_code,
        func.count(models.FactFlightSession.session_id).label('cnt')
    ).join(
        models.FactFlightSession, models.DimAircraft.id == models.FactFlightSession.aircraft_id
    ).filter(
        models.DimAircraft.country_code.isnot(None)
    ).group_by(
        models.DimAircraft.country_code
    ).order_by(desc('cnt')).limit(limit).all()
    
    return [CountryStats(country_name=r[0], flight_count=r[1]) for r in results]


@router.get("/hourly_distribution", response_model=List[HourlyStats])
def get_hourly_distribution(db: Session = Depends(get_db)):
    """BI: Rush hour analysis (Heatmap by hour of day UTC)."""
    results = db.query(
        extract('hour', models.FactFlightSession.first_seen_ts).label('h'),
        func.count(models.FactFlightSession.session_id).label('cnt')
    ).group_by('h').all()
    
    hour_map = {int(r.h): r.cnt for r in results}
    return [HourlyStats(hour=h, flight_count=hour_map.get(h, 0)) for h in range(24)]


@router.get("/top_airports", response_model=List[AirportStats])
def get_top_airports(limit: int = 15, db: Session = Depends(get_db)):
    """BI: Busiest airports (combining departures and arrivals)."""
    # 1. Count departures
    dep_query = db.query(
        models.DimGeography.icao_code,
        func.count(models.FactFlightSession.session_id).label('cnt')
    ).join(models.FactFlightSession, models.DimGeography.id == models.FactFlightSession.dep_airport_id)\
     .group_by(models.DimGeography.icao_code).all()
     
    dep_dict = {r[0]: r[1] for r in dep_query if r[0]}
    
    # 2. Count arrivals
    arr_query = db.query(
        models.DimGeography.icao_code,
        func.count(models.FactFlightSession.session_id).label('cnt')
    ).join(models.FactFlightSession, models.DimGeography.id == models.FactFlightSession.arr_airport_id)\
     .group_by(models.DimGeography.icao_code).all()
     
    arr_dict = {r[0]: r[1] for r in arr_query if r[0]}
    
    # Merge results
    all_airports = set(dep_dict.keys()) | set(arr_dict.keys())
    combined = []
    for icao in all_airports:
        deps = dep_dict.get(icao, 0)
        arrs = arr_dict.get(icao, 0)
        combined.append(AirportStats(
            airport_icao=icao, 
            as_departure=deps, 
            as_arrival=arrs, 
            flight_count=deps + arrs
        ))
        
    combined.sort(key=lambda x: x.flight_count, reverse=True)
    return combined[:limit]


@router.get("/top_routes", response_model=List[RouteStats])
def get_top_routes(limit: int = 20, db: Session = Depends(get_db)):
    """BI: Most frequent flight corridors (Origin -> Destination)."""
    DepGeo = aliased(models.DimGeography)
    ArrGeo = aliased(models.DimGeography)
    
    results = db.query(
        DepGeo.icao_code.label('dep'),
        ArrGeo.icao_code.label('arr'),
        func.count(models.FactFlightSession.session_id).label('cnt')
    ).join(
        DepGeo, models.FactFlightSession.dep_airport_id == DepGeo.id
    ).join(
        ArrGeo, models.FactFlightSession.arr_airport_id == ArrGeo.id
    ).group_by(
        DepGeo.icao_code, ArrGeo.icao_code
    ).order_by(desc('cnt')).limit(limit).all()
    
    return [RouteStats(departure=r.dep, arrival=r.arr, flight_count=r.cnt) for r in results]


@router.get("/summary", response_model=AnalyticsSummary)
def get_summary(db: Session = Depends(get_db)):
    total = db.query(models.FactFlightSession).count()
    return AnalyticsSummary(
        total_flights=total,
        unique_countries=0, # To be implemented if needed
        unique_airports=db.query(models.DimGeography).count(),
        top_countries=[]
    )