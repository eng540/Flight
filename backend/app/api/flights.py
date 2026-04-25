"""
Enterprise Flight API Endpoints (v3.0)
Serves data from the Snowflake Schema.
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Optional
import logging

from app.database import get_db
from app.crud import FlightQueryCRUD
from app.schemas import FlightListResponse, FlightSessionResponse
# (Import fallback for legacy UI properties)
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/flights", tags=["flights"])

@router.get("", response_model=dict)
async def get_flights(
    page: int = Query(1, ge=1),
    page_size: int = Query(500, ge=1, le=1000), # Increased limit for the Map
    db: Session = Depends(get_db),
):
    """
    Get active flights (mapped to the legacy frontend format temporarily).
    """
    sessions, total = FlightQueryCRUD.get_active_flights_with_latest_track(db, limit=page_size)
    
    # --- SRE ADAPTER: Transform Enterprise Schema to Legacy Frontend Schema ---
    # Because the frontend expects the old flat structure, we map it here
    # until we upgrade the React UI.
    legacy_data = []
    for s in sessions:
        # We need the latest track for the map
        # Note: In a true prod system, we fetch this via a joined subquery.
        # For now, we simulate the last known position.
        from app.models import TrackTelemetry
        from sqlalchemy import desc
        
        last_track = db.query(TrackTelemetry).filter(
            TrackTelemetry.session_id == s.session_id
        ).order_by(desc(TrackTelemetry.timestamp)).first()

        if last_track:
            legacy_data.append({
                "id": s.session_id,
                "icao24": s.aircraft.icao24 if s.aircraft else "UNKNOWN",
                "callsign": s.callsign,
                "origin_country": s.aircraft.country_code if s.aircraft else None,
                "first_seen": int(s.first_seen_ts.timestamp()),
                "last_seen": int(last_track.timestamp.timestamp()),
                "est_departure_airport": s.dep_airport.icao_code if s.dep_airport else None,
                "est_arrival_airport": s.arr_airport.icao_code if s.arr_airport else None,
                "latitude": last_track.latitude,
                "longitude": last_track.longitude,
                "altitude": last_track.altitude_m,
                "velocity": last_track.velocity_kmh,
                "heading": last_track.heading_deg,
                "on_ground": last_track.is_on_ground,
                "duration_seconds": int((last_track.timestamp - s.first_seen_ts).total_seconds())
            })
            
    pages = (total + page_size - 1) // page_size
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "pages": pages,
        "data": legacy_data
    }

@router.get("/filter")
async def filter_flights(
    page: int = Query(1, ge=1),
    page_size: int = Query(500, ge=1, le=1000),
    db: Session = Depends(get_db),
    # Catch all legacy params
    **kwargs
):
    """Temporary fallback to get_flights to keep UI Map alive during migration."""
    return await get_flights(page=page, page_size=page_size, db=db)