"""
Enterprise Live API (v4.0)
Reads directly from the lightning-fast CurrentAircraftState table.
Supports SQL-level Bounding Box filtering and advanced search.
"""
from fastapi import APIRouter, Depends, Query, Response
from sqlalchemy.orm import Session
from sqlalchemy import and_
import pandas as pd
import io
import logging

from app.database import get_db
from app import models

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/flights", tags=["flights"])

def parse_bounds(bounds_str: str):
    """Parse bounds string: maxLat,minLat,minLon,maxLon (FR24 format)"""
    try:
        parts = [float(x.strip()) for x in bounds_str.split(",")]
        if len(parts) != 4:
            return None
        north, south, west, east = parts
        return south, west, north, east
    except Exception:
        return None

@router.get("/live", response_model=dict)
async def get_live_flights(
    bounds: str = Query(None, description="MaxLat,MinLat,MinLon,MaxLon"),
    callsign: str = Query(None, description="Filter by callsign"),
    export: bool = Query(False, description="Set to True to download Excel"),
    db: Session = Depends(get_db)
):
    """
    SRE FIX: Reads real-time data from DB with SQL-level filtering.
    """
    try:
        # 1. Base Query on the fast state table
        query = db.query(models.CurrentAircraftState).filter(
            models.CurrentAircraftState.latitude.isnot(None),
            models.CurrentAircraftState.longitude.isnot(None)
        )

        # 2. Apply Bounding Box Filter (SQL Level for Performance)
        if bounds:
            bbox = parse_bounds(bounds)
            if bbox:
                south, west, north, east = bbox
                query = query.filter(
                    and_(
                        models.CurrentAircraftState.latitude >= south,
                        models.CurrentAircraftState.latitude <= north,
                        models.CurrentAircraftState.longitude >= west,
                        models.CurrentAircraftState.longitude <= east
                    )
                )

        # 3. Apply Callsign Filter
        if callsign:
            query = query.filter(models.CurrentAircraftState.callsign.ilike(f"%{callsign}%"))

        # 4. Execute Query
        current_flights = query.order_by(models.CurrentAircraftState.last_updated.desc()).limit(2000).all()
        
        # 5. Map to UI Format
        mapped_flights = [{
            "id": f.icao24,
            "icao24": f.icao24,
            "callsign": f.callsign or "غير معروف",
            "origin_country": "غير معروف", # Can be joined later
            "latitude": f.latitude,
            "longitude": f.longitude,
            "altitude": f.altitude_m,
            "velocity": f.velocity_kmh,
            "heading": f.heading_deg,
            "est_departure_airport": f.dep_airport_iata,
            "est_arrival_airport": f.arr_airport_iata,
            "last_seen": f.last_updated.timestamp() if f.last_updated else None
        } for f in current_flights]
            
        # 6. Excel Export
        if export:
            df = pd.DataFrame(mapped_flights)
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='الرحلات الحية')
            buffer.seek(0)
            return Response(
                content=buffer.read(),
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": "attachment; filename=live_flights_export.xlsx"}
            )
            
        return {
            "total": len(mapped_flights),
            "data": mapped_flights
        }
        
    except Exception as e:
        logger.error(f"Error reading live flights from DB: {e}", exc_info=True)
        return {"total": 0, "data": []}