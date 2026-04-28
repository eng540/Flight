"""
Enterprise Live API (v4.0)
Reads directly from the lightning-fast CurrentAircraftState table.
No more external API calls from the UI!
"""
from fastapi import APIRouter, Depends, Query, Response
from sqlalchemy.orm import Session
from typing import Optional
import pandas as pd
import io
import logging

from app.database import get_db
from app.crud import FlightQueryCRUD

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/flights", tags=["flights"])

@router.get("/live", response_model=dict)
async def get_live_flights(
    bounds: str = Query(None, description="Kept for backward compatibility"),
    export: bool = Query(False, description="Set to True to download Excel"),
    db: Session = Depends(get_db)
):
    """
    SRE FIX: Reads real-time data from the DB State table.
    """
    try:
        # جلب البيانات من الجدول السريع
        current_flights, total = FlightQueryCRUD.get_active_flights_with_latest_track(db, limit=1000)
        
        mapped_flights = []
        for f in current_flights:
            if f.latitude and f.longitude:
                mapped_flights.append({
                    "id": f.icao24,
                    "icao24": f.icao24,
                    "callsign": f.callsign or "غير معروف",
                    "origin_country": "غير معروف", # يمكن ربطها لاحقاً
                    "latitude": f.latitude,
                    "longitude": f.longitude,
                    "altitude": f.altitude_m,
                    "velocity": f.velocity_kmh,
                    "heading": f.heading_deg,
                    "est_departure_airport": f.dep_airport_iata,
                    "est_arrival_airport": f.arr_airport_iata,
                })
            
        # ميزة تصدير الإكسل
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
        logger.error(f"Error reading live flights from DB: {e}")
        return {"total": 0, "data": []}