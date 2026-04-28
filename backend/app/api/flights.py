"""
Live Proxy API (MVP for Client Demo)
Bypasses DB temporarily to show REAL-TIME data from FR24 directly to the UI.
"""
from fastapi import APIRouter, HTTPException, Query, Response
from typing import Optional, List
import requests
import os
import logging
from pydantic import BaseModel
import pandas as pd
import io

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/flights", tags=["flights"])

# Temporary Schema for UI
class FlightLiveProxy(BaseModel):
    id: str
    icao24: str
    callsign: Optional[str]
    origin_country: Optional[str]
    latitude: float
    longitude: float
    altitude: float
    velocity: float
    heading: float
    est_departure_airport: Optional[str]
    est_arrival_airport: Optional[str]

@router.get("/live", response_model=dict)
async def get_live_flights_proxy(
    bounds: str = Query("63.0,12.0,25.0,42.0", description="MaxLat,MinLat,MinLon,MaxLon (e.g. Middle East)"),
    export: bool = Query(False, description="Set to True to download Excel")
):
    """
    WOW FACTOR: Directly fetches live flights from FR24 to show on the Map/Table immediately!
    """
    api_key = os.getenv("FR24_API_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="FR24_API_KEY is missing in server environment.")

    headers = {
        "Accept": "application/json",
        "Accept-Version": "v1",
        "Authorization": f"Bearer {api_key}"
    }
    
    url = f"https://fr24api.flightradar24.com/api/live/flight-positions/full?bounds={bounds}&limit=1000"
    
    try:
        res = requests.get(url, headers=headers, timeout=15)
        if res.status_code != 200:
            logger.error(f"FR24 Proxy Error: {res.text}")
            raise HTTPException(status_code=502, detail="Failed to fetch live data from provider.")
            
        flights_data = res.json().get("data", [])
        
        # Transform for our UI
        mapped_flights = []
        for f in flights_data:
            mapped_flights.append({
                "id": f.get("hex", "unknown"),
                "icao24": f.get("hex", "unknown"),
                "callsign": f.get("callsign") or f.get("flight"),
                "origin_country": f.get("reg")[:2] if f.get("reg") else "Unknown",
                "latitude": f.get("lat", 0.0),
                "longitude": f.get("lon", 0.0),
                "altitude": f.get("alt", 0) * 0.3048, # meters
                "velocity": f.get("gspeed", 0) * 1.852, # km/h
                "heading": f.get("track", 0),
                "est_departure_airport": f.get("orig_icao"),
                "est_arrival_airport": f.get("dest_icao"),
            })
            
        # Feature: Export to Excel on the fly!
        if export:
            df = pd.DataFrame(mapped_flights)
            # Create a bytes buffer
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Live Flights')
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
        
    except requests.Timeout:
        raise HTTPException(status_code=504, detail="Request to live provider timed out.")