"""Flight API endpoints with geographic and temporal filtering."""
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from typing import Optional
import logging, io
from datetime import datetime
import pandas as pd

from app.database import get_db
from app.crud import FlightCRUD
from app.schemas import FlightResponse, FlightListResponse, FlightWithTrajectory

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/flights", tags=["flights"])


@router.get("", response_model=FlightListResponse)
async def get_flights(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """Get all flights with pagination."""
    skip = (page - 1) * page_size
    flights, total = FlightCRUD.get_all(db, skip=skip, limit=page_size)
    pages = (total + page_size - 1) // page_size
    return FlightListResponse(total=total, page=page, page_size=page_size,
                               pages=pages, data=flights)


@router.get("/filter", response_model=FlightListResponse)
async def filter_flights(
    airline_id: Optional[int] = None,
    country: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    departure_airport: Optional[str] = None,
    arrival_airport: Optional[str] = None,
    region_key: Optional[str] = None,
    begin_ts: Optional[int] = None,
    end_ts: Optional[int] = None,
    lamin: Optional[float] = None,
    lomin: Optional[float] = None,
    lamax: Optional[float] = None,
    lomax: Optional[float] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """Filter flights by region, country, dates, airports and bounding box."""
    skip = (page - 1) * page_size
    flights, total = FlightCRUD.get_all(
        db, skip=skip, limit=page_size,
        airline_id=airline_id, country=country,
        date_from=date_from, date_to=date_to,
        departure_airport=departure_airport, arrival_airport=arrival_airport,
        region_key=region_key, begin_ts=begin_ts, end_ts=end_ts,
        lamin=lamin, lomin=lomin, lamax=lamax, lomax=lomax,
    )
    pages = (total + page_size - 1) // page_size
    return FlightListResponse(total=total, page=page, page_size=page_size,
                               pages=pages, data=flights)


@router.get("/export/excel")
async def export_flights_excel(
    country: Optional[str] = None,
    region_key: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    departure_airport: Optional[str] = None,
    arrival_airport: Optional[str] = None,
    begin_ts: Optional[int] = None,
    end_ts: Optional[int] = None,
    lamin: Optional[float] = None, lomin: Optional[float] = None,
    lamax: Optional[float] = None, lomax: Optional[float] = None,
    limit: int = Query(10000, ge=1, le=50000),
    db: Session = Depends(get_db),
):
    """Export filtered flights to Excel."""
    flights, _ = FlightCRUD.get_all(
        db, skip=0, limit=limit,
        country=country, region_key=region_key,
        date_from=date_from, date_to=date_to,
        departure_airport=departure_airport, arrival_airport=arrival_airport,
        begin_ts=begin_ts, end_ts=end_ts,
        lamin=lamin, lomin=lomin, lamax=lamax, lomax=lomax,
    )
    if not flights:
        raise HTTPException(status_code=404, detail="No flights found for export")

    data = []
    for f in flights:
        data.append({
            "ID": f.id, "ICAO24": f.icao24, "Callsign": f.callsign or "",
            "Airline": f.airline.name if f.airline else "Unknown",
            "Origin Country": f.origin_country or "",
            "Region": f.region_key or "",
            "Departure": f.est_departure_airport or "",
            "Arrival": f.est_arrival_airport or "",
            "First Seen": datetime.utcfromtimestamp(f.first_seen).strftime("%Y-%m-%d %H:%M:%S") if f.first_seen else "",
            "Last Seen": datetime.utcfromtimestamp(f.last_seen).strftime("%Y-%m-%d %H:%M:%S") if f.last_seen else "",
            "Duration (h)": round(f.duration_hours, 2) if f.duration_hours else "",
            "Latitude": f.latitude or "", "Longitude": f.longitude or "",
        })

    df = pd.DataFrame(data)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Flights')
        ws = writer.sheets['Flights']
        for col in ws.columns:
            w = max((len(str(c.value)) for c in col if c.value), default=10)
            ws.column_dimensions[col[0].column_letter].width = min(w + 2, 50)
    output.seek(0)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename=flights_{ts}.xlsx"},
    )


@router.get("/{flight_id}", response_model=FlightWithTrajectory)
async def get_flight(flight_id: int, db: Session = Depends(get_db)):
    """Get a single flight by ID including trajectory."""
    flight = FlightCRUD.get_by_id(db, flight_id)
    if not flight:
        raise HTTPException(status_code=404, detail="Flight not found")
    return flight
