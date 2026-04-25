"""
Enterprise Pydantic Schemas (v3.0)
Strict validation and typing for the Snowflake Architecture.
"""
from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List, Dict, Any
from datetime import datetime, date

# ── DIMENSIONS (Reference Data) ─────────────────────────────────────────────

class DimGeographyBase(BaseModel):
    icao_code: Optional[str] = Field(None, max_length=4)
    iata_code: Optional[str] = Field(None, max_length=3)
    name: str = Field(..., max_length=255)
    city: Optional[str] = Field(None, max_length=100)
    country_code: Optional[str] = Field(None, max_length=2)
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    elevation_m: Optional[float] = None

class DimGeographyResponse(DimGeographyBase):
    model_config = ConfigDict(from_attributes=True)
    id: int

class DimOperatorBase(BaseModel):
    icao_code: Optional[str] = Field(None, max_length=3)
    iata_code: Optional[str] = Field(None, max_length=2)
    name: str = Field(..., max_length=255)
    country_code: Optional[str] = Field(None, max_length=2)
    operator_type: Optional[str] = Field(None, max_length=50)

class DimOperatorResponse(DimOperatorBase):
    model_config = ConfigDict(from_attributes=True)
    id: int

class DimAircraftBase(BaseModel):
    icao24: str = Field(..., min_length=4, max_length=6)
    registration: Optional[str] = Field(None, max_length=20)
    manufacturer: Optional[str] = Field(None, max_length=100)
    model: Optional[str] = Field(None, max_length=100)
    type_code: Optional[str] = Field(None, max_length=10)

class DimAircraftResponse(DimAircraftBase):
    model_config = ConfigDict(from_attributes=True)
    id: int
    operator: Optional[DimOperatorResponse] = None

# ── FACTS & TELEMETRY (Operational Data) ────────────────────────────────────

class TrackTelemetryBase(BaseModel):
    timestamp: datetime
    latitude: float
    longitude: float
    altitude_m: Optional[float] = None
    velocity_kmh: Optional[float] = None
    heading_deg: Optional[float] = None
    is_on_ground: Optional[bool] = False

class FlightSessionBase(BaseModel):
    callsign: Optional[str] = Field(None, max_length=20)
    first_seen_ts: datetime
    last_seen_ts: datetime
    flight_status: Optional[str] = "active"

class FlightSessionResponse(FlightSessionBase):
    model_config = ConfigDict(from_attributes=True)
    session_id: int
    aircraft: Optional[DimAircraftResponse] = None
    operator: Optional[DimOperatorResponse] = None
    dep_airport: Optional[DimGeographyResponse] = None
    arr_airport: Optional[DimGeographyResponse] = None
    # For UI performance, we might include the latest track or all tracks
    tracks: Optional[List[TrackTelemetryBase]] = []

class FlightListResponse(BaseModel):
    total: int
    page: int
    page_size: int
    pages: int
    data: List[FlightSessionResponse]

# ── INGESTION (Internal Use) ────────────────────────────────────────────────
# This schema is used by the Worker to send flattened data to the CRUD layer,
# which then splits it into the proper Star Schema tables.
class RawIngestionPayload(BaseModel):
    icao24: str
    callsign: Optional[str] = None
    registration: Optional[str] = None
    operator_iata: Optional[str] = None
    operator_icao: Optional[str] = None
    origin_country: Optional[str] = None
    timestamp: int
    longitude: float
    latitude: float
    altitude: Optional[float] = 0.0
    velocity: Optional[float] = 0.0
    heading: Optional[float] = None
    on_ground: Optional[bool] = False
    est_departure_airport: Optional[str] = None
    est_arrival_airport: Optional[str] = None
    region_key: Optional[str] = "global"