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
    tracks: Optional[List[TrackTelemetryBase]] = []

class FlightListResponse(BaseModel):
    total: int
    page: int
    page_size: int
    pages: int
    data: List[FlightSessionResponse]

# ── INGESTION (Internal Use) ────────────────────────────────────────────────

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

# ── FALLBACK SCHEMAS FOR UI COMPATIBILITY ──────────────────────────────────
# These schemas prevent the Frontend from crashing during the Enterprise Migration.

class DailyFlightStats(BaseModel):
    date: str
    flight_count: int

class AirlineActivityStats(BaseModel):
    airline_icao24: str
    airline_name: Optional[str]
    flight_count: int

class CountryActivityStats(BaseModel):
    country_name: str
    flight_count: int

class FlightStatistics(BaseModel):
    total_flights: int
    daily_stats: List[DailyFlightStats]
    top_airlines: List[AirlineActivityStats]
    top_countries: List[CountryActivityStats]
    flights_today: int
    flights_this_week: int
    flights_this_month: int

class HealthCheck(BaseModel):
    status: str
    timestamp: datetime
    database: str
    version: str = "3.0.0-Enterprise"

class CountryStats(BaseModel):
    country_name: str
    flight_count: int

class DailyStats(BaseModel):
    date: str
    flight_count: int

class HourlyStats(BaseModel):
    hour: int
    flight_count: int

class AirportStats(BaseModel):
    airport_icao: str
    flight_count: int
    as_departure: int
    as_arrival: int

class RouteStats(BaseModel):
    departure: str
    arrival: str
    flight_count: int

class AnalyticsSummary(BaseModel):
    total_flights: int
    unique_countries: int
    unique_airports: int
    top_countries: List[CountryStats]

# ── REGIONS SCHEMA (For UI Compatibility) ──────────────────────────────────

class RegionResponse(BaseModel):
    key: str
    name: str
    name_ar: str
    lamin: float
    lomin: float
    lamax: float
    lomax: float
    center_lat: float
    center_lon: float

class IngestionJobResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    date_str: str
    region_key: str
    lamin: float
    lomin: float
    lamax: float
    lomax: float
    begin_ts: int
    end_ts: int
    status: str
    flights_ingested: int
    chunks_total: int
    chunks_done: int
    error_message: Optional[str] = None
    created_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

class IngestionJobListResponse(BaseModel):
    total: int
    data: List[IngestionJobResponse]

class IngestionStartRequest(BaseModel):
    begin_date: str = Field(..., description="YYYY-MM-DD")
    end_date: str = Field(..., description="YYYY-MM-DD inclusive")
    region_keys: List[str]
    force_reingest: bool = False