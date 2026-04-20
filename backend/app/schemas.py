"""Pydantic schemas for API validation."""
from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List, Any, Dict
from datetime import datetime


# ── Country ────────────────────────────────────────────────────────────────
class CountryBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    iso_code: Optional[str] = Field(None, max_length=3)

class CountryCreate(CountryBase):
    pass

class CountryResponse(CountryBase):
    model_config = ConfigDict(from_attributes=True)
    id: int
    created_at: Optional[datetime] = None


# ── Airline ────────────────────────────────────────────────────────────────
class AirlineBase(BaseModel):
    icao24: str = Field(..., min_length=4, max_length=6)
    name: Optional[str] = Field(None, max_length=200)
    callsign_prefix: Optional[str] = Field(None, max_length=10)

class AirlineCreate(AirlineBase):
    country_id: Optional[int] = None

class AirlineResponse(AirlineBase):
    model_config = ConfigDict(from_attributes=True)
    id: int
    country_id: Optional[int] = None
    country: Optional[CountryResponse] = None
    created_at: Optional[datetime] = None
    flight_count: Optional[int] = 0


# ── Flight ─────────────────────────────────────────────────────────────────
class FlightBase(BaseModel):
    icao24: str = Field(..., min_length=4, max_length=6)
    callsign: Optional[str] = Field(None, max_length=20)
    origin_country: Optional[str] = Field(None, max_length=100)

class FlightCreate(FlightBase):
    first_seen: Optional[int] = None
    last_seen: Optional[int] = None
    est_departure_airport: Optional[str] = Field(None, max_length=4)
    est_departure_airport_horiz_distance: Optional[int] = None
    est_departure_airport_vert_distance: Optional[int] = None
    est_arrival_airport: Optional[str] = Field(None, max_length=4)
    est_arrival_airport_horiz_distance: Optional[int] = None
    est_arrival_airport_vert_distance: Optional[int] = None
    est_departure_time: Optional[int] = None
    est_arrival_time: Optional[int] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    altitude: Optional[float] = None
    velocity: Optional[float] = None
    heading: Optional[float] = None
    on_ground: Optional[bool] = None
    trajectory: Optional[List[Dict[str, Any]]] = None
    region_key: Optional[str] = None
    unique_flight_id: str = Field(..., max_length=100)

class FlightResponse(FlightBase):
    model_config = ConfigDict(from_attributes=True)
    id: int
    airline_id: Optional[int] = None
    airline: Optional[AirlineResponse] = None
    first_seen: Optional[int] = None
    last_seen: Optional[int] = None
    est_departure_airport: Optional[str] = None
    est_arrival_airport: Optional[str] = None
    est_departure_time: Optional[int] = None
    est_arrival_time: Optional[int] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    altitude: Optional[float] = None
    velocity: Optional[float] = None
    heading: Optional[float] = None
    on_ground: Optional[bool] = None
    region_key: Optional[str] = None
    ingestion_time: Optional[datetime] = None
    duration_seconds: Optional[int] = None
    duration_minutes: Optional[float] = None
    duration_hours: Optional[float] = None

class FlightWithTrajectory(FlightResponse):
    trajectory: Optional[List[Dict[str, Any]]] = None

class FlightListResponse(BaseModel):
    total: int
    page: int
    page_size: int
    pages: int
    data: List[FlightResponse]


# ── Filter params (kept for compatibility) ──────────────────────────────────
class FlightFilterParams(BaseModel):
    airline_id: Optional[int] = None
    country: Optional[str] = None
    date_from: Optional[str] = Field(None, description="YYYY-MM-DD")
    date_to: Optional[str] = Field(None, description="YYYY-MM-DD")
    departure_airport: Optional[str] = Field(None, max_length=4)
    arrival_airport: Optional[str] = Field(None, max_length=4)
    region_key: Optional[str] = None
    begin_ts: Optional[int] = None
    end_ts: Optional[int] = None
    lamin: Optional[float] = None
    lomin: Optional[float] = None
    lamax: Optional[float] = None
    lomax: Optional[float] = None
    page: int = Field(1, ge=1)
    page_size: int = Field(50, ge=1, le=500)


# ── Statistics ──────────────────────────────────────────────────────────────
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


# ── Analytics ───────────────────────────────────────────────────────────────
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


# ── Ingestion Job ────────────────────────────────────────────────────────────
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

class IngestionStartRequest(BaseModel):
    begin_date: str = Field(..., description="YYYY-MM-DD")
    end_date: str = Field(..., description="YYYY-MM-DD inclusive")
    region_keys: List[str]
    force_reingest: bool = False

class IngestionJobListResponse(BaseModel):
    total: int
    data: List[IngestionJobResponse]


# ── Region ───────────────────────────────────────────────────────────────────
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


# ── Health ────────────────────────────────────────────────────────────────────
class HealthCheck(BaseModel):
    status: str
    timestamp: datetime
    database: str
    version: str = "2.0.0"
