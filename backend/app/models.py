"""
Enterprise Aviation Intelligence Models (v3.1 - FR24 Extended)
SQLAlchemy ORM representation of the Snowflake Schema.
"""
from sqlalchemy import (
    Column, Integer, String, Float, DateTime, ForeignKey,
    Boolean, Index, BigInteger, Text, Date
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import JSONB
from app.database import Base

# ═════════════════════════════════════════════════════════════════════════════
# 1. DIMENSION TABLES (Master Data / Reference Entities)
# ═════════════════════════════════════════════════════════════════════════════

class DimGeography(Base):
    """Airports, Regions, and Boundaries."""
    __tablename__ = "dim_geography"
    
    id = Column(Integer, primary_key=True, index=True)
    icao_code = Column(String(4), unique=True, nullable=True, index=True)
    iata_code = Column(String(3), nullable=True, index=True)
    name = Column(String(255), nullable=False)
    city = Column(String(100), nullable=True)
    country_code = Column(String(2), nullable=True, index=True)
    
    # Geo location
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    elevation_m = Column(Float, nullable=True)
    
    meta_data = Column(JSONB, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    def __repr__(self):
        return f"<DimGeography(icao='{self.icao_code}', name='{self.name}')>"


class DimOperator(Base):
    """Airlines and Operators."""
    __tablename__ = "dim_operator"
    
    id = Column(Integer, primary_key=True, index=True)
    icao_code = Column(String(3), unique=True, nullable=True, index=True)
    iata_code = Column(String(2), nullable=True, index=True)
    name = Column(String(255), nullable=False)
    country_code = Column(String(2), nullable=True)
    operator_type = Column(String(50), nullable=True)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    def __repr__(self):
        return f"<DimOperator(icao='{self.icao_code}', name='{self.name}')>"


class DimAircraft(Base):
    """The Physical Airplane Asset (SCD Type 2 Ready)."""
    __tablename__ = "dim_aircraft"
    
    id = Column(Integer, primary_key=True, index=True)
    icao24 = Column(String(6), nullable=False, index=True)
    registration = Column(String(20), nullable=True, index=True)
    
    manufacturer = Column(String(100), nullable=True)
    model = Column(String(100), nullable=True)
    type_code = Column(String(10), nullable=True, index=True)
    serial_number = Column(String(100), nullable=True)
    year_built = Column(Integer, nullable=True)
    
    operator_id = Column(Integer, ForeignKey("dim_operator.id"), nullable=True)
    country_code = Column(String(2), nullable=True)
    
    # SCD Type 2 boundaries
    valid_from = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    valid_to = Column(DateTime(timezone=True), nullable=True)
    
    # Relationships
    operator = relationship("DimOperator")

    __table_args__ = (
        Index('idx_aircraft_hex_active', 'icao24', 'valid_to'),
    )

    def __repr__(self):
        return f"<DimAircraft(icao24='{self.icao24}', reg='{self.registration}')>"


# ═════════════════════════════════════════════════════════════════════════════
# 2. OPERATIONAL FACT TABLES
# ═════════════════════════════════════════════════════════════════════════════

class FactFlightSession(Base):
    """The specific journey of an aircraft."""
    __tablename__ = "fact_flight_session"
    
    session_id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    
    aircraft_id = Column(Integer, ForeignKey("dim_aircraft.id"), nullable=False, index=True)
    operator_id = Column(Integer, ForeignKey("dim_operator.id"), nullable=True, index=True)
    callsign = Column(String(20), nullable=True, index=True)
    
    dep_airport_id = Column(Integer, ForeignKey("dim_geography.id"), nullable=True, index=True)
    arr_airport_id = Column(Integer, ForeignKey("dim_geography.id"), nullable=True, index=True)
    
    first_seen_ts = Column(DateTime(timezone=True), nullable=False, index=True)
    last_seen_ts = Column(DateTime(timezone=True), nullable=False, index=True)
    actual_takeoff_ts = Column(DateTime(timezone=True), nullable=True)
    actual_landing_ts = Column(DateTime(timezone=True), nullable=True)
    
    flight_status = Column(String(20), default="active", index=True)
    total_distance_km = Column(Float, nullable=True)
    max_altitude_m = Column(Float, nullable=True)

    # ── FR24 EXTENSIONS ──────────────────────────────────────────────────
    fr24_id = Column(String(50), unique=True, index=True, nullable=True)
    flight_number = Column(String(20), nullable=True)
    
    # Relationships
    aircraft = relationship("DimAircraft")
    operator = relationship("DimOperator")
    dep_airport = relationship("DimGeography", foreign_keys=[dep_airport_id])
    arr_airport = relationship("DimGeography", foreign_keys=[arr_airport_id])
    
    tracks = relationship("TrackTelemetry", back_populates="session", cascade="all, delete-orphan")

    __table_args__ = (
        Index('idx_flight_search', 'callsign', 'first_seen_ts'),
        Index('idx_flight_route', 'dep_airport_id', 'arr_airport_id'),
    )

    def __repr__(self):
        return f"<FlightSession(id={self.session_id}, callsign='{self.callsign}')>"


class TrackTelemetry(Base):
    """Time-series radar breadcrumbs."""
    __tablename__ = "track_telemetry"
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime(timezone=True), primary_key=True, nullable=False)
    
    session_id = Column(BigInteger, ForeignKey("fact_flight_session.session_id", ondelete="CASCADE"), nullable=False, index=True)
    
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    altitude_m = Column(Float, nullable=True)
    velocity_kmh = Column(Float, nullable=True)
    heading_deg = Column(Float, nullable=True)
    vertical_rate_ms = Column(Float, nullable=True)

    # ── FR24 EXTENSIONS ──────────────────────────────────────────────────
    vspeed_fpm = Column(Float, nullable=True)
    
    is_on_ground = Column(Boolean, default=False)
    squawk = Column(String(4), nullable=True)
    
    session = relationship("FactFlightSession", back_populates="tracks")

    __table_args__ = (
        Index('idx_tracks_session_time', 'session_id', 'timestamp', postgresql_using='btree'),
        Index('idx_tracks_geo', 'latitude', 'longitude'),
    )


class FactAviationEvent(Base):
    """The Intelligence Layer - tracks anomalies, emergencies, and state changes."""
    __tablename__ = "fact_aviation_events"
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    
    aircraft_id = Column(Integer, ForeignKey("dim_aircraft.id"), nullable=False)
    session_id = Column(BigInteger, ForeignKey("fact_flight_session.session_id"), nullable=True)
    
    event_category = Column(String(50), nullable=False) # e.g., EMERGENCY, GEOFENCE
    event_type = Column(String(50), nullable=False)     # e.g., SQUAWK_7700
    
    event_details = Column(JSONB, nullable=True)
    
    __table_args__ = (
        Index('idx_events_lookup', 'aircraft_id', 'event_category', 'timestamp'),
    )


# ═════════════════════════════════════════════════════════════════════════════
# 3. UI ACCELERATION (Denormalized)
# ═════════════════════════════════════════════════════════════════════════════

class CurrentAircraftState(Base):
    """Lightning fast flat table for the Live Map UI."""
    __tablename__ = "current_aircraft_state"
    
    icao24 = Column(String(6), primary_key=True, nullable=False)
    aircraft_id = Column(Integer, nullable=True)
    session_id = Column(BigInteger, nullable=True)
    
    callsign = Column(String(20), nullable=True)
    operator_name = Column(String(255), nullable=True)
    aircraft_model = Column(String(100), nullable=True)
    
    dep_airport_iata = Column(String(4), nullable=True)
    arr_airport_iata = Column(String(4), nullable=True)
    
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    altitude_m = Column(Float, nullable=True)
    velocity_kmh = Column(Float, nullable=True)
    heading_deg = Column(Float, nullable=True)
    on_ground = Column(Boolean, nullable=True)
    squawk = Column(String(4), nullable=True)
    
    last_updated = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    
    __table_args__ = (
        Index('idx_current_state_updated', 'last_updated'),
    )


# ═════════════════════════════════════════════════════════════════════════════
# 4. MAINTENANCE
# ═════════════════════════════════════════════════════════════════════════════

class IngestionJob(Base):
    """Tracks worker jobs and API budget usage."""
    __tablename__ = "ingestion_jobs"
    
    id = Column(Integer, primary_key=True, index=True)
    job_type = Column(String(50), nullable=False)
    target_date = Column(Date, nullable=True)
    region_key = Column(String(50), nullable=False)
    
    status = Column(String(20), default="pending", nullable=False)
    records_processed = Column(Integer, default=0)
    
    api_calls = Column(Integer, default=0)
    credits_used = Column(Integer, default=0)
    
    error_message = Column(Text, nullable=True)
    started_at = Column(DateTime(timezone=True), server_default=func.now())
    completed_at = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index('idx_ingestion_lookup', 'job_type', 'target_date', 'region_key'),
    )