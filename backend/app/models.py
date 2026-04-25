"""
Enterprise Aviation Intelligence Schema (v3.0 - 50-Year Architecture)
Implements Snowflake Schema, Event Sourcing, and Time-Series optimized structures.
"""
from sqlalchemy import (
    Column, Integer, String, Float, DateTime, ForeignKey, 
    Boolean, Index, BigInteger, Text, JSON, Date
)
from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import JSONB
from app.database import Base

# ═════════════════════════════════════════════════════════════════════════════
# 1. DIMENSION TABLES (Master Data / Reference Entities)
# These tables change rarely and hold the "truth" about physical & legal entities.
# ═════════════════════════════════════════════════════════════════════════════

class DimGeography(Base):
    """
    Airports and Geographical boundaries.
    Future-proofed for PostGIS integration (Polygon/Point storage).
    """
    __tablename__ = "dim_geography"
    
    id = Column(Integer, primary_key=True, index=True)
    icao_code = Column(String(4), unique=True, nullable=True, index=True) # e.g., OMDB
    iata_code = Column(String(3), nullable=True, index=True)              # e.g., DXB
    name = Column(String(255), nullable=False)                            # Dubai International Airport
    city = Column(String(100), nullable=True)
    country_code = Column(String(2), nullable=True, index=True)           # ISO 3166-1 alpha-2 (AE)
    
    # Coordinates for quick reference
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    elevation_m = Column(Float, nullable=True)
    
    # Metadata (Timezone, Runway counts, etc.)
    meta_data = Column(JSONB, nullable=True)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    def __repr__(self):
        return f"<Airport {self.icao_code} - {self.name}>"


class DimOperator(Base):
    """
    Airlines, Militaries, and Private Operators.
    """
    __tablename__ = "dim_operator"
    
    id = Column(Integer, primary_key=True, index=True)
    icao_code = Column(String(3), unique=True, nullable=True, index=True) # e.g., UAE
    iata_code = Column(String(2), nullable=True, index=True)              # e.g., EK
    name = Column(String(255), nullable=False)                            # Emirates
    country_code = Column(String(2), nullable=True)
    operator_type = Column(String(50), nullable=True)                     # Commercial, Cargo, Military, VIP
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    def __repr__(self):
        return f"<Operator {self.icao_code} - {self.name}>"


class DimAircraft(Base):
    """
    The Physical Asset. 
    Implements Slowly Changing Dimension (SCD Type 2) logic via valid_from/to.
    """
    __tablename__ = "dim_aircraft"
    
    id = Column(Integer, primary_key=True, index=True)
    icao24 = Column(String(6), nullable=False, index=True)                # The absolute Hex ID (e.g., 89617a)
    registration = Column(String(20), nullable=True, index=True)          # Tail Number (e.g., A6-EGV)
    
    # Physical Attributes
    manufacturer = Column(String(100), nullable=True)                     # Boeing
    model = Column(String(100), nullable=True)                            # 777-300ER
    type_code = Column(String(10), nullable=True, index=True)             # B77W
    serial_number = Column(String(100), nullable=True)                    # MSN
    year_built = Column(Integer, nullable=True)
    
    # Ownership/Operation
    operator_id = Column(Integer, ForeignKey("dim_operator.id"), nullable=True)
    country_code = Column(String(2), nullable=True)                       # Country of registration
    
    # SCD Type 2 Fields (For Historical Accuracy of Ownership)
    valid_from = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    valid_to = Column(DateTime(timezone=True), nullable=True)             # If NULL, this is the current active record
    
    # Relationships
    operator = relationship("DimOperator")
    
    __table_args__ = (
        Index('idx_aircraft_hex_active', 'icao24', 'valid_to'),           # Fast lookup for currently active planes
    )

    def __repr__(self):
        return f"<Aircraft {self.icao24} ({self.registration})>"


# ═════════════════════════════════════════════════════════════════════════════
# 2. OPERATIONAL FACT TABLES (The Business Process)
# Records the "Intent" and the lifecycle of a single journey.
# ═════════════════════════════════════════════════════════════════════════════

class FactFlightSession(Base):
    """
    A single journey from takeoff to landing.
    Answers: "How many flights did this plane do?", "Top Routes", "Delays".
    """
    __tablename__ = "fact_flight_session"
    
    session_id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    
    # Identities
    aircraft_id = Column(Integer, ForeignKey("dim_aircraft.id"), nullable=False, index=True)
    operator_id = Column(Integer, ForeignKey("dim_operator.id"), nullable=True, index=True)
    callsign = Column(String(20), nullable=True, index=True)              # The flight number (e.g., UAE214)
    
    # Routing (Intent)
    dep_airport_id = Column(Integer, ForeignKey("dim_geography.id"), nullable=True, index=True)
    arr_airport_id = Column(Integer, ForeignKey("dim_geography.id"), nullable=True, index=True)
    
    # Temporal Data (The Journey Timeline)
    first_seen_ts = Column(DateTime(timezone=True), nullable=False, index=True)
    last_seen_ts = Column(DateTime(timezone=True), nullable=False, index=True)
    actual_takeoff_ts = Column(DateTime(timezone=True), nullable=True)
    actual_landing_ts = Column(DateTime(timezone=True), nullable=True)
    
    # Status & Analytics
    flight_status = Column(String(20), default="active", index=True)      # active, landed, diverted, lost_signal
    total_distance_km = Column(Float, nullable=True)                      # Calculated post-flight
    max_altitude_m = Column(Float, nullable=True)
    
    # Relationships
    aircraft = relationship("DimAircraft")
    operator = relationship("DimOperator")
    dep_airport = relationship("DimGeography", foreign_keys=[dep_airport_id])
    arr_airport = relationship("DimGeography", foreign_keys=[arr_airport_id])

    __table_args__ = (
        Index('idx_flight_search', 'callsign', 'first_seen_ts'),
        Index('idx_flight_route', 'dep_airport_id', 'arr_airport_id'),
    )


# ═════════════════════════════════════════════════════════════════════════════
# 3. TIME-SERIES TELEMETRY (The Big Data Lake)
# Billions of rows. Designed for TimescaleDB partitioning. Append-only.
# ═════════════════════════════════════════════════════════════════════════════

class TrackTelemetry(Base):
    """
    The Radar Breadcrumbs. 
    Answers: "Where exactly was this plane at 14:02:05?", "Draw the flight path".
    """
    __tablename__ = "track_telemetry"
    
    # Composite Primary Key required for TimescaleDB (time + id)
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime(timezone=True), primary_key=True, nullable=False) # MUST BE PK for Time-Series
    
    session_id = Column(BigInteger, ForeignKey("fact_flight_session.session_id", ondelete="CASCADE"), nullable=False, index=True)
    
    # Kinematics
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    altitude_m = Column(Float, nullable=True)                             # Barometric altitude
    velocity_kmh = Column(Float, nullable=True)                           # Ground speed
    heading_deg = Column(Float, nullable=True)                            # True track (0-360)
    vertical_rate_ms = Column(Float, nullable=True)                       # Climb/Descent rate
    
    # System Status
    is_on_ground = Column(Boolean, default=False)
    squawk = Column(String(4), nullable=True)                             # Transponder code (e.g., 7700 for emergency)
    
    __table_args__ = (
        Index('idx_tracks_session_time', 'session_id', 'timestamp', postgresql_using='btree'),
        Index('idx_tracks_geo', 'latitude', 'longitude'),                 # Future-proof for PostGIS
    )


# ═════════════════════════════════════════════════════════════════════════════
# 4. EVENT SOURCING (The Intelligence & Forensics Log)
# ═════════════════════════════════════════════════════════════════════════════

class FactAviationEvent(Base):
    """
    Significant occurrences that happen to an aircraft or during a flight.
    Answers: "When did it cross the border?", "When did it declare an emergency?".
    """
    __tablename__ = "fact_aviation_events"
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    
    aircraft_id = Column(Integer, ForeignKey("dim_aircraft.id"), nullable=False)
    session_id = Column(BigInteger, ForeignKey("fact_flight_session.session_id"), nullable=True) # Nullable because planes can be sold while grounded
    
    event_category = Column(String(50), nullable=False, index=True)       # e.g., EMERGENCY, GEO_FENCE, OWNERSHIP_CHANGE
    event_type = Column(String(50), nullable=False)                       # e.g., SQUAWK_7700, ENTERED_SAUDI_ARABIA
    
    event_details = Column(JSONB, nullable=True)                          # Flexible schema for specific event data
    
    __table_args__ = (
        Index('idx_events_lookup', 'aircraft_id', 'event_category', 'timestamp'),
    )


# ═════════════════════════════════════════════════════════════════════════════
# 5. SYSTEM MAINTENANCE (Audit & Worker Jobs)
# ═════════════════════════════════════════════════════════════════════════════

class IngestionJob(Base):
    """Tracks historical ingestion jobs to prevent duplicate work."""
    __tablename__ = "ingestion_jobs"
    
    id = Column(Integer, primary_key=True, index=True)
    job_type = Column(String(50), nullable=False)                         # 'historical_fill', 'live_radar', 'reference_sync'
    target_date = Column(Date, nullable=True, index=True)                 
    region_key = Column(String(50), nullable=False, index=True)
    
    status = Column(String(20), default="pending", nullable=False, index=True)
    records_processed = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
    
    started_at = Column(DateTime(timezone=True), server_default=func.now())
    completed_at = Column(DateTime(timezone=True), nullable=True)
    
    __table_args__ = (
        Index('idx_ingestion_lookup', 'job_type', 'target_date', 'region_key'),
    )