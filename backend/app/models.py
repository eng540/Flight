"""SQLAlchemy models for the Flight Intelligence database."""
from sqlalchemy import (Column, Integer, String, Float, DateTime, ForeignKey,
                        Boolean, Index, BigInteger, Text, UniqueConstraint)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import JSONB
from app.database import Base
from datetime import datetime
from typing import Optional


class Country(Base):
    __tablename__ = "countries"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), unique=True, nullable=False, index=True)
    iso_code = Column(String(3), unique=True, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    airlines = relationship("Airline", back_populates="country")

    def __repr__(self):
        return f"<Country(name='{self.name}')>"


class Airline(Base):
    __tablename__ = "airlines"
    id = Column(Integer, primary_key=True, index=True)
    icao24 = Column(String(6), unique=True, nullable=False, index=True)
    name = Column(String(200), nullable=True)
    callsign_prefix = Column(String(10), nullable=True)
    country_id = Column(Integer, ForeignKey("countries.id"), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    country = relationship("Country", back_populates="airlines")
    flights = relationship("Flight", back_populates="airline")
    __table_args__ = (Index('idx_airline_icao24_name', 'icao24', 'name'),)

    def __repr__(self):
        return f"<Airline(icao24='{self.icao24}', name='{self.name}')>"


class Flight(Base):
    __tablename__ = "flights"
    id = Column(Integer, primary_key=True, index=True)
    icao24 = Column(String(6), nullable=False, index=True)
    callsign = Column(String(20), nullable=True, index=True)
    airline_id = Column(Integer, ForeignKey("airlines.id"), nullable=True, index=True)
    origin_country = Column(String(100), nullable=True, index=True)
    first_seen = Column(BigInteger, nullable=True, index=True)
    last_seen = Column(BigInteger, nullable=True, index=True)
    est_departure_airport = Column(String(4), nullable=True, index=True)
    est_departure_airport_horiz_distance = Column(Integer, nullable=True)
    est_departure_airport_vert_distance = Column(Integer, nullable=True)
    est_arrival_airport = Column(String(4), nullable=True, index=True)
    est_arrival_airport_horiz_distance = Column(Integer, nullable=True)
    est_arrival_airport_vert_distance = Column(Integer, nullable=True)
    est_departure_time = Column(BigInteger, nullable=True)
    est_arrival_time = Column(BigInteger, nullable=True)
    # Geographic position
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    altitude = Column(Float, nullable=True)
    velocity = Column(Float, nullable=True)
    heading = Column(Float, nullable=True)
    on_ground = Column(Boolean, nullable=True)
    # Trajectory stored as JSONB list of {ts,lat,lon,alt,vel,hdg}
    trajectory = Column(JSONB, nullable=True)
    # Region tag for fast filtering
    region_key = Column(String(50), nullable=True, index=True)
    ingestion_time = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    unique_flight_id = Column(String(100), unique=True, nullable=False, index=True)
    airline = relationship("Airline", back_populates="flights")

    __table_args__ = (
        Index('idx_flight_time_range', 'first_seen', 'last_seen'),
        Index('idx_flight_airports', 'est_departure_airport', 'est_arrival_airport'),
        Index('idx_flight_ingestion', 'ingestion_time'),
        Index('idx_flight_country', 'origin_country'),
        Index('idx_flight_geo', 'latitude', 'longitude'),
        Index('idx_flight_region', 'region_key'),
    )

    def __repr__(self):
        return f"<Flight(icao24='{self.icao24}', callsign='{self.callsign}')>"

    @property
    def duration_seconds(self) -> Optional[int]:
        if self.first_seen and self.last_seen:
            return self.last_seen - self.first_seen
        return None

    @property
    def duration_minutes(self) -> Optional[float]:
        d = self.duration_seconds
        return d / 60 if d else None

    @property
    def duration_hours(self) -> Optional[float]:
        d = self.duration_minutes
        return d / 60 if d else None


class IngestionJob(Base):
    """Tracks historical ingestion jobs – prevents duplicate work."""
    __tablename__ = "ingestion_jobs"
    id = Column(Integer, primary_key=True, index=True)
    date_str = Column(String(10), nullable=False, index=True)      # YYYY-MM-DD
    region_key = Column(String(50), nullable=False, index=True)
    lamin = Column(Float, nullable=False)
    lomin = Column(Float, nullable=False)
    lamax = Column(Float, nullable=False)
    lomax = Column(Float, nullable=False)
    begin_ts = Column(BigInteger, nullable=False)
    end_ts = Column(BigInteger, nullable=False)
    # status: pending | running | completed | failed
    status = Column(String(20), default="pending", nullable=False, index=True)
    flights_ingested = Column(Integer, default=0)
    chunks_total = Column(Integer, default=0)
    chunks_done = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        UniqueConstraint('date_str', 'region_key', name='uq_ingestion_date_region'),
        Index('idx_ingestion_status', 'status'),
        Index('idx_ingestion_date_region', 'date_str', 'region_key'),
    )
