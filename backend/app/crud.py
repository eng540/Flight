"""
Enterprise CRUD Operations (v3.0)
Handles the complex routing of flat incoming data into the Snowflake Schema.
"""
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func, and_, desc
from sqlalchemy.exc import IntegrityError
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime
import logging

from app import models, schemas

logger = logging.getLogger(__name__)

class EnterpriseDataRouter:
    """
    Acts as an Orchestrator. Takes a flat radar ping and safely routes it
    to Dimensions (Aircraft/Geo) and Facts (Session/Tracks).
    """
    
    @staticmethod
    def process_telemetry_batch(db: Session, payloads: List[schemas.RawIngestionPayload]) -> Dict[str, int]:
        stats = {"new_aircrafts": 0, "new_sessions": 0, "tracks_recorded": 0, "errors": 0}
        
        # Cache for current batch to minimize DB hits
        aircraft_cache = {}
        geo_cache = {}
        
        for payload in payloads:
            try:
                # 1. Resolve Geography (Airports)
                dep_id = None
                arr_id = None
                if payload.est_departure_airport:
                    dep = EnterpriseDataRouter._get_or_create_geo(db, geo_cache, payload.est_departure_airport)
                    dep_id = dep.id if dep else None
                if payload.est_arrival_airport:
                    arr = EnterpriseDataRouter._get_or_create_geo(db, geo_cache, payload.est_arrival_airport)
                    arr_id = arr.id if arr else None

                # 2. Resolve Aircraft (Dimension)
                aircraft = aircraft_cache.get(payload.icao24)
                if not aircraft:
                    aircraft = db.query(models.DimAircraft).filter(
                        models.DimAircraft.icao24 == payload.icao24,
                        models.DimAircraft.valid_to.is_(None)
                    ).first()
                    
                    if not aircraft:
                        aircraft = models.DimAircraft(
                            icao24=payload.icao24,
                            registration=payload.registration,
                            country_code=payload.origin_country[:2].upper() if payload.origin_country else None
                        )
                        db.add(aircraft)
                        db.flush() # Get ID without committing
                        stats["new_aircrafts"] += 1
                    
                    aircraft_cache[payload.icao24] = aircraft

                # 3. Resolve Flight Session (The Journey)
                # Find an active session for this aircraft
                dt_timestamp = datetime.utcfromtimestamp(payload.timestamp)
                session = db.query(models.FactFlightSession).filter(
                    models.FactFlightSession.aircraft_id == aircraft.id,
                    models.FactFlightSession.flight_status == "active"
                ).order_by(desc(models.FactFlightSession.last_seen_ts)).first()
                
                # If no session or session is too old (e.g., > 12 hours), create new
                if not session or (dt_timestamp - session.last_seen_ts).total_seconds() > 43200:
                    session = models.FactFlightSession(
                        aircraft_id=aircraft.id,
                        callsign=payload.callsign,
                        dep_airport_id=dep_id,
                        arr_airport_id=arr_id,
                        first_seen_ts=dt_timestamp,
                        last_seen_ts=dt_timestamp,
                        flight_status="active"
                    )
                    db.add(session)
                    db.flush()
                    stats["new_sessions"] += 1
                else:
                    # Update session's last seen and airports if we got new data
                    session.last_seen_ts = dt_timestamp
                    if dep_id and not session.dep_airport_id: session.dep_airport_id = dep_id
                    if arr_id and not session.arr_airport_id: session.arr_airport_id = arr_id

                # 4. Insert Track Telemetry (The Time-Series Data)
                track = models.TrackTelemetry(
                    timestamp=dt_timestamp,
                    session_id=session.session_id,
                    latitude=payload.latitude,
                    longitude=payload.longitude,
                    altitude_m=payload.altitude,
                    velocity_kmh=payload.velocity,
                    heading_deg=payload.heading,
                    is_on_ground=payload.on_ground
                )
                
                # Use merge/upsert logically here if exact timestamp exists, 
                # but standard add is fine for append-only time-series
                try:
                    db.add(track)
                    db.flush()
                    stats["tracks_recorded"] += 1
                except IntegrityError:
                    db.rollback() # Skip duplicate timestamp for same session
                    
            except Exception as e:
                logger.error(f"Error routing payload {payload.icao24}: {e}")
                db.rollback()
                stats["errors"] += 1
                
        # Commit the entire batch
        db.commit()
        return stats

    @staticmethod
    def _get_or_create_geo(db: Session, cache: dict, icao: str) -> Optional[models.DimGeography]:
        if not icao: return None
        icao = icao.upper()
        if icao in cache: return cache[icao]
        
        geo = db.query(models.DimGeography).filter(models.DimGeography.icao_code == icao).first()
        if not geo:
            geo = models.DimGeography(icao_code=icao, name=f"Airport {icao}")
            db.add(geo)
            db.flush()
        
        cache[icao] = geo
        return geo

# --- Query Layer (For the API) ---
class FlightQueryCRUD:
    @staticmethod
    def get_active_flights_with_latest_track(db: Session, limit: int = 500) -> Tuple[List[models.FactFlightSession], int]:
        """Returns currently flying aircrafts for the Map UI."""
        # In a real Time-Series DB, we'd use a Materialized View here.
        # For now, we query active sessions.
        query = db.query(models.FactFlightSession).options(
            joinedload(models.FactFlightSession.aircraft),
            joinedload(models.FactFlightSession.dep_airport),
            joinedload(models.FactFlightSession.arr_airport)
        ).filter(models.FactFlightSession.flight_status == "active")
        
        total = query.count()
        sessions = query.order_by(desc(models.FactFlightSession.last_seen_ts)).limit(limit).all()
        return sessions, total

# Analytics & Maintenance Stubs (To be expanded in API Layer package)
class AnalyticsCRUD:
    pass

class IngestionJobCRUD:
    pass