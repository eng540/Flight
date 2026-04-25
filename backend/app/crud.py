"""
Enterprise CRUD Operations (v3.0)
Handles the complex routing of flat incoming data into the Snowflake Schema.
"""
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func, and_, desc
from sqlalchemy.exc import IntegrityError
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timezone  # <-- أضفنا timezone هنا
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
        
        # 1. Pre-process Airports to ensure they exist and are committed
        geo_cache = {}
        for p in payloads:
            if p.est_departure_airport:
                EnterpriseDataRouter._ensure_geo(db, geo_cache, p.est_departure_airport)
            if p.est_arrival_airport:
                EnterpriseDataRouter._ensure_geo(db, geo_cache, p.est_arrival_airport)
        
        db.commit() # Commit airports first to prevent ForeignKey violations

        # 2. Process Flights and Tracks
        aircraft_cache = {}
        
        for payload in payloads:
            try:
                dep_id = geo_cache.get(payload.est_departure_airport.upper()) if payload.est_departure_airport else None
                arr_id = geo_cache.get(payload.est_arrival_airport.upper()) if payload.est_arrival_airport else None

                # Resolve Aircraft
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
                        db.flush()
                        stats["new_aircrafts"] += 1
                    
                    aircraft_cache[payload.icao24] = aircraft

                # Resolve Session
                # ----- إصلاح المنطقة الزمنية: استخدام fromtimestamp مع timezone.utc -----
                dt_timestamp = datetime.fromtimestamp(payload.timestamp, tz=timezone.utc)
                
                session = db.query(models.FactFlightSession).filter(
                    models.FactFlightSession.aircraft_id == aircraft.id,
                    models.FactFlightSession.flight_status == "active"
                ).order_by(desc(models.FactFlightSession.last_seen_ts)).first()
                
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
                    session.last_seen_ts = dt_timestamp
                    if dep_id and not session.dep_airport_id: session.dep_airport_id = dep_id
                    if arr_id and not session.arr_airport_id: session.arr_airport_id = arr_id

                # Insert Track
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
                
                try:
                    db.add(track)
                    db.flush()
                    stats["tracks_recorded"] += 1
                except IntegrityError:
                    db.rollback() 
                    
            except Exception as e:
                logger.error(f"Error routing payload {payload.icao24}: {e}")
                db.rollback()
                stats["errors"] += 1
                
        db.commit()
        return stats

    @staticmethod
    def _ensure_geo(db: Session, cache: dict, icao: str):
        icao = icao.upper()
        if icao in cache:
            return
            
        geo = db.query(models.DimGeography).filter(models.DimGeography.icao_code == icao).first()
        if not geo:
            geo = models.DimGeography(icao_code=icao, name=f"Airport {icao}")
            db.add(geo)
            db.flush()
            
        cache[icao] = geo.id


# --- Query Layer (For the API) ---
class FlightQueryCRUD:
    @staticmethod
    def get_active_flights_with_latest_track(db: Session, limit: int = 500) -> Tuple[List[models.FactFlightSession], int]:
        """Returns currently flying aircrafts for the Map UI."""
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