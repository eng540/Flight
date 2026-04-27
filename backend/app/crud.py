"""
Enterprise CRUD Operations (v3.1 - Production Ready)
Implements nested transactions, data quality filters, session transitions,
event sourcing, and operator resolution.
"""
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, desc
from sqlalchemy.exc import IntegrityError
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timezone
import logging
import math

from app import models, schemas

logger = logging.getLogger(__name__)

class DataQualityValidator:
    """SRE: Data Quality Pipeline before DB insertion."""
    
    @staticmethod
    def validate_point(payload: schemas.RawIngestionPayload) -> bool:
        # 1. Reject impossible speeds (> 1200 km/h is unrealistic for commercial)
        if payload.velocity and payload.velocity > 1200:
            logger.debug(f"Rejected {payload.icao24}: Speed {payload.velocity} too high.")
            return False
            
        # 2. Reject impossible altitudes (> 60,000 ft ~ 18,288m)
        if payload.altitude and payload.altitude > 18500:
            logger.debug(f"Rejected {payload.icao24}: Altitude {payload.altitude} too high.")
            return False
            
        return True


class EnterpriseDataRouter:
    """Orchestrator for routing flat radar pings to the Snowflake Schema."""
    
    @staticmethod
    def process_telemetry_batch(db: Session, payloads: List[schemas.RawIngestionPayload]) -> Dict[str, int]:
        stats = {"new_aircrafts": 0, "new_sessions": 0, "tracks_recorded": 0, "events": 0, "rejected": 0, "errors": 0}
        
        # 1. Pre-process Geographies & Operators
        geo_cache = {}
        operator_cache = {}
        
        for p in payloads:
            if not DataQualityValidator.validate_point(p):
                stats["rejected"] += 1
                continue
                
            if p.est_departure_airport:
                EnterpriseDataRouter._ensure_geo(db, geo_cache, p.est_departure_airport)
            if p.est_arrival_airport:
                EnterpriseDataRouter._ensure_geo(db, geo_cache, p.est_arrival_airport)
            if p.operator_icao:
                EnterpriseDataRouter._ensure_operator(db, operator_cache, p.operator_icao)
                
        db.commit() # Commit reference data first

        # 2. Process Flights, Tracks, and Current State
        aircraft_cache = {}
        
        for payload in payloads:
            if not DataQualityValidator.validate_point(payload):
                continue
                
            try:
                # Resolve References
                dep_id = geo_cache.get(payload.est_departure_airport.upper()) if payload.est_departure_airport else None
                arr_id = geo_cache.get(payload.est_arrival_airport.upper()) if payload.est_arrival_airport else None
                op_id = operator_cache.get(payload.operator_icao.upper()) if payload.operator_icao else None

                # Resolve Aircraft (Dimension)
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
                            country_code=payload.origin_country[:2].upper() if payload.origin_country else None,
                            operator_id=op_id
                        )
                        db.add(aircraft)
                        db.flush()
                        stats["new_aircrafts"] += 1
                    
                    aircraft_cache[payload.icao24] = aircraft

                # Safe Timezone Handling
                dt_timestamp = datetime.fromtimestamp(payload.timestamp, tz=timezone.utc)

                # Resolve Flight Session (The Journey) - Complex Transition Logic
                session = db.query(models.FactFlightSession).filter(
                    models.FactFlightSession.aircraft_id == aircraft.id,
                    models.FactFlightSession.flight_status == "active"
                ).order_by(desc(models.FactFlightSession.last_seen_ts)).first()
                
                # Fetch Current State to check previous ground status
                current_state = db.query(models.CurrentAircraftState).filter(
                    models.CurrentAircraftState.icao24 == payload.icao24
                ).first()
                
                last_on_ground = current_state.on_ground if current_state else payload.on_ground

                # Session Logic: 
                # Open new session IF: No session OR (Was on ground, now flying) OR (Signal lost > 20 mins)
                is_new_flight = (
                    not session or 
                    (last_on_ground and not payload.on_ground) or 
                    ((dt_timestamp - session.last_seen_ts).total_seconds() > 1200)
                )

                if is_new_flight:
                    # Close old session if it exists and was active
                    if session and session.flight_status == "active":
                        session.flight_status = "landed" if last_on_ground else "lost_signal"
                        
                    session = models.FactFlightSession(
                        aircraft_id=aircraft.id,
                        operator_id=op_id,
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
                    # Update active session
                    session.last_seen_ts = dt_timestamp
                    if dep_id and not session.dep_airport_id: session.dep_airport_id = dep_id
                    if arr_id and not session.arr_airport_id: session.arr_airport_id = arr_id
                    if op_id and not session.operator_id: session.operator_id = op_id

                # Insert Track Telemetry (Nested Transaction / Savepoint)
                track = models.TrackTelemetry(
                    timestamp=dt_timestamp,
                    session_id=session.session_id,
                    latitude=payload.latitude,
                    longitude=payload.longitude,
                    altitude_m=payload.altitude,
                    velocity_kmh=payload.velocity,
                    heading_deg=payload.heading,
                    is_on_ground=payload.on_ground,
                    squawk=payload.squawk if hasattr(payload, 'squawk') else None
                )
                
                try:
                    with db.begin_nested(): # SRE Fix: Prevents full rollback on duplicates
                        db.add(track)
                        db.flush()
                        stats["tracks_recorded"] += 1
                except IntegrityError:
                    pass # Skip duplicate point safely

                # Event Sourcing: Check for Squawk Emergency
                squawk = payload.squawk if hasattr(payload, 'squawk') else None
                if squawk in ["7500", "7600", "7700"]:
                    last_squawk = current_state.squawk if current_state else None
                    if squawk != last_squawk:
                        event = models.FactAviationEvent(
                            timestamp=dt_timestamp,
                            aircraft_id=aircraft.id,
                            session_id=session.session_id,
                            event_category="EMERGENCY",
                            event_type=f"SQUAWK_{squawk}"
                        )
                        db.add(event)
                        stats["events"] += 1

                # Update Lightning-Fast Map UI Table (Current State)
                if not current_state:
                    current_state = models.CurrentAircraftState(icao24=payload.icao24)
                    db.add(current_state)
                
                # Fetch names for UI performance
                operator_name = None
                if op_id:
                    op_record = db.query(models.DimOperator).filter(models.DimOperator.id == op_id).first()
                    operator_name = op_record.name if op_record else None

                current_state.aircraft_id = aircraft.id
                current_state.session_id = session.session_id
                current_state.callsign = payload.callsign
                current_state.operator_name = operator_name
                current_state.aircraft_model = aircraft.model
                current_state.dep_airport_iata = payload.est_departure_airport
                current_state.arr_airport_iata = payload.est_arrival_airport
                current_state.latitude = payload.latitude
                current_state.longitude = payload.longitude
                current_state.altitude_m = payload.altitude
                current_state.velocity_kmh = payload.velocity
                current_state.heading_deg = payload.heading
                current_state.on_ground = payload.on_ground
                current_state.squawk = squawk
                current_state.last_updated = dt_timestamp
                    
            except Exception as e:
                logger.error(f"Error routing payload {payload.icao24}: {e}", exc_info=True)
                stats["errors"] += 1
                
        db.commit() # Final commit for the entire batch
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

    @staticmethod
    def _ensure_operator(db: Session, cache: dict, icao: str):
        icao = icao.upper()
        if icao in cache:
            return
            
        operator = db.query(models.DimOperator).filter(models.DimOperator.icao_code == icao).first()
        if not operator:
            operator = models.DimOperator(icao_code=icao, name=f"Operator {icao}")
            db.add(operator)
            db.flush()
            
        cache[icao] = operator.id


# --- Query Layer (For the API) ---
class FlightQueryCRUD:
    @staticmethod
    def get_active_flights_with_latest_track(db: Session, limit: int = 500):
        """Returns currently flying aircrafts for the Map UI using the hyper-fast State table."""
        # Only return flights updated in the last 15 minutes to avoid showing dead data
        fifteen_mins_ago = datetime.utcnow().timestamp() - 900
        cutoff_dt = datetime.fromtimestamp(fifteen_mins_ago, tz=timezone.utc)
        
        query = db.query(models.CurrentAircraftState).filter(
            models.CurrentAircraftState.last_updated >= cutoff_dt
        )
        
        total = query.count()
        current_flights = query.order_by(desc(models.CurrentAircraftState.last_updated)).limit(limit).all()
        
        return current_flights, total

# Analytics & Maintenance Stubs
class AnalyticsCRUD:
    pass

class IngestionJobCRUD:
    pass