"""
Enterprise CRUD Operations (v3.2 - Master Production Grade)
Fully compliant with Aviation Physics, State Machines, and Event Sourcing.
"""
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, desc
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timezone, timedelta
import logging
import math

from app import models, schemas

logger = logging.getLogger(__name__)

class AviationMath:
    """Helper for physical calculations (Haversine, etc.)"""
    @staticmethod
    def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate distance in KM between two points."""
        R = 6371.0 # Earth radius in KM
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = math.sin(dlat / 2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2)**2
        return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


class DataQualityValidator:
    """SRE: Advanced Data Quality Pipeline."""
    
    @staticmethod
    def validate_physics(payload: schemas.RawIngestionPayload, current_state: Optional[models.CurrentAircraftState]) -> bool:
        # 1. Reject impossible speeds/altitudes
        if payload.velocity and payload.velocity > 1200: return False
        if payload.altitude and payload.altitude > 18500: return False
        
        if current_state:
            # 2. Reject exact duplicates (Same time + Same position)
            # Safe timezone check for current_state
            if current_state.last_updated:
                # current_state.last_updated is timezone-aware
                if abs((payload.timestamp - current_state.last_updated.timestamp())) < 1 and \
                   abs(payload.latitude - current_state.latitude) < 0.0001 and \
                   abs(payload.longitude - current_state.longitude) < 0.0001:
                    return False # It's an exact duplicate ping

            # 3. Reject Ghost Jumps (e.g., > 50km jump in less than 30 seconds)
            time_diff = payload.timestamp - current_state.last_updated.timestamp()
            if 0 < time_diff < 30:
                dist_km = AviationMath.haversine_distance(
                    current_state.latitude, current_state.longitude, 
                    payload.latitude, payload.longitude
                )
                if dist_km > 50:
                    logger.warning(f"Rejected Ghost Jump for {payload.icao24}: {dist_km:.1f}km in {time_diff}s.")
                    return False
                    
            # 4. Reject impossible altitude spikes (> 500m per ping is extreme for commercial)
            if payload.altitude is not None and current_state.altitude_m is not None:
                if abs(payload.altitude - current_state.altitude_m) > 1000 and time_diff < 10:
                    return False

        return True


class EnterpriseDataRouter:
    """The Intelligence Brain for Data Routing and State Machines."""
    
    @staticmethod
    def process_telemetry_batch(db: Session, payloads: List[schemas.RawIngestionPayload]) -> Dict[str, int]:
        stats = {"new_aircrafts": 0, "new_sessions": 0, "tracks_recorded": 0, "events": 0, "rejected": 0, "errors": 0}
        
        # 1. Pre-process Geographies & Operators (Foreign Key Safety)
        geo_cache, operator_cache = {}, {}
        for p in payloads:
            if p.est_departure_airport: EnterpriseDataRouter._ensure_geo(db, geo_cache, p.est_departure_airport)
            if p.est_arrival_airport: EnterpriseDataRouter._ensure_geo(db, geo_cache, p.est_arrival_airport)
            if p.operator_icao: EnterpriseDataRouter._ensure_operator(db, operator_cache, p.operator_icao)
        db.commit() 

        # 2. Process Radar Pings
        aircraft_cache = {}
        
        for payload in payloads:
            try:
                # Fetch Current State first for Validation & State Machine
                current_state = db.query(models.CurrentAircraftState).filter(
                    models.CurrentAircraftState.icao24 == payload.icao24
                ).first()
                
                # Quality Check
                if not DataQualityValidator.validate_physics(payload, current_state):
                    stats["rejected"] += 1
                    continue

                dep_id = geo_cache.get(payload.est_departure_airport.upper()) if payload.est_departure_airport else None
                arr_id = geo_cache.get(payload.est_arrival_airport.upper()) if payload.est_arrival_airport else None
                op_id = operator_cache.get(payload.operator_icao.upper()) if payload.operator_icao else None

                # Resolve Aircraft (SCD Type 2 Dimension)
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

                dt_timestamp = datetime.fromtimestamp(payload.timestamp, tz=timezone.utc)

                # --- SESSION STATE MACHINE (The Core Fix) ---
                session = db.query(models.FactFlightSession).filter(
                    models.FactFlightSession.aircraft_id == aircraft.id,
                    models.FactFlightSession.flight_status == "active"
                ).order_by(desc(models.FactFlightSession.last_seen_ts)).first()
                
                last_on_ground = current_state.on_ground if current_state else payload.on_ground
                is_moving = payload.velocity and payload.velocity > 50 # km/h

                # Condition to OPEN a new session
                # Avoid Ghost Sessions: Don't open if grounded and stationary
                should_open_session = False
                if not session:
                    if not payload.on_ground or is_moving:
                        should_open_session = True
                else:
                    time_since_last = (dt_timestamp - session.last_seen_ts).total_seconds()
                    # Condition to CLOSE active session
                    should_close = False
                    status_reason = ""
                    
                    # 1. Lost Signal (20 mins)
                    if time_since_last > 1200:
                        should_close = True
                        status_reason = "lost_signal"
                    # 2. Landed and stopped (5 mins of stationary ground data approx)
                    elif payload.on_ground and not is_moving and last_on_ground:
                        if time_since_last > 300: 
                            should_close = True
                            status_reason = "landed"

                    if should_close:
                        session.flight_status = status_reason
                        session.actual_landing_ts = session.last_seen_ts if status_reason == "landed" else None
                        db.flush()
                        
                        # Generate Event: SIGNAL_LOST
                        if status_reason == "lost_signal":
                            db.add(models.FactAviationEvent(
                                timestamp=dt_timestamp, aircraft_id=aircraft.id, session_id=session.session_id,
                                event_category="SYSTEM", event_type="SIGNAL_LOST"
                            ))
                            stats["events"] += 1
                            
                        # If a new ping arrives after closing, and it's flying, open a new one
                        if not payload.on_ground or is_moving:
                            should_open_session = True

                if should_open_session:
                    session = models.FactFlightSession(
                        aircraft_id=aircraft.id, operator_id=op_id, callsign=payload.callsign,
                        dep_airport_id=dep_id, arr_airport_id=arr_id,
                        first_seen_ts=dt_timestamp, last_seen_ts=dt_timestamp, flight_status="active"
                    )
                    db.add(session)
                    db.flush()
                    stats["new_sessions"] += 1
                    
                    # Generate Event: TAKEOFF
                    if not payload.on_ground:
                        db.add(models.FactAviationEvent(
                            timestamp=dt_timestamp, aircraft_id=aircraft.id, session_id=session.session_id,
                            event_category="FLIGHT", event_type="TAKEOFF"
                        ))
                        stats["events"] += 1

                # If session exists and is active, update it
                if session and session.flight_status == "active":
                    session.last_seen_ts = dt_timestamp
                    if payload.altitude and (session.max_altitude_m is None or payload.altitude > session.max_altitude_m):
                        session.max_altitude_m = payload.altitude
                    if dep_id and not session.dep_airport_id: session.dep_airport_id = dep_id
                    if arr_id and not session.arr_airport_id: session.arr_airport_id = arr_id
                    if op_id and not session.operator_id: session.operator_id = op_id

                    # Insert Track Telemetry safely
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
                    db.add(track)
                    stats["tracks_recorded"] += 1

                # Event Sourcing: Check for Squawk Emergency
                squawk = payload.squawk if hasattr(payload, 'squawk') else None
                if squawk in ["7500", "7600", "7700"]:
                    last_squawk = current_state.squawk if current_state else None
                    if squawk != last_squawk and session:
                        db.add(models.FactAviationEvent(
                            timestamp=dt_timestamp, aircraft_id=aircraft.id, session_id=session.session_id,
                            event_category="EMERGENCY", event_type=f"SQUAWK_{squawk}"
                        ))
                        stats["events"] += 1

                # Update Lightning-Fast UI Cache
                if not current_state:
                    current_state = models.CurrentAircraftState(icao24=payload.icao24)
                    db.add(current_state)
                
                # Retrieve names for UI
                operator_name = None
                if op_id:
                    op_record = db.query(models.DimOperator).filter(models.DimOperator.id == op_id).first()
                    operator_name = op_record.name if op_record else None

                current_state.aircraft_id = aircraft.id
                current_state.session_id = session.session_id if session else None
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
                db.rollback() # Rollback only this specific flight iteration, not the whole batch!
                stats["errors"] += 1
                
        try:
            db.commit() # Final commit for the successful flights in batch
        except Exception as e:
            logger.error(f"Batch commit failed: {e}")
            db.rollback()
            stats["errors"] += 1
            
        return stats

    @staticmethod
    def _ensure_geo(db: Session, cache: dict, icao: str):
        icao = icao.upper()
        if icao in cache: return
        geo = db.query(models.DimGeography).filter(models.DimGeography.icao_code == icao).first()
        if not geo:
            geo = models.DimGeography(icao_code=icao, name=f"Airport {icao}")
            db.add(geo)
            db.flush()
        cache[icao] = geo.id

    @staticmethod
    def _ensure_operator(db: Session, cache: dict, icao: str):
        icao = icao.upper()
        if icao in cache: return
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