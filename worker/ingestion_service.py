"""
Enterprise Ingestion Service (v5.1 - FR24 Historical Fetch)
Strictly compliant with FR24 OpenAPI v1 Specification.
Features: Smart Bounding Box, ISO8601 parsing, Circuit Breaking, and Fallbacks.
Added: fetch_and_store_summaries for historical data ingestion.
"""
import logging
import sys
import os
import time
import requests
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

from app.schemas import RawIngestionPayload
from app.crud import EnterpriseDataRouter
from app import models

logger = logging.getLogger(__name__)

class FlightIngestionService:

    def __init__(self):
        self._db = None
        self.fr24_base_url = "https://fr24api.flightradar24.com"
        self.fr24_api_key = os.getenv("FR24_API_KEY")
        
        # SRE: Advanced Circuit Breaker
        self.consecutive_failures = 0
        self.pause_until = 0.0

    def __enter__(self):
        from app.database import SessionLocal
        self._db = SessionLocal()
        return self

    def __exit__(self, *_):
        if self._db:
            self._db.close()
            self._db = None

    def _new_db(self):
        from app.database import SessionLocal
        return SessionLocal()

    # ── SRE Utilities & Circuit Breaker ───────────────────────────────────────

    def _safe_request(self, endpoint: str, params: dict) -> Optional[dict]:
        """SRE: Resilient HTTP Requester compliant with FR24 Specs."""
        if not self.fr24_api_key:
            logger.error("[FR24 Auth] API key missing! Set FR24_API_KEY in Railway.")
            return None

        if time.time() < self.pause_until:
            logger.warning("[Circuit Breaker] API paused. Skipping request.")
            return None

        headers = {
            "Accept": "application/json",
            "Accept-Version": "v1",
            "Authorization": f"Bearer {self.fr24_api_key}"
        }

        url = f"{self.fr24_base_url}{endpoint}"

        try:
            response = requests.get(url, headers=headers, params=params, timeout=15)
            
            if response.status_code == 200:
                self.consecutive_failures = 0
                return response.json()
            
            elif response.status_code == 429:
                logger.warning("[FR24] 429 Too Many Requests. Applying backpressure (10s sleep).")
                time.sleep(10)
                return None
                
            elif response.status_code == 401:
                logger.error("[FR24] 401 Unauthorized. Invalid Token. Pausing for 10 mins.")
                self.pause_until = time.time() + 600
                return None
                
            elif response.status_code == 402:
                logger.critical("[FR24] 402 Payment Required! Credit limit reached. Pausing for 1 hour.")
                self.pause_until = time.time() + 3600
                return None
                
            else:
                logger.error(f"[FR24] HTTP {response.status_code}: {response.text}")
                self.consecutive_failures += 1
                if self.consecutive_failures >= 3:
                    logger.error("[Circuit Breaker] 3 consecutive errors. Pausing for 2 minutes.")
                    self.pause_until = time.time() + 120
                return None

        except requests.exceptions.RequestException as e:
            logger.error(f"[FR24 Network Error] {e}")
            return None

    # ── FlightRadar24 Master Engine ──────────────────────────────────────────

    def ingest_live_radar_from_fr24(self, regions) -> Dict[str, int]:
        """
        Uses FR24 /api/live/flight-positions/full to get ALL data in one strike.
        Parses exactly according to the FR24 OpenAPI spec.
        """
        total_stats = {"new_aircrafts": 0, "new_sessions": 0, "tracks_recorded": 0, "events": 0, "rejected": 0, "errors": 0}
        db = self._new_db()
        now_ts = int(time.time())

        try:
            for region in regions:
                logger.info(f"[FR24] Scanning airspace: {region.name_ar}...")
                
                # FR24 Bounding Box: north, south, west, east (lamax, lamin, lomin, lomax)
                bounds = f"{region.lamax},{region.lamin},{region.lomin},{region.lomax}"
                
                data = self._safe_request("/api/live/flight-positions/full", {"bounds": bounds, "limit": 1000})
                
                if not data:
                    continue # Circuit breaker handles logging

                flights = data.get("data", [])
                if not flights:
                    logger.info(f"[{region.key}] Empty airspace (0 flights).")
                    continue

                payloads = []
                for f in flights:
                    # Map FR24 variables exactly as per OpenAPI specification
                    icao24 = f.get("hex")
                    if not icao24:
                        continue
                        
                    callsign = f.get("callsign") or f.get("flight")
                    
                    # Parse ISO 8601 Timestamp to Unix Epoch (e.g., "2023-11-08T10:10:00Z")
                    ts_str = f.get("timestamp")
                    try:
                        dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                        flight_ts = int(dt.timestamp())
                    except:
                        flight_ts = now_ts
                    
                    unique_id = hashlib.md5(f"{icao24}_{callsign}_{flight_ts}".encode()).hexdigest()

                    # FR24 supplies AMSL in feet, Speed in knots. We convert to standard metric.
                    altitude_ft = f.get("alt")
                    speed_kts = f.get("gspeed")
                    
                    payloads.append(RawIngestionPayload(
                        icao24=str(icao24).lower()[:6],
                        callsign=callsign,
                        registration=f.get("reg"),
                        operator_icao=f.get("operating_as") or f.get("painted_as"),
                        origin_country=None, # FR24 doesn't supply country flag directly here
                        timestamp=flight_ts,
                        longitude=float(f.get("lon", 0)),
                        latitude=float(f.get("lat", 0)),
                        altitude=float(altitude_ft) * 0.3048 if altitude_ft else 0.0,
                        velocity=float(speed_kts) * 1.852 if speed_kts else 0.0,
                        heading=float(f.get("track", 0)),
                        on_ground=True if altitude_ft == 0 else False,
                        est_departure_airport=f.get("orig_icao"),
                        est_arrival_airport=f.get("dest_icao"),
                        squawk=f.get("squawk"),
                        region_key=region.key
                    ))

                # Route through our Enterprise Brain
                batch_stats = EnterpriseDataRouter.process_telemetry_batch(db, payloads)
                
                for k in total_stats:
                    total_stats[k] += batch_stats.get(k, 0)
                
                logger.info(f"[{region.key}] FR24 Processed {len(payloads)} flights. Stats: {batch_stats}")
                
        except Exception as e:
            logger.error(f"[FR24 Master] Critical Exception: {e}", exc_info=True)
        finally:
            db.close()
            
        return total_stats

    def cleanup_old_data(self, days: int) -> int:
        logger.info(f"[cleanup] SRE Note: Handled by DB partitioning.")
        return 0

    # ── Historical Data Fetch ─────────────────────────────────────────────────
    def fetch_and_store_summaries(self, date_from: str, date_to: str, airports: Optional[str] = None) -> Dict[str, int]:
        """
        Fetches historical flight summaries from FR24's /api/flight-summary/light endpoint.
        Stores the results into fact_flight_session, resolving all dimensions.
        date_from/date_to formats: '2026-02-01T00:00:00Z' (ISO 8601)
        airports: optional comma-separated list of airport codes (e.g., 'OMDB,OKBK')
        """
        stats = {"fetched": 0, "inserted": 0, "skipped_duplicates": 0, "errors": 0}
        db = self._new_db()
        
        params = {
            "flight_datetime_from": date_from,
            "flight_datetime_to": date_to
        }
        if airports:
            params["airports"] = airports

        logger.info(f"[FR24 History] Fetching summaries from {date_from} to {date_to} for airports: {airports or 'ALL'}")
        
        try:
            data = self._safe_request("/api/flight-summary/light", params)
            if not data or "data" not in data:
                return stats

            flights = data["data"]
            stats["fetched"] = len(flights)

            # Caches to prevent repeated DB lookups
            geo_cache, operator_cache, aircraft_cache = {}, {}, {}

            for f in flights:
                fr24_id = f.get("fr24_id")
                if not fr24_id:
                    continue

                # Check for duplicates (Idempotency)
                exists = db.query(models.FactFlightSession.session_id).filter(
                    models.FactFlightSession.fr24_id == fr24_id
                ).first()
                
                if exists:
                    stats["skipped_duplicates"] += 1
                    continue

                try:
                    dep_icao = f.get("orig_icao")
                    arr_icao = f.get("dest_icao")
                    op_icao = f.get("operating_as")
                    hex_code = str(f.get("hex")).lower()[:6] if f.get("hex") else None

                    # Resolve Dimension: Geography (Airports)
                    for icao in [dep_icao, arr_icao]:
                        if icao and icao not in geo_cache:
                            EnterpriseDataRouter._ensure_geo(db, geo_cache, icao)
                    
                    # Resolve Dimension: Operator (Airline)
                    if op_icao and op_icao not in operator_cache:
                        EnterpriseDataRouter._ensure_operator(db, operator_cache, op_icao)

                    # Resolve Dimension: Aircraft
                    aircraft_id = None
                    if hex_code:
                        if hex_code not in aircraft_cache:
                            aircraft = db.query(models.DimAircraft).filter(models.DimAircraft.icao24 == hex_code).first()
                            if not aircraft:
                                aircraft = models.DimAircraft(
                                    icao24=hex_code,
                                    registration=f.get("reg"),
                                    type_code=f.get("type"),
                                    operator_id=operator_cache.get(op_icao)
                                )
                                db.add(aircraft)
                                db.flush()
                            aircraft_cache[hex_code] = aircraft.id
                        aircraft_id = aircraft_cache[hex_code]

                    # Safe date parsing
                    def parse_dt(dt_str):
                        if not dt_str: 
                            return None
                        try:
                            return datetime.fromisoformat(dt_str).replace(tzinfo=timezone.utc)
                        except (ValueError, TypeError):
                            return None

                    # Create and insert the Flight Session
                    session = models.FactFlightSession(
                        fr24_id=fr24_id,
                        flight_number=f.get("flight"),
                        callsign=f.get("callsign"),
                        aircraft_id=aircraft_id,
                        operator_id=operator_cache.get(op_icao),
                        dep_airport_id=geo_cache.get(dep_icao) if dep_icao else None,
                        arr_airport_id=geo_cache.get(arr_icao) if arr_icao else None,
                        first_seen_ts=parse_dt(f.get("first_seen")) or datetime.now(timezone.utc),
                        last_seen_ts=parse_dt(f.get("last_seen")) or datetime.now(timezone.utc),
                        actual_takeoff_ts=parse_dt(f.get("datetime_takeoff")),
                        actual_landing_ts=parse_dt(f.get("datetime_landed")),
                        flight_status="historical" if f.get("flight_ended") else "active"
                    )
                    db.add(session)
                    stats["inserted"] += 1

                except Exception as e:
                    logger.error(f"[FR24 History] Error processing flight {fr24_id}: {e}")
                    stats["errors"] += 1
                    db.rollback()
                    continue

            db.commit()
            logger.info(f"[FR24 History] Done. Stats: {stats}")
            return stats

        except Exception as e:
            logger.error(f"[FR24 History] Critical Error: {e}", exc_info=True)
            db.rollback()
            return stats
        finally:
            db.close()