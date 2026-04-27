"""
Enterprise Ingestion Service (v4.1 - FR24 Official Schema Compliant)
Strictly adheres to the Flightradar24 OpenAPI 3.1 specification.
SRE Hardened: Circuit Breaker, Graceful Degradation, Smart Enrichment.
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

logger = logging.getLogger(__name__)

class FlightIngestionService:
    """
    Multi-stage ingestion pipeline for Flightradar24 API (Official Schema).
    Tier 1: Live Pulse (Map Data) - /live/flight-positions/full
    Tier 2: Deep Inspector (Enrichment) - /flight-summary/full (On-Demand)
    Tier 3: Time Machine (Historical) - /flight-tracks (On-Demand)
    """

    def __init__(self):
        self._db = None
        # SRE: Base URL must NOT end with /api because endpoints start with /api
        self.fr24_base_url = "https://fr24api.flightradar24.com"
        self.fr24_api_key = os.getenv("FR24_API_KEY")
        
        # SRE Circuit Breaker
        self.consecutive_failures = 0
        self.max_failures_before_pause = 3
        self.pause_until = 0

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

    # ── SRE Utilities ────────────────────────────────────────────────
    def _is_circuit_open(self) -> bool:
        if time.time() < self.pause_until:
            return True
        if self.consecutive_failures >= self.max_failures_before_pause:
            logger.error("[FR24 Circuit Breaker] API repeatedly failed. Pausing for 5 minutes.")
            self.pause_until = time.time() + 300
            self.consecutive_failures = 0
            return True
        return False

    def _record_success(self):
        self.consecutive_failures = 0
        self.pause_until = 0

    def _record_failure(self):
        self.consecutive_failures += 1

    def _safe_request(self, endpoint: str, params: dict) -> Optional[dict]:
        """SRE: Centralized fault-tolerant HTTP requester."""
        if not self.fr24_api_key:
            logger.error("[FR24 Auth] API key missing! Set FR24_API_KEY.")
            return None

        if self._is_circuit_open():
            logger.warning("[FR24] Request blocked by Circuit Breaker.")
            return None

        headers = {
            "Accept": "application/json",
            "Accept-Version": "v1",  # Mandatory per OpenAPI
            "Authorization": f"Bearer {self.fr24_api_key}"
        }

        url = f"{self.fr24_base_url}{endpoint}"  # endpoint must start with /api/...

        try:
            response = requests.get(url, headers=headers, params=params, timeout=15)
            
            if response.status_code == 200:
                self._record_success()
                return response.json()
            elif response.status_code == 429:
                logger.warning(f"[FR24 Rate Limit] 429 on {endpoint}")
                self._record_failure()
                time.sleep(10)
                return None
            elif response.status_code in [401, 403]:
                logger.error(f"[FR24 Auth] Fatal auth error ({response.status_code}).")
                self.pause_until = time.time() + 3600
                return None
            else:
                logger.error(f"[FR24 Error] HTTP {response.status_code}: {response.text}")
                self._record_failure()
                return None
        except requests.exceptions.Timeout:
            logger.error(f"[FR24 Timeout] {endpoint}")
            self._record_failure()
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"[FR24 Network] {e}")
            self._record_failure()
            return None

    # ── Tier 1: Live Pulse (Map + Positions) ─────────────────────────
    def ingest_live_radar_from_fr24(self, regions: List[Any]) -> Dict[str, int]:
        """
        Tier 1 (Pulse): Fetches /api/live/flight-positions/full for given regions.
        Returns statistics about new aircrafts, sessions, tracks.
        """
        total_stats = {"new_aircrafts": 0, "new_sessions": 0, "tracks_recorded": 0, "errors": 0, "rejected": 0}
        db = self._new_db()

        try:
            for region in regions:
                logger.info(f"[FR24 Live] Fetching region: {region.key}")
                
                # Official sort order: north, south, west, east
                bounds = f"{region.lamax},{region.lamin},{region.lomin},{region.lomax}"
                params = {
                    "bounds": bounds,
                    "limit": 500  # Control cost; max 30000
                }
                
                data = self._safe_request("/api/live/flight-positions/full", params)
                if not data:
                    continue

                flights = data.get("data", [])
                if not flights:
                    logger.info(f"[{region.key}] 0 flights returned.")
                    continue

                payloads = []
                for f in flights:
                    hex_code = f.get("hex")
                    if not hex_code:
                        continue

                    # Convert ISO timestamp to unix if needed
                    ts_str = f.get("timestamp")
                    ts_unix = int(datetime.fromisoformat(ts_str.replace('Z', '+00:00')).timestamp()) if ts_str else int(time.time())

                    # Handle missing 'on_ground' – calculate from physics
                    alt_ft = f.get("alt", 0)
                    gspeed_kts = f.get("gspeed", 0)
                    on_ground = (alt_ft is not None and alt_ft < 100) and (gspeed_kts is not None and gspeed_kts < 30)

                    payloads.append(RawIngestionPayload(
                        icao24=str(hex_code).lower()[:6],
                        callsign=f.get("callsign"),
                        registration=f.get("reg"),               # Official: "reg"
                        operator_icao=f.get("operating_as"),     # Use operating_as
                        origin_country=None,                     # FR24 doesn't provide directly
                        timestamp=ts_unix,
                        longitude=float(f.get("lon", 0)),
                        latitude=float(f.get("lat", 0)),
                        altitude=float(alt_ft) * 0.3048 if alt_ft else 0.0,  # feet -> meters
                        velocity=float(gspeed_kts) * 1.852 if gspeed_kts else 0.0,  # knots -> km/h
                        heading=float(f.get("track", 0)),        # Official: "track"
                        on_ground=on_ground,
                        est_departure_airport=f.get("orig_iata"),   # Official: orig_iata
                        est_arrival_airport=f.get("dest_iata"),     # Official: dest_iata
                        squawk=f.get("squawk"),
                        region_key=region.key
                    ))

                # Process batch through your Enterprise Data Router
                batch_stats = EnterpriseDataRouter.process_telemetry_batch(db, payloads)
                for k in total_stats:
                    total_stats[k] += batch_stats.get(k, 0)
                
                logger.info(f"[{region.key}] Processed: {batch_stats}")

        except Exception as e:
            logger.error(f"[FR24 Live] Critical: {e}", exc_info=True)
        finally:
            db.close()
        
        return total_stats

    # ── Tier 2: Deep Inspector (On-Demand) ───────────────────────────
    def enrich_flight_details(self, flight_id: str) -> Optional[Dict]:
        """
        Tier 2 (Inspector): Fetches /api/flight-summary/full for a specific flight.
        Can be called on-demand from UI or when a new session is created.
        """
        if not flight_id:
            return None
        
        params = {"flight_ids": flight_id}
        data = self._safe_request("/api/flight-summary/full", params)
        if not data or not data.get("data"):
            return None
        
        summary = data["data"][0]
        logger.info(f"Enriched {flight_id}: {summary.get('callsign')} {summary.get('orig_icao')}->{summary.get('dest_icao')}")
        return summary

    # ── Tier 3: Time Machine (Historical Tracks) ────────────────────
    def fetch_historical_track(self, flight_id: str) -> Optional[Dict]:
        """
        Tier 3 (Time Machine): Fetches /api/flight-tracks for a specific flight.
        Returns full track array.
        """
        if not flight_id:
            return None
        
        params = {"flight_id": flight_id}
        data = self._safe_request("/api/flight-tracks", params)
        if not data:
            return None
        
        logger.info(f"Fetched historical track for {flight_id}: {len(data.get('tracks', []))} points")
        return data