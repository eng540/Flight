"""
Enterprise Ingestion Service (v3.0)
Fetches data from AirLabs and sends it to the EnterpriseDataRouter.
"""
import logging
import sys
import os
import time
from typing import List, Dict, Any, Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

from app.schemas import RawIngestionPayload
from app.crud import EnterpriseDataRouter

logger = logging.getLogger(__name__)

class FlightIngestionService:

    def __init__(self):
        self._db = None

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

    # ── AirLabs (Real-time Tracker & Router) ──────────────────────────────────

    def ingest_from_airlabs(self, regions) -> Dict[str, int]:
        """
        SRE Master Integration: Fetches real-time flights from AirLabs API
        and routes them through the new Enterprise Schema.
        """
        import requests
        
        api_key = os.getenv("AIRLABS_API_KEY")
        if not api_key:
            logger.error("[AirLabs] API key missing! Set AIRLABS_API_KEY in Railway.")
            return {"new_aircrafts": 0, "new_sessions": 0, "tracks_recorded": 0, "errors": 0}

        total_stats = {"new_aircrafts": 0, "new_sessions": 0, "tracks_recorded": 0, "errors": 0}
        db = self._new_db()
        now_ts = int(time.time())

        try:
            for region in regions:
                logger.info(f"[AirLabs] Fetching live flights for region: {region.key}...")
                
                # AirLabs bbox format: minLat,minLng,maxLat,maxLng
                bbox = f"{region.lamin},{region.lomin},{region.lamax},{region.lomax}"
                url = f"https://airlabs.co/api/v9/flights?api_key={api_key}&bbox={bbox}"
                
                response = requests.get(url, timeout=20)
                if response.status_code != 200:
                    logger.error(f"[AirLabs] API Error for {region.key}: {response.status_code}")
                    continue

                data = response.json()
                flights = data.get("response", [])
                
                if not flights:
                    logger.info(f"[{region.key}] AirLabs returned 0 flights.")
                    continue

                payloads = []
                for f in flights:
                    icao24 = f.get("hex")
                    if not icao24:
                        continue
                        
                    callsign = f.get("flight_iata") or f.get("flight_icao") or f.get("reg_number")

                    # Map to the new RawIngestionPayload Schema
                    payloads.append(RawIngestionPayload(
                        icao24=str(icao24).lower()[:6],
                        callsign=callsign,
                        registration=f.get("reg_number"),
                        origin_country=f.get("flag"),
                        timestamp=now_ts,
                        longitude=float(f.get("lng", 0)),
                        latitude=float(f.get("lat", 0)),
                        altitude=float(f.get("alt", 0)) * 0.3048 if f.get("alt") else 0.0,
                        velocity=float(f.get("speed", 0)) * 1.852 if f.get("speed") else 0.0,
                        heading=float(f.get("dir", 0)) if f.get("dir") else 0.0,
                        on_ground=f.get("alt") == 0,
                        est_departure_airport=f.get("dep_icao"),
                        est_arrival_airport=f.get("arr_icao"),
                        region_key=region.key
                    ))

                # Route through the Enterprise DB Router
                batch_stats = EnterpriseDataRouter.process_telemetry_batch(db, payloads)
                
                # Accumulate stats
                for k in total_stats:
                    total_stats[k] += batch_stats.get(k, 0)
                
                logger.info(f"[{region.key}] AirLabs success: {batch_stats}")
                time.sleep(1.5)
                
        except Exception as e:
            logger.error(f"[AirLabs] Critical Exception: {e}")
        finally:
            db.close()
            
        return total_stats

    def cleanup_old_data(self, days: int) -> int:
        """Stub for maintenance task"""
        # TODO: Implement Time-Series partition dropping here in the future
        logger.info(f"[cleanup] SRE Note: TimescaleDB retention policies should handle this natively.")
        return 0