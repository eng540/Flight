"""Flight data ingestion service."""
import logging
import sys
import os
import time
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

from worker.opensky_client import OpenSkyClient
from worker.data_processor import FlightDataProcessor

logger = logging.getLogger(__name__)

MAX_CHUNK_SECONDS = 7200  # OpenSky /flights/area max window


class FlightIngestionService:
    """Ingests flight data from OpenSky into PostgreSQL."""

    def __init__(self):
        self.client = OpenSkyClient()
        self.processor = FlightDataProcessor()
        self._db = None

    def __enter__(self):
        from app.database import SessionLocal
        self._db = SessionLocal()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._db:
            self._db.close()
            self._db = None

    def _db_session(self):
        """Get a new session for code paths that don't use context manager."""
        from app.database import SessionLocal
        return SessionLocal()

    # ── Global ingestion (no geo filter) ─────────────────────────────────────

    def ingest_recent_flights(self, hours: int = 2) -> Dict[str, int]:
        logger.info(f"[global] Ingesting last {hours} hours")
        raw = self.client.get_recent_flights(hours)
        if not raw:
            logger.info("[global] No flights returned by OpenSky")
            return {"created": 0, "updated": 0, "skipped": 0}
        processed = self.processor.process_flights(raw)
        unique = self.processor.remove_duplicates(processed)
        logger.info(f"[global] Processing {len(unique)} unique flights")
        return self._ingest_dicts(unique)

    def ingest_flights_by_time_range(self, begin: int, end: int) -> Dict[str, int]:
        logger.info(f"[global] Time range {begin}–{end}")
        raw = self.client.get_all_flights(begin, end)
        if not raw:
            return {"created": 0, "updated": 0, "skipped": 0}
        processed = self.processor.process_flights(raw)
        unique = self.processor.remove_duplicates(processed)
        return self._ingest_dicts(unique)

    # ── Geo-filtered recent ingestion ─────────────────────────────────────────

    def ingest_recent_for_regions(
        self, regions, lookback_hours: int = 2
    ) -> Dict[str, int]:
        from app.config import settings as cfg
        now = int(time.time())
        begin = now - lookback_hours * 3600
        total = {"created": 0, "updated": 0}
        db = self._db_session()
        try:
            for region in regions:
                raw = self.client.get_flights_by_bounding_box(
                    begin=begin, end=now,
                    lamin=region.lamin, lomin=region.lomin,
                    lamax=region.lamax, lomax=region.lomax,
                )
                if raw:
                    r = self._ingest_raw(db, raw, region.key)
                    total["created"] += r.get("created", 0)
                    total["updated"] += r.get("updated", 0)
                time.sleep(cfg.INGESTION_DELAY_SECONDS)
        finally:
            db.close()
        return total

    # ── Historical geo ingestion (chunked, idempotent) ────────────────────────

    def ingest_date_range_for_region(
        self, begin_ts: int, end_ts: int, region,
        force_reingest: bool = False,
    ) -> Dict[str, Any]:
        from app.config import settings as cfg
        from app.crud import IngestionJobCRUD

        total = {"jobs_processed": 0, "jobs_skipped": 0,
                 "flights_created": 0, "flights_updated": 0}

        db = self._db_session()
        try:
            day = datetime.utcfromtimestamp(begin_ts).replace(
                hour=0, minute=0, second=0, microsecond=0)
            end_dt = datetime.utcfromtimestamp(end_ts)

            while day <= end_dt:
                date_str = day.strftime("%Y-%m-%d")
                day_begin = int(day.timestamp())
                day_end = min(int((day + timedelta(days=1)).timestamp()) - 1, end_ts)

                if not force_reingest and IngestionJobCRUD.is_completed(db, date_str, region.key):
                    logger.info(f"[{region.key}] {date_str} already done – skipping")
                    total["jobs_skipped"] += 1
                    day += timedelta(days=1)
                    continue

                chunks_total = max(
                    1, (day_end - day_begin + MAX_CHUNK_SECONDS - 1) // MAX_CHUNK_SECONDS)

                existing = IngestionJobCRUD.get_by_date_region(db, date_str, region.key)
                if existing:
                    IngestionJobCRUD.update_status(
                        db, existing.id, "pending", flights_ingested=0, chunks_done=0)
                    job = existing
                else:
                    job = IngestionJobCRUD.create(
                        db, date_str, region.key,
                        region.lamin, region.lomin, region.lamax, region.lomax,
                        day_begin, day_end, chunks_total)

                IngestionJobCRUD.update_status(db, job.id, "running")
                logger.info(f"[{region.key}] {date_str}: {chunks_total} chunks")

                created = updated = chunks_done = 0
                cursor = day_begin
                try:
                    while cursor < day_end:
                        chunk_end = min(cursor + MAX_CHUNK_SECONDS, day_end)
                        raw = self.client.get_flights_by_bounding_box(
                            begin=cursor, end=chunk_end,
                            lamin=region.lamin, lomin=region.lomin,
                            lamax=region.lamax, lomax=region.lomax,
                        )
                        if raw:
                            r = self._ingest_raw(db, raw, region.key)
                            created += r.get("created", 0)
                            updated += r.get("updated", 0)
                        chunks_done += 1
                        IngestionJobCRUD.update_status(
                            db, job.id, "running",
                            flights_ingested=created + updated,
                            chunks_done=chunks_done)
                        cursor = chunk_end + 1
                        if cursor < day_end:
                            time.sleep(cfg.INGESTION_DELAY_SECONDS)

                    IngestionJobCRUD.update_status(
                        db, job.id, "completed",
                        flights_ingested=created + updated, chunks_done=chunks_done)
                    total["jobs_processed"] += 1
                    total["flights_created"] += created
                    total["flights_updated"] += updated

                except Exception as e:
                    logger.error(f"[{region.key}] {date_str} failed: {e}")
                    IngestionJobCRUD.update_status(db, job.id, "failed", error_message=str(e))

                day += timedelta(days=1)
        finally:
            db.close()

        return total

    # ── Cleanup ───────────────────────────────────────────────────────────────

    def cleanup_old_data(self, days: int) -> int:
        from app.crud import FlightCRUD
        if days <= 0:
            return 0
        return FlightCRUD.delete_old_flights(self._db, days=days)

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _ingest_raw(self, db, raw: List[Dict], region_key: str) -> Dict[str, int]:
        """Parse raw /flights/area records and upsert to DB."""
        from app.schemas import FlightCreate
        from app.crud import FlightCRUD

        processed = self.processor.process_flights(raw)
        unique = self.processor.remove_duplicates(processed)
        schemas_list = []
        for fd in unique:
            fd["region_key"] = region_key
            try:
                schemas_list.append(FlightCreate(**fd))
            except Exception as e:
                logger.debug(f"Schema skip: {e}")
        if not schemas_list:
            return {"created": 0, "updated": 0}
        return FlightCRUD.bulk_create(db, schemas_list)

    def _ingest_dicts(self, flight_dicts: List[Dict]) -> Dict[str, int]:
        """Ingest pre-processed dicts using context-manager DB session."""
        from app.schemas import FlightCreate
        from app.crud import FlightCRUD

        schemas_list = []
        skipped = 0
        for fd in flight_dicts:
            try:
                schemas_list.append(FlightCreate(**fd))
            except Exception as e:
                logger.debug(f"Schema skip: {e}")
                skipped += 1

        if not schemas_list:
            return {"created": 0, "updated": 0, "skipped": skipped}

        result = FlightCRUD.bulk_create(self._db, schemas_list)
        result["skipped"] = skipped + result.get("skipped", 0)
        return result
