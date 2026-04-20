"""Flight data ingestion service with circuit breaker awareness."""
import logging
import sys
import os
import time
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

from worker.opensky_client import OpenSkyClient

logger = logging.getLogger(__name__)

MAX_CHUNK_SECONDS   = 7200   # OpenSky /flights/area max window
MAX_SKIP_BEFORE_ABORT = 5    # abort day job if N consecutive chunks all return 0 data
                              # (indicates circuit open / IP blocked)


class FlightIngestionService:

    def __init__(self):
        self.client = OpenSkyClient()
        self.processor = None   # lazy import to avoid circular issues
        self._db = None

    def _get_processor(self):
        if self.processor is None:
            from worker.data_processor import FlightDataProcessor
            self.processor = FlightDataProcessor()
        return self.processor

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

    # ── Global ingestion ──────────────────────────────────────────────────────

    def ingest_recent_flights(self, hours: int = 2) -> Dict[str, int]:
        if self.client.circuit_is_open:
            logger.warning("[global] Circuit open – skipping ingestion")
            return {"created": 0, "updated": 0, "skipped": 0,
                    "error": "circuit_open"}
        logger.info(f"[global] Ingesting last {hours} hours")
        raw = self.client.get_recent_flights(hours)
        if not raw:
            return {"created": 0, "updated": 0, "skipped": 0}
        proc = self._get_processor()
        processed = proc.process_flights(raw)
        unique = proc.remove_duplicates(processed)
        logger.info(f"[global] {len(unique)} unique flights to ingest")
        return self._ingest_dicts(unique)

    # ── Geo recent ────────────────────────────────────────────────────────────

    def ingest_recent_for_regions(self, regions, lookback_hours: int = 2) -> Dict[str, int]:
        if self.client.circuit_is_open:
            logger.warning("[geo-recent] Circuit open – skipping")
            return {"created": 0, "updated": 0, "error": "circuit_open"}

        from app.config import settings as cfg
        now = int(time.time())
        begin = now - lookback_hours * 3600
        total = {"created": 0, "updated": 0}
        db = self._new_db()
        try:
            for region in regions:
                if self.client.circuit_is_open:
                    logger.warning(f"[{region.key}] Circuit opened mid-run – stopping")
                    break
                raw = self.client.get_flights_by_bounding_box(
                    begin=begin, end=now,
                    lamin=region.lamin, lomin=region.lomin,
                    lamax=region.lamax, lomax=region.lomax)
                if raw:
                    r = self._ingest_raw(db, raw, region.key)
                    total["created"] += r.get("created", 0)
                    total["updated"] += r.get("updated", 0)
                time.sleep(cfg.INGESTION_DELAY_SECONDS)
        finally:
            db.close()
        return total

    # ── Historical geo (chunked, idempotent) ──────────────────────────────────

    def ingest_date_range_for_region(
        self, begin_ts: int, end_ts: int, region,
        force_reingest: bool = False,
    ) -> Dict[str, Any]:
        from app.config import settings as cfg
        from app.crud import IngestionJobCRUD

        total = {"jobs_processed": 0, "jobs_skipped": 0,
                 "flights_created": 0, "flights_updated": 0}

        db = self._new_db()
        try:
            day = datetime.utcfromtimestamp(begin_ts).replace(
                hour=0, minute=0, second=0, microsecond=0)
            end_dt = datetime.utcfromtimestamp(end_ts)

            while day <= end_dt:
                # Check circuit before each day
                if self.client.circuit_is_open:
                    logger.warning(
                        f"[{region.key}] Circuit breaker OPEN – "
                        f"OpenSky unreachable. Aborting remaining days. "
                        f"Will retry in {300//60} min.")
                    break

                date_str  = day.strftime("%Y-%m-%d")
                day_begin = int(day.timestamp())
                day_end   = min(int((day + timedelta(days=1)).timestamp()) - 1, end_ts)

                if not force_reingest and IngestionJobCRUD.is_completed(
                        db, date_str, region.key):
                    logger.info(f"[{region.key}] {date_str} already done – skip")
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
                empty_streak = 0   # consecutive chunks returning 0 flights
                cursor = day_begin

                try:
                    while cursor < day_end:
                        # Circuit breaker: stop mid-day if network is blocked
                        if self.client.circuit_is_open:
                            logger.warning(
                                f"[{region.key}] {date_str} chunk {chunks_done+1}: "
                                f"circuit open, aborting day")
                            raise RuntimeError("OpenSky API unreachable – circuit open")

                        chunk_end = min(cursor + MAX_CHUNK_SECONDS, day_end)
                        raw = self.client.get_flights_by_bounding_box(
                            begin=cursor, end=chunk_end,
                            lamin=region.lamin, lomin=region.lomin,
                            lamax=region.lamax, lomax=region.lomax)

                        if raw:
                            r = self._ingest_raw(db, raw, region.key)
                            created += r.get("created", 0)
                            updated += r.get("updated", 0)
                            empty_streak = 0
                        else:
                            empty_streak += 1
                            # If many consecutive chunks return nothing AND
                            # circuit is approaching open, abort early
                            if (empty_streak >= MAX_SKIP_BEFORE_ABORT
                                    and self.client.consecutive_failures >= 3):
                                logger.warning(
                                    f"[{region.key}] {date_str}: "
                                    f"{empty_streak} empty chunks + "
                                    f"{self.client.consecutive_failures} failures – "
                                    f"OpenSky likely blocking. Aborting day.")
                                raise RuntimeError(
                                    f"OpenSky returned no data for {empty_streak} "
                                    f"consecutive chunks – likely IP blocked")

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
                        flights_ingested=created + updated,
                        chunks_done=chunks_done)
                    total["jobs_processed"] += 1
                    total["flights_created"] += created
                    total["flights_updated"] += updated
                    logger.info(
                        f"[{region.key}] {date_str} done: "
                        f"+{created} created, +{updated} updated")

                except RuntimeError as e:
                    logger.error(f"[{region.key}] {date_str} aborted: {e}")
                    IngestionJobCRUD.update_status(
                        db, job.id, "failed", error_message=str(e))
                    # Don't continue other days if network is broken
                    if "circuit" in str(e).lower() or "blocked" in str(e).lower():
                        break

                except Exception as e:
                    logger.error(f"[{region.key}] {date_str} error: {e}")
                    IngestionJobCRUD.update_status(
                        db, job.id, "failed", error_message=str(e))

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

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _ingest_raw(self, db, raw: List[Dict], region_key: str) -> Dict[str, int]:
        from app.schemas import FlightCreate
        from app.crud import FlightCRUD
        proc = self._get_processor()
        processed = proc.process_flights(raw)
        unique    = proc.remove_duplicates(processed)
        schemas   = []
        for fd in unique:
            fd["region_key"] = region_key
            try:
                schemas.append(FlightCreate(**fd))
            except Exception as e:
                logger.debug(f"Schema skip: {e}")
        return FlightCRUD.bulk_create(db, schemas) if schemas else {"created": 0, "updated": 0}

    def _ingest_dicts(self, dicts: List[Dict]) -> Dict[str, int]:
        from app.schemas import FlightCreate
        from app.crud import FlightCRUD
        schemas = []
        skipped = 0
        for fd in dicts:
            try:
                schemas.append(FlightCreate(**fd))
            except Exception:
                skipped += 1
        if not schemas:
            return {"created": 0, "updated": 0, "skipped": skipped}
        r = FlightCRUD.bulk_create(self._db, schemas)
        r["skipped"] = skipped + r.get("skipped", 0)
        return r
