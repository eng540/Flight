"""Celery tasks – updated for FR24 Ingestion Service (v4.1).

All task definitions must match beat_schedule entries exactly.
Currently active task: ingest_recent_geo_task (now powered by FR24).
Other tasks are safely stubbed to prevent Celery errors.
"""
from celery import shared_task
from celery.exceptions import MaxRetriesExceededError, SoftTimeLimitExceeded
import logging
import sys
import os
import time
from typing import List, Optional

from sqlalchemy import create_engine, inspect

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

from worker.ingestion_service import FlightIngestionService
from app.config import settings

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# 1. LIVE RADAR – THE ACTIVE TASK (FR24 Tier 1)
# ─────────────────────────────────────────────────────────────────────────────

@shared_task(
    bind=True, max_retries=3, default_retry_delay=60,
    soft_time_limit=600, time_limit=900,
    name="worker.tasks.ingest_recent_geo_task",
    queue="ingestion",
)
def ingest_recent_geo_task(self, region_keys: Optional[List[str]] = None,
                            lookback_hours: int = 2):
    """
    SRE PRODUCTION: Ingest live flight positions from Flightradar24.
    Replaces the old AirLabs / AviationStack logic.
    """
    # ── SRE GUARD: Wait for database tables to be ready ──────────────────
    engine = create_engine(settings.DATABASE_URL)
    inspector = inspect(engine)
    for attempt in range(30):
        if 'dim_geography' in inspector.get_table_names():
            logger.info("Database tables are ready. Starting FR24 ingestion...")
            break
        logger.warning(f"Waiting for database tables... attempt {attempt+1}/30")
        time.sleep(2)
    else:
        logger.error("Database tables not ready after 60s. Aborting FR24 ingestion.")
        return {'status': 'error', 'message': 'Tables not ready'}
    # ──────────────────────────────────────────────────────────────────────

    try:
        active_keys = region_keys or settings.get_active_region_keys()
        regions = [r for r in (settings.get_region(k) for k in active_keys) if r]

        if not regions:
            logger.warning("[FR24] No valid regions configured")
            return {"status": "skipped", "reason": "no regions"}

        # Instantiate FR24-specific ingestion service
        svc = FlightIngestionService()
        logger.info(f"[FR24 Master] Running live ingestion for {[r.key for r in regions]}")

        # Call the Tier 1 live radar method (FR24)
        final_result = svc.ingest_live_radar_from_fr24(regions)

        logger.info(f"[FR24 Master] Ingestion completed: {final_result}")
        return {"status": "success", "result": final_result}

    except SoftTimeLimitExceeded:
        logger.warning("[FR24] Task timed out")
        return {"status": "timeout"}
    except Exception as exc:
        logger.error(f"[FR24 Master] Critical failure: {exc}", exc_info=True)
        try:
            self.retry(exc=exc)
        except MaxRetriesExceededError:
            return {"status": "failed", "error": str(exc)}


# ─────────────────────────────────────────────────────────────────────────────
# 2. STUBBED / LEGACY TASKS (safely disabled to avoid crashes)
# ─────────────────────────────────────────────────────────────────────────────

@shared_task(
    bind=True, max_retries=3, default_retry_delay=60,
    soft_time_limit=300, time_limit=600,
    name="worker.tasks.ingest_flights_task",
    queue="ingestion",
)
def ingest_flights_task(self, hours: int = 2):
    """
    LEGACY STUB: Global flight ingestion (no geo filter).
    Disabled because `ingest_recent_flights` does not exist in FR24 service yet.
    """
    logger.warning("[global] This task is currently disabled (FR24 migration).")
    return {"status": "skipped", "reason": "disabled_during_fr24_migration"}


@shared_task(
    bind=True, max_retries=2, default_retry_delay=120,
    soft_time_limit=3600, time_limit=7200,
    name="worker.tasks.ingest_historical_flights",
    queue="ingestion",
)
def ingest_historical_flights(self, begin_date: str, end_date: str,
                               region_keys: List[str],
                               force_reingest: bool = False):
    """
    LEGACY STUB: Historical flight ingestion.
    Disabled because `ingest_date_range_for_region` does not exist in FR24 service yet.
    """
    logger.warning("[historical] This task is currently disabled (FR24 migration).")
    return {"status": "skipped", "reason": "disabled_during_fr24_migration"}


@shared_task(
    bind=True, max_retries=3, default_retry_delay=60,
    name="worker.tasks.cleanup_old_data_task",
    queue="maintenance",
)
def cleanup_old_data_task(self, days: int = 0):
    """
    LEGACY STUB: Cleanup old flights.
    Disabled because `cleanup_old_data` does not exist in FR24 service yet.
    """
    logger.warning("[cleanup] This task is currently disabled (FR24 migration).")
    return {"status": "skipped", "reason": "disabled_during_fr24_migration"}


@shared_task(
    bind=True,
    name="worker.tasks.run_realtime_radar_task",
    queue="ingestion",
)
def run_realtime_radar_task(self):
    """
    LEGACY STUB: Previously used for AirLabs realtime radar.
    Now replaced by `ingest_recent_geo_task` which calls FR24.
    """
    logger.info("[realtime] Legacy realtime task called – no action (FR24 now active).")
    return {"status": "skipped", "reason": "replaced_by_fr24"}


@shared_task(
    bind=True,
    name="worker.tasks.ingest_aviationstack_task",
    queue="ingestion",
)
def ingest_aviationstack_task(self):
    """
    LEGACY STUB: AviationStack has been completely deprecated.
    """
    logger.warning("[AviationStack] DEPRECATED. FR24 is now the only live source.")
    return {"status": "skipped", "reason": "deprecated"}