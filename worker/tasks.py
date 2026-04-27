"""Celery tasks – updated for FR24 Ingestion Service (v5.0).

All task definitions must match beat_schedule entries exactly.
Active: ingest_recent_geo_task (powered by FR24).
Other tasks are safely stubbed to prevent Celery errors.
"""
from celery import shared_task
from celery.exceptions import MaxRetriesExceededError, SoftTimeLimitExceeded
import logging
import sys
import os
from typing import List, Optional

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
    """SRE Production: FR24 Global Aggregation"""
    try:
        active_keys = region_keys or settings.get_active_region_keys()
        regions = [r for r in (settings.get_region(k) for k in active_keys) if r]
        
        if not regions:
            return {"status": "skipped", "reason": "no regions"}

        svc = FlightIngestionService()
        logger.info(f"[FR24 Task] Starting live sweep for {[r.key for r in regions]}")
        
        final_result = svc.ingest_live_radar_from_fr24(regions)

        logger.info(f"[FR24 Task] Sweep Complete: {final_result}")
        return {"status": "success", "result": final_result}
        
    except SoftTimeLimitExceeded:
        return {"status": "timeout"}
    except Exception as exc:
        logger.error(f"[FR24 Task] Failed: {exc}", exc_info=True)
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
    """LEGACY STUB: Global flight ingestion (no geo filter)."""
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
    """LEGACY STUB: Historical flight ingestion."""
    logger.warning("[historical] This task is currently disabled (FR24 migration).")
    return {"status": "skipped", "reason": "disabled_during_fr24_migration"}


@shared_task(
    bind=True, max_retries=3, default_retry_delay=60,
    name="worker.tasks.cleanup_old_data_task",
    queue="maintenance",
)
def cleanup_old_data_task(self, days: int = 0):
    """LEGACY STUB: Cleanup old flights."""
    logger.warning("[cleanup] This task is currently disabled (FR24 migration).")
    return {"status": "skipped", "reason": "disabled_during_fr24_migration"}


@shared_task(
    bind=True,
    name="worker.tasks.run_realtime_radar_task",
    queue="ingestion",
)
def run_realtime_radar_task(self):
    """LEGACY STUB: Previously used for AirLabs realtime radar."""
    logger.info("[realtime] Legacy realtime task called – no action (FR24 now active).")
    return {"status": "skipped", "reason": "replaced_by_fr24"}


@shared_task(
    bind=True,
    name="worker.tasks.ingest_aviationstack_task",
    queue="ingestion",
)
def ingest_aviationstack_task(self):
    """LEGACY STUB: AviationStack has been completely deprecated."""
    logger.warning("[AviationStack] DEPRECATED. FR24 is now the only live source.")
    return {"status": "skipped", "reason": "deprecated"}