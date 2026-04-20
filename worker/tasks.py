"""Celery tasks – all task definitions must match beat_schedule entries exactly."""
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
# Global ingestion (no geo filter – legacy, runs every 5 min)
# ─────────────────────────────────────────────────────────────────────────────

@shared_task(
    bind=True, max_retries=3, default_retry_delay=60,
    soft_time_limit=300, time_limit=600,
    name="worker.tasks.ingest_flights_task",
    queue="ingestion",
)
def ingest_flights_task(self, hours: int = 2):
    """Ingest recent global flights (no geo filter)."""
    try:
        logger.info(f"[global] Starting ingestion for last {hours} hours")
        with FlightIngestionService() as svc:
            stats = svc.ingest_recent_flights(hours)
        logger.info(f"[global] Done: {stats}")
        return {"status": "success", "stats": stats}
    except SoftTimeLimitExceeded:
        logger.warning("[global] Task timed out")
        return {"status": "timeout"}
    except Exception as exc:
        logger.error(f"[global] Failed: {exc}", exc_info=True)
        try:
            self.retry(exc=exc)
        except MaxRetriesExceededError:
            return {"status": "failed", "error": str(exc)}


# ─────────────────────────────────────────────────────────────────────────────
# Geo-filtered periodic ingestion (runs every 30 min)
# ─────────────────────────────────────────────────────────────────────────────

@shared_task(
    bind=True, max_retries=3, default_retry_delay=60,
    soft_time_limit=600, time_limit=900,
    name="worker.tasks.ingest_recent_geo_task",
    queue="ingestion",
)
def ingest_recent_geo_task(self, region_keys: Optional[List[str]] = None,
                            lookback_hours: int = 2):
    """Ingest recent flights for configured geographic regions."""
    try:
        active_keys = region_keys or settings.get_active_region_keys()
        regions = [r for r in (settings.get_region(k) for k in active_keys) if r]
        if not regions:
            logger.warning("[geo] No valid regions configured")
            return {"status": "skipped", "reason": "no regions"}

        logger.info(f"[geo] Ingesting {[r.key for r in regions]}")
        svc = FlightIngestionService()
        result = svc.ingest_recent_for_regions(regions, lookback_hours)
        logger.info(f"[geo] Done: {result}")
        return {"status": "success", "result": result}
    except SoftTimeLimitExceeded:
        return {"status": "timeout"}
    except Exception as exc:
        logger.error(f"[geo] Failed: {exc}", exc_info=True)
        try:
            self.retry(exc=exc)
        except MaxRetriesExceededError:
            return {"status": "failed", "error": str(exc)}


# ─────────────────────────────────────────────────────────────────────────────
# Historical ingestion (one-off, chunked, idempotent)
# ─────────────────────────────────────────────────────────────────────────────

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
    Ingest historical flights for a date range and list of regions.
    Each (date × region) is tracked as an IngestionJob – skipped if completed.
    """
    from datetime import datetime, timedelta

    logger.info(f"[historical] {begin_date}→{end_date} regions={region_keys} force={force_reingest}")

    try:
        begin_ts = int(datetime.strptime(begin_date, "%Y-%m-%d").timestamp())
        end_ts   = int((datetime.strptime(end_date, "%Y-%m-%d")
                        + timedelta(days=1)).timestamp()) - 1
    except ValueError as e:
        return {"status": "failed", "error": f"Invalid date: {e}"}

    regions = [r for r in (settings.get_region(k) for k in region_keys) if r]
    if not regions:
        return {"status": "failed", "error": "No valid regions"}

    totals = {"jobs_processed": 0, "jobs_skipped": 0,
              "flights_created": 0, "flights_updated": 0}
    svc = FlightIngestionService()
    try:
        for region in regions:
            result = svc.ingest_date_range_for_region(
                begin_ts=begin_ts, end_ts=end_ts,
                region=region, force_reingest=force_reingest)
            for k in totals:
                totals[k] += result.get(k, 0)
    except Exception as exc:
        logger.error(f"[historical] Failed: {exc}", exc_info=True)
        try:
            self.retry(exc=exc)
        except MaxRetriesExceededError:
            return {"status": "failed", "error": str(exc)}

    logger.info(f"[historical] Completed: {totals}")
    return {"status": "success", **totals}


# ─────────────────────────────────────────────────────────────────────────────
# Cleanup (runs daily)
# ─────────────────────────────────────────────────────────────────────────────

@shared_task(
    bind=True, max_retries=3, default_retry_delay=60,
    name="worker.tasks.cleanup_old_data_task",
    queue="maintenance",
)
def cleanup_old_data_task(self, days: int = 0):
    """Remove flights older than `days`. 0 = respect DATA_RETENTION_DAYS env (default keep all)."""
    retention = settings.DATA_RETENTION_DAYS if days == 0 else days
    if not retention or retention <= 0:
        logger.info("[cleanup] DATA_RETENTION_DAYS=0 – keeping all data")
        return {"status": "skipped", "deleted": 0}
    try:
        with FlightIngestionService() as svc:
            deleted = svc.cleanup_old_data(retention)
        logger.info(f"[cleanup] Deleted {deleted} flights older than {retention} days")
        return {"status": "success", "deleted": deleted}
    except Exception as exc:
        logger.error(f"[cleanup] Failed: {exc}")
        try:
            self.retry(exc=exc)
        except MaxRetriesExceededError:
            return {"status": "failed", "error": str(exc)}


# ─────────────────────────────────────────────────────────────────────────────
# Legacy stub – prevents "unregistered task" errors in beat logs
# ─────────────────────────────────────────────────────────────────────────────

@shared_task(
    bind=True,
    name="worker.tasks.run_realtime_radar_task",
    queue="ingestion",
)
def run_realtime_radar_task(self):
    """Legacy stub – kept to prevent beat 'unregistered task' errors."""
    logger.info("[realtime] Legacy task called – no action")
    return {"status": "skipped", "reason": "legacy task"}
