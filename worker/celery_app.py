"""Celery application – supports rediss:// (Upstash/TLS) and redis://."""
import os
import ssl
import logging

from celery import Celery
from celery.signals import task_failure, task_success

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")


def _ssl_options() -> dict:
    """SSL options for rediss:// (Upstash uses self-signed certs)."""
    return {
        "ssl_cert_reqs": ssl.CERT_NONE,
        "ssl_ca_certs": None,
        "ssl_certfile": None,
        "ssl_keyfile": None,
    }


celery_app = Celery(
    "flight_intelligence",
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=["worker.tasks"],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_ignore_result=False,
    result_expires=3600,
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=1000,
    # Suppress Celery 6.0 deprecation warning
    broker_connection_retry_on_startup=True,
    beat_schedule_filename="/tmp/celerybeat-schedule",

    beat_schedule={
        "ingest-geo-every-30-minutes": {
            "task": "worker.tasks.ingest_recent_geo_task",
            "schedule": 1800.0,
        },
        "ingest-global-every-5-minutes": {
            "task": "worker.tasks.ingest_flights_task",
            "schedule": 300.0,
            "args": (2,),
        },
        "cleanup-old-data-daily": {
            "task": "worker.tasks.cleanup_old_data_task",
            "schedule": 86400.0,
            "args": (0,),
        },
    },

    task_routes={
        "worker.tasks.ingest_flights_task":       {"queue": "ingestion"},
        "worker.tasks.ingest_recent_geo_task":    {"queue": "ingestion"},
        "worker.tasks.ingest_historical_flights": {"queue": "ingestion"},
        "worker.tasks.cleanup_old_data_task":     {"queue": "maintenance"},
    },
)

# ── SSL for rediss:// (Upstash / Railway Redis TLS) ──────────────────────────
if REDIS_URL.startswith("rediss://"):
    logger.info("rediss:// detected – configuring SSL for Celery broker/backend")
    ssl_opts = _ssl_options()
    celery_app.conf.broker_use_ssl = ssl_opts
    celery_app.conf.redis_backend_use_ssl = ssl_opts
    celery_app.conf.broker_transport_options = {
        "visibility_timeout": 3600,
        "socket_timeout": 30,
        "socket_connect_timeout": 30,
    }


@task_success.connect
def on_success(sender=None, result=None, **kwargs):
    logger.info(f"Task {sender.name} OK: {result}")


@task_failure.connect
def on_failure(sender=None, exception=None, **kwargs):
    logger.error(f"Task {sender.name} FAILED: {exception}")


@celery_app.task(bind=True)
def health_check_task(self):
    return {"status": "healthy", "worker": self.request.hostname}


if __name__ == "__main__":
    celery_app.start()
