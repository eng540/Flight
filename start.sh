#!/bin/bash
set -e

echo "🚀 Flight Intelligence – Starting up"

# ── Validate DATABASE_URL ─────────────────────────────────────────────────────
if [ -z "$DATABASE_URL" ]; then
    echo "❌ ERROR: DATABASE_URL is not set."
    echo "   In Railway: add DATABASE_URL = \${{Postgres.DATABASE_URL}}"
    exit 1
fi

# Convert postgres:// → postgresql:// (Railway / Heroku compatibility)
if [[ "$DATABASE_URL" == postgres://* ]]; then
    export DATABASE_URL="${DATABASE_URL/postgres:\/\//postgresql:\/\/}"
    echo "✅ Converted postgres:// → postgresql://"
fi
echo "✅ DATABASE_URL OK"

# ── REDIS_URL ─────────────────────────────────────────────────────────────────
if [ -z "$REDIS_URL" ]; then
    echo "⚠️  REDIS_URL not set – Celery will not run"
else
    echo "✅ REDIS_URL OK (${REDIS_URL:0:25}…)"
fi

# ── Run Alembic migrations ────────────────────────────────────────────────────
# SRE: Alembic migrations are DISABLED for factory reset.
# Database tables will be created automatically by FastAPI on startup.
echo "ℹ️  SRE: Database will be created by FastAPI (SQLAlchemy create_all)."
echo "   Alembic is temporarily bypassed for Enterprise Schema deployment."

# ── Start Celery worker & beat ────────────────────────────────────────────────
if [ -n "$REDIS_URL" ]; then
    echo "🔄 Starting Celery worker..."
    celery -A worker.celery_app worker \
        -l info \
        -Q ingestion,maintenance,default \
        --concurrency=2 \
        --without-gossip \
        --without-mingle &

    echo "🔄 Starting Celery beat..."
    celery -A worker.celery_app beat -l info &
else
    echo "⚠️  Celery skipped (no REDIS_URL)"
fi

# ── Start FastAPI ─────────────────────────────────────────────────────────────
echo "🚀 Starting FastAPI on port ${PORT:-8000}..."
cd /app/backend
exec uvicorn app.main:app \
    --host 0.0.0.0 \
    --port "${PORT:-8000}" \
    --workers 1