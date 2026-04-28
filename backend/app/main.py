"""Main FastAPI application – Flight Intelligence v3."""
import asyncio
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
import logging, time, os, mimetypes

mimetypes.add_type('application/javascript', '.js')
mimetypes.add_type('text/css', '.css')
mimetypes.add_type('image/svg+xml', '.svg')

from app.config import settings
from app.api import flights, stats, airlines, analytics, ingestion, regions

logging.basicConfig(
    level=logging.INFO if not settings.DEBUG else logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)


# SRE FIX: دالة مساعدة لتشغيل سكريبت التهيئة في الخلفية
async def run_data_seeder_async():
    import sys
    import os
    import importlib.util
    
    logger.info("SRE: Initiating background data seeding process...")
    try:
        # 1. تحديد مسار السكريبت
        script_path = os.path.join(os.path.dirname(__file__), "..", "seed_reference_data.py")
        
        if not os.path.exists(script_path):
            logger.error(f"SRE Error: Seeder script not found at {script_path}")
            return

        # 2. تحميل السكريبت كـ وحدة برمجية (Module)
        spec = importlib.util.spec_from_file_location("seed_module", script_path)
        seed_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(seed_module)
        
        # 3. تشغيل الدوال داخل الجلسة (Session)
        from app.database import SessionLocal
        with SessionLocal() as db:
            # نستخدم تشغيل متزامن (Synchronous) داخل ثريد (Thread) لتجنب تجميد الـ Async Loop
            await asyncio.to_thread(seed_module.seed_geography, db)
            await asyncio.to_thread(seed_module.seed_operators, db)
            
        logger.info("SRE: Background data seeding finished successfully.")
    except Exception as e:
        logger.error(f"SRE Error during auto-seeding: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"Starting {settings.APP_NAME} v3.0 Enterprise")
    
    # SRE FIX: تشغيل سحب المطارات والشركات آلياً في الخلفية عند الإقلاع
    # استخدام create_task يضمن أن السيرفر يقلع فوراً ولا ينتظر اكتمال التحميل
    asyncio.create_task(run_data_seeder_async())
    
    yield
    logger.info(f"Shutting down {settings.APP_NAME}")


app = FastAPI(
    title=settings.APP_NAME,
    version="3.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start = time.time()
    response = await call_next(request)
    response.headers["X-Process-Time"] = str(time.time() - start)
    return response


# ── API Routers ───────────────────────────────────────────────────────────────
app.include_router(flights.router)
app.include_router(stats.router)
app.include_router(airlines.router)
app.include_router(analytics.router)
app.include_router(ingestion.router)
app.include_router(regions.router)


@app.get("/health")
async def health():
    return {"status": "healthy", "version": "3.0.0"}


# ── Serve React frontend ──────────────────────────────────────────────────────
# In production the frontend is built into /app/frontend/dist (see Dockerfile)
frontend_dist = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../frontend/dist"))

if os.path.exists(frontend_dist):
    logger.info(f"Serving frontend from: {frontend_dist}")
    assets_path = os.path.join(frontend_dist, "assets")
    if os.path.exists(assets_path):
        app.mount("/assets", StaticFiles(directory=assets_path), name="assets")

    @app.get("/{file_name}.{ext}")
    async def serve_root_files(file_name: str, ext: str):
        p = os.path.join(frontend_dist, f"{file_name}.{ext}")
        if os.path.isfile(p):
            return FileResponse(p)
        raise HTTPException(status_code=404, detail="Not found")

    @app.get("/{catchall:path}")
    async def serve_react_app(catchall: str):
        idx = os.path.join(frontend_dist, "index.html")
        if os.path.exists(idx):
            return FileResponse(idx)
        raise HTTPException(status_code=404, detail="index.html not found")
else:
    logger.warning("Frontend build not found – API-only mode")

    @app.get("/")
    async def root():
        return {"message": "Flight Intelligence API v3 is running", "docs": "/docs"}