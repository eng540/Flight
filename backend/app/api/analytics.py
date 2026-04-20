"""Analytics API endpoints."""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Optional, List
import logging

from app.database import get_db
from app.crud import AnalyticsCRUD
from app.schemas import CountryStats, DailyStats, HourlyStats, AirportStats, RouteStats, AnalyticsSummary

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/analytics", tags=["analytics"])


def _kw(begin_ts, end_ts, region_key, lamin, lomin, lamax, lomax):
    return dict(begin_ts=begin_ts, end_ts=end_ts, region_key=region_key,
                lamin=lamin, lomin=lomin, lamax=lamax, lomax=lomax)


@router.get("/top_countries", response_model=List[CountryStats])
def get_top_countries(
    begin_ts: Optional[int] = None, end_ts: Optional[int] = None,
    region_key: Optional[str] = None,
    lamin: Optional[float] = None, lomin: Optional[float] = None,
    lamax: Optional[float] = None, lomax: Optional[float] = None,
    limit: int = Query(15, ge=1, le=50),
    db: Session = Depends(get_db)
):
    try:
        return AnalyticsCRUD.get_top_countries(
            db, limit=limit, **_kw(begin_ts, end_ts, region_key, lamin, lomin, lamax, lomax))
    except Exception as e:
        logger.error(f"top_countries error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/daily_trend", response_model=List[DailyStats])
def get_daily_trend(
    begin_ts: int = Query(...), end_ts: int = Query(...),
    region_key: Optional[str] = None,
    lamin: Optional[float] = None, lomin: Optional[float] = None,
    lamax: Optional[float] = None, lomax: Optional[float] = None,
    db: Session = Depends(get_db)
):
    try:
        if end_ts - begin_ts > 366 * 86400:
            raise HTTPException(status_code=400, detail="Range too large (max 366 days)")
        return AnalyticsCRUD.get_daily_trend(
            db, begin_ts=begin_ts, end_ts=end_ts,
            region_key=region_key, lamin=lamin, lomin=lomin, lamax=lamax, lomax=lomax)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"daily_trend error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/hourly_distribution", response_model=List[HourlyStats])
def get_hourly_distribution(
    begin_ts: Optional[int] = None, end_ts: Optional[int] = None,
    region_key: Optional[str] = None,
    lamin: Optional[float] = None, lomin: Optional[float] = None,
    lamax: Optional[float] = None, lomax: Optional[float] = None,
    db: Session = Depends(get_db)
):
    try:
        return AnalyticsCRUD.get_hourly_distribution(
            db, **_kw(begin_ts, end_ts, region_key, lamin, lomin, lamax, lomax))
    except Exception as e:
        logger.error(f"hourly error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/top_airports", response_model=List[AirportStats])
def get_top_airports(
    begin_ts: Optional[int] = None, end_ts: Optional[int] = None,
    region_key: Optional[str] = None,
    lamin: Optional[float] = None, lomin: Optional[float] = None,
    lamax: Optional[float] = None, lomax: Optional[float] = None,
    limit: int = Query(15, ge=1, le=50),
    db: Session = Depends(get_db)
):
    try:
        return AnalyticsCRUD.get_top_airports(
            db, limit=limit, **_kw(begin_ts, end_ts, region_key, lamin, lomin, lamax, lomax))
    except Exception as e:
        logger.error(f"top_airports error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/top_routes", response_model=List[RouteStats])
def get_top_routes(
    begin_ts: Optional[int] = None, end_ts: Optional[int] = None,
    region_key: Optional[str] = None,
    lamin: Optional[float] = None, lomin: Optional[float] = None,
    lamax: Optional[float] = None, lomax: Optional[float] = None,
    limit: int = Query(20, ge=1, le=50),
    db: Session = Depends(get_db)
):
    try:
        return AnalyticsCRUD.get_top_routes(
            db, limit=limit, **_kw(begin_ts, end_ts, region_key, lamin, lomin, lamax, lomax))
    except Exception as e:
        logger.error(f"top_routes error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/summary", response_model=AnalyticsSummary)
def get_summary(
    begin_ts: Optional[int] = None, end_ts: Optional[int] = None,
    region_key: Optional[str] = None,
    lamin: Optional[float] = None, lomin: Optional[float] = None,
    lamax: Optional[float] = None, lomax: Optional[float] = None,
    db: Session = Depends(get_db)
):
    try:
        return AnalyticsCRUD.get_summary(
            db, **_kw(begin_ts, end_ts, region_key, lamin, lomin, lamax, lomax))
    except Exception as e:
        logger.error(f"summary error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
