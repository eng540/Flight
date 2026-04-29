"""
Enterprise Airlines API Endpoints (v4.0)
"""
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.database import get_db
from app import models

router = APIRouter(prefix="/airlines", tags=["airlines"])

@router.get("")
async def get_airlines(skip: int = 0, limit: int = 500, db: Session = Depends(get_db)):
    """SRE FIX: Restored DB read for UI Dropdowns."""
    operators = db.query(models.DimOperator).offset(skip).limit(limit).all()
    return [{"id": op.id, "icao24": op.icao_code, "name": op.name} for op in operators]