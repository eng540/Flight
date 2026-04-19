"""Airline API endpoints."""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
import logging

from app.database import get_db
from app.crud import AirlineCRUD
from app.schemas import AirlineResponse, AirlineCreate
from typing import List

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/airlines", tags=["airlines"])


@router.get("", response_model=List[AirlineResponse])
async def get_airlines(
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
    db: Session = Depends(get_db)
):
    """
    Get all airlines with pagination.
    
    Returns a list of all airlines in the database.
    """
    try:
        airlines = AirlineCRUD.get_all(db, skip=skip, limit=limit)
        return airlines
    except Exception as e:
        logger.error(f"Error fetching airlines: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/{airline_id}", response_model=AirlineResponse)
async def get_airline(
    airline_id: int,
    db: Session = Depends(get_db)
):
    """
    Get a specific airline by ID.
    
    Returns detailed information about a single airline.
    """
    airline = AirlineCRUD.get_by_id(db, airline_id)
    if not airline:
        raise HTTPException(status_code=404, detail="Airline not found")
    return airline


@router.get("/icao/{icao24}", response_model=AirlineResponse)
async def get_airline_by_icao(
    icao24: str,
    db: Session = Depends(get_db)
):
    """
    Get a specific airline by ICAO24 code.
    
    Returns detailed information about a single airline.
    """
    airline = AirlineCRUD.get_by_icao24(db, icao24)
    if not airline:
        raise HTTPException(status_code=404, detail="Airline not found")
    return airline


@router.post("", response_model=AirlineResponse, status_code=201)
async def create_airline(
    airline_data: AirlineCreate,
    db: Session = Depends(get_db)
):
    """
    Create a new airline.
    
    This endpoint allows manual creation of airline records.
    """
    try:
        # Check if airline already exists
        existing = AirlineCRUD.get_by_icao24(db, airline_data.icao24)
        if existing:
            raise HTTPException(status_code=409, detail="Airline with this ICAO24 already exists")
        
        airline = AirlineCRUD.create(db, airline_data)
        return airline
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating airline: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
