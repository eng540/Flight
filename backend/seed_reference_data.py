"""
Enterprise SRE Data Seeding Script (v1.0)
Populates 'dim_geography' and 'dim_operator' with global reference data.
Sources: OurAirports (Airports) & OpenAirlines/OpenSky (Operators).
Run this script ONCE per fresh deployment or major update.
"""
import pandas as pd
import requests
import io
import logging
from sqlalchemy.orm import Session
import sys
import os

# Ensure we can import our app modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.database import SessionLocal, engine
from app.models import DimGeography, DimOperator

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")
logger = logging.getLogger("DataSeeder")

OUR_AIRPORTS_URL = "https://davidmegginson.github.io/ourairports-data/airports.csv"
# We use a reliable open-source airlines dataset
AIRLINES_URL = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airlines.dat"

def seed_geography(db: Session):
    logger.info("Starting Geography (Airports) Seeding from OurAirports...")
    
    try:
        response = requests.get(OUR_AIRPORTS_URL, timeout=30)
        response.raise_for_status()
        
        # Load CSV into pandas for fast cleaning
        df = pd.read_csv(io.StringIO(response.text))
        
        # Filter: We only want actual airports with ICAO codes (ignore heliports/closed for now to save space, or keep all)
        df_filtered = df[df['type'].isin(['large_airport', 'medium_airport'])]
        df_filtered = df_filtered.dropna(subset=['ident']) # ident is usually the ICAO code
        
        existing_icaos = {code[0] for code in db.query(DimGeography.icao_code).all()}
        
        new_airports = []
        for _, row in df_filtered.iterrows():
            icao = str(row['ident']).strip().upper()
            if len(icao) != 4 or icao in existing_icaos:
                continue # Skip if not a valid ICAO or already exists
                
            new_airports.append(
                DimGeography(
                    icao_code=icao,
                    iata_code=str(row['iata_code']).upper() if pd.notna(row['iata_code']) else None,
                    name=str(row['name']),
                    city=str(row['municipality']) if pd.notna(row['municipality']) else None,
                    country_code=str(row['iso_country']).upper() if pd.notna(row['iso_country']) else None,
                    latitude=float(row['latitude_deg']),
                    longitude=float(row['longitude_deg']),
                    elevation_m=float(row['elevation_ft']) * 0.3048 if pd.notna(row['elevation_ft']) else None
                )
            )
            existing_icaos.add(icao) # Update local cache to prevent duplicates in the same batch

        if new_airports:
            db.bulk_save_objects(new_airports)
            db.commit()
            logger.info(f"Successfully seeded {len(new_airports)} new airports into dim_geography.")
        else:
            logger.info("No new airports to seed.")
            
    except Exception as e:
        logger.error(f"Failed to seed geography: {e}")
        db.rollback()

def seed_operators(db: Session):
    logger.info("Starting Operators (Airlines) Seeding...")
    
    try:
        response = requests.get(AIRLINES_URL, timeout=30)
        response.raise_for_status()
        
        # OpenFlights Airlines format: Airline ID, Name, Alias, IATA, ICAO, Callsign, Country, Active
        columns = ["id", "name", "alias", "iata", "icao", "callsign", "country", "active"]
        df = pd.read_csv(io.StringIO(response.text), names=columns, na_values=['\\N', '-'])
        
        # Filter active airlines with valid ICAO codes
        df_filtered = df[(df['active'] == 'Y') & (df['icao'].notna())]
        
        existing_icaos = {code[0] for code in db.query(DimOperator.icao_code).all()}
        
        new_operators = []
        for _, row in df_filtered.iterrows():
            icao = str(row['icao']).strip().upper()
            if len(icao) != 3 or icao in existing_icaos:
                continue
                
            # Attempt to map country to a 2-letter code if needed (simplified here)
            country = str(row['country']) if pd.notna(row['country']) else None
            
            new_operators.append(
                DimOperator(
                    icao_code=icao,
                    iata_code=str(row['iata']).upper() if pd.notna(row['iata']) else None,
                    name=str(row['name']),
                    country_code=None, # Requires a Country-to-ISO lookup table for perfection, leaving None for now
                    operator_type="Commercial" # OpenFlights mostly contains commercial
                )
            )
            existing_icaos.add(icao)

        if new_operators:
            db.bulk_save_objects(new_operators)
            db.commit()
            logger.info(f"Successfully seeded {len(new_operators)} new operators into dim_operator.")
        else:
            logger.info("No new operators to seed.")

    except Exception as e:
        logger.error(f"Failed to seed operators: {e}")
        db.rollback()

if __name__ == "__main__":
    logger.info("=== SRE Data Seeding Initiated ===")
    with SessionLocal() as db:
        seed_geography(db)
        seed_operators(db)
    logger.info("=== SRE Data Seeding Completed ===")