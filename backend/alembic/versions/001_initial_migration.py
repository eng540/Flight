"""Initial migration - create countries, airlines, and flights tables

Revision ID: 001
Revises: 
Create Date: 2024-01-01 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '001'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create countries table
    op.create_table(
        'countries',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(length=100), nullable=False),
        sa.Column('iso_code', sa.String(length=3), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('iso_code'),
        sa.UniqueConstraint('name')
    )
    op.create_index(op.f('ix_countries_id'), 'countries', ['id'], unique=False)
    op.create_index(op.f('ix_countries_name'), 'countries', ['name'], unique=False)
    
    # Create airlines table
    op.create_table(
        'airlines',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('icao24', sa.String(length=6), nullable=False),
        sa.Column('name', sa.String(length=200), nullable=True),
        sa.Column('callsign_prefix', sa.String(length=10), nullable=True),
        sa.Column('country_id', sa.Integer(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['country_id'], ['countries.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('icao24')
    )
    op.create_index(op.f('ix_airlines_country_id'), 'airlines', ['country_id'], unique=False)
    op.create_index(op.f('ix_airlines_icao24'), 'airlines', ['icao24'], unique=False)
    op.create_index(op.f('ix_airlines_id'), 'airlines', ['id'], unique=False)
    op.create_index('idx_airline_icao24_name', 'airlines', ['icao24', 'name'], unique=False)
    
    # Create flights table
    op.create_table(
        'flights',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('icao24', sa.String(length=6), nullable=False),
        sa.Column('callsign', sa.String(length=20), nullable=True),
        sa.Column('airline_id', sa.Integer(), nullable=True),
        sa.Column('origin_country', sa.String(length=100), nullable=True),
        sa.Column('first_seen', sa.BigInteger(), nullable=True),
        sa.Column('last_seen', sa.BigInteger(), nullable=True),
        sa.Column('est_departure_airport', sa.String(length=4), nullable=True),
        sa.Column('est_departure_airport_horiz_distance', sa.Integer(), nullable=True),
        sa.Column('est_departure_airport_vert_distance', sa.Integer(), nullable=True),
        sa.Column('est_arrival_airport', sa.String(length=4), nullable=True),
        sa.Column('est_arrival_airport_horiz_distance', sa.Integer(), nullable=True),
        sa.Column('est_arrival_airport_vert_distance', sa.Integer(), nullable=True),
        sa.Column('est_departure_time', sa.BigInteger(), nullable=True),
        sa.Column('est_arrival_time', sa.BigInteger(), nullable=True),
        sa.Column('ingestion_time', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('unique_flight_id', sa.String(length=100), nullable=False),
        sa.ForeignKeyConstraint(['airline_id'], ['airlines.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('unique_flight_id')
    )
    op.create_index(op.f('ix_flights_airline_id'), 'flights', ['airline_id'], unique=False)
    op.create_index(op.f('ix_flights_callsign'), 'flights', ['callsign'], unique=False)
    op.create_index(op.f('ix_flights_est_arrival_airport'), 'flights', ['est_arrival_airport'], unique=False)
    op.create_index(op.f('ix_flights_est_departure_airport'), 'flights', ['est_departure_airport'], unique=False)
    op.create_index(op.f('ix_flights_first_seen'), 'flights', ['first_seen'], unique=False)
    op.create_index(op.f('ix_flights_icao24'), 'flights', ['icao24'], unique=False)
    op.create_index(op.f('ix_flights_id'), 'flights', ['id'], unique=False)
    op.create_index(op.f('ix_flights_last_seen'), 'flights', ['last_seen'], unique=False)
    op.create_index(op.f('ix_flights_origin_country'), 'flights', ['origin_country'], unique=False)
    op.create_index(op.f('ix_flights_unique_flight_id'), 'flights', ['unique_flight_id'], unique=False)
    op.create_index('idx_flight_airports', 'flights', ['est_departure_airport', 'est_arrival_airport'], unique=False)
    op.create_index('idx_flight_country', 'flights', ['origin_country'], unique=False)
    op.create_index('idx_flight_ingestion', 'flights', ['ingestion_time'], unique=False)
    op.create_index('idx_flight_time_range', 'flights', ['first_seen', 'last_seen'], unique=False)


def downgrade() -> None:
    op.drop_table('flights')
    op.drop_table('airlines')
    op.drop_table('countries')
