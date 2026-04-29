"""Add FR24 specific fields

Revision ID: 002
Revises: 001
Create Date: 2026-04-29 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

revision = '002'
down_revision = '001'
branch_labels = None
depends_on = None

def upgrade() -> None:
    # Add columns to fact_flight_session
    op.add_column('fact_flight_session', sa.Column('fr24_id', sa.String(length=50), nullable=True))
    op.add_column('fact_flight_session', sa.Column('flight_number', sa.String(length=20), nullable=True))
    op.create_index(op.f('ix_fact_flight_session_fr24_id'), 'fact_flight_session', ['fr24_id'], unique=True)

    # Add column to track_telemetry
    op.add_column('track_telemetry', sa.Column('vspeed_fpm', sa.Float(), nullable=True))

def downgrade() -> None:
    op.drop_column('track_telemetry', 'vspeed_fpm')
    op.drop_index(op.f('ix_fact_flight_session_fr24_id'), table_name='fact_flight_session')
    op.drop_column('fact_flight_session', 'flight_number')
    op.drop_column('fact_flight_session', 'fr24_id')