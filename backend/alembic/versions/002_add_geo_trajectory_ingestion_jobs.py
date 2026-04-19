"""Add geo fields, trajectory, region_key to flights; add ingestion_jobs table

Revision ID: 002
Revises: 001
Create Date: 2026-04-18
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = '002'
down_revision = '001'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add new columns to flights (all nullable so existing rows are unaffected)
    op.add_column('flights', sa.Column('latitude',  sa.Float(), nullable=True))
    op.add_column('flights', sa.Column('longitude', sa.Float(), nullable=True))
    op.add_column('flights', sa.Column('altitude',  sa.Float(), nullable=True))
    op.add_column('flights', sa.Column('velocity',  sa.Float(), nullable=True))
    op.add_column('flights', sa.Column('heading',   sa.Float(), nullable=True))
    op.add_column('flights', sa.Column('on_ground', sa.Boolean(), nullable=True))
    op.add_column('flights', sa.Column('trajectory',
                  postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column('flights', sa.Column('region_key', sa.String(50), nullable=True))

    op.create_index('idx_flight_geo',    'flights', ['latitude', 'longitude'])
    op.create_index('idx_flight_region', 'flights', ['region_key'])

    # Create ingestion_jobs table
    op.create_table(
        'ingestion_jobs',
        sa.Column('id',               sa.Integer(),     nullable=False),
        sa.Column('date_str',         sa.String(10),    nullable=False),
        sa.Column('region_key',       sa.String(50),    nullable=False),
        sa.Column('lamin',            sa.Float(),        nullable=False),
        sa.Column('lomin',            sa.Float(),        nullable=False),
        sa.Column('lamax',            sa.Float(),        nullable=False),
        sa.Column('lomax',            sa.Float(),        nullable=False),
        sa.Column('begin_ts',         sa.BigInteger(),   nullable=False),
        sa.Column('end_ts',           sa.BigInteger(),   nullable=False),
        sa.Column('status',           sa.String(20),    nullable=False,
                  server_default='pending'),
        sa.Column('flights_ingested', sa.Integer(),     nullable=True,
                  server_default='0'),
        sa.Column('chunks_total',     sa.Integer(),     nullable=True,
                  server_default='0'),
        sa.Column('chunks_done',      sa.Integer(),     nullable=True,
                  server_default='0'),
        sa.Column('error_message',    sa.Text(),         nullable=True),
        sa.Column('created_at',       sa.DateTime(timezone=True),
                  server_default=sa.text('now()'), nullable=True),
        sa.Column('started_at',       sa.DateTime(timezone=True), nullable=True),
        sa.Column('completed_at',     sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('date_str', 'region_key',
                            name='uq_ingestion_date_region'),
    )
    op.create_index(op.f('ix_ingestion_jobs_id'),
                    'ingestion_jobs', ['id'], unique=False)
    op.create_index('idx_ingestion_status',
                    'ingestion_jobs', ['status'])
    op.create_index('idx_ingestion_date_region',
                    'ingestion_jobs', ['date_str', 'region_key'])


def downgrade() -> None:
    op.drop_table('ingestion_jobs')
    for idx in ('idx_flight_region', 'idx_flight_geo'):
        op.drop_index(idx, table_name='flights')
    for col in ('region_key', 'trajectory', 'on_ground', 'heading',
                'velocity', 'altitude', 'longitude', 'latitude'):
        op.drop_column('flights', col)
