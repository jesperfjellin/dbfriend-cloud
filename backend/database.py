"""
database.py – DB schema and helpers for dbfriend-cloud
Supports mixed-dimension (2D/3D/4D) geometries without an
ALTER-TABLE hack by using GeoAlchemy2 with `use_typmod=False`.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import AsyncGenerator

from geoalchemy2 import Geometry
from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    Index,
    Integer,
    JSON,
    String,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import TIMESTAMP, UUID
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from config import settings

# ────────────────────────────────────────────────────────────────────────────
# Engine / session factory
# ────────────────────────────────────────────────────────────────────────────

engine = create_async_engine(
    settings.DATABASE_URL,
    echo=settings.DEBUG,
    future=True,
)
AsyncSessionLocal = async_sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)


# ────────────────────────────────────────────────────────────────────────────
# Base model
# ────────────────────────────────────────────────────────────────────────────

class Base(DeclarativeBase):  # type: ignore
    """Declarative base for all ORM models."""
    pass


# ────────────────────────────────────────────────────────────────────────────
# ORM models
# ────────────────────────────────────────────────────────────────────────────

class Dataset(Base):
    """A table in a customer PostGIS database that we monitor."""
    __tablename__ = "datasets"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)

    # connection meta
    host: Mapped[str] = mapped_column(String(255), nullable=False)
    port: Mapped[int] = mapped_column(Integer, default=5432)
    database: Mapped[str] = mapped_column(String(255), nullable=False)
    schema_name: Mapped[str] = mapped_column(String(255), default="public")
    table_name: Mapped[str] = mapped_column(String(255), nullable=False)
    geometry_column: Mapped[str] = mapped_column(String(255), default="geom")

    ssl_mode: Mapped[str] = mapped_column(String(20), default="prefer")
    read_only: Mapped[bool] = mapped_column(Boolean, default=True)
    connection_string: Mapped[str] = mapped_column(Text, nullable=False)  # TODO: encrypt

    # connection health
    connection_status: Mapped[str] = mapped_column(String(20), default="unknown")
    last_connection_test: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True)
    )
    connection_error: Mapped[str | None] = mapped_column(Text)

    # meta
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

    # monitoring cadence
    check_interval_minutes: Mapped[int] = mapped_column(Integer, default=60)
    last_check_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))

    __table_args__ = (
        Index("idx_datasets_name", "name"),
        Index("idx_datasets_active", "is_active"),
        Index("idx_datasets_host", "host"),
        Index("idx_datasets_connection_status", "connection_status"),
    )


class GeometrySnapshot(Base):
    """One logical version of a feature at time T."""
    __tablename__ = "geometry_snapshots"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    dataset_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)

    source_id: Mapped[str | None] = mapped_column(String(255))
    geometry_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    attributes_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    composite_hash: Mapped[str] = mapped_column(String(64), nullable=False)

    # mixed-dimension geometry column - defined manually via SQL after table creation
    # We'll add this column via raw SQL to ensure proper mixed-dimension support

    attributes: Mapped[dict | None] = mapped_column(JSON)

    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    __table_args__ = (
        Index("idx_geometry_snapshots_dataset", "dataset_id"),
        Index("idx_geometry_snapshots_geom_hash", "geometry_hash"),
        Index("idx_geometry_snapshots_composite_hash", "composite_hash"),
        # Geometry index will be added manually after column creation
    )


class GeometryDiff(Base):
    """NEW / UPDATED / DELETED detection."""
    __tablename__ = "geometry_diffs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    dataset_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)

    diff_type: Mapped[str] = mapped_column(String(50), nullable=False)
    confidence_score: Mapped[float | None] = mapped_column()

    old_snapshot_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    new_snapshot_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))

    geometry_changed: Mapped[bool] = mapped_column(Boolean, default=False)
    attributes_changed: Mapped[bool] = mapped_column(Boolean, default=False)
    changes_summary: Mapped[dict | None] = mapped_column(JSON)

    status: Mapped[str] = mapped_column(String(20), default="PENDING")
    reviewed_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    reviewed_by: Mapped[str | None] = mapped_column(String(255))

    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    __table_args__ = (
        Index("idx_geometry_diffs_dataset", "dataset_id"),
        Index("idx_geometry_diffs_type", "diff_type"),
        Index("idx_geometry_diffs_status", "status"),
        Index("idx_geometry_diffs_created", "created_at"),
    )


class SpatialCheck(Base):
    """Quality-check result for a snapshot."""
    __tablename__ = "spatial_checks"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    dataset_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    snapshot_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)

    check_type: Mapped[str] = mapped_column(String(50), nullable=False)
    check_result: Mapped[str] = mapped_column(String(20), nullable=False)
    error_message: Mapped[str | None] = mapped_column(Text)
    error_details: Mapped[dict | None] = mapped_column(JSON)

    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    __table_args__ = (
        Index("idx_spatial_checks_dataset", "dataset_id"),
        Index("idx_spatial_checks_type", "check_type"),
        Index("idx_spatial_checks_result", "check_result"),
    )


# ────────────────────────────────────────────────────────────────────────────
# DB helpers
# ────────────────────────────────────────────────────────────────────────────

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency that yields an async session."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


async def init_db() -> None:
    """Initialize database with smart restart behavior."""
    import logging
    from config import settings

    logger = logging.getLogger("dbfriend-cloud")
    try:
        async with engine.begin() as conn:
            # Ensure PostGIS extension is enabled first
            await _ensure_postgis_extension(conn)
            
            # Check startup behavior - preserve connections or full reset
            if settings.PRESERVE_CONNECTIONS_ON_RESTART:
                # Smart restart: Preserve connections, reset monitoring
                logger.info("🔄 Smart restart: preserving dataset connections, resetting monitoring")
                await _smart_restart_reset(conn)
            else:
                # Full reset: Clean slate (useful for development)
                logger.info("🧹 Full reset: dropping all data including connections")
                await conn.run_sync(Base.metadata.drop_all)
                await conn.run_sync(Base.metadata.create_all)
                
            await _apply_postgres_optimizations(conn)
        logger.info("✓ dbfriend-cloud database initialised")
    except Exception as exc:  # pragma: no cover
        logger.warning(f"Database init failed: {exc}")


async def _ensure_postgis_extension(conn) -> None:
    """Ensure PostGIS extension is enabled in the database."""
    import logging
    
    logger = logging.getLogger("dbfriend-cloud")
    try:
        # Enable PostGIS extension if not already enabled
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
        logger.info("✓ PostGIS extension ensured")
    except Exception as exc:
        logger.warning(f"Could not enable PostGIS extension: {exc}")
        raise RuntimeError("PostGIS extension is required but could not be enabled") from exc


async def _smart_restart_reset(conn) -> None:
    """
    Smart restart: Preserve dataset connections, reset monitoring data.
    This prevents complex versioning issues from missed changes during downtime.
    """
    import logging
    
    logger = logging.getLogger("dbfriend-cloud")
    
    # First ensure all tables exist
    await conn.run_sync(Base.metadata.create_all)
    logger.info("✓ Ensured all tables exist")
    
    # Clear monitoring data in dependency order (preserve datasets table)
    await conn.execute(text("DELETE FROM spatial_checks"))
    logger.info("✓ Cleared spatial_checks")
    
    await conn.execute(text("DELETE FROM geometry_diffs")) 
    logger.info("✓ Cleared geometry_diffs")
    
    await conn.execute(text("DELETE FROM geometry_snapshots"))
    logger.info("✓ Cleared geometry_snapshots")
    
    # Reset monitoring state in datasets table (preserve connection configs)
    datasets_reset = await conn.execute(text("""
        UPDATE datasets SET 
            last_check_at = NULL,
            connection_status = 'unknown',
            connection_error = NULL,
            last_connection_test = NULL
        WHERE true
    """))
    
    # Get count of preserved datasets
    result = await conn.execute(text("SELECT COUNT(*) FROM datasets WHERE is_active = true"))
    active_datasets = result.scalar()
    
    logger.info(f"✓ Reset monitoring state for {active_datasets} dataset connections")
    
    # Clean geometry_columns view if needed
    await conn.execute(text("""
        DELETE FROM geometry_columns 
        WHERE f_table_name IN ('geometry_snapshots','geometry_diffs')
    """))
    logger.info("✓ Cleaned PostGIS metadata")
    
    # Re-add geometry column for snapshots if needed
    try:
        await conn.execute(text("""
            ALTER TABLE geometry_snapshots ADD COLUMN IF NOT EXISTS geometry geometry
        """))
        
        await conn.execute(text("""
            INSERT INTO geometry_columns 
            (f_table_catalog, f_table_schema, f_table_name, f_geometry_column, coord_dimension, srid, type)
            VALUES ('', 'public', 'geometry_snapshots', 'geometry', 4, 4326, 'GEOMETRY')
            ON CONFLICT DO NOTHING
        """))
        
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_geometry_snapshots_geom 
            ON geometry_snapshots USING GIST (geometry)
        """))
        
    except Exception as e:
        logger.debug(f"Geometry column setup (non-critical): {e}")
    
    logger.info(f"📊 Smart restart complete: {active_datasets} connections preserved, monitoring reset")


async def _apply_postgres_optimizations(conn) -> None:
    """Set TOAST compression + external storage for heavy columns."""
    import logging

    logger = logging.getLogger("dbfriend-cloud")
    applied: list[str] = []

    # move big columns out of main heap
    for col in ("geometry", "attributes"):
        try:
            await conn.execute(
                text(f"ALTER TABLE geometry_snapshots ALTER COLUMN {col} SET STORAGE EXTERNAL")
            )
            applied.append(f"{col} external")
        except Exception:
            pass

    # try LZ4
    try:
        rs = await conn.execute(
            text("SELECT setting FROM pg_settings WHERE name = 'default_toast_compression'")
        )
        if rs.fetchone():
            await conn.execute(
                text("ALTER TABLE geometry_snapshots SET (toast_compression='lz4')")
            )
            applied.append("lz4 compression")
    except Exception:
        # fallback to pglz if lz4 unavailable
        try:
            await conn.execute(
                text("ALTER TABLE geometry_snapshots SET (toast_compression='pglz')")
            )
            applied.append("pglz compression")
        except Exception:
            pass

    if applied:
        logger.info(f"PostgreSQL optimisations applied: {', '.join(applied)}")


async def reset_db_for_development() -> None:
    """Convenience helper to blow away all tables and recreate them."""
    async with engine.begin() as conn:
        # Ensure PostGIS extension is enabled first
        await _ensure_postgis_extension(conn)
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
        await _apply_postgres_optimizations(conn)
