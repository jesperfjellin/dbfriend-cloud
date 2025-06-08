"""
Database configuration and models for dbfriend-cloud
Async PostgreSQL with PostGIS support using SQLAlchemy
"""

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, DateTime, Integer, Text, Boolean, JSON, Index
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID
from geoalchemy2 import Geometry
from sqlalchemy import text 
from typing import AsyncGenerator
import uuid
from datetime import datetime, timezone

from config import settings


# Create async engine
engine = create_async_engine(
    settings.DATABASE_URL,
    echo=settings.DEBUG,
    future=True
)

# Create async session factory
AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False
)


class Base(DeclarativeBase):
    """Base class for all database models."""
    pass


class Dataset(Base):
    """Dataset represents a collection of spatial data being monitored."""
    __tablename__ = "datasets"
    
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=True)
    
    # Connection metadata (safe to store)
    host: Mapped[str] = mapped_column(String(255), nullable=False)
    port: Mapped[int] = mapped_column(Integer, default=5432)
    database: Mapped[str] = mapped_column(String(255), nullable=False)
    schema_name: Mapped[str] = mapped_column(String(255), default="public")
    table_name: Mapped[str] = mapped_column(String(255), nullable=False)
    geometry_column: Mapped[str] = mapped_column(String(255), default="geom")
    
    # Security settings
    ssl_mode: Mapped[str] = mapped_column(String(20), default="prefer")
    read_only: Mapped[bool] = mapped_column(Boolean, default=True)
    
    # Credentials (encrypted in production)
    connection_string: Mapped[str] = mapped_column(Text, nullable=False)  # Temporary - will be replaced with encrypted storage
    
    # Connection health monitoring
    connection_status: Mapped[str] = mapped_column(String(20), default="unknown")  # unknown, success, failed, testing
    last_connection_test: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    connection_error: Mapped[str] = mapped_column(Text, nullable=True)
    
    # Metadata
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    
    # Monitoring settings
    check_interval_minutes: Mapped[int] = mapped_column(Integer, default=60)
    last_check_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    
    __table_args__ = (
        Index("idx_datasets_name", "name"),
        Index("idx_datasets_active", "is_active"),
        Index("idx_datasets_host", "host"),
        Index("idx_datasets_connection_status", "connection_status"),
    )


class GeometrySnapshot(Base):
    """Snapshot of geometry state at a point in time."""
    __tablename__ = "geometry_snapshots"
    
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    dataset_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    
    # Geometry identification
    source_id: Mapped[str] = mapped_column(String(255), nullable=True)  # Original ID from source table
    geometry_hash: Mapped[str] = mapped_column(String(64), nullable=False)  # MD5 of geometry WKB
    attributes_hash: Mapped[str] = mapped_column(String(64), nullable=False)  # MD5 of attributes
    composite_hash: Mapped[str] = mapped_column(String(64), nullable=False)  # Combined hash
    
    # Geometry data
    geometry: Mapped[Geometry] = mapped_column(Geometry('GEOMETRY', srid=4326))
    attributes: Mapped[dict] = mapped_column(JSON, nullable=True)
    
    # Metadata
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), default=lambda: datetime.now(timezone.utc))
    
    __table_args__ = (
        Index("idx_geometry_snapshots_dataset", "dataset_id"),
        Index("idx_geometry_snapshots_geom_hash", "geometry_hash"),
        Index("idx_geometry_snapshots_composite_hash", "composite_hash"),
        Index("idx_geometry_snapshots_geom", "geometry", postgresql_using="gist"),
    )


class GeometryDiff(Base):
    """Represents a detected difference between geometry snapshots."""
    __tablename__ = "geometry_diffs"
    
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    dataset_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    
    # Diff classification
    diff_type: Mapped[str] = mapped_column(String(50), nullable=False)  # NEW, UPDATED, DELETED
    confidence_score: Mapped[float] = mapped_column(nullable=True)  # 0.0 to 1.0
    
    # Geometry references
    old_snapshot_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=True)
    new_snapshot_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=True)
    
    # Diff details
    geometry_changed: Mapped[bool] = mapped_column(Boolean, default=False)
    attributes_changed: Mapped[bool] = mapped_column(Boolean, default=False)
    changes_summary: Mapped[dict] = mapped_column(JSON, nullable=True)
    
    # Status tracking
    status: Mapped[str] = mapped_column(String(20), default="PENDING")  # PENDING, ACCEPTED, REJECTED
    reviewed_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    reviewed_by: Mapped[str] = mapped_column(String(255), nullable=True)
    
    # Metadata
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), default=lambda: datetime.now(timezone.utc))
    
    __table_args__ = (
        Index("idx_geometry_diffs_dataset", "dataset_id"),
        Index("idx_geometry_diffs_type", "diff_type"),
        Index("idx_geometry_diffs_status", "status"),
        Index("idx_geometry_diffs_created", "created_at"),
    )


class SpatialCheck(Base):
    """Represents a spatial quality check result."""
    __tablename__ = "spatial_checks"
    
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    dataset_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    snapshot_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    
    # Check details
    check_type: Mapped[str] = mapped_column(String(50), nullable=False)  # TOPOLOGY, VALIDITY, DUPLICATE, etc.
    check_result: Mapped[str] = mapped_column(String(20), nullable=False)  # PASS, FAIL, WARNING
    error_message: Mapped[str] = mapped_column(Text, nullable=True)
    error_details: Mapped[dict] = mapped_column(JSON, nullable=True)
    
    # Metadata
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), default=lambda: datetime.now(timezone.utc))
    
    __table_args__ = (
        Index("idx_spatial_checks_dataset", "dataset_id"),
        Index("idx_spatial_checks_type", "check_type"),
        Index("idx_spatial_checks_result", "check_result"),
    )


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Dependency to get database session."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


async def init_db():
    """Initialize OUR database for storing analysis results and user configurations."""
    try:
        async with engine.begin() as conn:
            # For development: drop and recreate all tables to match updated schema
            # TODO: In production, use proper Alembic migrations
            await conn.run_sync(Base.metadata.drop_all)
            await conn.run_sync(Base.metadata.create_all)
        
        import logging
        logger = logging.getLogger("dbfriend-cloud")
        logger.info("[green]✓ dbfriend-cloud database initialized (tables recreated)[/green]")
        
    except Exception as e:
        import logging
        logger = logging.getLogger("dbfriend-cloud")
        logger.warning(f"[yellow]⚠ Database connection failed: {e}[/yellow]")
        logger.warning("[yellow]⚠ Running in development mode without database[/yellow]")
        # Continue running without database for development


async def reset_db_for_development():
    """Drop and recreate all tables. USE ONLY IN DEVELOPMENT!"""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    
    import logging
    logger = logging.getLogger("dbfriend-cloud")
    logger.info("[yellow]⚠ Database tables reset for development[/yellow]")

