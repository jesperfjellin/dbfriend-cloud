"""
Pydantic models for API serialization
These models define the contract between frontend and backend
"""

from typing import Optional, Dict, Any, List
from datetime import datetime
from pydantic import BaseModel, Field, ConfigDict
from uuid import UUID


class DatasetBase(BaseModel):
    """Base dataset model with common fields."""
    name: str = Field(..., description="Name of the dataset")
    description: Optional[str] = Field(None, description="Description of the dataset")
    connection_string: str = Field(..., description="PostgreSQL connection string")
    schema_name: str = Field(default="public", description="Database schema name")
    table_name: str = Field(..., description="Table name containing geometries")
    geometry_column: str = Field(default="geom", description="Name of the geometry column")
    check_interval_minutes: int = Field(default=60, description="Check interval in minutes")


class DatasetCreate(DatasetBase):
    """Model for creating a new dataset."""
    pass


class DatasetUpdate(BaseModel):
    """Model for updating an existing dataset."""
    name: Optional[str] = None
    description: Optional[str] = None
    check_interval_minutes: Optional[int] = None
    is_active: Optional[bool] = None


class Dataset(DatasetBase):
    """Complete dataset model with all fields."""
    model_config = ConfigDict(from_attributes=True)
    
    id: UUID
    created_at: datetime
    updated_at: datetime
    is_active: bool
    last_check_at: Optional[datetime] = None


class GeometrySnapshotBase(BaseModel):
    """Base geometry snapshot model."""
    source_id: Optional[str] = None
    geometry_hash: str
    attributes_hash: str
    composite_hash: str
    attributes: Optional[Dict[str, Any]] = None


class GeometrySnapshot(GeometrySnapshotBase):
    """Complete geometry snapshot model."""
    model_config = ConfigDict(from_attributes=True)
    
    id: UUID
    dataset_id: UUID
    created_at: datetime


class GeometryDiffBase(BaseModel):
    """Base geometry diff model."""
    diff_type: str = Field(..., description="Type of diff: NEW, UPDATED, DELETED")
    confidence_score: Optional[float] = Field(None, ge=0.0, le=1.0, description="Confidence score 0-1")
    geometry_changed: bool = False
    attributes_changed: bool = False
    changes_summary: Optional[Dict[str, Any]] = None


class GeometryDiff(GeometryDiffBase):
    """Complete geometry diff model."""
    model_config = ConfigDict(from_attributes=True)
    
    id: UUID
    dataset_id: UUID
    old_snapshot_id: Optional[UUID] = None
    new_snapshot_id: Optional[UUID] = None
    status: str = Field(default="PENDING", description="Status: PENDING, ACCEPTED, REJECTED")
    reviewed_at: Optional[datetime] = None
    reviewed_by: Optional[str] = None
    created_at: datetime


class DiffReview(BaseModel):
    """Model for reviewing a diff."""
    status: str = Field(..., description="New status: ACCEPTED or REJECTED")
    reviewed_by: str = Field(..., description="Name/ID of reviewer")


class SpatialCheckBase(BaseModel):
    """Base spatial check model."""
    check_type: str = Field(..., description="Type of check: TOPOLOGY, VALIDITY, DUPLICATE, etc.")
    check_result: str = Field(..., description="Result: PASS, FAIL, WARNING")
    error_message: Optional[str] = None
    error_details: Optional[Dict[str, Any]] = None


class SpatialCheck(SpatialCheckBase):
    """Complete spatial check model."""
    model_config = ConfigDict(from_attributes=True)
    
    id: UUID
    dataset_id: UUID
    snapshot_id: UUID
    created_at: datetime


class DiffStats(BaseModel):
    """Statistics about diffs for a dataset."""
    total_diffs: int = 0
    pending_diffs: int = 0
    accepted_diffs: int = 0
    rejected_diffs: int = 0
    new_geometries: int = 0
    updated_geometries: int = 0
    deleted_geometries: int = 0


class DatasetStats(BaseModel):
    """Comprehensive statistics for a dataset."""
    dataset_id: UUID
    total_snapshots: int = 0
    last_check_at: Optional[datetime] = None
    diff_stats: DiffStats
    spatial_checks: Dict[str, int] = Field(default_factory=dict)  # check_type -> count


class GeometryDiffDetails(GeometryDiff):
    """Detailed diff model with geometry data."""
    old_snapshot: Optional[GeometrySnapshot] = None
    new_snapshot: Optional[GeometrySnapshot] = None
    
    # GeoJSON representations for the frontend
    old_geometry_geojson: Optional[Dict[str, Any]] = None
    new_geometry_geojson: Optional[Dict[str, Any]] = None


class DiffBatch(BaseModel):
    """Batch processing request for multiple diffs."""
    diff_ids: List[UUID] = Field(..., description="List of diff IDs to process")
    action: str = Field(..., description="Batch action: ACCEPT or REJECT")
    reviewed_by: str = Field(..., description="Name/ID of reviewer")


class GeometryImportRequest(BaseModel):
    """Request model for importing geometry data from external source."""
    dataset_id: UUID
    force_reimport: bool = Field(default=False, description="Force reimport even if no changes detected")


class GeometryImportResponse(BaseModel):
    """Response model for geometry import operation."""
    dataset_id: UUID
    snapshots_created: int = 0
    diffs_detected: int = 0
    import_duration_seconds: float = 0.0
    status: str = Field(..., description="Import status: SUCCESS, PARTIAL, FAILED")
    error_message: Optional[str] = None 