"""
Dataset API endpoints
Manage spatial datasets and trigger geometry imports
"""

from typing import List
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from database import get_db, Dataset
from models import (
    DatasetCreate, DatasetUpdate, Dataset as DatasetModel,
    GeometryImportRequest, GeometryImportResponse, DatasetStats, DiffStats
)
from services.geometry_service import GeometryService

router = APIRouter()


@router.post("/", response_model=DatasetModel)
async def create_dataset(
    dataset: DatasetCreate,
    db: AsyncSession = Depends(get_db)
):
    """Create a new dataset for monitoring."""
    
    # TODO: Add validation for connection string and table existence
    
    db_dataset = Dataset(**dataset.model_dump())
    db.add(db_dataset)
    await db.commit()
    await db.refresh(db_dataset)
    
    return db_dataset


@router.get("/", response_model=List[DatasetModel])
async def list_datasets(
    skip: int = 0,
    limit: int = 100,
    active_only: bool = True,
    db: AsyncSession = Depends(get_db)
):
    """List all datasets with optional filtering."""
    
    try:
        query = select(Dataset)
        
        if active_only:
            query = query.where(Dataset.is_active == True)
        
        query = query.offset(skip).limit(limit)
        
        result = await db.execute(query)
        datasets = result.scalars().all()
        
        return datasets
    
    except Exception:
        # Return empty list if database is not available (development mode)
        return []


@router.get("/{dataset_id}", response_model=DatasetModel)
async def get_dataset(
    dataset_id: UUID,
    db: AsyncSession = Depends(get_db)
):
    """Get a specific dataset by ID."""
    
    result = await db.execute(
        select(Dataset).where(Dataset.id == dataset_id)
    )
    dataset = result.scalar_one_or_none()
    
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    return dataset


@router.put("/{dataset_id}", response_model=DatasetModel)
async def update_dataset(
    dataset_id: UUID,
    dataset_update: DatasetUpdate,
    db: AsyncSession = Depends(get_db)
):
    """Update a dataset."""
    
    result = await db.execute(
        select(Dataset).where(Dataset.id == dataset_id)
    )
    dataset = result.scalar_one_or_none()
    
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    # Update only provided fields
    update_data = dataset_update.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(dataset, field, value)
    
    await db.commit()
    await db.refresh(dataset)
    
    return dataset


@router.delete("/{dataset_id}")
async def delete_dataset(
    dataset_id: UUID,
    db: AsyncSession = Depends(get_db)
):
    """Delete a dataset (soft delete by setting is_active=False)."""
    
    result = await db.execute(
        select(Dataset).where(Dataset.id == dataset_id)
    )
    dataset = result.scalar_one_or_none()
    
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    dataset.is_active = False
    await db.commit()
    
    return {"message": "Dataset deleted successfully"}


@router.post("/{dataset_id}/import", response_model=GeometryImportResponse)
async def import_geometries(
    dataset_id: UUID,
    import_request: GeometryImportRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db)
):
    """
    Trigger geometry import and diff detection for a dataset.
    This is the core functionality that detects changes.
    """
    
    # Verify dataset exists
    result = await db.execute(
        select(Dataset).where(Dataset.id == dataset_id)
    )
    dataset = result.scalar_one_or_none()
    
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    if not dataset.is_active:
        raise HTTPException(status_code=400, detail="Dataset is not active")
    
    # Create geometry service and perform import
    geometry_service = GeometryService(db)
    
    try:
        response = await geometry_service.import_geometries_from_external_source(
            dataset, import_request.force_reimport
        )
        
        # Update last check time
        from datetime import datetime
        dataset.last_check_at = datetime.utcnow()
        await db.commit()
        
        return response
        
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Error importing geometries: {str(e)}"
        )


@router.get("/{dataset_id}/stats", response_model=DatasetStats)
async def get_dataset_stats(
    dataset_id: UUID,
    db: AsyncSession = Depends(get_db)
):
    """Get comprehensive statistics for a dataset."""
    
    # Verify dataset exists
    result = await db.execute(
        select(Dataset).where(Dataset.id == dataset_id)
    )
    dataset = result.scalar_one_or_none()
    
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    # Import required models at function level to avoid circular imports
    from ...database import GeometrySnapshot, GeometryDiff, SpatialCheck
    
    # Get total snapshots
    snapshots_result = await db.execute(
        select(func.count(GeometrySnapshot.id))
        .where(GeometrySnapshot.dataset_id == dataset_id)
    )
    total_snapshots = snapshots_result.scalar() or 0
    
    # Get diff statistics
    diffs_result = await db.execute(
        select(
            func.count(GeometryDiff.id).label("total"),
            func.sum(func.case((GeometryDiff.status == "PENDING", 1), else_=0)).label("pending"),
            func.sum(func.case((GeometryDiff.status == "ACCEPTED", 1), else_=0)).label("accepted"),
            func.sum(func.case((GeometryDiff.status == "REJECTED", 1), else_=0)).label("rejected"),
            func.sum(func.case((GeometryDiff.diff_type == "NEW", 1), else_=0)).label("new"),
            func.sum(func.case((GeometryDiff.diff_type == "UPDATED", 1), else_=0)).label("updated"),
            func.sum(func.case((GeometryDiff.diff_type == "DELETED", 1), else_=0)).label("deleted"),
        )
        .where(GeometryDiff.dataset_id == dataset_id)
    )
    diff_stats_row = diffs_result.fetchone()
    
    diff_stats = DiffStats(
        total_diffs=int(diff_stats_row.total or 0),
        pending_diffs=int(diff_stats_row.pending or 0),
        accepted_diffs=int(diff_stats_row.accepted or 0),
        rejected_diffs=int(diff_stats_row.rejected or 0),
        new_geometries=int(diff_stats_row.new or 0),
        updated_geometries=int(diff_stats_row.updated or 0),
        deleted_geometries=int(diff_stats_row.deleted or 0),
    )
    
    # Get spatial check statistics
    checks_result = await db.execute(
        select(
            SpatialCheck.check_type,
            func.count(SpatialCheck.id).label("count")
        )
        .where(SpatialCheck.dataset_id == dataset_id)
        .group_by(SpatialCheck.check_type)
    )
    
    spatial_checks = {}
    for row in checks_result:
        spatial_checks[row.check_type] = int(row.count)
    
    return DatasetStats(
        dataset_id=dataset_id,
        total_snapshots=total_snapshots,
        last_check_at=dataset.last_check_at,
        diff_stats=diff_stats,
        spatial_checks=spatial_checks
    ) 