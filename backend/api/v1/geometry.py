"""
Geometry API endpoints
Spatial analysis and quality checking functionality
"""

from typing import List, Optional
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_

from database import get_db, GeometrySnapshot, SpatialCheck
from models import SpatialCheck as SpatialCheckModel
from services.geometry_service import GeometryService

router = APIRouter()


@router.get("/snapshots/{snapshot_id}/geojson")
async def get_geometry_geojson(
    snapshot_id: UUID,
    db: AsyncSession = Depends(get_db)
):
    """
    Get geometry as GeoJSON for visualization.
    Used by the frontend map components.
    """
    
    # Verify snapshot exists
    result = await db.execute(
        select(GeometrySnapshot).where(GeometrySnapshot.id == snapshot_id)
    )
    snapshot = result.scalar_one_or_none()
    
    if not snapshot:
        raise HTTPException(status_code=404, detail="Geometry snapshot not found")
    
    # Get GeoJSON representation
    geometry_service = GeometryService(db)
    geojson = await geometry_service.get_geometry_as_geojson(snapshot_id)
    
    if geojson is None:
        raise HTTPException(
            status_code=500, 
            detail="Error converting geometry to GeoJSON"
        )
    
    return {
        "snapshot_id": snapshot_id,
        "geometry": geojson,
        "attributes": snapshot.attributes
    }


@router.post("/snapshots/{snapshot_id}/spatial-checks", response_model=List[SpatialCheckModel])
async def run_spatial_checks(
    snapshot_id: UUID,
    db: AsyncSession = Depends(get_db)
):
    """
    Run spatial quality checks on a geometry snapshot.
    This implements the automated checks mentioned in the roadmap.
    """
    
    # Verify snapshot exists
    result = await db.execute(
        select(GeometrySnapshot).where(GeometrySnapshot.id == snapshot_id)
    )
    snapshot = result.scalar_one_or_none()
    
    if not snapshot:
        raise HTTPException(status_code=404, detail="Geometry snapshot not found")
    
    # Run spatial checks
    geometry_service = GeometryService(db)
    checks = await geometry_service.perform_spatial_checks(snapshot)
    
    # Save checks to database
    for check in checks:
        db.add(check)
    
    await db.commit()
    
    # Refresh to get IDs
    for check in checks:
        await db.refresh(check)
    
    return checks


@router.get("/spatial-checks/", response_model=List[SpatialCheckModel])
async def list_spatial_checks(
    dataset_id: Optional[UUID] = Query(None, description="Filter by dataset ID"),
    check_type: Optional[str] = Query(None, description="Filter by check type"),
    check_result: Optional[str] = Query(None, description="Filter by result: PASS, FAIL, WARNING"),
    skip: int = Query(0, description="Number of records to skip"),
    limit: int = Query(100, description="Maximum number of records to return"),
    db: AsyncSession = Depends(get_db)
):
    """
    List spatial checks with filtering and pagination.
    Useful for quality assurance dashboards.
    """
    
    query = select(SpatialCheck)
    
    # Apply filters
    conditions = []
    if dataset_id:
        conditions.append(SpatialCheck.dataset_id == dataset_id)
    if check_type:
        conditions.append(SpatialCheck.check_type == check_type)
    if check_result:
        conditions.append(SpatialCheck.check_result == check_result)
    
    if conditions:
        query = query.where(and_(*conditions))
    
    # Apply pagination
    query = query.offset(skip).limit(limit)
    
    result = await db.execute(query)
    checks = result.scalars().all()
    
    return checks


@router.get("/spatial-checks/stats")
async def get_spatial_check_stats(
    dataset_id: Optional[UUID] = Query(None, description="Filter by dataset ID"),
    db: AsyncSession = Depends(get_db)
):
    """
    Get statistics about spatial checks for dashboards.
    """
    
    from sqlalchemy import func
    
    # Base query
    query = select(
        SpatialCheck.check_type,
        SpatialCheck.check_result,
        func.count(SpatialCheck.id).label("count")
    )
    
    if dataset_id:
        query = query.where(SpatialCheck.dataset_id == dataset_id)
    
    query = query.group_by(SpatialCheck.check_type, SpatialCheck.check_result)
    
    result = await db.execute(query)
    rows = result.fetchall()
    
    # Organize results
    stats = {}
    for row in rows:
        check_type = row.check_type
        check_result = row.check_result
        count = row.count
        
        if check_type not in stats:
            stats[check_type] = {}
        
        stats[check_type][check_result] = count
    
    return {
        "dataset_id": dataset_id,
        "check_stats": stats
    }


@router.get("/snapshots/{snapshot_id}/spatial-checks", response_model=List[SpatialCheckModel])
async def get_snapshot_spatial_checks(
    snapshot_id: UUID,
    db: AsyncSession = Depends(get_db)
):
    """
    Get all spatial checks for a specific geometry snapshot.
    """
    
    result = await db.execute(
        select(SpatialCheck).where(SpatialCheck.snapshot_id == snapshot_id)
    )
    checks = result.scalars().all()
    
    return checks


@router.get("/topology/validate")
async def validate_dataset_topology(
    dataset_id: UUID,
    db: AsyncSession = Depends(get_db)
):
    """
    Run topology validation on an entire dataset.
    This would be expanded to include advanced topology checks.
    """
    
    # Verify dataset exists
    from ...database import Dataset
    result = await db.execute(
        select(Dataset).where(Dataset.id == dataset_id)
    )
    dataset = result.scalar_one_or_none()
    
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    # Get all snapshots for the dataset
    result = await db.execute(
        select(GeometrySnapshot).where(GeometrySnapshot.dataset_id == dataset_id)
    )
    snapshots = result.scalars().all()
    
    # Run topology checks
    geometry_service = GeometryService(db)
    total_checks = 0
    failed_checks = 0
    
    for snapshot in snapshots:
        checks = await geometry_service.perform_spatial_checks(snapshot)
        
        for check in checks:
            db.add(check)
            total_checks += 1
            if check.check_result == "FAIL":
                failed_checks += 1
    
    await db.commit()
    
    return {
        "dataset_id": dataset_id,
        "total_geometries": len(snapshots),
        "total_checks": total_checks,
        "failed_checks": failed_checks,
        "success_rate": (total_checks - failed_checks) / total_checks if total_checks > 0 else 1.0
    } 