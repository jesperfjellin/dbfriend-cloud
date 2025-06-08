"""
Geometry Diff API endpoints
Git-style accept/reject queue for geometry changes
"""

from typing import List, Optional
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, desc
from sqlalchemy.orm import selectinload

from ...database import get_db, GeometryDiff, GeometrySnapshot
from ...models import (
    GeometryDiff as GeometryDiffModel,
    GeometryDiffDetails,
    DiffReview,
    DiffBatch
)
from ...services.geometry_service import GeometryService

router = APIRouter()


@router.get("/", response_model=List[GeometryDiffModel])
async def list_diffs(
    dataset_id: Optional[UUID] = Query(None, description="Filter by dataset ID"),
    status: Optional[str] = Query(None, description="Filter by status: PENDING, ACCEPTED, REJECTED"),
    diff_type: Optional[str] = Query(None, description="Filter by diff type: NEW, UPDATED, DELETED"),
    skip: int = Query(0, description="Number of records to skip"),
    limit: int = Query(100, description="Maximum number of records to return"),
    db: AsyncSession = Depends(get_db)
):
    """
    List geometry diffs with filtering and pagination.
    This is the main queue view for reviewers.
    """
    
    query = select(GeometryDiff)
    
    # Apply filters
    conditions = []
    if dataset_id:
        conditions.append(GeometryDiff.dataset_id == dataset_id)
    if status:
        conditions.append(GeometryDiff.status == status)
    if diff_type:
        conditions.append(GeometryDiff.diff_type == diff_type)
    
    if conditions:
        query = query.where(and_(*conditions))
    
    # Order by creation date (newest first)
    query = query.order_by(desc(GeometryDiff.created_at))
    
    # Apply pagination
    query = query.offset(skip).limit(limit)
    
    result = await db.execute(query)
    diffs = result.scalars().all()
    
    return diffs


@router.get("/{diff_id}", response_model=GeometryDiffDetails)
async def get_diff_details(
    diff_id: UUID,
    db: AsyncSession = Depends(get_db)
):
    """
    Get detailed information about a specific diff, including geometry data.
    This is used for the detailed diff viewer in the frontend.
    """
    
    # Get the diff with related snapshots
    result = await db.execute(
        select(GeometryDiff)
        .options(
            selectinload(GeometryDiff.old_snapshot),
            selectinload(GeometryDiff.new_snapshot)
        )
        .where(GeometryDiff.id == diff_id)
    )
    diff = result.scalar_one_or_none()
    
    if not diff:
        raise HTTPException(status_code=404, detail="Diff not found")
    
    # Convert to detailed model
    geometry_service = GeometryService(db)
    
    # Get GeoJSON representations for frontend
    old_geometry_geojson = None
    new_geometry_geojson = None
    
    if diff.old_snapshot_id:
        old_geometry_geojson = await geometry_service.get_geometry_as_geojson(diff.old_snapshot_id)
    
    if diff.new_snapshot_id:
        new_geometry_geojson = await geometry_service.get_geometry_as_geojson(diff.new_snapshot_id)
    
    # Create detailed response
    diff_details = GeometryDiffDetails(
        **diff.__dict__,
        old_geometry_geojson=old_geometry_geojson,
        new_geometry_geojson=new_geometry_geojson
    )
    
    return diff_details


@router.put("/{diff_id}/review", response_model=GeometryDiffModel)
async def review_diff(
    diff_id: UUID,
    review: DiffReview,
    db: AsyncSession = Depends(get_db)
):
    """
    Review a diff - accept or reject it.
    This is the core accept/reject functionality.
    """
    
    if review.status not in ["ACCEPTED", "REJECTED"]:
        raise HTTPException(
            status_code=400, 
            detail="Status must be either ACCEPTED or REJECTED"
        )
    
    # Get the diff
    result = await db.execute(
        select(GeometryDiff).where(GeometryDiff.id == diff_id)
    )
    diff = result.scalar_one_or_none()
    
    if not diff:
        raise HTTPException(status_code=404, detail="Diff not found")
    
    if diff.status != "PENDING":
        raise HTTPException(
            status_code=400, 
            detail=f"Diff has already been reviewed (status: {diff.status})"
        )
    
    # Update diff status
    from datetime import datetime
    diff.status = review.status
    diff.reviewed_at = datetime.utcnow()
    diff.reviewed_by = review.reviewed_by
    
    await db.commit()
    await db.refresh(diff)
    
    return diff


@router.post("/batch-review")
async def batch_review_diffs(
    batch: DiffBatch,
    db: AsyncSession = Depends(get_db)
):
    """
    Review multiple diffs in batch - useful for bulk operations.
    """
    
    if batch.action not in ["ACCEPT", "REJECT"]:
        raise HTTPException(
            status_code=400, 
            detail="Action must be either ACCEPT or REJECT"
        )
    
    # Convert action to status
    status = "ACCEPTED" if batch.action == "ACCEPT" else "REJECTED"
    
    # Get all requested diffs
    result = await db.execute(
        select(GeometryDiff).where(GeometryDiff.id.in_(batch.diff_ids))
    )
    diffs = result.scalars().all()
    
    if len(diffs) != len(batch.diff_ids):
        raise HTTPException(
            status_code=404, 
            detail="One or more diffs not found"
        )
    
    # Check that all diffs are pending
    non_pending = [d for d in diffs if d.status != "PENDING"]
    if non_pending:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot review {len(non_pending)} diffs that are not pending"
        )
    
    # Update all diffs
    from datetime import datetime
    updated_count = 0
    
    for diff in diffs:
        diff.status = status
        diff.reviewed_at = datetime.utcnow()
        diff.reviewed_by = batch.reviewed_by
        updated_count += 1
    
    await db.commit()
    
    return {
        "message": f"Successfully {batch.action.lower()}ed {updated_count} diffs",
        "updated_count": updated_count,
        "status": status
    }


@router.get("/{diff_id}/spatial-difference")
async def get_spatial_difference(
    diff_id: UUID,
    db: AsyncSession = Depends(get_db)
):
    """
    Calculate and return the spatial difference between old and new geometries.
    This provides detailed geometric analysis for the diff.
    """
    
    # Get the diff
    result = await db.execute(
        select(GeometryDiff).where(GeometryDiff.id == diff_id)
    )
    diff = result.scalar_one_or_none()
    
    if not diff:
        raise HTTPException(status_code=404, detail="Diff not found")
    
    if not diff.old_snapshot_id or not diff.new_snapshot_id:
        raise HTTPException(
            status_code=400, 
            detail="Cannot calculate spatial difference for diffs without both old and new geometries"
        )
    
    # Calculate spatial difference using PostGIS
    geometry_service = GeometryService(db)
    
    difference = await geometry_service.calculate_geometry_difference(
        diff.old_snapshot_id, diff.new_snapshot_id
    )
    
    if difference is None:
        raise HTTPException(
            status_code=500, 
            detail="Error calculating spatial difference"
        )
    
    return {
        "diff_id": diff_id,
        "spatial_difference": difference
    }


@router.get("/pending/count")
async def get_pending_count(
    dataset_id: Optional[UUID] = Query(None, description="Filter by dataset ID"),
    db: AsyncSession = Depends(get_db)
):
    """
    Get count of pending diffs - useful for dashboard indicators.
    """
    
    from sqlalchemy import func
    
    query = select(func.count(GeometryDiff.id)).where(GeometryDiff.status == "PENDING")
    
    if dataset_id:
        query = query.where(GeometryDiff.dataset_id == dataset_id)
    
    result = await db.execute(query)
    count = result.scalar()
    
    return {
        "pending_count": count or 0,
        "dataset_id": dataset_id
    } 