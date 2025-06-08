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
    DatasetConnectionTest, DatasetConnectionTestResponse,
    GeometryImportRequest, GeometryImportResponse, DatasetStats, DiffStats
)
from services.geometry_service import GeometryService

router = APIRouter()


@router.post("/test-connection", response_model=DatasetConnectionTestResponse)
async def test_connection(
    connection_test: DatasetConnectionTest
):
    """
    Test database connection and return available information.
    This is used by the frontend form to validate connections.
    """
    import asyncpg
    import asyncio
    
    try:
        # Build connection string
        connection_string = f"postgresql://{connection_test.username}:{connection_test.password}@{connection_test.host}:{connection_test.port}/{connection_test.database}"
        
        # Test basic connectivity
        conn = await asyncpg.connect(connection_string, ssl=connection_test.ssl_mode)
        
        try:
            # Test PostGIS availability
            postgis_version = None
            try:
                result = await conn.fetchval("SELECT PostGIS_Version()")
                postgis_version = result
            except:
                pass
            
            # Get user permissions
            permissions = []
            try:
                # Check if user can read from information_schema
                can_read_schema = await conn.fetchval("""
                    SELECT has_schema_privilege(current_user, 'information_schema', 'USAGE')
                """)
                if can_read_schema:
                    permissions.append("READ_SCHEMA")
                
                # Check basic table access (we'll test specific tables later)
                permissions.append("CONNECT")
                
            except Exception as e:
                # Basic connection works even if we can't check permissions
                permissions.append("CONNECT")
            
            # Get schema information
            schema_info = {}
            try:
                schemas = await conn.fetch("""
                    SELECT schema_name 
                    FROM information_schema.schemata 
                    WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                    ORDER BY schema_name
                """)
                schema_info["available_schemas"] = [row["schema_name"] for row in schemas]
            except:
                schema_info["available_schemas"] = []
            
            await conn.close()
            
            return DatasetConnectionTestResponse(
                success=True,
                message=f"Successfully connected to {connection_test.host}:{connection_test.port}/{connection_test.database}",
                postgis_version=postgis_version,
                permissions=permissions,
                schema_info=schema_info
            )
            
        except Exception as inner_e:
            await conn.close()
            return DatasetConnectionTestResponse(
                success=False,
                message=f"Connected to database but error during testing: {str(inner_e)}",
                permissions=["CONNECT"]
            )
            
    except Exception as e:
        return DatasetConnectionTestResponse(
            success=False,
            message=f"Connection failed: {str(e)}",
            permissions=[]
        )


@router.post("/", response_model=DatasetModel)
async def create_dataset(
    dataset: DatasetCreate,
    db: AsyncSession = Depends(get_db)
):
    """Create a new dataset for monitoring."""
    
    # Create dataset with individual connection fields
    # Note: In production, password would be encrypted before storage
    db_dataset = Dataset(
        name=dataset.name,
        description=dataset.description,
        host=dataset.host,
        port=dataset.port,
        database=dataset.database,
        schema_name=dataset.schema_name,
        table_name=dataset.table_name,
        geometry_column=dataset.geometry_column,
        check_interval_minutes=dataset.check_interval_minutes,
        ssl_mode=dataset.ssl_mode,
        read_only=dataset.read_only,
        # TODO: Encrypt and store credentials securely
        connection_string=f"postgresql://{dataset.username}:{dataset.password}@{dataset.host}:{dataset.port}/{dataset.database}"
    )
    
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
        from datetime import datetime, timezone
        dataset.last_check_at = datetime.now(timezone.utc)
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