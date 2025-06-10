"""
Monitoring API endpoints
Admin endpoints for system health and storage monitoring
"""

from typing import List, Dict, Any, Optional
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text, select, func
from pydantic import BaseModel

from database import get_db, Dataset, GeometrySnapshot
from services.geometry_service import GeometryService

router = APIRouter()

# In-memory status tracking for quality checks
# In production, this would be stored in Redis or database
QUALITY_CHECK_STATUS = {}  # dataset_id -> {"status": "running/completed/failed", "started_at": datetime, ...}


class QualityCheckRequest(BaseModel):
    dataset_id: UUID


class QualityCheckProgress(BaseModel):
    current: int
    total: int
    phase: str
    percentage: Optional[float] = None

class QualityCheckStatus(BaseModel):
    dataset_id: UUID
    dataset_name: str
    status: str  # "idle", "running", "completed", "failed"
    snapshot_count: int
    snapshots_complete: bool
    last_check_at: Optional[str] = None
    check_results: Optional[Dict[str, int]] = None
    error_message: Optional[str] = None
    progress: Optional[QualityCheckProgress] = None


class DatasetMonitoringStatus(BaseModel):
    dataset_id: UUID
    dataset_name: str
    connection_status: str
    snapshots_complete: bool
    snapshot_count: int
    last_change_check: Optional[str] = None
    last_quality_check: Optional[str] = None
    quality_check_status: str
    pending_diffs: int


@router.get("/storage-usage")
async def get_storage_usage(db: AsyncSession = Depends(get_db)):
    """
    Get current database storage usage by table.
    Useful for monitoring storage costs and optimization impact.
    """
    
    storage_query = text("""
        SELECT 
            schemaname,
            tablename,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
            pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size,
            pg_size_pretty(pg_indexes_size(schemaname||'.'||tablename)) as index_size,
            pg_total_relation_size(schemaname||'.'||tablename) as total_bytes
        FROM pg_tables 
        WHERE schemaname = 'public'
        ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
    """)
    
    result = await db.execute(storage_query)
    tables = result.fetchall()
    
    # Calculate total database size
    total_query = text("""
        SELECT pg_size_pretty(pg_database_size(current_database())) as total_db_size,
               pg_database_size(current_database()) as total_db_bytes
    """)
    
    total_result = await db.execute(total_query)
    total_info = total_result.fetchone()
    
    return {
        "database_total": {
            "size_pretty": total_info.total_db_size,
            "size_bytes": total_info.total_db_bytes
        },
        "tables": [
            {
                "schema": row.schemaname,
                "table": row.tablename,
                "total_size": row.total_size,
                "table_size": row.table_size,
                "index_size": row.index_size,
                "total_bytes": row.total_bytes
            }
            for row in tables
        ]
    }


@router.get("/storage-by-dataset")
async def get_storage_by_dataset(db: AsyncSession = Depends(get_db)):
    """
    Get storage usage broken down by dataset.
    Critical for understanding per-tenant costs.
    """
    
    dataset_storage_query = text("""
        SELECT 
            d.id as dataset_id,
            d.name as dataset_name,
            COUNT(gs.id) as snapshot_count,
            COUNT(gd.id) as diff_count,
            COUNT(sc.id) as check_count,
            -- Estimate storage usage (based on real data: ~1.3KB per snapshot)
            COUNT(gs.id) * 1300 as estimated_snapshot_bytes,  -- ~1.3KB per snapshot (empirical)
            COUNT(gd.id) * 300 as estimated_diff_bytes        -- ~300B per diff estimate
        FROM datasets d
        LEFT JOIN geometry_snapshots gs ON d.id = gs.dataset_id
        LEFT JOIN geometry_diffs gd ON d.id = gd.dataset_id  
        LEFT JOIN spatial_checks sc ON d.id = sc.dataset_id
        WHERE d.is_active = true
        GROUP BY d.id, d.name
        ORDER BY COUNT(gs.id) DESC
    """)
    
    result = await db.execute(dataset_storage_query)
    datasets = result.fetchall()
    
    return {
        "datasets": [
            {
                "dataset_id": str(row.dataset_id),
                "dataset_name": row.dataset_name,
                "snapshot_count": row.snapshot_count or 0,
                "diff_count": row.diff_count or 0,
                "check_count": row.check_count or 0,
                "estimated_storage_bytes": (row.estimated_snapshot_bytes or 0) + (row.estimated_diff_bytes or 0),
                "estimated_storage_mb": round(((row.estimated_snapshot_bytes or 0) + (row.estimated_diff_bytes or 0)) / 1024 / 1024, 2)
            }
            for row in datasets
        ]
    }


@router.post("/reset-monitoring")
async def reset_monitoring_data(db: AsyncSession = Depends(get_db)):
    """
    Reset monitoring data while preserving dataset connections.
    This is useful for administrators who want to clear accumulated monitoring
    data without restarting the service or losing connection configurations.
    """
    
    from database import _smart_restart_reset
    
    try:
        async with db.begin():
            # Use the same smart reset logic as startup
            await _smart_restart_reset(db.connection())
            
        return {
            "status": "success",
            "message": "Monitoring data reset successfully",
            "preserved": "Dataset connections and configurations",
            "cleared": "Snapshots, diffs, spatial checks, and monitoring state"
        }
        
    except Exception as e:
        return {
            "status": "error", 
            "message": f"Failed to reset monitoring data: {str(e)}"
        }


@router.get("/health")
async def get_system_health(db: AsyncSession = Depends(get_db)):
    """
    Basic system health check with key metrics.
    """
    
    # Count records in key tables
    counts_query = text("""
        SELECT 
            (SELECT COUNT(*) FROM datasets WHERE is_active = true) as active_datasets,
            (SELECT COUNT(*) FROM geometry_snapshots) as total_snapshots,
            (SELECT COUNT(*) FROM geometry_diffs WHERE status = 'PENDING') as pending_diffs,
            (SELECT COUNT(*) FROM spatial_checks WHERE check_result = 'FAIL') as failed_checks
    """)
    
    result = await db.execute(counts_query)
    counts = result.fetchone()
    
    return {
        "status": "healthy",
        "metrics": {
            "active_datasets": counts.active_datasets,
            "total_snapshots": counts.total_snapshots,
            "pending_diffs": counts.pending_diffs,
            "failed_checks": counts.failed_checks
        }
    }


@router.get("/datasets/status", response_model=List[DatasetMonitoringStatus])
async def get_datasets_monitoring_status(db: AsyncSession = Depends(get_db)):
    """
    Get monitoring status for all datasets including snapshot completion status.
    Used by frontend to show which datasets are ready for quality checks.
    """
    
    datasets_query = text("""
        SELECT 
            d.id as dataset_id,
            d.name as dataset_name,
            d.connection_status,
            d.last_check_at,
            COUNT(gs.id) as snapshot_count,
            -- Check if snapshots are "complete" (dataset has been checked at least once)
            CASE 
                WHEN d.last_check_at IS NOT NULL THEN true 
                ELSE false 
            END as snapshots_complete,
            COUNT(gd.id) FILTER (WHERE gd.status = 'PENDING') as pending_diffs,
            -- Get last quality check from spatial_checks table
            MAX(sc.created_at) as last_quality_check
        FROM datasets d
        LEFT JOIN geometry_snapshots gs ON d.id = gs.dataset_id
        LEFT JOIN geometry_diffs gd ON d.id = gd.dataset_id
        LEFT JOIN spatial_checks sc ON d.id = sc.dataset_id
        WHERE d.is_active = true
        GROUP BY d.id, d.name, d.connection_status, d.last_check_at
        ORDER BY d.name
    """)
    
    result = await db.execute(datasets_query)
    datasets = result.fetchall()
    
    return [
        DatasetMonitoringStatus(
            dataset_id=row.dataset_id,
            dataset_name=row.dataset_name,
            connection_status=row.connection_status,
            snapshots_complete=row.snapshots_complete,
            snapshot_count=row.snapshot_count or 0,
            last_change_check=row.last_check_at.isoformat() if row.last_check_at else None,
            last_quality_check=row.last_quality_check.isoformat() if row.last_quality_check else None,
            quality_check_status="idle",  # Will be enhanced with actual status tracking
            pending_diffs=row.pending_diffs or 0
        )
        for row in datasets
    ]


@router.post("/datasets/{dataset_id}/quality-checks/start")
async def start_quality_checks(
    dataset_id: UUID,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db)
):
    """
    Start quality checks for a specific dataset.
    Only allowed if snapshots are complete.
    """
    
    # Get dataset and verify it exists and snapshots are complete
    dataset_result = await db.execute(
        select(Dataset).where(Dataset.id == dataset_id, Dataset.is_active == True)
    )
    dataset = dataset_result.scalar_one_or_none()
    
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found or inactive")
    
    # Check if snapshots are complete (dataset has been checked at least once)
    if dataset.last_check_at is None:
        raise HTTPException(
            status_code=400, 
            detail="Cannot run quality checks: Initial snapshot baseline not complete. Please wait for change detection to finish."
        )
    
    # Check if quality checks are already running
    if str(dataset_id) in QUALITY_CHECK_STATUS and QUALITY_CHECK_STATUS[str(dataset_id)]["status"] == "running":
        raise HTTPException(
            status_code=400,
            detail="Quality checks are already running for this dataset"
        )
    
    # Set status to running with initial progress
    from datetime import datetime, timezone
    QUALITY_CHECK_STATUS[str(dataset_id)] = {
        "status": "running",
        "started_at": datetime.now(timezone.utc),
        "dataset_name": dataset.name,
        "progress": {
            "current": 0,
            "total": 0,
            "phase": "initializing"
        }
    }
    
    # Run quality checks in background
    background_tasks.add_task(run_quality_checks_background, dataset_id)
    
    return {
        "status": "started",
        "message": f"Quality checks started for dataset {dataset.name}",
        "dataset_id": str(dataset_id)
    }


@router.get("/datasets/{dataset_id}/quality-checks/status", response_model=QualityCheckStatus)
async def get_quality_check_status(
    dataset_id: UUID,
    db: AsyncSession = Depends(get_db)
):
    """
    Get the current status of quality checks for a dataset.
    """
    
    # Get dataset info
    dataset_result = await db.execute(
        select(Dataset).where(Dataset.id == dataset_id)
    )
    dataset = dataset_result.scalar_one_or_none()
    
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    # Get snapshot count
    snapshot_count_result = await db.execute(
        select(func.count(GeometrySnapshot.id)).where(GeometrySnapshot.dataset_id == dataset_id)
    )
    snapshot_count = snapshot_count_result.scalar() or 0
    
    # Get latest quality check results
    quality_check_query = text("""
        SELECT 
            check_type,
            check_result,
            COUNT(*) as count,
            MAX(created_at) as latest_check
        FROM spatial_checks 
        WHERE dataset_id = :dataset_id
        GROUP BY check_type, check_result
        ORDER BY latest_check DESC
    """)
    
    check_result = await db.execute(quality_check_query, {"dataset_id": dataset_id})
    check_rows = check_result.fetchall()
    
    # Build check results summary
    check_results = {}
    latest_check = None
    for row in check_rows:
        key = f"{row.check_type.lower()}_{row.check_result.lower()}"
        check_results[key] = row.count
        if latest_check is None or (row.latest_check and row.latest_check > latest_check):
            latest_check = row.latest_check
    
    # Get current status from tracking
    current_status = "idle"
    progress_data = None
    error_message = None
    
    if str(dataset_id) in QUALITY_CHECK_STATUS:
        status_info = QUALITY_CHECK_STATUS[str(dataset_id)]
        current_status = status_info["status"]
        
        # Extract progress information if available
        if "progress" in status_info:
            progress = status_info["progress"]
            percentage = (progress["current"] / progress["total"]) * 100 if progress["total"] > 0 else 0
            progress_data = QualityCheckProgress(
                current=progress["current"],
                total=progress["total"],
                phase=progress["phase"],
                percentage=round(percentage, 1)
            )
        
        # Extract error message if failed
        if "error" in status_info:
            error_message = status_info["error"]
        
        # For completed status, use tracking results if available and fresher
        if current_status == "completed" and "check_results" in status_info:
            # Use the fresh results from the completed task
            check_results = status_info["check_results"]
    
    return QualityCheckStatus(
        dataset_id=dataset_id,
        dataset_name=dataset.name,
        status=current_status,
        snapshot_count=snapshot_count,
        snapshots_complete=dataset.last_check_at is not None,
        last_check_at=latest_check.isoformat() if latest_check else None,
        check_results=check_results if check_results else None,
        error_message=error_message,
        progress=progress_data
    )


async def run_quality_checks_background(dataset_id: UUID):
    """
    Background task to run quality checks for a dataset.
    This runs independently from the API request.
    """
    import logging
    from datetime import datetime, timezone
    from database import AsyncSessionLocal
    
    logger = logging.getLogger("dbfriend-cloud.quality-checks")
    
    async with AsyncSessionLocal() as db:
        try:
            # Get dataset
            dataset_result = await db.execute(
                select(Dataset).where(Dataset.id == dataset_id)
            )
            dataset = dataset_result.scalar_one_or_none()
            
            if not dataset:
                logger.error(f"Dataset {dataset_id} not found for quality checks")
                return
            
            logger.info(f"🧪 Starting user-requested quality checks for dataset: {dataset.name}")
            
            # Define progress callback to update status
            def update_progress(current: int, total: int, phase: str):
                QUALITY_CHECK_STATUS[str(dataset_id)] = {
                    **QUALITY_CHECK_STATUS[str(dataset_id)],
                    "progress": {
                        "current": current,
                        "total": total,
                        "phase": phase
                    }
                }
            
            # Run quality checks with progress tracking
            geometry_service = GeometryService(db)
            check_results = await geometry_service.run_quality_checks(dataset, update_progress)
            
            if "error" not in check_results:
                total_checks = sum(v for k, v in check_results.items() if k.endswith('_checks'))
                failed_checks = check_results.get('failed_checks', 0)
                
                # Update status to completed
                QUALITY_CHECK_STATUS[str(dataset_id)] = {
                    "status": "completed",
                    "completed_at": datetime.now(timezone.utc),
                    "dataset_name": dataset.name,
                    "total_checks": total_checks,
                    "failed_checks": failed_checks,
                    "check_results": check_results  # Include the actual results
                }
                
                logger.info(f"✅ User-requested quality checks completed for {dataset.name}: "
                           f"{total_checks} checks run, {failed_checks} failed")
                
                # Clean up status after 5 minutes to prevent memory leaks
                import asyncio
                async def cleanup_status():
                    await asyncio.sleep(300)  # Wait 5 minutes
                    if str(dataset_id) in QUALITY_CHECK_STATUS:
                        logger.info(f"🧹 Cleaning up quality check status for dataset {dataset_id}")
                        del QUALITY_CHECK_STATUS[str(dataset_id)]
                
                # Schedule cleanup (fire and forget)
                asyncio.create_task(cleanup_status())
            else:
                # Update status to failed
                QUALITY_CHECK_STATUS[str(dataset_id)] = {
                    "status": "failed",
                    "failed_at": datetime.now(timezone.utc),
                    "dataset_name": dataset.name,
                    "error": check_results['error']
                }
                logger.error(f"❌ User-requested quality checks failed for {dataset.name}: {check_results['error']}")
                
        except Exception as e:
            # Update status to failed
            QUALITY_CHECK_STATUS[str(dataset_id)] = {
                "status": "failed",
                "failed_at": datetime.now(timezone.utc),
                "dataset_name": dataset.get('name', 'Unknown') if dataset else 'Unknown',
                "error": str(e)
            }
            logger.error(f"❌ Error in user-requested quality checks for dataset {dataset_id}: {e}") 