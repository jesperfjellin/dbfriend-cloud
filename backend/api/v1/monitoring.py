"""
Monitoring API endpoints
Admin endpoints for system health and storage monitoring
"""

from typing import List, Dict, Any
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from database import get_db

router = APIRouter()


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