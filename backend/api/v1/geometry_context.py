"""
Geometry Context API
Handles buffer-based geometry context queries for spatial analysis
"""

from fastapi import APIRouter, HTTPException, Depends
from typing import List, Optional
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from database import get_db


class GeometryContextRequest(BaseModel):
    geometry_id: str
    buffer_distance_meters: int = 500
    max_context_geometries: int = 50


class GeometryContextItem(BaseModel):
    geometry_id: str
    geometry: dict  # GeoJSON geometry
    attributes: dict
    is_primary: bool
    distance_meters: Optional[float] = None


class GeometryContextResponse(BaseModel):
    geometries: List[GeometryContextItem]
    buffer_geometry: dict  # GeoJSON of the buffer polygon
    total_found: int


router = APIRouter()


@router.post("/geometry-context", response_model=GeometryContextResponse)
async def get_geometry_context(
    request: GeometryContextRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Get geometries within a buffer distance of a target geometry.
    
    This is used to provide spatial context for flagged geometries,
    helping users understand why a geometry was flagged (intersections, overlaps, etc.)
    """
    
    try:
        # First, get the primary geometry  
        primary_query = text("""
        SELECT 
            id,
            ST_AsGeoJSON(geometry)::json as geometry,
            attributes
        FROM geometry_snapshots 
        WHERE id = :geometry_id
        """)
        
        primary_result = await db.execute(primary_query, {"geometry_id": request.geometry_id})
        primary_row = primary_result.fetchone()
        if not primary_row:
            raise HTTPException(status_code=404, detail=f"Geometry {request.geometry_id} not found")
        
        # Create buffer and find intersecting geometries using PostGIS
        context_query = text("""
        WITH target_geometry AS (
            SELECT geometry 
            FROM geometry_snapshots 
            WHERE id = :geometry_id
        ),
        buffer_area AS (
            SELECT ST_Buffer(ST_Transform(geometry, 3857), :buffer_meters) as buffer_geom
            FROM target_geometry
        )
        SELECT 
            g.id,
            ST_AsGeoJSON(g.geometry)::json as geometry,
            g.attributes,
            ST_Distance(
                ST_Transform(g.geometry, 3857), 
                ST_Transform(tg.geometry, 3857)
            ) as distance_meters
        FROM geometry_snapshots g, target_geometry tg, buffer_area ba
        WHERE g.id != :geometry_id  -- Exclude the primary geometry
        AND ST_Intersects(ST_Transform(g.geometry, 3857), ba.buffer_geom)
        ORDER BY ST_Distance(
            ST_Transform(g.geometry, 3857), 
            ST_Transform(tg.geometry, 3857)
        )
        LIMIT :max_results
        """)
        
        context_results = await db.execute(
            context_query, 
            {
                "geometry_id": request.geometry_id,
                "buffer_meters": request.buffer_distance_meters,
                "max_results": request.max_context_geometries
            }
        )
        
        # Get the buffer geometry for visualization
        buffer_query = text("""
        SELECT ST_AsGeoJSON(
            ST_Transform(
                ST_Buffer(ST_Transform(geometry, 3857), :buffer_meters), 
                4326
            )
        )::json as buffer_geometry
        FROM geometry_snapshots 
        WHERE id = :geometry_id
        """)
        
        buffer_result = await db.execute(
            buffer_query, 
            {
                "geometry_id": request.geometry_id,
                "buffer_meters": request.buffer_distance_meters
            }
        )
        buffer_row = buffer_result.fetchone()
        
        # Build response
        geometries = [
            GeometryContextItem(
                geometry_id=str(primary_row.id),
                geometry=primary_row.geometry,
                attributes=primary_row.attributes or {},
                is_primary=True,
                distance_meters=0.0
            )
        ]
        
        # Add context geometries
        context_rows = context_results.fetchall()
        for row in context_rows:
            geometries.append(
                GeometryContextItem(
                    geometry_id=str(row.id),
                    geometry=row.geometry,
                    attributes=row.attributes or {},
                    is_primary=False,
                    distance_meters=float(row.distance_meters) if row.distance_meters else None
                )
            )
        
        return GeometryContextResponse(
            geometries=geometries,
            buffer_geometry=buffer_row.buffer_geometry if buffer_row else None,
            total_found=len(context_rows)
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting geometry context: {str(e)}")


@router.get("/geometry-context/{geometry_id}")
async def get_geometry_context_simple(
    geometry_id: str,
    buffer_meters: int = 500,
    max_results: int = 50,
    db: AsyncSession = Depends(get_db)
):
    """
    Simple GET endpoint for geometry context (alternative to POST)
    """
    request = GeometryContextRequest(
        geometry_id=geometry_id,
        buffer_distance_meters=buffer_meters,
        max_context_geometries=max_results
    )
    
    return await get_geometry_context(request, db) 