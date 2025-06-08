"""
Geometry Service - Core spatial operations and diff detection
Implements advanced geometry comparison using PostGIS spatial functions
"""

import hashlib
import logging
from typing import List, Dict, Any, Optional, Tuple
from uuid import UUID
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text, func
from sqlalchemy.orm import selectinload
from geoalchemy2 import functions as spatial_func
from geoalchemy2.elements import WKTElement
import geopandas as gpd
import pandas as pd
from shapely.geometry import shape
from shapely import wkb

from ..database import GeometrySnapshot, GeometryDiff, SpatialCheck, Dataset
from ..models import GeometryImportResponse
from ..config import settings

logger = logging.getLogger("dbfriend-cloud.geometry_service")


class GeometryService:
    """Service for handling geometry operations and diff detection."""
    
    def __init__(self, db_session: AsyncSession):
        self.db = db_session
    
    @staticmethod
    def compute_geometry_hash(geometry_wkb: bytes) -> str:
        """Compute MD5 hash of geometry WKB for comparison."""
        return hashlib.md5(geometry_wkb).hexdigest()
    
    @staticmethod
    def compute_attributes_hash(attributes: Dict[str, Any]) -> str:
        """Compute MD5 hash of feature attributes."""
        if not attributes:
            return hashlib.md5("".encode()).hexdigest()
        
        # Sort attributes for consistent hashing
        sorted_attrs = sorted(attributes.items())
        attrs_string = "|".join([f"{k}:{v}" for k, v in sorted_attrs])
        return hashlib.md5(attrs_string.encode('utf-8')).hexdigest()
    
    @staticmethod
    def compute_composite_hash(geometry_hash: str, attributes_hash: str) -> str:
        """Compute composite hash combining geometry and attributes."""
        composite_string = f"geom:{geometry_hash}|attrs:{attributes_hash}"
        return hashlib.md5(composite_string.encode('utf-8')).hexdigest()
    
    async def import_geometries_from_external_source(
        self, 
        dataset: Dataset,
        force_reimport: bool = False
    ) -> GeometryImportResponse:
        """
        Import geometries from external PostgreSQL source and detect changes.
        This is the core diff detection functionality.
        """
        start_time = datetime.utcnow()
        
        try:
            # Build connection to external data source
            # Note: In production, this would use connection pooling and proper security
            external_query = f"""
                SELECT 
                    *,
                    ST_AsBinary({dataset.geometry_column}) as geometry_wkb,
                    MD5(ST_AsBinary({dataset.geometry_column})) as geometry_hash
                FROM {dataset.schema_name}.{dataset.table_name}
                WHERE {dataset.geometry_column} IS NOT NULL
            """
            
            # Execute query against external source
            # For prototype, we'll simulate this with a direct query
            result = await self.db.execute(text(external_query))
            external_rows = result.fetchall()
            
            snapshots_created = 0
            diffs_detected = 0
            
            # Get existing snapshots for comparison
            existing_snapshots = await self._get_existing_snapshots(dataset.id)
            existing_hashes = {snap.composite_hash: snap for snap in existing_snapshots}
            
            # Process each external geometry
            for row in external_rows:
                # Extract geometry and attributes
                geometry_wkb = row.geometry_wkb
                geometry_hash = row.geometry_hash
                
                # Build attributes dict (exclude geometry columns)
                attributes = {}
                for key, value in row._mapping.items():
                    if key not in [dataset.geometry_column, 'geometry_wkb', 'geometry_hash']:
                        attributes[key] = value
                
                attributes_hash = self.compute_attributes_hash(attributes)
                composite_hash = self.compute_composite_hash(geometry_hash, attributes_hash)
                
                # Check if this is a new or changed geometry
                if composite_hash not in existing_hashes:
                    # Create new snapshot
                    snapshot = GeometrySnapshot(
                        dataset_id=dataset.id,
                        source_id=attributes.get('id') or attributes.get('gid'),
                        geometry_hash=geometry_hash,
                        attributes_hash=attributes_hash,
                        composite_hash=composite_hash,
                        geometry=WKTElement(f"SRID=4326;{wkb.loads(geometry_wkb).wkt}"),
                        attributes=attributes
                    )
                    self.db.add(snapshot)
                    snapshots_created += 1
                    
                    # Determine diff type and create diff record
                    diff_type = await self._determine_diff_type(
                        geometry_hash, attributes_hash, existing_snapshots
                    )
                    
                    diff = await self._create_geometry_diff(
                        dataset.id, diff_type, snapshot, existing_snapshots
                    )
                    if diff:
                        diffs_detected += 1
            
            # Check for deleted geometries
            current_hashes = {self.compute_composite_hash(row.geometry_hash, 
                             self.compute_attributes_hash({k: v for k, v in row._mapping.items() 
                                                          if k not in [dataset.geometry_column, 'geometry_wkb', 'geometry_hash']}))
                             for row in external_rows}
            
            for existing_hash, existing_snapshot in existing_hashes.items():
                if existing_hash not in current_hashes:
                    # Create deletion diff
                    diff = GeometryDiff(
                        dataset_id=dataset.id,
                        diff_type="DELETED",
                        old_snapshot_id=existing_snapshot.id,
                        geometry_changed=True,
                        confidence_score=1.0
                    )
                    self.db.add(diff)
                    diffs_detected += 1
            
            await self.db.commit()
            
            duration = (datetime.utcnow() - start_time).total_seconds()
            
            return GeometryImportResponse(
                dataset_id=dataset.id,
                snapshots_created=snapshots_created,
                diffs_detected=diffs_detected,
                import_duration_seconds=duration,
                status="SUCCESS"
            )
            
        except Exception as e:
            await self.db.rollback()
            logger.error(f"Error importing geometries for dataset {dataset.id}: {e}")
            
            duration = (datetime.utcnow() - start_time).total_seconds()
            return GeometryImportResponse(
                dataset_id=dataset.id,
                import_duration_seconds=duration,
                status="FAILED",
                error_message=str(e)
            )
    
    async def _get_existing_snapshots(self, dataset_id: UUID) -> List[GeometrySnapshot]:
        """Get all existing snapshots for a dataset."""
        result = await self.db.execute(
            select(GeometrySnapshot).where(GeometrySnapshot.dataset_id == dataset_id)
        )
        return result.scalars().all()
    
    async def _determine_diff_type(
        self, 
        geometry_hash: str, 
        attributes_hash: str, 
        existing_snapshots: List[GeometrySnapshot]
    ) -> str:
        """Determine the type of diff based on hash comparison."""
        
        # Check if geometry exists with different attributes
        for snapshot in existing_snapshots:
            if snapshot.geometry_hash == geometry_hash:
                if snapshot.attributes_hash != attributes_hash:
                    return "UPDATED"  # Same geometry, different attributes
                else:
                    return "DUPLICATE"  # Exact match (shouldn't happen due to composite hash check)
        
        # New geometry
        return "NEW"
    
    async def _create_geometry_diff(
        self,
        dataset_id: UUID,
        diff_type: str,
        new_snapshot: GeometrySnapshot,
        existing_snapshots: List[GeometrySnapshot]
    ) -> Optional[GeometryDiff]:
        """Create a geometry diff record."""
        
        old_snapshot_id = None
        geometry_changed = True
        attributes_changed = False
        confidence_score = 1.0
        
        if diff_type == "UPDATED":
            # Find the old snapshot with same geometry but different attributes
            for snapshot in existing_snapshots:
                if snapshot.geometry_hash == new_snapshot.geometry_hash:
                    old_snapshot_id = snapshot.id
                    geometry_changed = False
                    attributes_changed = True
                    break
        
        diff = GeometryDiff(
            dataset_id=dataset_id,
            diff_type=diff_type,
            old_snapshot_id=old_snapshot_id,
            new_snapshot_id=new_snapshot.id,
            geometry_changed=geometry_changed,
            attributes_changed=attributes_changed,
            confidence_score=confidence_score
        )
        
        self.db.add(diff)
        return diff
    
    async def perform_spatial_checks(
        self, 
        snapshot: GeometrySnapshot
    ) -> List[SpatialCheck]:
        """Perform various spatial quality checks on a geometry."""
        checks = []
        
        # Validity check using PostGIS
        validity_result = await self.db.execute(
            select(spatial_func.ST_IsValid(snapshot.geometry))
        )
        is_valid = validity_result.scalar()
        
        validity_check = SpatialCheck(
            dataset_id=snapshot.dataset_id,
            snapshot_id=snapshot.id,
            check_type="VALIDITY",
            check_result="PASS" if is_valid else "FAIL",
            error_message=None if is_valid else "Invalid geometry detected"
        )
        checks.append(validity_check)
        
        # Topology checks would go here
        # - Self-intersection check
        # - Ring orientation check
        # - Minimum area/length checks
        
        # Duplicate geometry check
        duplicate_result = await self.db.execute(
            select(func.count(GeometrySnapshot.id))
            .where(
                GeometrySnapshot.dataset_id == snapshot.dataset_id,
                GeometrySnapshot.geometry_hash == snapshot.geometry_hash,
                GeometrySnapshot.id != snapshot.id
            )
        )
        duplicate_count = duplicate_result.scalar()
        
        if duplicate_count > 0:
            duplicate_check = SpatialCheck(
                dataset_id=snapshot.dataset_id,
                snapshot_id=snapshot.id,
                check_type="DUPLICATE",
                check_result="WARNING",
                error_message=f"Found {duplicate_count} duplicate geometries",
                error_details={"duplicate_count": duplicate_count}
            )
            checks.append(duplicate_check)
        
        return checks
    
    async def get_geometry_as_geojson(self, snapshot_id: UUID) -> Optional[Dict[str, Any]]:
        """Get geometry as GeoJSON for frontend visualization."""
        try:
            result = await self.db.execute(
                select(spatial_func.ST_AsGeoJSON(GeometrySnapshot.geometry))
                .where(GeometrySnapshot.id == snapshot_id)
            )
            geojson_text = result.scalar()
            
            if geojson_text:
                import json
                return json.loads(geojson_text)
            return None
            
        except Exception as e:
            logger.error(f"Error converting geometry to GeoJSON: {e}")
            return None
    
    async def calculate_geometry_difference(
        self, 
        old_snapshot_id: UUID, 
        new_snapshot_id: UUID
    ) -> Optional[Dict[str, Any]]:
        """Calculate spatial difference between two geometries using PostGIS."""
        try:
            result = await self.db.execute(
                text("""
                    SELECT 
                        ST_AsGeoJSON(ST_Difference(new_geom.geometry, old_geom.geometry)) as added_area,
                        ST_AsGeoJSON(ST_Difference(old_geom.geometry, new_geom.geometry)) as removed_area,
                        ST_Area(ST_Difference(new_geom.geometry, old_geom.geometry)) as added_area_size,
                        ST_Area(ST_Difference(old_geom.geometry, new_geom.geometry)) as removed_area_size
                    FROM 
                        geometry_snapshots old_geom,
                        geometry_snapshots new_geom
                    WHERE 
                        old_geom.id = :old_id 
                        AND new_geom.id = :new_id
                """),
                {"old_id": old_snapshot_id, "new_id": new_snapshot_id}
            )
            
            row = result.fetchone()
            if row:
                import json
                return {
                    "added_area": json.loads(row.added_area) if row.added_area else None,
                    "removed_area": json.loads(row.removed_area) if row.removed_area else None,
                    "added_area_size": float(row.added_area_size) if row.added_area_size else 0.0,
                    "removed_area_size": float(row.removed_area_size) if row.removed_area_size else 0.0
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error calculating geometry difference: {e}")
            return None 