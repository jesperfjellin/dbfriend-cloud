"""
Geometry Service - Core spatial operations and diff detection
Implements advanced geometry comparison using PostGIS spatial functions
"""

import hashlib
import logging
from typing import List, Dict, Any, Optional, Tuple
from uuid import UUID
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text, func
from sqlalchemy.orm import selectinload
from geoalchemy2 import functions as spatial_func
from geoalchemy2.elements import WKTElement
import geopandas as gpd
import pandas as pd
from shapely.geometry import shape
from shapely import wkb

from database import GeometrySnapshot, GeometryDiff, SpatialCheck, Dataset
from models import GeometryImportResponse
from config import settings
from .test_config import TestConfig

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
    
    @staticmethod
    def create_wkt_element(geometry_wkb: bytes, srid: int = 4326) -> WKTElement:
        """
        Create a WKTElement from WKB that handles 2D/3D/4D geometries safely.
        Returns WKTElement compatible with PostGIS.
        """
        try:
            geom = wkb.loads(geometry_wkb)
            # Let PostGIS handle the geometry as-is, don't force dimensions
            wkt_string = geom.wkt
            return WKTElement(f"SRID={srid};{wkt_string}")
        except Exception as e:
            logger.error(f"Error creating WKT from WKB: {e}")
            # Fallback: try to get basic geometry info
            return WKTElement(f"SRID={srid};POINT EMPTY")
    
    async def monitor_dataset_changes(
        self, 
        dataset: Dataset,
        force_reimport: bool = False
    ) -> GeometryImportResponse:
        """
        Monitor dataset for CHANGES only - separate from quality checks.
        Only flags geometries that pass the "problematic" threshold.
        """
        start_time = datetime.now(timezone.utc)
        
        try:
            import asyncpg
            
            # Connect to external PostGIS database (user's database)
            external_conn = await asyncpg.connect(dataset.connection_string)
            
            try:
                # Query external PostGIS database for current geometry state
                external_query = f"""
                    SELECT 
                        *,
                        ST_AsBinary({dataset.geometry_column}) as geometry_wkb,
                        MD5(ST_AsBinary({dataset.geometry_column})) as geometry_hash,
                        ST_IsValid({dataset.geometry_column}) as is_valid,
                        ST_IsValidReason({dataset.geometry_column}) as validity_reason,
                        ST_IsSimple({dataset.geometry_column}) as is_simple,
                        ST_Area({dataset.geometry_column}) as geom_area,
                        ST_Length({dataset.geometry_column}) as geom_length,
                        ST_NPoints({dataset.geometry_column}) as num_points,
                        ST_GeometryType({dataset.geometry_column}) as geom_type,
                        -- Ring orientation check (for polygons)
                        CASE 
                            WHEN ST_GeometryType({dataset.geometry_column}) LIKE '%Polygon%' 
                            THEN ST_IsPolygonCCW({dataset.geometry_column})
                            ELSE NULL 
                        END as is_ccw_oriented,
                        -- Self-intersection check (more detailed than just ST_IsValid)
                        ST_IsValid({dataset.geometry_column}) AND ST_IsSimple({dataset.geometry_column}) as is_topologically_clean,
                        -- Coordinate bounds checking
                        ST_XMin({dataset.geometry_column}) as min_x,
                        ST_XMax({dataset.geometry_column}) as max_x,
                        ST_YMin({dataset.geometry_column}) as min_y,
                        ST_YMax({dataset.geometry_column}) as max_y
                    FROM {dataset.schema_name}.{dataset.table_name}
                    WHERE {dataset.geometry_column} IS NOT NULL
                """
                
                # Execute query against external source
                external_rows = await external_conn.fetch(external_query)
                
                snapshots_created = 0
                diffs_detected = 0
                
                # Get existing snapshots for comparison (from our internal database)
                existing_snapshots = await self._get_existing_snapshots(dataset.id)
                existing_hashes = {snap.composite_hash: snap for snap in existing_snapshots}
                
                logger.info(f"Dataset {dataset.id}: Found {len(existing_snapshots)} existing snapshots")
                
                logger.info(f"Found {len(external_rows)} geometries in external database, {len(existing_snapshots)} existing snapshots")
                logger.debug(f"Existing composite hashes: {len(existing_hashes)} unique")
                
                # Check if this is the first time monitoring this dataset (baseline establishment)
                is_baseline_run = len(existing_snapshots) == 0
                
                if is_baseline_run:
                    logger.info(f"📊 BASELINE RUN: Establishing baseline for {len(external_rows)} geometries (no diffs will be created)")
                else:
                    logger.info(f"🔍 CHANGE DETECTION: Comparing {len(external_rows)} current vs {len(existing_snapshots)} baseline geometries")
                
                # Process each external geometry
                for row in external_rows:
                    # Extract geometry and attributes
                    geometry_wkb = row['geometry_wkb']
                    geometry_hash = row['geometry_hash']
                    is_valid = row['is_valid']
                    geom_area = row['geom_area'] or 0
                    
                    # Build attributes dict (exclude geometry columns)
                    attributes = {}
                    for key, value in row.items():
                        if key not in [dataset.geometry_column, 'geometry_wkb', 'geometry_hash', 'is_valid', 'geom_area']:
                            # Convert any special types to JSON-serializable
                            if value is not None:
                                attributes[key] = str(value) if not isinstance(value, (str, int, float, bool)) else value
                    
                    attributes_hash = self.compute_attributes_hash(attributes)
                    composite_hash = self.compute_composite_hash(geometry_hash, attributes_hash)
                    
                    # Check if this is a new or changed geometry
                    is_new_geometry = composite_hash not in existing_hashes
                    
                    if is_baseline_run:
                        # BASELINE RUN: Create snapshots for all geometries, no diffs
                        logger.debug(f"📊 Baseline geometry: composite={composite_hash[:8]}..., geom={geometry_hash[:8]}...")
                        try:
                            snapshot = GeometrySnapshot(
                                dataset_id=dataset.id,
                                source_id=str(attributes.get('id') or attributes.get('gid') or ''),
                                geometry_hash=geometry_hash,
                                attributes_hash=attributes_hash,
                                composite_hash=composite_hash,
                                geometry=self.create_wkt_element(geometry_wkb),
                                attributes=attributes
                            )
                            self.db.add(snapshot)
                            await self.db.flush()
                            snapshots_created += 1
                        except Exception as snapshot_error:
                            # Check if this is a dimension constraint violation
                            error_str = str(snapshot_error)
                            if "enforce_dims_geometry" in error_str or "violates check constraint" in error_str:
                                logger.warning(f"🔧 Detected PostGIS dimension constraint violation - auto-fixing...")
                                await self.db.rollback()
                                await self._ensure_mixed_dimension_support()
                                
                                # Retry the snapshot creation
                                try:
                                    snapshot = GeometrySnapshot(
                                        dataset_id=dataset.id,
                                        source_id=str(attributes.get('id') or attributes.get('gid') or ''),
                                        geometry_hash=geometry_hash,
                                        attributes_hash=attributes_hash,
                                        composite_hash=composite_hash,
                                        geometry=self.create_wkt_element(geometry_wkb),
                                        attributes=attributes
                                    )
                                    self.db.add(snapshot)
                                    await self.db.flush()
                                    snapshots_created += 1
                                    logger.info(f"✅ Successfully created snapshot after constraint fix")
                                except Exception as retry_error:
                                    logger.error(f"Failed to create snapshot even after constraint fix: {retry_error}")
                                    continue
                            else:
                                logger.error(f"Error creating baseline snapshot for geometry {geometry_hash}: {snapshot_error}")
                                continue
                    else:
                        # CHANGE DETECTION RUN: Only process actual changes
                        if is_new_geometry:
                            logger.info(f"🆕 NEW geometry detected: composite={composite_hash[:8]}..., geom={geometry_hash[:8]}..., attrs={attributes_hash[:8]}...")
                            
                            logger.debug(f"New geometry detected: {geometry_hash[:8]}... (problematic: {self._is_geometry_problematic(row)})")
                            
                            # ⚠️ THRESHOLD CHECK: Only flag if geometry is "problematic"
                            if self._is_geometry_problematic(row):
                                # Check if we already have a pending diff for this geometry IN THIS DATASET
                                existing_pending_diff = await self.db.execute(
                                    select(GeometryDiff)
                                    .join(GeometrySnapshot, GeometryDiff.new_snapshot_id == GeometrySnapshot.id)
                                    .where(
                                        GeometrySnapshot.dataset_id == dataset.id,
                                        GeometrySnapshot.geometry_hash == geometry_hash,
                                        GeometryDiff.status == "PENDING"
                                    )
                                )
                                existing_diff_obj = existing_pending_diff.scalar_one_or_none()
                                has_pending_diff = existing_diff_obj is not None
                                
                                if has_pending_diff:
                                    logger.info(f"🔍 Found existing pending diff: diff_id={existing_diff_obj.id}, dataset={dataset.id}, geom_hash={geometry_hash[:8]}")
                                
                                if has_pending_diff:
                                    logger.info(f"⏭️ Pending diff already exists for geometry {geometry_hash[:8]}... IN DATASET {dataset.id}, skipping")
                                    # Still create snapshot for completeness
                                    try:
                                        snapshot = GeometrySnapshot(
                                            dataset_id=dataset.id,
                                            source_id=str(attributes.get('id') or attributes.get('gid') or ''),
                                            geometry_hash=geometry_hash,
                                            attributes_hash=attributes_hash,
                                            composite_hash=composite_hash,
                                            geometry=self.create_wkt_element(geometry_wkb),
                                            attributes=attributes
                                        )
                                        self.db.add(snapshot)
                                        await self.db.flush()
                                        snapshots_created += 1
                                    except Exception as snapshot_error:
                                        # Check if this is a dimension constraint violation
                                        error_str = str(snapshot_error)
                                        if "enforce_dims_geometry" in error_str or "violates check constraint" in error_str:
                                            logger.warning(f"🔧 Detected PostGIS dimension constraint violation - auto-fixing...")
                                            await self.db.rollback()
                                            await self._ensure_mixed_dimension_support()
                                            
                                            # Retry the snapshot creation
                                            try:
                                                snapshot = GeometrySnapshot(
                                                    dataset_id=dataset.id,
                                                    source_id=str(attributes.get('id') or attributes.get('gid') or ''),
                                                    geometry_hash=geometry_hash,
                                                    attributes_hash=attributes_hash,
                                                    composite_hash=composite_hash,
                                                    geometry=self.create_wkt_element(geometry_wkb),
                                                    attributes=attributes
                                                )
                                                self.db.add(snapshot)
                                                await self.db.flush()
                                                snapshots_created += 1
                                                logger.info(f"✅ Successfully created completeness snapshot after constraint fix")
                                            except Exception as retry_error:
                                                logger.error(f"Failed to create completeness snapshot even after constraint fix: {retry_error}")
                                        else:
                                            logger.error(f"Error creating snapshot for geometry {geometry_hash}: {snapshot_error}")
                                        continue
                            else:
                                # Geometry is not problematic - just create snapshot, no diff
                                try:
                                    snapshot = GeometrySnapshot(
                                        dataset_id=dataset.id,
                                        source_id=str(attributes.get('id') or attributes.get('gid') or ''),
                                        geometry_hash=geometry_hash,
                                        attributes_hash=attributes_hash,
                                        composite_hash=composite_hash,
                                        geometry=self.create_wkt_element(geometry_wkb),
                                        attributes=attributes
                                    )
                                    self.db.add(snapshot)
                                    await self.db.flush()
                                    snapshots_created += 1
                                except Exception as snapshot_error:
                                    # Check if this is a dimension constraint violation  
                                    error_str = str(snapshot_error)
                                    if "enforce_dims_geometry" in error_str or "violates check constraint" in error_str:
                                        logger.warning(f"🔧 Detected PostGIS dimension constraint violation - auto-fixing...")
                                        await self.db.rollback()
                                        await self._ensure_mixed_dimension_support()
                                        
                                        # Retry the snapshot creation
                                        try:
                                            snapshot = GeometrySnapshot(
                                                dataset_id=dataset.id,
                                                source_id=str(attributes.get('id') or attributes.get('gid') or ''),
                                                geometry_hash=geometry_hash,
                                                attributes_hash=attributes_hash,
                                                composite_hash=composite_hash,
                                                geometry=self.create_wkt_element(geometry_wkb),
                                                attributes=attributes
                                            )
                                            self.db.add(snapshot)
                                            await self.db.flush()
                                            snapshots_created += 1
                                            logger.info(f"✅ Successfully created snapshot after constraint fix")
                                        except Exception as retry_error:
                                            logger.error(f"Failed to create snapshot even after constraint fix: {retry_error}")
                                            continue
                                    else:
                                        logger.error(f"Error creating snapshot for geometry {geometry_hash}: {snapshot_error}")
                                        continue
                                
                                try:
                                    # Create new snapshot
                                    snapshot = GeometrySnapshot(
                                        dataset_id=dataset.id,
                                        source_id=str(attributes.get('id') or attributes.get('gid') or ''),
                                        geometry_hash=geometry_hash,
                                        attributes_hash=attributes_hash,
                                        composite_hash=composite_hash,
                                        geometry=self.create_wkt_element(geometry_wkb),
                                        attributes=attributes
                                    )
                                    self.db.add(snapshot)
                                    
                                    # Flush to get the ID without committing
                                    await self.db.flush()
                                    
                                    snapshots_created += 1
                                    
                                    # Determine diff type and create diff record
                                    diff_type = await self._determine_diff_type(
                                        geometry_hash, attributes_hash, existing_snapshots
                                    )
                                    
                                    # Check if diff already exists for this snapshot
                                    existing_diff_result = await self.db.execute(
                                        select(GeometryDiff).where(
                                            GeometryDiff.new_snapshot_id == snapshot.id
                                        )
                                    )
                                    existing_diff = existing_diff_result.scalar_one_or_none()
                                    
                                    if existing_diff:
                                        logger.debug(f"Diff already exists for snapshot {snapshot.id}, skipping")
                                        continue
                                    
                                    # Create diff record ONLY for problematic geometries
                                    diff = GeometryDiff(
                                        dataset_id=dataset.id,
                                        diff_type=diff_type,
                                        old_snapshot_id=None,  # Will be set if needed
                                        new_snapshot_id=snapshot.id,
                                        geometry_changed=True,
                                        attributes_changed=False,
                                        confidence_score=self._calculate_confidence_score(row)
                                    )
                                    
                                    if diff_type == "UPDATED":
                                        # Find the old snapshot with same geometry but different attributes
                                        for existing_snapshot in existing_snapshots:
                                            if existing_snapshot.geometry_hash == geometry_hash:
                                                diff.old_snapshot_id = existing_snapshot.id
                                                diff.geometry_changed = False
                                                diff.attributes_changed = True
                                                break
                                    
                                    self.db.add(diff)
                                    diffs_detected += 1
                                    logger.info(f"🚨 Created {diff_type} diff for geometry {geometry_hash[:8]}... (confidence: {diff.confidence_score})")
                                    
                                except Exception as snapshot_error:
                                    # Check if this is a dimension constraint violation
                                    error_str = str(snapshot_error)
                                    if "enforce_dims_geometry" in error_str or "violates check constraint" in error_str:
                                        logger.warning(f"🔧 Detected PostGIS dimension constraint violation - auto-fixing...")
                                        await self.db.rollback()
                                        await self._ensure_mixed_dimension_support()
                                        
                                        # Retry the snapshot and diff creation
                                        try:
                                            snapshot = GeometrySnapshot(
                                                dataset_id=dataset.id,
                                                source_id=str(attributes.get('id') or attributes.get('gid') or ''),
                                                geometry_hash=geometry_hash,
                                                attributes_hash=attributes_hash,
                                                composite_hash=composite_hash,
                                                geometry=self.create_wkt_element(geometry_wkb),
                                                attributes=attributes
                                            )
                                            self.db.add(snapshot)
                                            await self.db.flush()
                                            snapshots_created += 1
                                            
                                            # Continue with diff creation logic...
                                            diff_type = await self._determine_diff_type(
                                                geometry_hash, attributes_hash, existing_snapshots
                                            )
                                            
                                            diff = GeometryDiff(
                                                dataset_id=dataset.id,
                                                diff_type=diff_type,
                                                old_snapshot_id=None,
                                                new_snapshot_id=snapshot.id,
                                                geometry_changed=True,
                                                attributes_changed=False,
                                                confidence_score=self._calculate_confidence_score(row)
                                            )
                                            
                                            if diff_type == "UPDATED":
                                                for existing_snapshot in existing_snapshots:
                                                    if existing_snapshot.geometry_hash == geometry_hash:
                                                        diff.old_snapshot_id = existing_snapshot.id
                                                        diff.geometry_changed = False
                                                        diff.attributes_changed = True
                                                        break
                                            
                                            self.db.add(diff)
                                            diffs_detected += 1
                                            logger.info(f"✅ Successfully created snapshot and diff after constraint fix")
                                        except Exception as retry_error:
                                            logger.error(f"Failed to create snapshot even after constraint fix: {retry_error}")
                                            continue
                                    else:
                                        logger.error(f"Error processing geometry {geometry_hash}: {snapshot_error}")
                                        continue
                        else:
                            logger.debug(f"✅ EXISTING geometry found: composite={composite_hash[:8]}... - NO CHANGE, skipping")
                            continue  # Skip processing - no change detected
                
                # Check for deleted geometries (only in change detection runs, not baseline runs)
                if not is_baseline_run:
                    # IMPORTANT: Use the same attribute processing logic as the main loop
                    current_hashes = set()
                    for row in external_rows:
                        # Build attributes dict with same logic as main loop
                        attributes = {}
                        for key, value in row.items():
                            if key not in [dataset.geometry_column, 'geometry_wkb', 'geometry_hash', 'is_valid', 'geom_area']:
                                # Convert any special types to JSON-serializable (same as main loop)
                                if value is not None:
                                    attributes[key] = str(value) if not isinstance(value, (str, int, float, bool)) else value
                        
                        attributes_hash = self.compute_attributes_hash(attributes)
                        composite_hash = self.compute_composite_hash(row['geometry_hash'], attributes_hash)
                        current_hashes.add(composite_hash)
                    
                    logger.info(f"🔍 Deletion check: {len(existing_hashes)} existing vs {len(current_hashes)} current hashes")
                    
                    for existing_hash, existing_snapshot in existing_hashes.items():
                        if existing_hash not in current_hashes:
                            # Create deletion diff
                            logger.info(f"🗑️ Creating DELETED diff for missing geometry: {existing_hash[:8]}...")
                            diff = GeometryDiff(
                                dataset_id=dataset.id,
                                diff_type="DELETED",
                                old_snapshot_id=existing_snapshot.id,
                                geometry_changed=True,
                                confidence_score=1.0  # Deletions are always flagged
                            )
                            self.db.add(diff)
                            diffs_detected += 1
                else:
                    logger.debug("📊 Skipping deletion check for baseline run")
                
                # Commit all changes at once
                await self.db.commit()
                
                duration = (datetime.now(timezone.utc) - start_time).total_seconds()
                
                logger.info(f"✅ Change monitoring completed: {snapshots_created} snapshots, {diffs_detected} diffs flagged")
                
                return GeometryImportResponse(
                    dataset_id=dataset.id,
                    snapshots_created=snapshots_created,
                    diffs_detected=diffs_detected,
                    import_duration_seconds=duration,
                    status="SUCCESS"
                )
                
            finally:
                # Always close external connection
                await external_conn.close()
            
        except Exception as e:
            await self.db.rollback()
            logger.error(f"Error monitoring dataset changes {dataset.id}: {e}")
            
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()
            return GeometryImportResponse(
                dataset_id=dataset.id,
                import_duration_seconds=duration,
                status="FAILED",
                error_message=str(e)
            )
    
    def _is_geometry_problematic(self, row: dict) -> bool:
        """
        Determine if a geometry is "problematic" and should be flagged for review.
        This is the THRESHOLD logic that determines what gets sent to the diff queue.
        
        Uses the same test logic from spatial_tests module to avoid duplication.
        """
        # Quick critical checks for immediate flagging
        is_valid = row.get('is_valid', True)
        is_simple = row.get('is_simple', True)
        is_topologically_clean = row.get('is_topologically_clean', True)
        
        # 1. Critical OGC validity failures - always flag these
        if not is_valid:
            logger.debug(f"Geometry flagged: fails OGC validity (ST_IsValid)")
            return True
        
        if not is_simple:
            logger.debug(f"Geometry flagged: not simple (self-intersecting)")
            return True
        
        if not is_topologically_clean:
            logger.debug(f"Geometry flagged: topologically unclean")
            return True
        
        # 2. Quick size-based checks for immediate flagging
        geom_area = row.get('geom_area', 0) or 0
        geom_length = row.get('geom_length', 0) or 0
        geom_type = row.get('geom_type', '')
        
        # Zero/negative area/length - always problematic
        if 'Polygon' in geom_type and geom_area <= 0:
            logger.debug(f"Geometry flagged: zero/negative area polygon ({geom_area})")
            return True
        
        if ('LineString' in geom_type or 'Line' in geom_type) and geom_length <= 0:
            logger.debug(f"Geometry flagged: zero/negative length linestring ({geom_length})")
            return True
        
        # 3. Critical point count issues
        num_points = row.get('num_points', 0) or 0
        if num_points <= 1 and 'Point' not in geom_type:
            logger.debug(f"Geometry flagged: degenerate geometry with {num_points} points")
            return True
        
        # 4. Critical coordinate bounds issues
        coords_to_check = [row.get('min_x'), row.get('max_x'), row.get('min_y'), row.get('max_y')]
        for coord in coords_to_check:
            if coord is not None:
                if coord != coord or coord == float('inf') or coord == float('-inf'):  # NaN or Infinity
                    logger.debug(f"Geometry flagged: invalid coordinate ({coord})")
                    return True
        
        # 5. For other potential issues, use a confidence-based approach
        # This allows us to be more selective about what gets flagged
        confidence = self._calculate_confidence_score(row)
        threshold = TestConfig.get_confidence_threshold()  # Get threshold from config
        
        if confidence >= threshold:
            logger.debug(f"Geometry flagged: high confidence issue (confidence: {confidence})")
            return True
        
        return False  # Geometry passes critical checks
    
    def _calculate_confidence_score(self, row: dict) -> float:
        """
        Calculate confidence score for how likely this is a real issue.
        Higher scores = more confident this needs human review
        Lower scores = might be false positive or minor issue
        
        Now uses externalized configuration for all thresholds.
        """
        # Get confidence configuration
        conf_config = TestConfig.get_test_config("confidence")
        
        # Extract geometric properties
        is_valid = row.get('is_valid', True)
        is_simple = row.get('is_simple', True)
        is_topologically_clean = row.get('is_topologically_clean', True)
        geom_area = row.get('geom_area', 0) or 0
        geom_length = row.get('geom_length', 0) or 0
        num_points = row.get('num_points', 0) or 0
        geom_type = row.get('geom_type', '')
        
        # Start with base confidence
        confidence = conf_config.get("default_confidence", 0.5)
        
        # Critical issues = very high confidence
        if not is_valid:
            confidence = conf_config.get("invalid_geometry_confidence", 0.95)
        elif not is_simple:
            confidence = conf_config.get("non_simple_geometry_confidence", 0.90)
        elif not is_topologically_clean:
            confidence = conf_config.get("topologically_unclean_confidence", 0.85)
        
        # Size-based issues
        elif geom_area <= 0 and 'Polygon' in geom_type:
            confidence = conf_config.get("zero_area_polygon_confidence", 0.90)
        elif geom_length <= 0 and ('LineString' in geom_type or 'Line' in geom_type):
            confidence = conf_config.get("zero_length_line_confidence", 0.90)
        
        # Very large geometries (use configuration thresholds)
        elif geom_area > TestConfig.get_area_thresholds()["large_threshold"]:
            confidence = conf_config.get("large_geometry_confidence", 0.70)
        elif geom_length > TestConfig.get_length_thresholds()["large_threshold"]:
            confidence = conf_config.get("large_geometry_confidence", 0.65)
        
        # Degenerate geometries
        elif num_points <= 1:
            confidence = conf_config.get("degenerate_geometry_confidence", 0.95)
        elif 'Polygon' in geom_type and num_points < TestConfig.VALIDITY_CONFIG["min_polygon_points"]:
            confidence = conf_config.get("insufficient_points_confidence", 0.90)
        elif ('LineString' in geom_type or 'Line' in geom_type) and num_points < TestConfig.VALIDITY_CONFIG["min_linestring_points"]:
            confidence = conf_config.get("insufficient_points_confidence", 0.85)
        
        # Coordinate bounds issues
        coords_to_check = [row.get('min_x'), row.get('max_x'), row.get('min_y'), row.get('max_y')]
        for coord in coords_to_check:
            if coord is not None:
                if abs(coord) > TestConfig.get_coordinate_bounds():
                    confidence = max(confidence, conf_config.get("suspicious_coordinates_confidence", 0.75))
        
        # Adjust confidence based on geometry complexity
        # More complex geometries might have legitimate edge cases
        complex_threshold = conf_config.get("complex_geometry_point_threshold", 100)
        very_complex_threshold = conf_config.get("very_complex_geometry_point_threshold", 1000)
        
        if num_points > very_complex_threshold:
            confidence *= conf_config.get("very_complex_geometry_confidence_reduction", 0.8)
        elif num_points > complex_threshold:
            confidence *= conf_config.get("complex_geometry_confidence_reduction", 0.9)
        
        # Ensure confidence stays within bounds
        return max(0.0, min(1.0, confidence))
    
    async def run_quality_checks(self, dataset: Dataset) -> Dict[str, int]:
        """
        Separate workflow for running quality checks on existing data.
        This runs on a different timer (hourly) and populates SpatialCheck table.
        """
        start_time = datetime.now(timezone.utc)
        
        try:
            # Clear existing spatial checks for this dataset before running new ones
            # This prevents accumulation of old results
            delete_result = await self.db.execute(
                text("DELETE FROM spatial_checks WHERE dataset_id = :dataset_id"),
                {"dataset_id": dataset.id}
            )
            deleted_count = delete_result.rowcount
            if deleted_count > 0:
                logger.info(f"🧹 Cleared {deleted_count} existing spatial checks for dataset {dataset.name}")
            
            import asyncpg
            
            # Connect to external PostGIS database
            external_conn = await asyncpg.connect(dataset.connection_string)
            
            try:
                # Get ALL geometries for quality checking
                quality_query = f"""
                    SELECT 
                        *,
                        ST_AsBinary({dataset.geometry_column}) as geometry_wkb,
                        ST_IsValid({dataset.geometry_column}) as is_valid,
                        ST_IsValidReason({dataset.geometry_column}) as validity_reason,
                        ST_IsSimple({dataset.geometry_column}) as is_simple,
                        ST_Area({dataset.geometry_column}) as geom_area,
                        ST_Length({dataset.geometry_column}) as geom_length,
                        ST_NPoints({dataset.geometry_column}) as num_points,
                        ST_GeometryType({dataset.geometry_column}) as geom_type,
                        -- Ring orientation check (for polygons)
                        CASE 
                            WHEN ST_GeometryType({dataset.geometry_column}) LIKE '%Polygon%' 
                            THEN ST_IsPolygonCCW({dataset.geometry_column})
                            ELSE NULL 
                        END as is_ccw_oriented,
                        -- Self-intersection check (more detailed than just ST_IsValid)
                        ST_IsValid({dataset.geometry_column}) AND ST_IsSimple({dataset.geometry_column}) as is_topologically_clean,
                        -- Coordinate bounds checking
                        ST_XMin({dataset.geometry_column}) as min_x,
                        ST_XMax({dataset.geometry_column}) as max_x,
                        ST_YMin({dataset.geometry_column}) as min_y,
                        ST_YMax({dataset.geometry_column}) as max_y
                    FROM {dataset.schema_name}.{dataset.table_name}
                    WHERE {dataset.geometry_column} IS NOT NULL
                """
                
                external_rows = await external_conn.fetch(quality_query)
                
                check_results = {
                    "validity_checks": 0,
                    "duplicate_checks": 0,
                    "topology_checks": 0,
                    "area_checks": 0,
                    "failed_checks": 0
                }
                
                for row in external_rows:
                    # Get or create snapshot for this geometry
                    geometry_hash = hashlib.md5(row['geometry_wkb']).hexdigest()
                    
                    # Find corresponding snapshot (handle duplicates)
                    result = await self.db.execute(
                        select(GeometrySnapshot).where(
                            GeometrySnapshot.dataset_id == dataset.id,
                            GeometrySnapshot.geometry_hash == geometry_hash
                        )
                    )
                    snapshots = result.scalars().all()
                    snapshot = snapshots[0] if snapshots else None
                    
                    if len(snapshots) > 1:
                        logger.warning(f"Found {len(snapshots)} snapshots with same geometry_hash {geometry_hash[:8]}...")
                    
                    if not snapshot:
                        continue  # Skip if no snapshot exists
                    
                    # Run basic quality checks
                    checks = await self._run_basic_quality_checks(dataset.id, snapshot, row)
                    
                    for check in checks:
                        self.db.add(check)
                        
                        check_type_key = f"{check.check_type.lower()}_checks"
                        if check_type_key not in check_results:
                            check_results[check_type_key] = 0
                        check_results[check_type_key] += 1
                        
                        if check.check_result == "FAIL":
                            check_results["failed_checks"] += 1
                
                await self.db.commit()
                
                duration = (datetime.now(timezone.utc) - start_time).total_seconds()
                logger.info(f"✅ Quality checks completed: {check_results}")
                
                return check_results
                
            finally:
                await external_conn.close()
            
        except Exception as e:
            logger.error(f"Error running quality checks for dataset {dataset.id}: {e}")
            return {"error": str(e)}
    
    async def _run_basic_quality_checks(
        self, 
        dataset_id: UUID, 
        snapshot: GeometrySnapshot, 
        row: dict
    ) -> List[SpatialCheck]:
        """Run basic quality checks using the dedicated spatial tests module."""
        from .spatial_tests import run_basic_quality_checks
        return await run_basic_quality_checks(self.db, dataset_id, snapshot, row)

    # Keep existing methods for backward compatibility
    async def import_geometries_from_external_source(
        self, 
        dataset: Dataset,
        force_reimport: bool = False
    ) -> GeometryImportResponse:
        """Legacy method - redirects to new change monitoring."""
        return await self.monitor_dataset_changes(dataset, force_reimport)
    
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
            # First check if snapshot exists
            snapshot_result = await self.db.execute(
                select(GeometrySnapshot).where(GeometrySnapshot.id == snapshot_id)
            )
            snapshot = snapshot_result.scalar_one_or_none()
            
            if not snapshot:
                logger.error(f"Snapshot {snapshot_id} not found in database")
                return None
            
            logger.info(f"Converting geometry for snapshot {snapshot_id} to GeoJSON")
            
            # Convert geometry to GeoJSON using PostGIS with raw SQL
            # Note: geometry column is added manually via SQL, not in SQLAlchemy model
            result = await self.db.execute(
                text("""
                    SELECT ST_AsGeoJSON(geometry) 
                    FROM geometry_snapshots 
                    WHERE id = :snapshot_id
                """),
                {"snapshot_id": snapshot_id}
            )
            geojson_text = result.scalar()
            
            if geojson_text:
                import json
                geojson_data = json.loads(geojson_text)
                logger.info(f"Successfully converted geometry for snapshot {snapshot_id} to GeoJSON: {geojson_data['type']}")
                return geojson_data
            else:
                logger.error(f"ST_AsGeoJSON returned null for snapshot {snapshot_id} - geometry column may be null")
                return None
            
        except Exception as e:
            logger.error(f"Error converting geometry to GeoJSON for snapshot {snapshot_id}: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
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
    
    async def _ensure_mixed_dimension_support(self):
        """
        Automatically detect and fix PostGIS dimension constraints to support mixed dimensions.
        This is called automatically when constraint violations are detected.
        """
        try:
            logger.info("🔧 Auto-fixing PostGIS constraints for mixed-dimension support...")
            
            # Check for dimension constraints
            result = await self.db.execute(text("""
                SELECT constraint_name, check_clause
                FROM information_schema.check_constraints 
                WHERE constraint_name LIKE '%enforce_dims%geometry%'
            """))
            constraints = result.fetchall()
            
            # Remove any dimension constraints
            for constraint_name, check_clause in constraints:
                logger.info(f"🧹 Removing dimension constraint: {constraint_name}")
                await self.db.execute(text(f"""
                    ALTER TABLE geometry_snapshots DROP CONSTRAINT IF EXISTS {constraint_name}
                """))
            
            # Update geometry_columns for mixed dimensions
            logger.info("📝 Updating geometry_columns for mixed dimensions...")
            await self.db.execute(text("""
                UPDATE geometry_columns 
                SET coord_dimension = 4, type = 'GEOMETRY'
                WHERE f_table_name = 'geometry_snapshots' AND f_geometry_column = 'geometry'
            """))
            
            # Commit the constraint fixes
            await self.db.commit()
            logger.info("✅ Mixed-dimension support enabled successfully")
            
        except Exception as e:
            logger.error(f"Error fixing PostGIS constraints: {e}")
            await self.db.rollback()
            raise 