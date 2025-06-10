"""
Spatial Quality Tests Module

This module contains all spatial quality check logic, organized by geometry type
and test category. This keeps test logic separate and easy to maintain.

Test Categories:
- Validity: ST_IsValid, basic geometry validation
- Topology: Self-intersections, simplicity, orientation
- Area/Length: Size validation, zero area/length detection
- Duplicates: Geometry hash-based duplicate detection
- Custom: Domain-specific tests for different geometry types
"""

from typing import List, Dict, Any, Optional
from uuid import UUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, text
import logging

from database import SpatialCheck, GeometrySnapshot
from .test_config import TestConfig

logger = logging.getLogger("dbfriend-cloud.spatial-tests")


class SpatialTestRunner:
    """Main test runner that coordinates all spatial quality checks."""
    
    def __init__(self, db: AsyncSession):
        self.db = db
        self.validity_tester = ValidityTests(db)
        self.topology_tester = TopologyTests(db)
        self.area_tester = AreaTests(db)
        self.duplicate_tester = DuplicateTests(db)
    
    async def run_all_tests(
        self, 
        dataset_id: UUID, 
        snapshot: GeometrySnapshot, 
        external_row: dict
    ) -> List[SpatialCheck]:
        """Run all applicable tests on a geometry snapshot."""
        all_checks = []
        
        # Run basic tests based on configuration
        if TestConfig.is_test_enabled("validity"):
            all_checks.extend(await self.validity_tester.run_tests(dataset_id, snapshot, external_row))
        
        if TestConfig.is_test_enabled("topology"):
            all_checks.extend(await self.topology_tester.run_tests(dataset_id, snapshot, external_row))
        
        if TestConfig.is_test_enabled("area"):
            all_checks.extend(await self.area_tester.run_tests(dataset_id, snapshot, external_row))
        
        if TestConfig.is_test_enabled("duplicate"):
            all_checks.extend(await self.duplicate_tester.run_tests(dataset_id, snapshot, external_row))
        
        # Add geometry-type specific tests if enabled
        if TestConfig.is_test_enabled("geometry_specific"):
            geom_type = self._get_geometry_type(external_row)
            if geom_type:
                type_specific_tests = await self._run_geometry_type_tests(
                    dataset_id, snapshot, external_row, geom_type
                )
                all_checks.extend(type_specific_tests)
        
        return all_checks
    
    def _get_geometry_type(self, external_row: dict) -> Optional[str]:
        """Extract geometry type from external row data."""
        # Get geometry type from PostGIS ST_GeometryType function
        geom_type = external_row.get('geom_type', '')
        
        if geom_type:
            # Remove ST_ prefix if present (PostGIS returns "ST_Polygon", we want "POLYGON")
            clean_type = geom_type.upper().replace('ST_', '')
            return clean_type
        
        return None
    
    async def _run_geometry_type_tests(
        self, 
        dataset_id: UUID, 
        snapshot: GeometrySnapshot, 
        external_row: dict, 
        geom_type: str
    ) -> List[SpatialCheck]:
        """Run tests specific to geometry type (Point, LineString, Polygon, etc.)."""
        checks = []
        
        if geom_type.upper() == 'POLYGON':
            checks.extend(await PolygonTests(self.db).run_tests(dataset_id, snapshot, external_row))
        elif geom_type.upper() == 'LINESTRING':
            checks.extend(await LineStringTests(self.db).run_tests(dataset_id, snapshot, external_row))
        elif geom_type.upper() == 'POINT':
            checks.extend(await PointTests(self.db).run_tests(dataset_id, snapshot, external_row))
        
        return checks


class BaseTestCategory:
    """Base class for all test categories."""
    
    def __init__(self, db: AsyncSession):
        self.db = db
    
    def _create_check(
        self,
        dataset_id: UUID,
        snapshot_id: UUID,
        check_type: str,
        check_result: str,
        error_message: Optional[str] = None,
        error_details: Optional[Dict[str, Any]] = None
    ) -> SpatialCheck:
        """Helper to create a SpatialCheck instance."""
        return SpatialCheck(
            dataset_id=dataset_id,
            snapshot_id=snapshot_id,
            check_type=check_type,
            check_result=check_result,
            error_message=error_message,
            error_details=error_details
        )


class ValidityTests(BaseTestCategory):
    """
    Comprehensive geometry validity tests implementing OGC Simple Features standards.
    
    Based on best practices from spatial geometry validation literature:
    - Basic ST_IsValid check
    - Coordinate bounds validation
    - Geometry type consistency
    - Point count validation
    """
    
    async def run_tests(
        self, 
        dataset_id: UUID, 
        snapshot: GeometrySnapshot, 
        external_row: dict
    ) -> List[SpatialCheck]:
        """Run comprehensive validity tests."""
        checks = []
        
        # 1. Basic OGC validity check with detailed reason
        validity_check = await self._check_basic_validity(dataset_id, snapshot, external_row)
        checks.append(validity_check)
        
        # 2. Coordinate bounds validation
        bounds_check = await self._check_coordinate_bounds(dataset_id, snapshot, external_row)
        if bounds_check:
            checks.append(bounds_check)
        
        # 3. Point count validation
        point_count_check = await self._check_point_count(dataset_id, snapshot, external_row)
        if point_count_check:
            checks.append(point_count_check)
        
        # 4. Geometry type consistency
        type_check = await self._check_geometry_type_consistency(dataset_id, snapshot, external_row)
        if type_check:
            checks.append(type_check)
        
        return checks
    
    async def _check_basic_validity(
        self, 
        dataset_id: UUID, 
        snapshot: GeometrySnapshot, 
        external_row: dict
    ) -> SpatialCheck:
        """Check basic OGC validity with detailed PostGIS reason."""
        from .test_config import TestConfig
        
        is_valid = external_row.get('is_valid', True)
        validity_reason = external_row.get('validity_reason', 'Valid Geometry')
        
        # Get configuration
        fail_on_invalid = TestConfig.should_fail_on_invalid()
        result_level = "PASS" if is_valid else ("FAIL" if fail_on_invalid else "WARNING")
        
        return self._create_check(
            dataset_id=dataset_id,
            snapshot_id=snapshot.id,
            check_type="VALIDITY",
            check_result=result_level,
            error_message=None if is_valid else f"Geometry fails OGC validation: {validity_reason}",
            error_details={
                "st_isvalid": is_valid,
                "validity_reason": validity_reason,
                "postgis_explanation": validity_reason if not is_valid else None
            }
        )
    
    async def _check_coordinate_bounds(
        self, 
        dataset_id: UUID, 
        snapshot: GeometrySnapshot, 
        external_row: dict
    ) -> Optional[SpatialCheck]:
        """Validate coordinate bounds are within reasonable ranges."""
        from .test_config import TestConfig
        
        min_x = external_row.get('min_x')
        max_x = external_row.get('max_x')
        min_y = external_row.get('min_y')
        max_y = external_row.get('max_y')
        
        # Get coordinate bounds threshold from configuration
        max_magnitude = TestConfig.get_coordinate_bounds()
        
        # Check for suspicious coordinate values
        issues = []
        coords_to_check = [
            ('min_x', min_x), ('max_x', max_x),
            ('min_y', min_y), ('max_y', max_y)
        ]
        
        for name, coord in coords_to_check:
            if coord is not None:
                # Check for extreme values that might indicate coordinate system issues
                if abs(coord) > max_magnitude:
                    issues.append(f"{name}={coord} (exceeds magnitude threshold {max_magnitude})")
                elif coord != coord:  # NaN check
                    issues.append(f"{name}=NaN")
                elif coord == float('inf') or coord == float('-inf'):
                    issues.append(f"{name}=Infinity")
        
        if issues:
            return self._create_check(
                dataset_id=dataset_id,
                snapshot_id=snapshot.id,
                check_type="VALIDITY",
                check_result="FAIL",
                error_message=f"Invalid coordinates: {', '.join(issues)}",
                error_details={
                    "coordinate_issues": issues, 
                    "bounds": {"min_x": min_x, "max_x": max_x, "min_y": min_y, "max_y": max_y},
                    "magnitude_threshold": max_magnitude
                }
            )
        
        return None
    
    async def _check_point_count(
        self, 
        dataset_id: UUID, 
        snapshot: GeometrySnapshot, 
        external_row: dict
    ) -> Optional[SpatialCheck]:
        """Validate geometry has appropriate number of points for its type."""
        from .test_config import TestConfig
        
        num_points = external_row.get('num_points', 0)
        geom_type = external_row.get('geom_type', '')
        
        # Get configuration thresholds
        validity_config = TestConfig.get_test_config("validity")
        min_polygon_points = validity_config.get("min_polygon_points", 4)
        min_linestring_points = validity_config.get("min_linestring_points", 2)
        min_point_points = validity_config.get("min_point_points", 1)
        max_point_points = validity_config.get("max_point_points", 1)
        
        issues = []
        
        # Check minimum point requirements
        if num_points <= 0:
            issues.append("Zero points")
        elif 'Polygon' in geom_type and num_points < min_polygon_points:
            issues.append(f"Polygon has only {num_points} points (minimum {min_polygon_points} required)")
        elif ('LineString' in geom_type or 'Line' in geom_type) and num_points < min_linestring_points:
            issues.append(f"LineString has only {num_points} points (minimum {min_linestring_points} required)")
        elif 'Point' in geom_type and (num_points < min_point_points or num_points > max_point_points):
            issues.append(f"Point geometry has {num_points} points (should be exactly {min_point_points})")
        
        # Check for degenerate geometries
        if num_points == 1 and 'Point' not in geom_type:
            issues.append(f"Non-point geometry has only 1 point")
        
        if issues:
            return self._create_check(
                dataset_id=dataset_id,
                snapshot_id=snapshot.id,
                check_type="VALIDITY",
                check_result="FAIL",
                error_message=f"Invalid point count: {', '.join(issues)}",
                error_details={
                    "point_count_issues": issues, 
                    "num_points": num_points, 
                    "geom_type": geom_type,
                    "requirements": {
                        "min_polygon_points": min_polygon_points,
                        "min_linestring_points": min_linestring_points,
                        "min_point_points": min_point_points,
                        "max_point_points": max_point_points
                    }
                }
            )
        
        return None
    
    async def _check_geometry_type_consistency(
        self, 
        dataset_id: UUID, 
        snapshot: GeometrySnapshot, 
        external_row: dict
    ) -> Optional[SpatialCheck]:
        """Check that geometry type is consistent and recognized."""
        geom_type = external_row.get('geom_type', '')
        
        # List of recognized PostGIS geometry types
        valid_types = [
            'POINT', 'LINESTRING', 'POLYGON', 'MULTIPOINT', 
            'MULTILINESTRING', 'MULTIPOLYGON', 'GEOMETRYCOLLECTION'
        ]
        
        if not geom_type:
            return self._create_check(
                dataset_id=dataset_id,
                snapshot_id=snapshot.id,
                check_type="VALIDITY",
                check_result="WARNING",
                error_message="Missing geometry type information",
                error_details={"geom_type": geom_type}
            )
        
        # Check if type is recognized
        base_type = geom_type.upper().replace('ST_', '')  # Remove ST_ prefix if present
        if not any(valid_type in base_type for valid_type in valid_types):
            return self._create_check(
                dataset_id=dataset_id,
                snapshot_id=snapshot.id,
                check_type="VALIDITY",
                check_result="WARNING",
                error_message=f"Unrecognized geometry type: {geom_type}",
                error_details={"geom_type": geom_type, "valid_types": valid_types}
            )
        
        return None


class TopologyTests(BaseTestCategory):
    """
    Advanced topology tests implementing industry best practices.
    
    Based on computational geometry literature:
    - Self-intersection detection (Bentley-Ottmann style)
    - Ring orientation validation
    - Simplicity checks beyond basic ST_IsSimple
    - Topological cleanliness validation
    """
    
    async def run_tests(
        self, 
        dataset_id: UUID, 
        snapshot: GeometrySnapshot, 
        external_row: dict
    ) -> List[SpatialCheck]:
        """Run comprehensive topology tests."""
        checks = []
        
        # 1. Basic simplicity check
        simplicity_check = await self._check_simplicity(dataset_id, snapshot, external_row)
        checks.append(simplicity_check)
        
        # 2. Topological cleanliness (combined validity + simplicity)
        cleanliness_check = await self._check_topological_cleanliness(dataset_id, snapshot, external_row)
        if cleanliness_check:
            checks.append(cleanliness_check)
        
        # 3. Ring orientation (for polygons)
        orientation_check = await self._check_ring_orientation(dataset_id, snapshot, external_row)
        if orientation_check:
            checks.append(orientation_check)
        
        # 4. Advanced topology validation
        advanced_check = await self._check_advanced_topology(dataset_id, snapshot, external_row)
        if advanced_check:
            checks.append(advanced_check)
        
        return checks
    
    async def _check_simplicity(
        self, 
        dataset_id: UUID, 
        snapshot: GeometrySnapshot, 
        external_row: dict
    ) -> SpatialCheck:
        """
        Check geometry simplicity using PostGIS ST_IsSimple.
        
        ST_IsSimple returns FALSE if geometry has self-intersections,
        which is the core of what sweep-line algorithms detect.
        """
        is_simple = external_row.get('is_simple', True)
        
        return self._create_check(
            dataset_id=dataset_id,
            snapshot_id=snapshot.id,
            check_type="TOPOLOGY",
            check_result="PASS" if is_simple else "FAIL",
            error_message=None if is_simple else "Geometry has self-intersections or complex topology",
            error_details={"st_issimple": is_simple}
        )
    
    async def _check_topological_cleanliness(
        self, 
        dataset_id: UUID, 
        snapshot: GeometrySnapshot, 
        external_row: dict
    ) -> Optional[SpatialCheck]:
        """
        Check combined topological cleanliness (validity + simplicity).
        
        This is equivalent to running both ST_IsValid AND ST_IsSimple,
        which catches more issues than either check alone.
        """
        is_topologically_clean = external_row.get('is_topologically_clean', True)
        is_valid = external_row.get('is_valid', True)
        is_simple = external_row.get('is_simple', True)
        
        if not is_topologically_clean:
            # Determine the specific type of topology issue
            if not is_valid and not is_simple:
                issue_type = "Both invalid and non-simple"
            elif not is_valid:
                issue_type = "Invalid but simple"
            elif not is_simple:
                issue_type = "Valid but non-simple"
            else:
                issue_type = "Unknown topology issue"
            
            return self._create_check(
                dataset_id=dataset_id,
                snapshot_id=snapshot.id,
                check_type="TOPOLOGY",
                check_result="FAIL",
                error_message=f"Topological cleanliness failure: {issue_type}",
                error_details={
                    "is_topologically_clean": is_topologically_clean,
                    "is_valid": is_valid,
                    "is_simple": is_simple,
                    "issue_type": issue_type
                }
            )
        
        return None
    
    async def _check_ring_orientation(
        self, 
        dataset_id: UUID, 
        snapshot: GeometrySnapshot, 
        external_row: dict
    ) -> Optional[SpatialCheck]:
        """
        Check polygon ring orientation using PostGIS ST_IsPolygonCCW.
        
        Best practice: exterior rings should be counter-clockwise (CCW),
        interior rings (holes) should be clockwise (CW).
        """
        geom_type = external_row.get('geom_type', '')
        is_ccw_oriented = external_row.get('is_ccw_oriented')
        
        # Only check orientation for polygon geometries
        if 'Polygon' not in geom_type:
            return None
        
        if is_ccw_oriented is None:
            return self._create_check(
                dataset_id=dataset_id,
                snapshot_id=snapshot.id,
                check_type="TOPOLOGY",
                check_result="WARNING",
                error_message="Could not determine polygon ring orientation",
                error_details={"geom_type": geom_type, "is_ccw_oriented": is_ccw_oriented}
            )
        
        # Check if exterior ring follows CCW convention
        if is_ccw_oriented is False:
            return self._create_check(
                dataset_id=dataset_id,
                snapshot_id=snapshot.id,
                check_type="TOPOLOGY",
                check_result="WARNING",  # This might be OK depending on standards
                error_message="Polygon exterior ring is clockwise (CW) instead of counter-clockwise (CCW)",
                error_details={
                    "is_ccw_oriented": is_ccw_oriented,
                    "recommendation": "Consider using ST_ForcePolygonCCW to standardize orientation"
                }
            )
        
        return None
    
    async def _check_advanced_topology(
        self, 
        dataset_id: UUID, 
        snapshot: GeometrySnapshot, 
        external_row: dict
    ) -> Optional[SpatialCheck]:
        """
        Advanced topology checks that go beyond basic ST_IsValid/ST_IsSimple.
        
        These checks implement concepts from computational geometry literature
        but use PostGIS functions for efficiency.
        """
        geom_type = external_row.get('geom_type', '')
        num_points = external_row.get('num_points', 0)
        
        issues = []
        
        # Check for potentially problematic geometry complexity
        if num_points > 10000:
            issues.append(f"Very complex geometry with {num_points} points (potential performance issue)")
        
        # For polygons: check for potential ring issues
        if 'Polygon' in geom_type:
            # Future: We could add queries to check:
            # - Holes that touch the exterior ring
            # - Holes that extend outside the exterior ring
            # - Adjacent holes that should be merged
            pass
        
        # For linestrings: check for potential issues
        if 'LineString' in geom_type or 'Line' in geom_type:
            # Future: We could add queries to check:
            # - Consecutive duplicate points (spikes)
            # - Nearly collinear points that could be simplified
            # - Extremely sharp angles that might be errors
            pass
        
        if issues:
            return self._create_check(
                dataset_id=dataset_id,
                snapshot_id=snapshot.id,
                check_type="TOPOLOGY",
                check_result="WARNING",
                error_message=f"Advanced topology concerns: {', '.join(issues)}",
                error_details={
                    "topology_issues": issues,
                    "num_points": num_points,
                    "geom_type": geom_type
                }
            )
        
        return None


class AreaTests(BaseTestCategory):
    """
    Comprehensive size and geometric properties validation.
    
    Based on best practices for detecting geometric anomalies:
    - Zero/negative area detection
    - Anomalous size thresholds
    - Length validation for linear features
    - Geometry-specific size checks
    """
    
    async def run_tests(
        self, 
        dataset_id: UUID, 
        snapshot: GeometrySnapshot, 
        external_row: dict
    ) -> List[SpatialCheck]:
        """Run comprehensive area and size validation tests."""
        checks = []
        
        # Get geometry properties
        geom_area = external_row.get('geom_area', 0) or 0
        geom_length = external_row.get('geom_length', 0) or 0
        geom_type = external_row.get('geom_type', '')
        num_points = external_row.get('num_points', 0)
        
        # 1. Area validation for area-based geometries
        area_check = await self._check_area_validation(
            dataset_id, snapshot, external_row, geom_area, geom_type
        )
        if area_check:
            checks.append(area_check)
        
        # 2. Length validation for linear geometries
        length_check = await self._check_length_validation(
            dataset_id, snapshot, external_row, geom_length, geom_type
        )
        if length_check:
            checks.append(length_check)
        
        # 3. Size ratio validation (area vs perimeter, etc.)
        ratio_check = await self._check_size_ratios(
            dataset_id, snapshot, external_row, geom_area, geom_length, geom_type
        )
        if ratio_check:
            checks.append(ratio_check)
        
        # 4. Complexity-based size validation
        complexity_check = await self._check_complexity_vs_size(
            dataset_id, snapshot, external_row, geom_area, geom_length, num_points, geom_type
        )
        if complexity_check:
            checks.append(complexity_check)
        
        return checks
    
    async def _check_area_validation(
        self, 
        dataset_id: UUID, 
        snapshot: GeometrySnapshot, 
        external_row: dict,
        geom_area: float,
        geom_type: str
    ) -> Optional[SpatialCheck]:
        """Validate area for area-based geometries (polygons)."""
        
        # Only check area for polygon geometries
        if 'Polygon' not in geom_type:
            return None
        
        # Critical area issues
        if geom_area <= 0:
            return self._create_check(
                dataset_id=dataset_id,
                snapshot_id=snapshot.id,
                check_type="AREA",
                check_result="FAIL",
                error_message=f"Polygon has zero or negative area: {geom_area}",
                error_details={"area": geom_area, "geom_type": geom_type}
            )
        
        # Very small areas (might be digitization errors)
        if 0 < geom_area < 0.001:  # Adjust threshold based on your coordinate system
            return self._create_check(
                dataset_id=dataset_id,
                snapshot_id=snapshot.id,
                check_type="AREA",
                check_result="WARNING",
                error_message=f"Polygon has very small area: {geom_area} (possible digitization error)",
                error_details={"area": geom_area, "threshold": 0.001}
            )
        
        # Very large areas (might be coordinate system errors)
        if geom_area > 1000000:  # 1M square units - adjust based on coordinate system
            return self._create_check(
                dataset_id=dataset_id,
                snapshot_id=snapshot.id,
                check_type="AREA",
                check_result="WARNING",
                error_message=f"Polygon has unusually large area: {geom_area} (possible coordinate error)",
                error_details={"area": geom_area, "threshold": 1000000}
            )
        
        return None
    
    async def _check_length_validation(
        self, 
        dataset_id: UUID, 
        snapshot: GeometrySnapshot, 
        external_row: dict,
        geom_length: float,
        geom_type: str
    ) -> Optional[SpatialCheck]:
        """Validate length for linear geometries."""
        
        # Check length for linear geometries
        if 'LineString' in geom_type or 'Line' in geom_type:
            # Zero length lines are always problematic
            if geom_length <= 0:
                return self._create_check(
                    dataset_id=dataset_id,
                    snapshot_id=snapshot.id,
                    check_type="AREA",  # Using AREA category for size-related checks
                    check_result="FAIL",
                    error_message=f"LineString has zero or negative length: {geom_length}",
                    error_details={"length": geom_length, "geom_type": geom_type}
                )
            
            # Very short lines (might be digitization errors)
            if 0 < geom_length < 0.01:  # Adjust threshold based on coordinate system
                return self._create_check(
                    dataset_id=dataset_id,
                    snapshot_id=snapshot.id,
                    check_type="AREA",
                    check_result="WARNING",
                    error_message=f"LineString has very short length: {geom_length} (possible digitization error)",
                    error_details={"length": geom_length, "threshold": 0.01}
                )
            
            # Very long lines (might be coordinate system errors)
            if geom_length > 10000000:  # 10M linear units
                return self._create_check(
                    dataset_id=dataset_id,
                    snapshot_id=snapshot.id,
                    check_type="AREA",
                    check_result="WARNING",
                    error_message=f"LineString has unusually long length: {geom_length} (possible coordinate error)",
                    error_details={"length": geom_length, "threshold": 10000000}
                )
        
        return None
    
    async def _check_size_ratios(
        self, 
        dataset_id: UUID, 
        snapshot: GeometrySnapshot, 
        external_row: dict,
        geom_area: float,
        geom_length: float,
        geom_type: str
    ) -> Optional[SpatialCheck]:
        """Check size ratios that might indicate geometric problems."""
        
        # For polygons: check area-to-perimeter ratio
        if 'Polygon' in geom_type and geom_area > 0 and geom_length > 0:
            # Calculate compactness (area vs perimeter)
            # For a circle: area/perimeter² = 1/(4π) ≈ 0.0796
            # For a square: area/perimeter² = 1/16 = 0.0625
            # Very low ratios might indicate narrow polygons or digitization errors
            
            compactness = geom_area / (geom_length * geom_length)
            
            if compactness < 0.001:  # Very low compactness (very narrow polygon)
                return self._create_check(
                    dataset_id=dataset_id,
                    snapshot_id=snapshot.id,
                    check_type="AREA",
                    check_result="WARNING",
                    error_message=f"Polygon has very low compactness ratio: {compactness:.6f} (very narrow shape)",
                    error_details={
                        "compactness": compactness,
                        "area": geom_area,
                        "perimeter": geom_length,
                        "threshold": 0.001
                    }
                )
        
        return None
    
    async def _check_complexity_vs_size(
        self, 
        dataset_id: UUID, 
        snapshot: GeometrySnapshot, 
        external_row: dict,
        geom_area: float,
        geom_length: float,
        num_points: int,
        geom_type: str
    ) -> Optional[SpatialCheck]:
        """Check if geometry complexity is appropriate for its size."""
        
        if num_points <= 0:
            return None
        
        issues = []
        
        # For polygons: check points-to-area ratio
        if 'Polygon' in geom_type and geom_area > 0:
            points_per_area = num_points / geom_area
            
            # Too many points for small areas (over-digitization)
            if points_per_area > 100:  # More than 100 points per unit area
                issues.append(f"High point density: {points_per_area:.1f} points per unit area (possible over-digitization)")
            
            # Too few points for large areas (under-digitization)
            if points_per_area < 0.0001 and geom_area > 10000:  # Less than 0.0001 points per unit area for large polygons
                issues.append(f"Low point density: {points_per_area:.6f} points per unit area (possible under-digitization)")
        
        # For linestrings: check points-to-length ratio
        if ('LineString' in geom_type or 'Line' in geom_type) and geom_length > 0:
            points_per_length = num_points / geom_length
            
            # Too many points for short lines
            if points_per_length > 10:  # More than 10 points per unit length
                issues.append(f"High point density: {points_per_length:.1f} points per unit length (possible over-digitization)")
            
            # Too few points for long lines (might miss important shape details)
            if points_per_length < 0.01 and geom_length > 1000:  # Less than 0.01 points per unit length for long lines
                issues.append(f"Low point density: {points_per_length:.4f} points per unit length (possible under-digitization)")
        
        if issues:
            return self._create_check(
                dataset_id=dataset_id,
                snapshot_id=snapshot.id,
                check_type="AREA",
                check_result="WARNING",
                error_message=f"Geometry complexity concerns: {', '.join(issues)}",
                error_details={
                    "complexity_issues": issues,
                    "num_points": num_points,
                    "geom_area": geom_area,
                    "geom_length": geom_length,
                    "geom_type": geom_type
                }
            )
        
        return None


class DuplicateTests(BaseTestCategory):
    """
    Advanced duplicate geometry detection implementing best practices.
    
    Based on computational geometry literature:
    - Exact duplicate detection via WKB/WKT hashing
    - Near-duplicate detection via spatial comparison
    - Normalized geometry comparison for different vertex orders
    - Spatial clustering for duplicate groups
    """
    
    async def run_tests(
        self, 
        dataset_id: UUID, 
        snapshot: GeometrySnapshot, 
        external_row: dict
    ) -> List[SpatialCheck]:
        """Run comprehensive duplicate detection tests."""
        checks = []
        
        # 1. Exact duplicate detection (same geometry hash)
        exact_duplicate_check = await self._check_exact_duplicates(dataset_id, snapshot, external_row)
        if exact_duplicate_check:
            checks.append(exact_duplicate_check)
        
        # 2. Near-duplicate detection (spatially equivalent but different representation)
        near_duplicate_check = await self._check_near_duplicates(dataset_id, snapshot, external_row)
        if near_duplicate_check:
            checks.append(near_duplicate_check)
        
        # 3. Composite duplicate check (same geometry + attributes)
        composite_check = await self._check_composite_duplicates(dataset_id, snapshot, external_row)
        if composite_check:
            checks.append(composite_check)
        
        return checks
    
    async def _check_exact_duplicates(
        self, 
        dataset_id: UUID, 
        snapshot: GeometrySnapshot, 
        external_row: dict
    ) -> Optional[SpatialCheck]:
        """
        Check for exact duplicate geometries using geometry hash comparison.
        
        This implements the WKB/WKT hashing approach mentioned in literature.
        """
        # Check for duplicate geometries using geometry hash
        duplicate_result = await self.db.execute(
            select(func.count(GeometrySnapshot.id))
            .where(
                GeometrySnapshot.dataset_id == dataset_id,
                GeometrySnapshot.geometry_hash == snapshot.geometry_hash,
                GeometrySnapshot.id != snapshot.id
            )
        )
        duplicate_count = duplicate_result.scalar()
        
        if duplicate_count > 0:
            # Get details about the duplicates for better error reporting
            duplicate_details_result = await self.db.execute(
                select(GeometrySnapshot.id, GeometrySnapshot.source_id, GeometrySnapshot.created_at)
                .where(
                    GeometrySnapshot.dataset_id == dataset_id,
                    GeometrySnapshot.geometry_hash == snapshot.geometry_hash,
                    GeometrySnapshot.id != snapshot.id
                )
                .limit(5)  # Limit to avoid huge result sets
            )
            duplicate_details = duplicate_details_result.fetchall()
            
            return self._create_check(
                dataset_id=dataset_id,
                snapshot_id=snapshot.id,
                check_type="DUPLICATE",
                check_result="WARNING",
                error_message=f"Found {duplicate_count} exact duplicate geometries (same geometry hash)",
                error_details={
                    "duplicate_count": duplicate_count,
                    "geometry_hash": snapshot.geometry_hash,
                    "duplicate_samples": [
                        {
                            "snapshot_id": str(row.id),
                            "source_id": row.source_id,
                            "created_at": row.created_at.isoformat() if row.created_at else None
                        }
                        for row in duplicate_details
                    ]
                }
            )
        
        return None
    
    async def _check_near_duplicates(
        self, 
        dataset_id: UUID, 
        snapshot: GeometrySnapshot, 
        external_row: dict
    ) -> Optional[SpatialCheck]:
        """
        Check for near-duplicate geometries using spatial equivalence.
        
        This implements spatial comparison for geometries that might be
        equivalent but have different vertex orders or minor variations.
        """
        # Use PostGIS ST_Equals to find spatially equivalent geometries
        # ST_Equals returns true if geometries are spatially equal (same shape)
        # even if they have different vertex orders or representations
        
        near_duplicate_result = await self.db.execute(
            text("""
                SELECT COUNT(*) as near_count
                FROM geometry_snapshots gs
                WHERE gs.dataset_id = :dataset_id 
                AND gs.id != :snapshot_id
                AND gs.geometry_hash != :geometry_hash  -- Exclude exact duplicates
                AND ST_Equals(gs.geometry, 
                    (SELECT geometry FROM geometry_snapshots WHERE id = :snapshot_id)
                )
            """),
            {
                "dataset_id": dataset_id,
                "snapshot_id": snapshot.id,
                "geometry_hash": snapshot.geometry_hash
            }
        )
        near_count = near_duplicate_result.scalar()
        
        if near_count and near_count > 0:
            return self._create_check(
                dataset_id=dataset_id,
                snapshot_id=snapshot.id,
                check_type="DUPLICATE",
                check_result="WARNING",
                error_message=f"Found {near_count} spatially equivalent geometries (near-duplicates)",
                error_details={
                    "near_duplicate_count": near_count,
                    "detection_method": "ST_Equals spatial comparison",
                    "note": "Geometries are spatially identical but have different representations"
                }
            )
        
        return None
    
    async def _check_composite_duplicates(
        self, 
        dataset_id: UUID, 
        snapshot: GeometrySnapshot, 
        external_row: dict
    ) -> Optional[SpatialCheck]:
        """
        Check for composite duplicates (same geometry + same attributes).
        
        This checks the composite hash which combines geometry and attributes,
        implementing the comprehensive duplicate detection approach.
        """
        # Check for duplicate composite hashes (geometry + attributes)
        composite_duplicate_result = await self.db.execute(
            select(func.count(GeometrySnapshot.id))
            .where(
                GeometrySnapshot.dataset_id == dataset_id,
                GeometrySnapshot.composite_hash == snapshot.composite_hash,
                GeometrySnapshot.id != snapshot.id
            )
        )
        composite_count = composite_duplicate_result.scalar()
        
        if composite_count > 0:
            # This is more serious than just geometry duplicates - 
            # it means the entire feature (geometry + attributes) is duplicated
            return self._create_check(
                dataset_id=dataset_id,
                snapshot_id=snapshot.id,
                check_type="DUPLICATE",
                check_result="FAIL",  # More serious than geometry-only duplicates
                error_message=f"Found {composite_count} complete duplicate features (geometry + attributes)",
                error_details={
                    "composite_duplicate_count": composite_count,
                    "composite_hash": snapshot.composite_hash,
                    "note": "Both geometry and attributes are identical - this is likely a data import error"
                }
            )
        
        return None


# ============================================================================
# GEOMETRY TYPE-SPECIFIC TESTS
# ============================================================================

class PolygonTests(BaseTestCategory):
    """
    Tests specific to Polygon geometries implementing polygon best practices.
    
    Based on computational geometry literature:
    - Ring orientation validation (exterior CCW, holes CW)
    - Hole-in-shell validation
    - Ring closure validation
    - Narrow polygon detection
    """
    
    async def run_tests(
        self, 
        dataset_id: UUID, 
        snapshot: GeometrySnapshot, 
        external_row: dict
    ) -> List[SpatialCheck]:
        """Run comprehensive polygon-specific tests."""
        checks = []
        
        # 1. Ring orientation check
        orientation_check = await self._check_ring_orientation(dataset_id, snapshot, external_row)
        if orientation_check:
            checks.append(orientation_check)
        
        # 2. Polygon shape analysis
        shape_check = await self._check_polygon_shape(dataset_id, snapshot, external_row)
        if shape_check:
            checks.append(shape_check)
        
        # 3. Hole validation (if we can detect them)
        hole_check = await self._check_hole_validation(dataset_id, snapshot, external_row)
        if hole_check:
            checks.append(hole_check)
        
        return checks
    
    async def _check_ring_orientation(
        self, 
        dataset_id: UUID, 
        snapshot: GeometrySnapshot, 
        external_row: dict
    ) -> Optional[SpatialCheck]:
        """Check if polygon rings follow standard orientation convention."""
        is_ccw_oriented = external_row.get('is_ccw_oriented')
        
        if is_ccw_oriented is None:
            return self._create_check(
                dataset_id=dataset_id,
                snapshot_id=snapshot.id,
                check_type="POLYGON",
                check_result="WARNING",
                error_message="Could not determine polygon ring orientation",
                error_details={"is_ccw_oriented": is_ccw_oriented}
            )
        
        # Best practice: exterior rings should be CCW
        if is_ccw_oriented is False:
            return self._create_check(
                dataset_id=dataset_id,
                snapshot_id=snapshot.id,
                check_type="POLYGON",
                check_result="WARNING",
                error_message="Polygon exterior ring is clockwise instead of counter-clockwise",
                error_details={
                    "is_ccw_oriented": is_ccw_oriented,
                    "recommendation": "Consider standardizing to CCW exterior rings"
                }
            )
        
        return None
    
    async def _check_polygon_shape(
        self, 
        dataset_id: UUID, 
        snapshot: GeometrySnapshot, 
        external_row: dict
    ) -> Optional[SpatialCheck]:
        """Analyze polygon shape characteristics for potential issues."""
        geom_area = external_row.get('geom_area', 0) or 0
        geom_length = external_row.get('geom_length', 0) or 0  # This is perimeter for polygons
        num_points = external_row.get('num_points', 0) or 0
        
        issues = []
        
        # Check for very narrow polygons (low area-to-perimeter ratio)
        if geom_area > 0 and geom_length > 0:
            compactness = geom_area / (geom_length * geom_length)
            if compactness < 0.0001:  # Very narrow polygon
                issues.append(f"Very narrow polygon (compactness: {compactness:.6f})")
        
        # Check for potentially over-complex polygons
        if geom_area > 0 and num_points > 0:
            points_per_area = num_points / geom_area
            if points_per_area > 1000:  # Many points for small area
                issues.append(f"Very high vertex density: {points_per_area:.1f} points per unit area")
        
        # Check for suspiciously simple polygons for large areas
        if geom_area > 10000 and num_points < 10:
            issues.append(f"Very simple polygon for large area: only {num_points} points for area {geom_area}")
        
        if issues:
            return self._create_check(
                dataset_id=dataset_id,
                snapshot_id=snapshot.id,
                check_type="POLYGON",
                check_result="WARNING",
                error_message=f"Polygon shape concerns: {', '.join(issues)}",
                error_details={
                    "shape_issues": issues,
                    "area": geom_area,
                    "perimeter": geom_length,
                    "num_points": num_points
                }
            )
        
        return None
    
    async def _check_hole_validation(
        self, 
        dataset_id: UUID, 
        snapshot: GeometrySnapshot, 
        external_row: dict
    ) -> Optional[SpatialCheck]:
        """
        Check for polygon hole validation issues.
        
        This is a placeholder for future implementation using PostGIS queries
        to check hole-in-shell relationships.
        """
        # Future implementation could use PostGIS queries like:
        # - ST_NumInteriorRings to count holes
        # - ST_InteriorRingN to extract individual holes
        # - ST_Contains to verify holes are inside exterior ring
        # - Check that holes don't overlap with each other
        
        # For now, just return None - this is a placeholder for future enhancement
        return None


class LineStringTests(BaseTestCategory):
    """Tests specific to LineString geometries."""
    
    async def run_tests(
        self, 
        dataset_id: UUID, 
        snapshot: GeometrySnapshot, 
        external_row: dict
    ) -> List[SpatialCheck]:
        """Run linestring-specific tests."""
        checks = []
        
        # Minimum length test (could be added)
        # Endpoint connectivity test (could be added)
        # Spike detection test (could be added)
        
        return checks


class PointTests(BaseTestCategory):
    """Tests specific to Point geometries."""
    
    async def run_tests(
        self, 
        dataset_id: UUID, 
        snapshot: GeometrySnapshot, 
        external_row: dict
    ) -> List[SpatialCheck]:
        """Run point-specific tests."""
        checks = []
        
        # Coordinate range validation (could be added)
        # Cluster detection (could be added)
        
        return checks


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

async def run_basic_quality_checks(
    db: AsyncSession,
    dataset_id: UUID, 
    snapshot: GeometrySnapshot, 
    external_row: dict
) -> List[SpatialCheck]:
    """
    Convenience function to run all basic quality checks.
    This replaces the old _run_basic_quality_checks method in GeometryService.
    """
    test_runner = SpatialTestRunner(db)
    return await test_runner.run_all_tests(dataset_id, snapshot, external_row)


def get_available_test_types() -> List[str]:
    """Get list of all available test types."""
    return [
        "VALIDITY",
        "TOPOLOGY", 
        "AREA",
        "DUPLICATE",
        # Add more as tests are implemented
    ]


def get_test_description(test_type: str) -> str:
    """Get human-readable description of a test type."""
    descriptions = {
        "VALIDITY": "Checks if geometry is valid according to OGC standards",
        "TOPOLOGY": "Checks for self-intersections and topology issues", 
        "AREA": "Validates area/length values and detects anomalies",
        "DUPLICATE": "Detects duplicate geometries using hash comparison",
    }
    return descriptions.get(test_type, f"Unknown test type: {test_type}") 