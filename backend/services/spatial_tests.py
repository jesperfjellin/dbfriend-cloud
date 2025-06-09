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
from sqlalchemy import select, func
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
        # This could be enhanced to actually inspect the geometry
        # For now, return None to skip type-specific tests
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
    """Geometry validity tests using PostGIS ST_IsValid."""
    
    async def run_tests(
        self, 
        dataset_id: UUID, 
        snapshot: GeometrySnapshot, 
        external_row: dict
    ) -> List[SpatialCheck]:
        """Run validity tests."""
        checks = []
        
        # Basic validity check
        is_valid = external_row.get('is_valid', True)
        validity_check = self._create_check(
            dataset_id=dataset_id,
            snapshot_id=snapshot.id,
            check_type="VALIDITY",
            check_result="PASS" if is_valid else "FAIL",
            error_message=None if is_valid else "Invalid geometry detected"
        )
        checks.append(validity_check)
        
        return checks


class TopologyTests(BaseTestCategory):
    """Topology-related tests (simplicity, self-intersections, etc.)."""
    
    async def run_tests(
        self, 
        dataset_id: UUID, 
        snapshot: GeometrySnapshot, 
        external_row: dict
    ) -> List[SpatialCheck]:
        """Run topology tests."""
        checks = []
        
        # Simplicity check (basic topology)
        is_simple = external_row.get('is_simple', True)
        simplicity_check = self._create_check(
            dataset_id=dataset_id,
            snapshot_id=snapshot.id,
            check_type="TOPOLOGY",
            check_result="PASS" if is_simple else "FAIL",
            error_message=None if is_simple else "Non-simple geometry (self-intersections)"
        )
        checks.append(simplicity_check)
        
        return checks


class AreaTests(BaseTestCategory):
    """Area and length validation tests."""
    
    async def run_tests(
        self, 
        dataset_id: UUID, 
        snapshot: GeometrySnapshot, 
        external_row: dict
    ) -> List[SpatialCheck]:
        """Run area/length tests."""
        checks = []
        
        geom_area = external_row.get('geom_area', 0) or 0
        
        # Area validation
        area_result = "PASS"
        area_message = None
        
        if geom_area <= 0:
            area_result = "FAIL"
            area_message = f"Zero or negative area: {geom_area}"
        elif geom_area > 1000000:
            area_result = "WARNING"
            area_message = f"Unusually large area: {geom_area}"
        
        area_check = self._create_check(
            dataset_id=dataset_id,
            snapshot_id=snapshot.id,
            check_type="AREA",
            check_result=area_result,
            error_message=area_message,
            error_details={"area": geom_area} if area_message else None
        )
        checks.append(area_check)
        
        return checks


class DuplicateTests(BaseTestCategory):
    """Duplicate geometry detection tests."""
    
    async def run_tests(
        self, 
        dataset_id: UUID, 
        snapshot: GeometrySnapshot, 
        external_row: dict
    ) -> List[SpatialCheck]:
        """Run duplicate detection tests."""
        checks = []
        
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
            duplicate_check = self._create_check(
                dataset_id=dataset_id,
                snapshot_id=snapshot.id,
                check_type="DUPLICATE",
                check_result="WARNING",
                error_message=f"Found {duplicate_count} duplicate geometries",
                error_details={"duplicate_count": duplicate_count}
            )
            checks.append(duplicate_check)
        
        return checks


# ============================================================================
# GEOMETRY TYPE-SPECIFIC TESTS
# ============================================================================

class PolygonTests(BaseTestCategory):
    """Tests specific to Polygon geometries."""
    
    async def run_tests(
        self, 
        dataset_id: UUID, 
        snapshot: GeometrySnapshot, 
        external_row: dict
    ) -> List[SpatialCheck]:
        """Run polygon-specific tests."""
        checks = []
        
        # Ring orientation test (could be added)
        # Hole validation test (could be added)
        # Minimum area threshold test (could be added)
        
        return checks


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