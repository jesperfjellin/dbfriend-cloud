"""
Spatial Tests Configuration

This file contains configuration for all spatial quality tests.
Easy to modify test behavior, enable/disable tests, and set thresholds.

Based on ChatGPT feedback: All numeric thresholds externalized for easy tuning.
"""

from typing import Dict, Any, Optional


class TestConfig:
    """Configuration settings for spatial quality tests with externalized thresholds."""
    
    # ============================================================================
    # GENERAL TEST SETTINGS
    # ============================================================================
    
    # Which test categories to run
    ENABLED_TEST_CATEGORIES = {
        "validity": True,
        "topology": True, 
        "area": True,
        "duplicate": True,
        "geometry_specific": True,  # Enable geometry-specific tests
    }
    
    # ============================================================================
    # VALIDITY TESTS
    # ============================================================================
    
    VALIDITY_CONFIG = {
        "enabled": True,
        "fail_on_invalid": True,
        # Coordinate bounds validation
        "max_coordinate_magnitude": 20000000,  # Coordinates with abs() > this are flagged
        "check_coordinate_bounds": True,
        # Point count validation
        "min_polygon_points": 4,
        "min_linestring_points": 2,
        "min_point_points": 1,
        "max_point_points": 1,
    }
    
    # ============================================================================
    # TOPOLOGY TESTS  
    # ============================================================================
    
    TOPOLOGY_CONFIG = {
        "enabled": True,
        "check_simplicity": True,
        "check_topological_cleanliness": True,
        "check_ring_orientation": True,
        "fail_on_self_intersection": True,
        # Performance optimization thresholds
        "max_points_for_full_topology_check": 10000,  # Skip expensive checks on very complex geometries
        "bbox_snap_grid_for_intersection_probe": 1.0,  # Grid size for fast intersection probe
    }
    
    # ============================================================================
    # AREA/LENGTH TESTS
    # ============================================================================
    
    AREA_CONFIG = {
        "enabled": True,
        # Area thresholds
        "zero_area_threshold": 0.0,
        "small_area_threshold": 0.001,  # Very small areas (possible digitization errors)
        "large_area_threshold": 1000000,  # Very large areas (possible coordinate errors)
        # Length thresholds  
        "zero_length_threshold": 0.0,
        "small_length_threshold": 0.01,  # Very short lines
        "large_length_threshold": 10000000,  # Very long lines
        # Compactness and complexity
        "min_compactness_ratio": 0.001,  # area / (perimeter^2) - flags very narrow polygons
        "max_point_density_per_area": 1000,  # points per unit area - over-digitization
        "min_point_density_per_area": 0.0001,  # points per unit area - under-digitization  
        "max_point_density_per_length": 10,  # points per unit length
        "min_point_density_per_length": 0.01,  # points per unit length
        "min_area_for_density_check": 10000,  # Only check density for areas > this
        "min_length_for_density_check": 1000,  # Only check density for lengths > this
        # Result levels
        "fail_on_zero_area": True,
        "fail_on_zero_length": True,
        "warn_on_large_geometries": True,
        "warn_on_small_geometries": True,
        "warn_on_complexity_issues": True,
    }
    
    # ============================================================================
    # DUPLICATE TESTS
    # ============================================================================
    
    DUPLICATE_CONFIG = {
        "enabled": True,
        "check_exact_duplicates": True,
        "check_near_duplicates": True,  # ST_Equals spatial comparison
        "check_composite_duplicates": True,  # geometry + attributes
        # Result levels by duplicate type
        "exact_duplicate_result": "WARNING",
        "near_duplicate_result": "WARNING", 
        "composite_duplicate_result": "FAIL",  # More serious - likely import error
        # Performance settings
        "max_duplicate_samples_in_details": 5,  # Limit result set size
        "enable_spatial_duplicate_search": True,  # Can disable for performance
    }
    
    # ============================================================================
    # GEOMETRY TYPE SPECIFIC TESTS
    # ============================================================================
    
    POLYGON_CONFIG = {
        "enabled": True,
        "check_ring_orientation": True,
        "prefer_ccw_exterior": True,  # Counter-clockwise exterior rings
        "check_polygon_shape": True,
        "check_hole_validation": False,  # Future implementation
        # Shape analysis thresholds
        "min_compactness_for_narrow_warning": 0.0001,
        "max_point_density_for_complexity_warning": 1000,
        "min_points_for_large_area": 10,
        "large_area_threshold": 10000,
    }
    
    LINESTRING_CONFIG = {
        "enabled": True,
        "check_length_validation": True,
        "check_point_density": True,
        "detect_spikes": False,  # Future implementation
        "check_connectivity": False,  # Future implementation
    }
    
    POINT_CONFIG = {
        "enabled": True,
        "check_coordinate_range": True,
        "cluster_detection": False,  # Future implementation
    }
    
    # ============================================================================
    # CONFIDENCE SCORING
    # ============================================================================
    
    CONFIDENCE_CONFIG = {
        # Base confidence scores by issue type
        "invalid_geometry_confidence": 0.95,
        "non_simple_geometry_confidence": 0.90,
        "topologically_unclean_confidence": 0.85,
        "zero_area_polygon_confidence": 0.90,
        "zero_length_line_confidence": 0.90,
        "large_geometry_confidence": 0.70,
        "very_large_geometry_confidence": 0.65,
        "degenerate_geometry_confidence": 0.95,
        "insufficient_points_confidence": 0.90,
        "suspicious_coordinates_confidence": 0.75,
        "default_confidence": 0.50,
        # Confidence adjustments
        "complex_geometry_confidence_reduction": 0.9,  # Multiply by this for complex geoms
        "very_complex_geometry_confidence_reduction": 0.8,
        "complex_geometry_point_threshold": 100,
        "very_complex_geometry_point_threshold": 1000,
        # Flagging threshold
        "problematic_geometry_confidence_threshold": 0.75,  # Only flag high-confidence issues
    }
    
    # ============================================================================
    # UNIT SCALING (Per ChatGPT feedback on SRID/units)
    # ============================================================================
    
    UNIT_SCALING = {
        # Default unit scale (for projected coordinates in meters)
        "default_unit_scale": 1.0,
        # Common unit scales
        "meters": 1.0,
        "feet": 0.3048,
        "degrees": 111319.5,  # Approximate meters per degree at equator
        # Dataset-specific overrides (can be expanded)
        "unit_overrides": {
            # "dataset_id": scale_factor
        }
    }
    
    # ============================================================================
    # PERFORMANCE OPTIMIZATION
    # ============================================================================
    
    PERFORMANCE_CONFIG = {
        "use_streaming_queries": True,  # Use cursors for large result sets
        "max_rows_in_memory": 10000,  # Stream if more than this
        "skip_expensive_checks_threshold": 50000,  # Skip some checks for huge datasets
        "enable_parallel_processing": True,  # Future implementation
        "max_concurrent_dataset_tasks": 5,  # Limit concurrent processing
    }
    
    # ============================================================================
    # CONVENIENCE METHODS
    # ============================================================================
    
    @classmethod
    def is_test_enabled(cls, test_category: str) -> bool:
        """Check if a test category is enabled."""
        return cls.ENABLED_TEST_CATEGORIES.get(test_category, False)
    
    @classmethod
    def get_test_config(cls, test_category: str) -> Dict[str, Any]:
        """Get configuration for a specific test category."""
        config_map = {
            "validity": cls.VALIDITY_CONFIG,
            "topology": cls.TOPOLOGY_CONFIG,
            "area": cls.AREA_CONFIG,
            "duplicate": cls.DUPLICATE_CONFIG,
            "polygon": cls.POLYGON_CONFIG,
            "linestring": cls.LINESTRING_CONFIG,
            "point": cls.POINT_CONFIG,
            "confidence": cls.CONFIDENCE_CONFIG,
            "performance": cls.PERFORMANCE_CONFIG,
        }
        return config_map.get(test_category, {})
    
    @classmethod
    def get_area_thresholds(cls, unit_scale: float = 1.0) -> Dict[str, float]:
        """Get area validation thresholds scaled for coordinate system."""
        base = cls.AREA_CONFIG
        scale_sq = unit_scale * unit_scale  # Area scales with square of linear scale
        return {
            "zero_threshold": base["zero_area_threshold"],
            "small_threshold": base["small_area_threshold"] * scale_sq,
            "large_threshold": base["large_area_threshold"] * scale_sq,
        }
    
    @classmethod
    def get_length_thresholds(cls, unit_scale: float = 1.0) -> Dict[str, float]:
        """Get length validation thresholds scaled for coordinate system."""
        base = cls.AREA_CONFIG
        return {
            "zero_threshold": base["zero_length_threshold"],
            "small_threshold": base["small_length_threshold"] * unit_scale,
            "large_threshold": base["large_length_threshold"] * unit_scale,
        }
    
    @classmethod
    def get_coordinate_bounds(cls) -> float:
        """Get coordinate magnitude threshold."""
        return cls.VALIDITY_CONFIG["max_coordinate_magnitude"]
    
    @classmethod
    def get_confidence_threshold(cls) -> float:
        """Get threshold for flagging geometries as problematic."""
        return cls.CONFIDENCE_CONFIG["problematic_geometry_confidence_threshold"]
    
    @classmethod
    def get_unit_scale(cls, dataset_id: Optional[str] = None, srid: Optional[int] = None) -> float:
        """
        Get unit scale factor for a dataset.
        
        Future enhancement: Could query dataset table for unit info or
        use SRID to determine coordinate system and appropriate scaling.
        """
        if dataset_id and dataset_id in cls.UNIT_SCALING["unit_overrides"]:
            return cls.UNIT_SCALING["unit_overrides"][dataset_id]
        
        # Future: Use SRID to determine appropriate scaling
        # For now, return default
        return cls.UNIT_SCALING["default_unit_scale"]
    
    @classmethod
    def should_fail_on_invalid(cls) -> bool:
        """Check if invalid geometries should fail tests."""
        return cls.VALIDITY_CONFIG.get("fail_on_invalid", True)
    
    @classmethod
    def get_duplicate_result_level(cls, duplicate_type: str = "exact") -> str:
        """Get the result level for duplicate detections."""
        if duplicate_type == "composite":
            return cls.DUPLICATE_CONFIG.get("composite_duplicate_result", "FAIL")
        elif duplicate_type == "near":
            return cls.DUPLICATE_CONFIG.get("near_duplicate_result", "WARNING")
        else:  # exact
            return cls.DUPLICATE_CONFIG.get("exact_duplicate_result", "WARNING")


# ============================================================================
# DATASET-SPECIFIC OVERRIDES (Future Enhancement)
# ============================================================================

class DatasetTestConfig:
    """
    Future enhancement: Allow per-dataset test configuration overrides.
    
    Example usage:
    config = DatasetTestConfig.get_config(dataset_id)
    if config.area_threshold_override:
        use_custom_threshold = config.area_threshold_override
    """
    
    @classmethod
    def get_config(cls, dataset_id: str) -> Dict[str, Any]:
        """Get dataset-specific test configuration overrides."""
        # This could be implemented to load from database or config files
        return {}
    
    @classmethod
    def has_overrides(cls, dataset_id: str) -> bool:
        """Check if a dataset has custom test configuration."""
        return False  # Not implemented yet 