"""
Spatial Tests Configuration

This file contains configuration for all spatial quality tests.
Easy to modify test behavior, enable/disable tests, and set thresholds.
"""

from typing import Dict, Any


class TestConfig:
    """Configuration settings for spatial quality tests."""
    
    # ============================================================================
    # GENERAL TEST SETTINGS
    # ============================================================================
    
    # Which test categories to run
    ENABLED_TEST_CATEGORIES = {
        "validity": True,
        "topology": True, 
        "area": True,
        "duplicate": True,
        "geometry_specific": False,  # Disabled for now, can be enabled later
    }
    
    # ============================================================================
    # VALIDITY TESTS
    # ============================================================================
    
    VALIDITY_CONFIG = {
        "enabled": True,
        "fail_on_invalid": True,  # If False, invalid geometries will be WARNING instead of FAIL
    }
    
    # ============================================================================
    # TOPOLOGY TESTS  
    # ============================================================================
    
    TOPOLOGY_CONFIG = {
        "enabled": True,
        "check_simplicity": True,
        "fail_on_self_intersection": True,
    }
    
    # ============================================================================
    # AREA/LENGTH TESTS
    # ============================================================================
    
    AREA_CONFIG = {
        "enabled": True,
        "zero_area_threshold": 0.0,  # Areas <= this are considered invalid
        "large_area_threshold": 1000000,  # Areas > this trigger warnings
        "fail_on_zero_area": True,  # If False, zero area will be WARNING
        "warn_on_large_area": True,
    }
    
    # ============================================================================
    # DUPLICATE TESTS
    # ============================================================================
    
    DUPLICATE_CONFIG = {
        "enabled": True,
        "duplicate_result": "WARNING",  # "WARNING", "FAIL", or "INFO"
        "check_geometry_hash": True,
        "check_full_match": False,  # Could be added to check attributes too
    }
    
    # ============================================================================
    # GEOMETRY TYPE SPECIFIC TESTS
    # ============================================================================
    
    POLYGON_CONFIG = {
        "enabled": False,  # Not implemented yet
        "check_ring_orientation": False,
        "check_hole_validity": False,
        "minimum_area": None,
    }
    
    LINESTRING_CONFIG = {
        "enabled": False,  # Not implemented yet
        "minimum_length": None,
        "check_connectivity": False,
        "detect_spikes": False,
    }
    
    POINT_CONFIG = {
        "enabled": False,  # Not implemented yet
        "coordinate_range": None,
        "cluster_detection": False,
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
        }
        return config_map.get(test_category, {})
    
    @classmethod
    def get_area_thresholds(cls) -> Dict[str, float]:
        """Get area validation thresholds."""
        return {
            "zero_threshold": cls.AREA_CONFIG["zero_area_threshold"],
            "large_threshold": cls.AREA_CONFIG["large_area_threshold"],
        }
    
    @classmethod
    def should_fail_on_invalid(cls) -> bool:
        """Check if invalid geometries should fail tests."""
        return cls.VALIDITY_CONFIG.get("fail_on_invalid", True)
    
    @classmethod
    def get_duplicate_result_level(cls) -> str:
        """Get the result level for duplicate detections."""
        return cls.DUPLICATE_CONFIG.get("duplicate_result", "WARNING")


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