"""
API v1 router - includes all v1 endpoints
"""

from fastapi import APIRouter

from .datasets import router as datasets_router
from .diffs import router as diffs_router
from .geometry import router as geometry_router

# Create main API router
api_router = APIRouter()

# Include all route modules
api_router.include_router(datasets_router, prefix="/datasets", tags=["datasets"])
api_router.include_router(diffs_router, prefix="/diffs", tags=["diffs"])
api_router.include_router(geometry_router, prefix="/geometry", tags=["geometry"]) 