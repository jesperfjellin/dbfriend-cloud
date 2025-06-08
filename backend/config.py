"""
Configuration settings for dbfriend-cloud
Uses Pydantic settings for environment-based configuration
"""

from typing import List, Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # Application settings
    APP_NAME: str = "dbfriend-cloud"
    APP_VERSION: str = "0.1.0"
    DEBUG: bool = Field(default=False, description="Enable debug mode")
    
    # Database settings
    DATABASE_URL: str = Field(
        default="postgresql+asyncpg://dbfriend:password@localhost:5432/dbfriend_cloud",
        description="PostgreSQL database URL with asyncpg driver"
    )
    
    # Note: We don't manage user PostGIS infrastructure (no extensions/schemas/tables)
    # We connect with read/write to analyze data and commit approved fixes
    # PostGIS extensions should already be enabled in user databases
    
    # CORS settings
    ALLOWED_ORIGINS: List[str] = Field(
        default=["http://localhost:3000", "http://localhost:8000"],
        description="Allowed CORS origins"
    )
    
    # Worker queue settings (for Celery)
    REDIS_URL: str = Field(
        default="redis://localhost:6379/0",
        description="Redis URL for Celery broker"
    )
    CELERY_TASK_ALWAYS_EAGER: bool = Field(
        default=False,
        description="Execute Celery tasks synchronously (useful for testing)"
    )
    
    # Geometry processing settings
    MAX_GEOMETRY_COMPLEXITY: int = Field(
        default=10000,
        description="Maximum number of vertices in a geometry for processing"
    )
    GEOMETRY_SIMPLIFICATION_TOLERANCE: float = Field(
        default=0.0001,
        description="Tolerance for geometry simplification in degrees"
    )
    
    # Diff processing settings
    DIFF_BATCH_SIZE: int = Field(
        default=1000,
        description="Number of geometries to process in a single batch"
    )
    DIFF_TIMEOUT_SECONDS: int = Field(
        default=300,
        description="Timeout for diff operations in seconds"
    )
    
    # Spatial analysis settings
    SPATIAL_INDEX_METHOD: str = Field(
        default="GIST",
        description="Spatial index method (GIST, SP-GIST, etc.)"
    )
    DEFAULT_SRID: int = Field(
        default=4326,
        description="Default SRID for geometry operations"
    )
    
    # Security settings
    SECRET_KEY: str = Field(
        default="your-secret-key-change-in-production",
        description="Secret key for JWT tokens and other cryptographic operations"
    )
    ACCESS_TOKEN_EXPIRE_MINUTES: int = Field(
        default=30,
        description="Access token expiration time in minutes"
    )
    
    # Logging settings
    LOG_LEVEL: str = Field(default="INFO", description="Logging level")
    LOG_FORMAT: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        description="Log format string"
    )


# Create global settings instance
settings = Settings() 