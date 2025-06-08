#!/usr/bin/env python3
"""
dbfriend-cloud: SaaS for geometry-diff QA on PostGIS
Main FastAPI application entry point
"""

import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn
from rich.console import Console
from rich.logging import RichHandler
import logging

from config import settings
from database import engine, init_db
from api.v1 import api_router

# Initialize rich console for logging
console = Console()

# Configure logging with rich
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[
        RichHandler(
            console=console,
            rich_tracebacks=True,
            show_path=False,
            show_time=True,
            markup=True
        )
    ]
)
logger = logging.getLogger("dbfriend-cloud")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle application startup and shutdown."""
    # Startup
    logger.info("[cyan]🚀 Starting dbfriend-cloud...[/cyan]")
    await init_db()
    logger.info("[green]✓ dbfriend-cloud ready[/green]")
    
    yield
    
    # Shutdown
    logger.info("[yellow]📋 Shutting down dbfriend-cloud...[/yellow]")


# Create FastAPI application
app = FastAPI(
    title="dbfriend-cloud",
    description="SaaS for geometry-diff QA on PostGIS: automated checks + Git-style accept/reject queue",
    version="0.1.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    lifespan=lifespan
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(api_router, prefix="/api/v1")


@app.get("/")
async def root():
    """Root endpoint with basic service info."""
    return {
        "service": "dbfriend-cloud",
        "version": "0.1.0",
        "description": "SaaS for geometry-diff QA on PostGIS",
        "docs": "/api/docs"
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    try:
        # Basic database connectivity check would go here
        return {"status": "healthy", "timestamp": None}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "error": str(e)}
        )


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    ) 