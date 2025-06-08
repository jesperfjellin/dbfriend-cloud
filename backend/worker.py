"""
Celery worker for background tasks
Handles scheduled dataset monitoring and spatial QA checks
"""

import os
import asyncio
from celery import Celery
from celery.schedules import crontab
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
import logging

from config import settings
from database import AsyncSessionLocal, Dataset
from services.geometry_service import GeometryService

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dbfriend-cloud.worker")

# Create Celery app
celery_app = Celery(
    "dbfriend-cloud-worker",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
    include=["worker"]
)

# Celery configuration
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_always_eager=settings.CELERY_TASK_ALWAYS_EAGER,
    task_routes={
        "worker.monitor_dataset": {"queue": "monitoring"},
        "worker.monitor_all_datasets": {"queue": "monitoring"},
    },
    beat_schedule={
        "monitor-all-datasets": {
            "task": "worker.monitor_all_datasets",
            "schedule": 60.0,  # Check every minute
        },
    },
)


async def async_monitor_dataset(dataset_id: str):
    """
    Async function to monitor a single dataset for spatial quality issues.
    This is the core QA monitoring functionality.
    """
    async with AsyncSessionLocal() as db:
        try:
            # Get dataset
            result = await db.execute(
                select(Dataset).where(Dataset.id == dataset_id)
            )
            dataset = result.scalar_one_or_none()
            
            if not dataset or not dataset.is_active:
                logger.warning(f"Dataset {dataset_id} not found or inactive")
                return {"status": "skipped", "reason": "inactive"}
            
            # Create geometry service
            geometry_service = GeometryService(db)
            
            logger.info(f"Starting QA monitoring for dataset: {dataset.name}")
            
            # This is where the magic happens:
            # 1. Connect to user's PostGIS database
            # 2. Read current geometry state
            # 3. Compare with previous snapshots
            # 4. Run spatial quality checks
            # 5. Flag problematic geometries for review
            response = await geometry_service.import_geometries_from_external_source(
                dataset, force_reimport=False
            )
            
            # Update dataset last check time
            from datetime import datetime, timezone
            dataset.last_check_at = datetime.now(timezone.utc)
            dataset.connection_status = "success" if response.status == "SUCCESS" else "failed"
            if response.status == "FAILED":
                dataset.connection_error = response.error_message
            else:
                dataset.connection_error = None
                
            await db.commit()
            
            logger.info(f"QA monitoring completed for {dataset.name}: "
                       f"{response.snapshots_created} new snapshots, "
                       f"{response.diffs_detected} issues flagged")
            
            return {
                "status": "completed",
                "dataset_id": dataset_id,
                "snapshots_created": response.snapshots_created,
                "diffs_detected": response.diffs_detected,
                "duration": response.import_duration_seconds
            }
            
        except Exception as e:
            logger.error(f"Error monitoring dataset {dataset_id}: {e}")
            
            # Update dataset with error status
            if dataset:
                dataset.connection_status = "failed"
                dataset.connection_error = str(e)
                await db.commit()
            
            return {
                "status": "error",
                "dataset_id": dataset_id,
                "error": str(e)
            }


@celery_app.task(bind=True)
def monitor_dataset(self, dataset_id: str):
    """
    Celery task to monitor a single dataset.
    This runs the spatial QA checks and flags problematic geometries.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        result = loop.run_until_complete(async_monitor_dataset(dataset_id))
        return result
    except Exception as e:
        logger.error(f"Task failed for dataset {dataset_id}: {e}")
        raise self.retry(countdown=60, max_retries=3)
    finally:
        loop.close()


@celery_app.task
def monitor_all_datasets():
    """
    Celery beat task that runs every minute to check all active datasets.
    This dispatches individual monitoring tasks based on each dataset's check interval.
    """
    async def async_dispatch():
        async with AsyncSessionLocal() as db:
            try:
                # Get all active datasets
                result = await db.execute(
                    select(Dataset).where(Dataset.is_active == True)
                )
                datasets = result.scalars().all()
                
                from datetime import datetime, timedelta, timezone
                now = datetime.now(timezone.utc)
                dispatched = 0
                
                for dataset in datasets:
                    # Check if it's time to monitor this dataset
                    if dataset.last_check_at is None:
                        # Never checked - do it now
                        should_check = True
                    else:
                        # Check based on interval
                        next_check = dataset.last_check_at + timedelta(
                            minutes=dataset.check_interval_minutes
                        )
                        should_check = now >= next_check
                    
                    if should_check:
                        logger.info(f"Dispatching monitoring task for dataset: {dataset.name}")
                        monitor_dataset.delay(str(dataset.id))
                        dispatched += 1
                    else:
                        # Log next check time
                        next_check = dataset.last_check_at + timedelta(
                            minutes=dataset.check_interval_minutes
                        )
                        logger.debug(f"Dataset {dataset.name} next check: {next_check}")
                
                logger.info(f"Dispatched {dispatched} monitoring tasks")
                return {"dispatched": dispatched, "total_datasets": len(datasets)}
                
            except Exception as e:
                logger.error(f"Error in monitor_all_datasets: {e}")
                return {"error": str(e)}
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        result = loop.run_until_complete(async_dispatch())
        return result
    finally:
        loop.close()


if __name__ == "__main__":
    # For testing purposes
    logger.info("Starting Celery worker...")
    celery_app.start() 