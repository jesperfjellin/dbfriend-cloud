"""
Development worker - runs monitoring tasks without Redis/Celery infrastructure
Perfect for local development and testing
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from config import settings
from database import AsyncSessionLocal, Dataset
from services.geometry_service import GeometryService
from sqlalchemy import select

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("dbfriend-cloud.dev-worker")


async def monitor_dataset(dataset_id: str):
    """
    Development version of dataset monitoring (no Celery required)
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
            
            logger.info(f"🔍 Starting QA monitoring for dataset: {dataset.name}")
            
            # Core monitoring functionality:
            # 1. Connect to user's PostGIS database
            # 2. Read current geometry state  
            # 3. Compare with previous snapshots
            # 4. Run spatial quality checks
            # 5. Flag problematic geometries for review
            response = await geometry_service.import_geometries_from_external_source(
                dataset, force_reimport=False
            )
            
            # Update dataset status
            dataset.last_check_at = datetime.now(timezone.utc)
            dataset.connection_status = "success" if response.status == "SUCCESS" else "failed"
            if response.status == "FAILED":
                dataset.connection_error = response.error_message
            else:
                dataset.connection_error = None
                
            await db.commit()
            
            logger.info(f"✅ QA monitoring completed for {dataset.name}: "
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
            logger.error(f"❌ Error monitoring dataset {dataset_id}: {e}")
            
            # Update dataset with error status
            if 'dataset' in locals() and dataset:
                dataset.connection_status = "failed"
                dataset.connection_error = str(e)
                await db.commit()
            
            return {
                "status": "error",
                "dataset_id": dataset_id,
                "error": str(e)
            }


async def monitor_all_datasets():
    """
    Development monitoring loop - checks all datasets according to their intervals
    """
    async with AsyncSessionLocal() as db:
        try:
            # Get all active datasets
            result = await db.execute(
                select(Dataset).where(Dataset.is_active == True)
            )
            datasets = result.scalars().all()
            
            now = datetime.now(timezone.utc)
            monitored = 0
            
            logger.info(f"🔍 Checking {len(datasets)} active datasets...")
            
            for dataset in datasets:
                # Check if it's time to monitor this dataset
                should_check = False
                
                if dataset.last_check_at is None:
                    # Never checked - do it now
                    should_check = True
                    logger.info(f"📋 {dataset.name}: First check (never checked before)")
                else:
                    # Check based on interval
                    next_check = dataset.last_check_at + timedelta(
                        minutes=dataset.check_interval_minutes
                    )
                    should_check = now >= next_check
                    
                    if should_check:
                        logger.info(f"⏰ {dataset.name}: Time for scheduled check (interval: {dataset.check_interval_minutes}m)")
                    else:
                        time_until_next = next_check - now
                        logger.debug(f"⏳ {dataset.name}: Next check in {time_until_next}")
                
                if should_check:
                    result = await monitor_dataset(str(dataset.id))
                    monitored += 1
                    
                    # Add small delay between datasets to avoid overwhelming connections
                    await asyncio.sleep(1)
            
            logger.info(f"✅ Monitoring cycle completed: {monitored}/{len(datasets)} datasets checked")
            return {"monitored": monitored, "total_datasets": len(datasets)}
            
        except Exception as e:
            logger.error(f"❌ Error in monitoring cycle: {e}")
            return {"error": str(e)}


async def development_monitoring_loop():
    """
    Main development monitoring loop - runs continuously
    """
    logger.info("🚀 Starting dbfriend-cloud development monitoring...")
    logger.info("💡 This replaces Celery+Redis for easier local development")
    
    while True:
        try:
            await monitor_all_datasets()
            
            # Wait 60 seconds before next monitoring cycle
            logger.info("😴 Waiting 60 seconds before next monitoring cycle...")
            await asyncio.sleep(60)
            
        except KeyboardInterrupt:
            logger.info("👋 Monitoring stopped by user")
            break
        except Exception as e:
            logger.error(f"❌ Unexpected error in monitoring loop: {e}")
            logger.info("⏳ Waiting 30 seconds before retry...")
            await asyncio.sleep(30)


if __name__ == "__main__":
    """
    Run the development worker
    Usage: python worker_dev.py
    """
    try:
        asyncio.run(development_monitoring_loop())
    except KeyboardInterrupt:
        logger.info("👋 Development worker stopped") 