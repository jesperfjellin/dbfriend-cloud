"""
dbfriend-cloud Development Worker
Simpler async worker without Redis dependency for local development
Implements dual-timer architecture: change detection + quality checks
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from config import settings
from database import AsyncSessionLocal, Dataset
from services.geometry_service import GeometryService

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Back to normal logging
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("dbfriend-cloud.dev-worker")


class DevelopmentWorker:
    """Development worker focused on change detection only. Quality checks now user-controlled."""
    
    def __init__(self):
        self.change_detection_interval = 60  # seconds
        self.running = False
    
    async def start(self):
        """Start the change detection monitoring system."""
        logger.info("🚀 Starting dbfriend-cloud development monitoring...")
        logger.info("💡 This replaces Celery+Redis for easier local development")
        logger.info(f"📊 Change detection: every {self.change_detection_interval}s")
        logger.info("🔍 Quality checks: now user-controlled via frontend")
        
        self.running = True
        
        # Start change detection loop only
        change_task = asyncio.create_task(self._change_detection_loop())
        
        try:
            await change_task
        except KeyboardInterrupt:
            logger.info("👋 Shutting down development worker...")
            self.running = False
            change_task.cancel()
    
    async def _change_detection_loop(self):
        """Main loop for detecting data changes (runs every minute)."""
        while self.running:
            try:
                await self._monitor_dataset_changes()
                await asyncio.sleep(self.change_detection_interval)
            except Exception as e:
                logger.error(f"❌ Error in change detection loop: {e}")
                await asyncio.sleep(30)  # Shorter retry interval
    

    
    async def _monitor_dataset_changes(self):
        """Monitor all datasets for CHANGES only."""
        async with AsyncSessionLocal() as db:
            try:
                # Get all active datasets
                from sqlalchemy import select
                result = await db.execute(
                    select(Dataset).where(Dataset.is_active == True)
                )
                datasets = result.scalars().all()
                
                now = datetime.now(timezone.utc)
                monitored = 0
                
                logger.info(f"🔍 Checking {len(datasets)} active datasets for changes...")
                
                for dataset in datasets:
                    # Check if it's time to monitor this dataset
                    if dataset.last_check_at is None:
                        # Never checked - do it now
                        should_check = True
                        logger.info(f"📋 {dataset.name}: First check (never checked before, dataset_id: {dataset.id})")
                    else:
                        # Check based on interval
                        next_check = dataset.last_check_at + timedelta(
                            minutes=dataset.check_interval_minutes
                        )
                        should_check = now >= next_check
                        
                        if should_check:
                            logger.info(f"⏰ {dataset.name}: Time for scheduled check (interval: {dataset.check_interval_minutes}m)")
                        else:
                            logger.debug(f"⏳ {dataset.name}: Next check at {next_check}")
                    
                    if should_check:
                        try:
                            # Create geometry service with fresh session for each dataset
                            async with AsyncSessionLocal() as dataset_db:
                                geometry_service = GeometryService(dataset_db)
                                
                                logger.info(f"🔍 Starting change monitoring for dataset: {dataset.name}")
                                
                                # Use the new change monitoring method
                                response = await geometry_service.monitor_dataset_changes(
                                    dataset, force_reimport=False
                                )
                                
                                # Update dataset status in main session
                                dataset.last_check_at = datetime.now(timezone.utc)
                                dataset.connection_status = "success" if response.status == "SUCCESS" else "failed"
                                if response.status == "FAILED":
                                    dataset.connection_error = response.error_message
                                else:
                                    dataset.connection_error = None
                                    
                                await db.commit()
                                
                                logger.info(f"✅ Change monitoring completed for {dataset.name}: "
                                           f"{response.snapshots_created} new snapshots, "
                                           f"{response.diffs_detected} issues flagged")
                                
                                monitored += 1
                            
                        except Exception as e:
                            logger.error(f"❌ Error monitoring dataset {dataset.name}: {e}")
                            # Update dataset with error status
                            dataset.connection_status = "failed"
                            dataset.connection_error = str(e)
                            await db.commit()
                
                logger.info(f"✅ Change detection cycle completed: {monitored}/{len(datasets)} datasets checked")
                
            except Exception as e:
                logger.error(f"❌ Error in change detection cycle: {e}")
                await db.rollback()
    



async def main():
    """Main entry point for development worker."""
    worker = DevelopmentWorker()
    await worker.start()


if __name__ == "__main__":
    asyncio.run(main()) 