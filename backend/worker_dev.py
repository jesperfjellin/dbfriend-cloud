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
    """Development worker with dual timers for change detection and quality checks."""
    
    def __init__(self):
        self.change_detection_interval = 60  # seconds
        self.quality_check_interval = 60   # seconds
        self.last_quality_check = None
        self.running = False
    
    async def start(self):
        """Start the dual-timer monitoring system."""
        logger.info("🚀 Starting dbfriend-cloud development monitoring...")
        logger.info("💡 This replaces Celery+Redis for easier local development")
        logger.info(f"📊 Change detection: every {self.change_detection_interval}s")
        logger.info(f"🔍 Quality checks: every {self.quality_check_interval}s")
        
        self.running = True
        
        # Start both monitoring loops concurrently
        change_task = asyncio.create_task(self._change_detection_loop())
        quality_task = asyncio.create_task(self._quality_check_loop())
        
        try:
            await asyncio.gather(change_task, quality_task)
        except KeyboardInterrupt:
            logger.info("👋 Shutting down development worker...")
            self.running = False
            change_task.cancel()
            quality_task.cancel()
    
    async def _change_detection_loop(self):
        """Main loop for detecting data changes (runs every minute)."""
        while self.running:
            try:
                await self._monitor_dataset_changes()
                await asyncio.sleep(self.change_detection_interval)
            except Exception as e:
                logger.error(f"❌ Error in change detection loop: {e}")
                await asyncio.sleep(30)  # Shorter retry interval
    
    async def _quality_check_loop(self):
        """Separate loop for quality checks (runs every hour)."""
        while self.running:
            try:
                await self._run_quality_checks()
                await asyncio.sleep(self.quality_check_interval)
            except Exception as e:
                logger.error(f"❌ Error in quality check loop: {e}")
                await asyncio.sleep(300)  # 5-minute retry for quality checks
    
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
    
    async def _run_quality_checks(self):
        """Run quality checks on all datasets (separate from change detection)."""
        async with AsyncSessionLocal() as db:
            try:
                # Get all active datasets
                from sqlalchemy import select
                result = await db.execute(
                    select(Dataset).where(Dataset.is_active == True)
                )
                datasets = result.scalars().all()
                
                logger.info(f"🔍 Running quality checks on {len(datasets)} datasets...")
                
                total_checks = 0
                failed_checks = 0
                
                for dataset in datasets:
                    try:
                        # Create geometry service with fresh session for each dataset
                        async with AsyncSessionLocal() as dataset_db:
                            geometry_service = GeometryService(dataset_db)
                            
                            logger.info(f"🧪 Running quality checks for dataset: {dataset.name}")
                            
                            # Run quality checks
                            check_results = await geometry_service.run_quality_checks(dataset)
                            
                            if "error" not in check_results:
                                total_checks += sum(v for k, v in check_results.items() if k.endswith('_checks'))
                                failed_checks += check_results.get('failed_checks', 0)
                                
                                logger.info(f"✅ Quality checks completed for {dataset.name}: {check_results}")
                            else:
                                logger.error(f"❌ Quality checks failed for {dataset.name}: {check_results['error']}")
                                
                    except Exception as e:
                        logger.error(f"❌ Error running quality checks for dataset {dataset.name}: {e}")
                
                self.last_quality_check = datetime.now(timezone.utc)
                logger.info(f"✅ Quality check cycle completed: {total_checks} checks run, {failed_checks} failed")
                
            except Exception as e:
                logger.error(f"❌ Error in quality check cycle: {e}")
                await db.rollback()


async def main():
    """Main entry point for development worker."""
    worker = DevelopmentWorker()
    await worker.start()


if __name__ == "__main__":
    asyncio.run(main()) 