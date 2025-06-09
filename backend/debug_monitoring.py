#!/usr/bin/env python3
import asyncio
from datetime import datetime, timezone, timedelta
from database import AsyncSessionLocal, Dataset
from sqlalchemy import select, text

async def debug_monitoring():
    print("🔍 Debugging monitoring system...")
    
    async with AsyncSessionLocal() as db:
        # Get all datasets
        result = await db.execute(select(Dataset))
        datasets = result.scalars().all()
        
        print(f"\n📊 Found {len(datasets)} dataset(s):")
        
        now = datetime.now(timezone.utc)
        
        for dataset in datasets:
            print(f"\n📁 Dataset: {dataset.name}")
            print(f"   ID: {dataset.id}")
            print(f"   Active: {dataset.is_active}")
            print(f"   Connection Status: {dataset.connection_status}")
            print(f"   Check Interval: {dataset.check_interval_minutes} minutes")
            print(f"   Last Check: {dataset.last_check_at}")
            print(f"   Current Time: {now}")
            
            if dataset.last_check_at:
                next_check = dataset.last_check_at + timedelta(minutes=dataset.check_interval_minutes)
                print(f"   Next Scheduled Check: {next_check}")
                should_check = now >= next_check
                time_until_check = (next_check - now).total_seconds()
                print(f"   Should Check Now: {should_check}")
                if not should_check:
                    print(f"   Time Until Next Check: {time_until_check:.0f} seconds")
            else:
                print(f"   Should Check Now: True (never checked)")
                
            if dataset.connection_error:
                print(f"   Connection Error: {dataset.connection_error}")
        
        # Force reset the last_check_at to trigger immediate checking
        print(f"\n🔧 Forcing immediate check by resetting last_check_at...")
        await db.execute(text("""
            UPDATE datasets 
            SET last_check_at = NULL 
            WHERE is_active = true
        """))
        await db.commit()
        print("✅ Reset complete - next worker cycle should process all datasets")

if __name__ == "__main__":
    asyncio.run(debug_monitoring()) 