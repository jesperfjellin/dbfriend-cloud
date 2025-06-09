#!/usr/bin/env python3
import asyncio
from database import AsyncSessionLocal, engine, Base
from sqlalchemy import text

async def check_table_exists(conn, table_name):
    """Check if a table exists in the database."""
    try:
        result = await conn.execute(text(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = :table_name)"
        ), {"table_name": table_name})
        return result.scalar()
    except Exception as e:
        print(f"❌ Error checking if table {table_name} exists: {e}")
        return False

async def check_db():
    print("🔍 Checking database state...")
    
    async with engine.begin() as conn:
        # First, check if our tables exist
        tables = ['datasets', 'geometry_snapshots', 'geometry_diffs', 'spatial_checks']
        
        print("\n📋 Table existence check:")
        tables_exist = {}
        for table in tables:
            exists = await check_table_exists(conn, table)
            tables_exist[table] = exists
            status = "✅" if exists else "❌"
            print(f"  {status} {table}: {'exists' if exists else 'missing'}")
        
        # If tables don't exist, try to create them
        if not all(tables_exist.values()):
            print("\n🏗️ Some tables missing, attempting to create...")
            try:
                await conn.run_sync(Base.metadata.create_all)
                print("  ✅ Tables created successfully")
                
                # Re-check table existence
                print("\n📋 Re-checking table existence:")
                for table in tables:
                    exists = await check_table_exists(conn, table)
                    status = "✅" if exists else "❌"
                    print(f"  {status} {table}: {'exists' if exists else 'still missing'}")
                    
            except Exception as e:
                print(f"❌ Error creating tables: {e}")
                return
    
    # Now check data counts using a separate session
    try:
        async with AsyncSessionLocal() as db:
            print("\n📊 Data counts:")
            
            # Check datasets
            try:
                result = await db.execute(text("SELECT COUNT(*) FROM datasets"))
                datasets_count = result.scalar()
                print(f"  📁 Datasets: {datasets_count}")
                
                if datasets_count > 0:
                    result = await db.execute(text("SELECT id, name, is_active FROM datasets LIMIT 5"))
                    datasets = result.fetchall()
                    for dataset in datasets:
                        print(f"    - {dataset.name} (ID: {str(dataset.id)[:8]}..., Active: {dataset.is_active})")
                        
            except Exception as e:
                print(f"  ❌ Error checking datasets: {e}")
            
            # Check snapshots
            try:
                result = await db.execute(text("SELECT COUNT(*) FROM geometry_snapshots"))
                snapshots_count = result.scalar()
                print(f"  📷 Geometry snapshots: {snapshots_count}")
            except Exception as e:
                print(f"  ❌ Error checking snapshots: {e}")
            
            # Check diffs
            try:
                result = await db.execute(text("SELECT COUNT(*) FROM geometry_diffs"))
                diffs_count = result.scalar()
                print(f"  🔍 Geometry diffs: {diffs_count}")
            except Exception as e:
                print(f"  ❌ Error checking diffs: {e}")
            
            # Check spatial checks
            try:
                result = await db.execute(text("SELECT COUNT(*) FROM spatial_checks"))
                checks_count = result.scalar()
                print(f"  ✅ Spatial checks: {checks_count}")
            except Exception as e:
                print(f"  ❌ Error checking spatial checks: {e}")
                
    except Exception as e:
        print(f"❌ Error connecting to database for data check: {e}")

if __name__ == "__main__":
    asyncio.run(check_db()) 