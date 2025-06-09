#!/usr/bin/env python3
import asyncio
from database import engine
from sqlalchemy import text

async def check_geom_column():
    async with engine.connect() as conn:
        # Check the actual geometry column definition
        print("🔍 Checking geometry column definition...")
        
        result = await conn.execute(text("""
            SELECT column_name, data_type, udt_name 
            FROM information_schema.columns 
            WHERE table_name = 'geometry_snapshots' 
            AND column_name = 'geometry'
        """))
        schema_info = result.fetchone()
        print(f"📋 Schema info: {schema_info}")
        
        # Check PostGIS geometry_columns view
        try:
            result = await conn.execute(text("""
                SELECT f_table_name, f_geometry_column, coord_dimension, srid, type
                FROM geometry_columns 
                WHERE f_table_name = 'geometry_snapshots'
            """))
            postgis_info = result.fetchone()
            print(f"🗺️ PostGIS info: {postgis_info}")
        except Exception as e:
            print(f"❌ PostGIS geometry_columns check failed: {e}")
        
        # Try to check what constraint exists
        try:
            result = await conn.execute(text("""
                SELECT constraint_name, check_clause
                FROM information_schema.check_constraints 
                WHERE constraint_name LIKE '%geometry_snapshots%geom%'
            """))
            constraints = result.fetchall()
            print(f"🔒 Constraints: {constraints}")
        except Exception as e:
            print(f"❌ Constraint check failed: {e}")

if __name__ == "__main__":
    asyncio.run(check_geom_column()) 