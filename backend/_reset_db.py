#!/usr/bin/env python3
"""
_reset_db.py – dev helper to quickly clear data and reset schema
Uses TRUNCATE for fast reset with large datasets.
"""

import asyncio
from sqlalchemy import text

from database import engine, Base, _apply_postgres_optimizations


async def reset_database_completely() -> None:
    # ── ENSURE TABLES EXIST phase ──────────────────────────────────
    async with engine.begin() as conn:
        print("🏗️  Ensuring PostGIS and tables exist …")
        
        # Ensure PostGIS extension is enabled first
        try:
            await conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
            print("✓ PostGIS extension ensured")
        except Exception as exc:
            print(f"❌ Could not enable PostGIS extension: {exc}")
            raise RuntimeError("PostGIS extension is required but could not be enabled") from exc
        
        # Ensure all tables exist (create if missing)
        try:
            await conn.run_sync(Base.metadata.create_all)
            print("✓ Tables ensured (created if missing)")
            
            # Add geometry column manually without typmod restrictions (if not already created by SQLAlchemy)
            print("🔗 Ensuring mixed-dimension geometry column...")
            await conn.execute(text("""
                ALTER TABLE geometry_snapshots ADD COLUMN IF NOT EXISTS geometry geometry
            """))
            print("✓ Geometry column ensured without dimension restrictions")
            
            # Manually register in geometry_columns for PostGIS awareness (handle duplicates)
            print("📝 Registering geometry column...")
            
            # Check if entry already exists
            result = await conn.execute(text("""
                SELECT COUNT(*) FROM geometry_columns 
                WHERE f_table_name = 'geometry_snapshots' AND f_geometry_column = 'geometry'
            """))
            exists = result.scalar() > 0
            
            if not exists:
                await conn.execute(text("""
                    INSERT INTO geometry_columns 
                    (f_table_catalog, f_table_schema, f_table_name, f_geometry_column, coord_dimension, srid, type)
                    VALUES ('', 'public', 'geometry_snapshots', 'geometry', 4, 4326, 'GEOMETRY')
                """))
                print("✓ Geometry column registered with mixed-dimension support")
            else:
                print("✓ Geometry column already registered")
            
            # Add geometry index
            print("📊 Creating geometry index...")
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_geometry_snapshots_geom ON geometry_snapshots USING GIST (geometry)
            """))
            print("✓ Geometry index created")
            
        except Exception as exc:
            print(f"❌ Error ensuring tables: {exc}")
            raise

    # ── FAST TRUNCATE phase ────────────────────────────────────────
    async with engine.begin() as conn:
        print("🧹 Fast data clearing with TRUNCATE …")

        try:
            # TRUNCATE all data tables at once - much faster than DROP
            # RESTART IDENTITY resets any auto-increment sequences
            # CASCADE handles foreign key dependencies automatically
            await conn.execute(text("""
                TRUNCATE TABLE spatial_checks, geometry_diffs, geometry_snapshots 
                RESTART IDENTITY CASCADE
            """))
            print("✓ All monitoring data cleared instantly")

            # Optionally clear datasets table (uncomment if you want to reset connections too)
            # await conn.execute(text("TRUNCATE TABLE datasets RESTART IDENTITY CASCADE"))
            # print("✓ Dataset connections cleared")

        except Exception as exc:
            print(f"❌ Error truncating tables: {exc}")
            raise

    # ── OPTIMIZATION phase ──────────────────────────────────────────
    try:
        async with engine.begin() as conn:
            await _apply_postgres_optimizations(conn)
            print("✓ Storage optimizations applied")
    except Exception as exc:
        print(f"⚠️  Storage optimizations failed (non-critical): {exc}")

    print("✅ Database reset complete - tables preserved, data cleared instantly")
    print("💡 Pro tip: TRUNCATE is much faster than DROP for large datasets!")


def main() -> None:
    asyncio.run(reset_database_completely())


if __name__ == "__main__":
    main()
