#!/usr/bin/env python3
"""
_reset_db.py – dev helper to blow away the schema and rebuild it
after model changes.
"""

import asyncio
from sqlalchemy import text

from database import engine, Base, _apply_postgres_optimizations


async def reset_database_completely() -> None:
    # ── DROP phase ──────────────────────────────────────────────
    async with engine.begin() as conn:
        print("🗑️  Dropping existing objects …")

        # drop the old faulty table if it exists
        await conn.execute(text("DROP TABLE IF EXISTS geometry_snapshots CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS geometry_diffs CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS spatial_checks CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS datasets CASCADE"))

        # clean geometry_columns view (harmless if nothing there)
        await conn.execute(
            text(
                "DELETE FROM geometry_columns "
                "WHERE f_table_name IN ('geometry_snapshots','geometry_diffs')"
            )
        )

        # ensure everything else is gone (good for dev)
        await conn.run_sync(Base.metadata.drop_all)

    # ── CREATE phase ────────────────────────────────────────────
    async with engine.begin() as conn:
        print("🏗️  Enabling PostGIS and creating fresh tables …")
        
        # Ensure PostGIS extension is enabled first
        try:
            await conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
            print("✓ PostGIS extension ensured")
        except Exception as exc:
            print(f"❌ Could not enable PostGIS extension: {exc}")
            raise RuntimeError("PostGIS extension is required but could not be enabled") from exc
        
        # Create all tables
        try:
            await conn.run_sync(Base.metadata.create_all)
            print("✓ Tables created successfully")
            
            # Add geometry column manually without typmod restrictions
            print("🔗 Adding mixed-dimension geometry column...")
            await conn.execute(text("""
                ALTER TABLE geometry_snapshots ADD COLUMN geometry geometry
            """))
            print("✓ Geometry column added without dimension restrictions")
            
            # Manually register in geometry_columns for PostGIS awareness
            print("📝 Registering geometry column...")
            await conn.execute(text("""
                INSERT INTO geometry_columns 
                (f_table_catalog, f_table_schema, f_table_name, f_geometry_column, coord_dimension, srid, type)
                VALUES ('', 'public', 'geometry_snapshots', 'geometry', 4, 4326, 'GEOMETRY')
            """))
            print("✓ Geometry column registered with mixed-dimension support")
            
            # Add geometry index
            print("📊 Creating geometry index...")
            await conn.execute(text("""
                CREATE INDEX idx_geometry_snapshots_geom ON geometry_snapshots USING GIST (geometry)
            """))
            print("✓ Geometry index created")
            
        except Exception as exc:
            print(f"❌ Error creating tables: {exc}")
            raise

    # Apply optimizations in a separate transaction (optional, won't fail reset)
    try:
        async with engine.begin() as conn:
            await _apply_postgres_optimizations(conn)
            print("✓ Storage optimizations applied")
    except Exception as exc:
        print(f"⚠️  Storage optimizations failed (non-critical): {exc}")

    print("✅  Database completely reset and ready")


def main() -> None:
    asyncio.run(reset_database_completely())


if __name__ == "__main__":
    main()
