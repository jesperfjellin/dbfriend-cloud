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
