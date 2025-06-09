#!/usr/bin/env python3
"""
debug_geometry.py – test geometry column generation
"""

import asyncio
from sqlalchemy import text, create_engine
from sqlalchemy.schema import CreateTable
from geoalchemy2 import Geometry
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class TestGeometry(Base):
    __tablename__ = "test_geometry"
    
    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    geometry = Column(Geometry("GEOMETRY", srid=4326, use_typmod=False))

def main():
    print("Testing geometry column definition...")
    
    # Print the CREATE TABLE statement
    from sqlalchemy.dialects import postgresql
    create_table_sql = CreateTable(TestGeometry.__table__).compile(dialect=postgresql.dialect())
    print("Generated SQL:")
    print(create_table_sql)
    print()
    
    # Test if PostGIS functions are available
    print("PostGIS test:")
    engine = create_engine("postgresql://postgres:postgres@localhost:5432/test", echo=True)
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT PostGIS_Version()"))
            print(f"PostGIS version: {result.fetchone()[0]}")
    except Exception as e:
        print(f"PostGIS not available: {e}")

if __name__ == "__main__":
    main() 