#!/usr/bin/env python3
"""
test_sql_generation.py – debug what SQL is generated for GeometrySnapshot
"""

from sqlalchemy.schema import CreateTable
from sqlalchemy.dialects import postgresql
from database import GeometrySnapshot

def main():
    print("=== GeometrySnapshot table creation SQL ===")
    create_table_sql = CreateTable(GeometrySnapshot.__table__).compile(dialect=postgresql.dialect())
    print(create_table_sql)
    print()
    
    print("=== Table columns ===")
    for col in GeometrySnapshot.__table__.columns:
        print(f"- {col.name}: {col.type}")
    print()
    
    print("=== Table constraints ===")
    for constraint in GeometrySnapshot.__table__.constraints:
        print(f"- {constraint}")

if __name__ == "__main__":
    main() 