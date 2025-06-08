#!/usr/bin/env python3
import asyncio
from database import reset_db_for_development

async def main():
    await reset_db_for_development()
    print("✅ Database reset complete")

if __name__ == "__main__":
    asyncio.run(main()) 