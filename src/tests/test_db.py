from src.db.database import get_db
import asyncio

if __name__ == '__main__':
    asyncio.run(get_db())
