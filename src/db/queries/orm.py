from src.db.database import Base
from src.db.database import async_engine, async_session_factory, connection
from src.db.models import VideosOrm, SnapshotsOrm

async def create_tables():
    async_engine.echo = True
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    async_engine.echo = True
