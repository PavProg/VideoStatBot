import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import Session, DeclarativeBase
from src.config.config import settings
from sqlalchemy import URL, text

async_engine = create_async_engine(
    url=settings.DATABASE_URL_asyncpg,
    echo=True,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    pool_recycle=300,
    connect_args={"timeout": 10, "command_timeout": 30},
)

async_session_factory = async_sessionmaker(async_engine, expire_on_commit=False)

# Декоратор для открытия сессии
def connection(method):
    async def wrapper(*args, **kwargs):     # Обертка принимает все аргементы функции
        async with async_session_factory() as session:
            try:
                return await method(*args, session=session, **kwargs)
            except Exception as e:
                await session.rollback()    # При ошибки она поднимается и вызывается ролбек
                raise e
            finally:
                await session.close()   # Сессия всегда закрывается
    return wrapper

class Base(DeclarativeBase):
    pass