import asyncio

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from src.config.config import settings

from src.db.queries.orm import create_tables
from src.db.models import VideosOrm, SnapshotsOrm

bot = Bot(token=settings.RE_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

async def db_startup(dp: Dispatcher):
    ...

async def main():
    ...

asyncio.run(create_tables())

# if __name__ == '__main__':
#     asyncio.run(main())