#!/usr/bin/env python3
import asyncio
import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage

from src.config.config import settings
from src.config.logs_config import setup_logging
from src.db.database import init_db
from src.bot.handlers.handlers import router

async def main():
    # Logs
    setup_logging(settings.LOG_LEVEL, settings.LOG_FILE)
    logger = logging.getLogger(__name__)
    logger.info('Запуск приложения...')

    if not settings.RE_TOKEN:
        logger.error('Не указан токен бота в .env файле!')
        sys.exit(1)

    if not settings.RE_YC_KEY:
        logger.error("Не указан YC_API_KEY в .env файле!")
        sys.exit(1)

    if not settings.RE_YC_FOLDER_ID:
        logger.error("Не указан YC_FOLDER_ID в .env файле!")
        sys.exit(1)

    logger.info('Все обязательные настройки проверены успешно')

    try:
        await init_db()
        logger.info('База данных инициализирована')
    except Exception as e:
        logger.error(f'Ошибка инициализации {e}')
        sys.exit(1)

    bot = Bot(
        token=settings.RE_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )

    dp = Dispatcher(storage=MemoryStorage())

    dp.include_router(router)

    logger.info('Бот запущен и готов к работе!')

    try:
        await dp.start_polling(bot)
    except KeyboardInterrupt:
        logger.info('Бот остановлен по запросу пользователя!')
    finally:
        await bot.session.close()

if __name__ == '__main__':
    asyncio.run(main())