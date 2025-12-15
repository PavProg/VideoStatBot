import logging

from aiogram import Router, F
from aiogram.filters import CommandStart, Command
from aiogram.types import Message
from sqlalchemy import text

from src.db.database import get_async_session
from src.llm_service.llm_service import YandexMLGPTQueryService, YandexGPTConfig
from src.config.config import settings

router = Router()
logger = logging.getLogger(__name__)

yc_config = YandexGPTConfig(
    api_key=settings.RE_YC_KEY,
    folder_id=settings.YC_FOLDER_ID,
    model=settings.RE_YC_MODELS,
    temperature=settings.RE_YC_TEMPERATURE,
    max_tokens=settings.RE_YC_MAX_TOKENS
)
yc_service = YandexMLGPTQueryService(yc_config)

@router.message(CommandStart())
async def cmd_start(message: Message):
    welcome_text = (
        "Добро пожаловать в бот для аналитики видео (YandexGPT)\n"
        "Отправь мне запрос на русском и я верну число."
    )
    await message.answer(welcome_text)

@router.message(F.text)
async def handle_text_query(message: Message):
    user_query = message.text.strip()
    logger.info(f'Получен запрос {user_query}')

    if user_query.startswith('/'):
        return

    processing_msg = await message.answer('Обрабатываю запрос через YandexGPT')

    try:
        sql_query = await yc_service.text_to_sql(user_query)

        if not sql_query:
            await processing_msg.edit_text('Не удалось понять запрос. Попробуй сформулировать иначе')
            return

        logger.info(f'Сгенерирован SQL: {sql_query}')

        async with get_async_session() as session:
            res = await session.execute(text(sql_query))
            row = res.fetchone()

            if not row or row[0] is None:
                await processing_msg.edit_text('Запросе не вернул результатов')
                return

            number = row[0]
            formatted_number = f"{number:,}".replace(",", " ")

            response = f"<b>Запрос:</b> <i>{user_query[:100]}...</i>\n\n <b>Результат:</b> <code>{formatted_number}</code>"
            await processing_msg.edit_text(response)

    except Exception as e:
        logger.error(f'Ошибка обработки запросов: {e}', exc_info=True)
        await processing_msg.edit_text(
            'Произошла ошибка при обработке запроса.\n'
            'Попробуйте сформулировать запрос проще или проверьте корректность ID'
        )

