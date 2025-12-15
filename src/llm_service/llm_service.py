import logging
from typing import Optional
from dataclasses import dataclass

from src.config.config import settings

from yandex_cloud_ml_sdk import AsyncYCloudML
from yandex_cloud_ml_sdk.auth import APIKeyAuth

logger = logging.getLogger(__name__)

@dataclass
class YandexGPTConfig:
    api_key: str = settings.RE_YC_KEY
    folder_id: str = settings.RE_YC_FOLDER_ID
    model: str = settings.RE_YC_MODELS
    temperature: float = settings.RE_YC_TEMPERATURE
    max_tokens: int = settings.RE_YC_MAX_TOKENS

class YandexMLGPTQueryService:
    def __init__(self, config: YandexGPTConfig):
        self.config = config
        try:
            self.sdk = AsyncYCloudML(
                folder_id=config.folder_id,
                auth=APIKeyAuth(api_key=config.api_key)  # Передаем аутентификацию через auth
            )
        except TypeError as e:
            logger.warning("ApiKeyAuth не поддерживается, пробуем простой ключ")
            self.sdk = AsyncYCloudML(
                folder_id=config.folder_id,
                auth=config.api_key
            )
        self.model = self.sdk.models.completions(self.config.model)
        self.model = self.model.configure(
            temperature=self.config.temperature,
            max_tokens=self.config.max_tokens,
        )
        self.sdk.setup_default_logging()

    async def text_to_sql(self, user_query: str) -> Optional[str]:
        prompt = self._create_sql_prompt(user_query)

        try:
            logger.info(f"Отправка запроса в YandexGPT: {user_query}")
            raw_sql = await self._send_yandexgpt_request(prompt)

            if not raw_sql:
                logger.warning("Пустой ответ от YandexGPT")
                return None

            logger.info(f"Сырой ответ от YandexGPT (repr): {repr(raw_sql)}")

            sql_query = self._clean_sql_response(raw_sql)
            logger.info(f"Очищенный SQL: {repr(sql_query)}")

            if self._validate_sql(sql_query):
                logger.info(f'Сгенерирован валидный SQL: {sql_query}')
                return sql_query
            else:
                logger.warning(f'SQL не прошел валидацию: {sql_query}')
                return None

        except Exception as e:
            logger.error(f'Ошибка преобразования запроса в SQL: {e}', exc_info=True)
            return None

    def _clean_sql_response(self, raw_sql: str) -> str:
        """Тщательная очистка SQL-ответа от gpt"""
        if not raw_sql:
            return ""

        # Удаляем BOM и невидимые символы
        sql = raw_sql.strip()
        if sql.startswith('\ufeff'):
            sql = sql[1:].strip()

        # Удаляем маркдаун-форматирование
        sql = sql.replace('```sql', '').replace('```', '').strip()

        # Удаляем возможные кавычки вокруг всего запроса
        if (sql.startswith('"') and sql.endswith('"')) or (sql.startswith("'") and sql.endswith("'")):
            sql = sql[1:-1].strip()

        # Заменяем множественные пробелы и переносы строк на один пробел
        import re
        sql = re.sub(r'\s+', ' ', sql)

        # Удаляем точку с запятой в конце (опционально, но для единообразия)
        sql = sql.rstrip(';')

        return sql.strip()

    def _create_sql_prompt(self, user_query: str) -> list:
        # Создание промпта для ИИ с описание схемы БД
        system_message = {
            'role': 'system',
            'text': """
        Ты — помощник для работы с базой данных статистики видео. Твоя задача: преобразовать запрос на русском языке в SQL запрос к PostgreSQL.

        СХЕМА БАЗЫ ДАННЫХ:

        1. Таблица 'videos' (видео):
           - id (integer, первичный ключ)
           - video_id (string, UUID, уникальный идентификатор видео)
           - creator_id (string, идентификатор автора видео)
           - video_created_at (datetime, когда было создано видео)
           - views_count (integer, количество просмотров)
           - likes_count (integer, количество лайков)
           - comments_count (integer, количество комментариев)
           - reports_count (integer, количество жалоб)
           - created_at (datetime, когда запись создана в БД)
           - updated_at (datetime, когда запись обновлена в БД)

        2. Таблица 'snapshots' (снапшоты — срезы статистики во времени):
           - id (integer, первичный ключ)
           - snapshot_id (string, UUID, уникальный идентификатор снапшота)
           - video_id (string, ссылка на videos.video_id)
           - views_count (integer, просмотры на момент снапшота)
           - likes_count (integer, лайки на момент снапшота)
           - comments_count (integer, комментарии на момент снапшота)
           - reports_count (integer, жалобы на момент снапшота)
           - delta_views_count (integer, прирост просмотров с предыдущего снапшота)
           - delta_likes_count (integer, прирост лайков с предыдущего снапшота)
           - delta_comments_count (integer, прирост комментариев с предыдущего снапшота)
           - delta_reports_count (integer, прирост жалоб с предыдущего снапшота)
           - created_at (datetime, когда создан снапшот)
           - updated_at (datetime, когда обновлен снапшот)

        ВАЖНЫЕ ПРАВИЛА:
        1. Запрос должен возвращать ОДНО ЧИСЛО (одно значение, одна строка, один столбец).
        2. Используй только SELECT запросы.
        3. Не используй INSERT, UPDATE, DELETE, DROP.
        4. Если нужно найти видео по video_id — используй точное совпадение.
        5. Если нужно найти по creator_id — используй точное совпадение.
        6. Для подсчета количества ВИДЕО используй COUNT(DISTINCT video_id).
        7. Для подсчета количества СНАПШОТОВ используй COUNT(*).
        8. Для суммирования используй SUM(поле).
        9. Для среднего значения используй AVG(поле).
        10. Для максимального/минимального используй MAX(поле)/MIN(поле).
        11. Всегда возвращай ТОЛЬКО SQL-запрос, без пояснений, без обратных кавычек ```, без markdown.
        12. Если не можешь создать запрос, верни 'NULL'.
        13. Для вопросов о датах (например "27 ноября 2025") используй функцию DATE(): WHERE DATE(created_at) = '2025-11-27'
        14. Для вопросов о "новых просмотрах" используй delta_views_count > 0
        15. Если вопрос о количестве ВИДЕО, всегда используй COUNT(DISTINCT ...) чтобы избежать дублирования.
        
        ИНТЕРПРЕТАЦИЯ ВОПРОСОВ:
        - "по итоговой статистике", "текущие показатели" → используй таблицу videos
        - "когда-либо имели", "в истории были" → используй таблицу snapshots с DISTINCT
        - "максимальные просмотры" → используй MAX(views_count) в подзапросе
        - "больше X просмотров" → проверяй views_count > X
        
        ПРИМЕРЫ:
        - "Сколько видео имеют > 10000 просмотров?" → SELECT COUNT(*) FROM videos WHERE views_count > 10000
        - "Сколько видео набрали > 10000 просмотров в истории?" → SELECT COUNT(DISTINCT video_id) FROM snapshots WHERE views_count > 10000
        - "Сколько видео у автора X имеют > 10000 просмотров?" → SELECT COUNT(*) FROM videos WHERE creator_id = 'X' AND views_count > 10000
        """
        }
        user_message = {
            'role': 'user',
            'text': user_query,
        }
        return [system_message, user_message]

    async def _send_yandexgpt_request(self, messages: list) -> str:
        try:
            result_list = await self.model.run(messages)
            if result_list and len(result_list) > 0:
                response_text = result_list[0].text
                if response_text:
                    cleaned_text = response_text.strip()
                    cleaned_text = cleaned_text.replace('```sql', '').replace('```', '')
                    return cleaned_text.strip()
            logger.error('Пустой ответ от YandexGPT API.')
            return ""
        except Exception as e:
            logger.error(f'Ошибка при вызове GPT {e}')
            return ""

    def _validate_sql(self, sql_query: str) -> bool:
        if not sql_query:
            logger.debug("Валидация: пустой запрос")
            return False

        sql = sql_query.strip().lower()
        logger.debug(f"Валидация: проверяем '{sql}'")

        if sql == 'null':
            logger.debug("Валидация: запрос равен 'null'")
            return False

        if not sql.startswith('select'):
            logger.debug(f"Валидация: не начинается с 'select', начинается с '{sql[:20]}'")
            return False

        # Проверка на опасные операции (расширенный список)
        dangerous_patterns = [
            r'\binsert\b', r'\bupdate\b', r'\bdelete\b', r'\bdrop\b',
            r'\btruncate\b', r'\balter\b', r'\bcreate\b', r'\bgrant\b',
            r'\brevoke\b', r'\bexec\b', r'\bexecute\b', r'\bunion\b.*\bselect\b'
        ]

        import re
        for pattern in dangerous_patterns:
            if re.search(pattern, sql):
                logger.debug(f"Валидация: найдено опасное слово по паттерну {pattern}")
                return False

        logger.debug("Валидация: запрос прошел проверку")
        return True