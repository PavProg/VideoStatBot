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
        
        ОСОБЫЕ ПРАВИЛА ДЛЯ ДАТ И ВРЕМЕНИ:
        13. Для фильтрации по КОНКРЕТНОЙ ДАТЕ используй функцию DATE(): WHERE DATE(created_at) = '2025-11-27'
        14. Для фильтрации по МЕСЯЦУ используй EXTRACT(): WHERE EXTRACT(YEAR FROM video_created_at) = 2025 AND EXTRACT(MONTH FROM video_created_at) = 6
        15. Для фильтрации по ГОДУ используй EXTRACT(): WHERE EXTRACT(YEAR FROM video_created_at) = 2025
        16. Для фильтрации по ПЕРИОДУ (с/по) используй BETWEEN: WHERE created_at BETWEEN '2025-06-01' AND '2025-06-30'
        17. Для фильтрации по ЧАСУ используй EXTRACT(HOUR FROM created_at): WHERE EXTRACT(HOUR FROM created_at) >= 10
        
        ОСОБЫЕ ПРАВИЛА ДЛЯ СТАТИСТИКИ:
        18. Для вопросов о "новых просмотрах" используй delta_views_count > 0
        19. Для вопросов о "суммарных просмотрах ВСЕХ видео" используй SUM(views_count) из таблицы videos
        20. Для вопросов о "суммарных просмотрах по снапшотам" используй SUM(views_count) из таблицы snapshots с DISTINCT или группировкой
        21. Для вопросов о "выросли/увеличились" используй фильтр delta_views_count > 0
        22. Для вопросов о "потеряли/уменьшились" используй фильтр delta_views_count < 0
        23. Для "абсолютного изменения" используй ABS(delta_views_count)
        
        ОСОБЫЕ ПРАВИЛА ДЛЯ ВРЕМЕННЫХ ИНТЕРВАЛОВ:
        24. Для интервалов "с X:00 до Y:00" используй полуоткрытый интервал: >= X AND < Y
        25. Пример: "с 10:00 до 15:00" → WHERE EXTRACT(HOUR FROM created_at) >= 10 AND EXTRACT(HOUR FROM created_at) < 15
        26. Для точных временных границ используй: WHERE created_at >= '2025-11-28 10:00:00' AND created_at < '2025-11-28 15:00:00'
        
        ИНТЕРПРЕТАЦИЯ ВОПРОСОВ:
        27. "по итоговой статистике", "текущие показатели", "всего" → используй таблицу videos
        28. "когда-либо имели", "в истории были", "по снапшотам" → используй таблицу snapshots с DISTINCT
        29. "максимальные просмотры" → используй MAX(views_count) в подзапросе или GROUP BY
        30. "опубликованные в [месяц] [год]" → используй EXTRACT(YEAR FROM video_created_at) = год AND EXTRACT(MONTH FROM video_created_at) = месяц
        31. "выросли в промежутке" → суммируй delta_views_count только с фильтром > 0
        32. "изменились в промежутке" → суммируй delta_views_count без фильтра
        
        ПРИМЕРЫ SQL-ЗАПРОСОВ:
        33. "Сколько видео имеют > 10000 просмотров?" → SELECT COUNT(*) FROM videos WHERE views_count > 10000
        34. "Сколько видео набрали > 10000 просмотров в истории?" → SELECT COUNT(DISTINCT video_id) FROM snapshots WHERE views_count > 10000
        35. "Сколько видео опубликовано в июне 2025?" → SELECT COUNT(*) FROM videos WHERE EXTRACT(YEAR FROM video_created_at) = 2025 AND EXTRACT(MONTH FROM video_created_at) = 6
        36. "Какое суммарное количество просмотров набрали все видео, опубликованные в июне 2025 года?" → SELECT SUM(views_count) FROM videos WHERE EXTRACT(YEAR FROM video_created_at) = 2025 AND EXTRACT(MONTH FROM video_created_at) = 6
        37. "Сколько разных видео получали новые просмотры 27 ноября 2025?" → SELECT COUNT(DISTINCT video_id) FROM snapshots WHERE DATE(created_at) = '2025-11-27' AND delta_views_count > 0
        38. "На сколько просмотров суммарно выросли все видео креатора X в промежутке с 10:00 до 15:00 28 ноября 2025?" → SELECT COALESCE(SUM(delta_views_count), 0) FROM snapshots WHERE created_at >= '2025-11-28 10:00:00' AND created_at < '2025-11-28 15:00:00' AND video_id IN (SELECT video_id FROM videos WHERE creator_id = 'X') AND delta_views_count > 0
        39. "Сколько видео у креатора X набрали больше 10000 просмотров по итоговой статистике?" → SELECT COUNT(*) FROM videos WHERE creator_id = 'X' AND views_count > 10000
        40. "Какое суммарное количество просмотров у автора X?" → SELECT SUM(views_count) FROM videos WHERE creator_id = 'X'
        41. "Среднее количество просмотров на видео у автора X?" → SELECT AVG(views_count) FROM videos WHERE creator_id = 'X'
        
        ДОПОЛНИТЕЛЬНЫЕ ПРАВИЛА:
        42. Если вопрос содержит "итоговый", "текущий", "общий" → обращайся к таблице videos
        43. Если вопрос содержит "в истории", "по замерам", "в снапшотах" → обращайся к таблице snapshots
        44. Если вопрос содержит "выросло", "увеличилось", "прибавилось" → добавляй delta_..._count > 0
        45. Если вопрос содержит "упало", "снизилось", "потеряло" → добавляй delta_..._count < 0
        46. Всегда используй COALESCE(..., 0) для функций агрегации чтобы избежать NULL
        47. Для JOIN используй явное указание таблиц: videos.video_id, snapshots.video_id
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