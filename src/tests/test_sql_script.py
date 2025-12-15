# test_sql_fix.py
import re
import sys

sys.path.insert(0, '.')

from src.llm_service.llm_service import YandexMLGPTQueryService, YandexGPTConfig
from src.config.config import settings


def test_sql_cleaning():
    """Тестирование очистки SQL"""

    # Тестовые данные с разными форматами
    test_cases = [
        # (исходный SQL, ожидаемый результат)
        (
            '"SELECT COUNT(DISTINCT video_id) \nFROM snapshots \nWHERE DATE(created_at) = \'2025-11-27\' \n  AND delta_views_count > 0"',
            "SELECT COUNT(DISTINCT video_id) FROM snapshots WHERE DATE(created_at) = '2025-11-27' AND delta_views_count > 0"
        ),
        (
            '```sql\nSELECT COUNT(*) FROM videos\n```',
            "SELECT COUNT(*) FROM videos"
        ),
        (
            '\ufeffSELECT * FROM snapshots',
            "SELECT * FROM snapshots"
        ),
        (
            'NULL',
            "null"
        ),
        (
            'SELECT COUNT(*) FROM videos;',
            "SELECT COUNT(*) FROM videos"
        ),
    ]

    print("=" * 60)
    print("ТЕСТИРОВАНИЕ ОЧИСТКИ SQL")
    print("=" * 60)

    config = YandexGPTConfig(
        api_key=settings.RE_YC_KEY,
        folder_id=settings.RE_YC_FOLDER_ID,
        model=settings.RE_YC_MODELS,
        temperature=settings.RE_YC_TEMPERATURE,
        max_tokens=settings.RE_YC_MAX_TOKENS
    )

    service = YandexMLGPTQueryService(config)

    for i, (input_sql, expected) in enumerate(test_cases, 1):
        print(f"\nТест #{i}:")
        print(f"Вход:  {repr(input_sql)}")
        cleaned = service._clean_sql_response(input_sql)
        print(f"Выход: {repr(cleaned)}")
        print(f"Ожидалось: {repr(expected)}")
        print(f"Совпадает: {'✅' if cleaned == expected else '❌'}")

    print("\n" + "=" * 60)
    print("ТЕСТИРОВАНИЕ ВАЛИДАЦИИ")
    print("=" * 60)

    test_validation = [
        ("SELECT COUNT(DISTINCT video_id) FROM snapshots WHERE DATE(created_at) = '2025-11-27' AND delta_views_count > 0",
         True),
        ("select count(*) from videos", True),
        ("NULL", False),
        ("DROP TABLE videos", False),
        ("SELECT * FROM users UNION SELECT * FROM passwords", False),
    ]

    for i, (sql, should_be_valid) in enumerate(test_validation, 1):
        is_valid = service._validate_sql(sql)
        print(f"Тест #{i}: {sql[:50]}...")
        print(
            f"  Результат: {'✅ ВАЛИДНЫЙ' if is_valid else '❌ НЕВАЛИДНЫЙ'} (ожидалось: {'валидный' if should_be_valid else 'невалидный'})")
        print(f"  Совпадает: {'✅' if is_valid == should_be_valid else '❌'}")


if __name__ == "__main__":
    test_sql_cleaning()