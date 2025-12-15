import json
import logging
import asyncio
from pathlib import Path
from datetime import datetime
from typing import Dict, List

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import text, delete

from src.db.models import VideosOrm, SnapshotsOrm
from src.db.database import get_async_session

logger = logging.getLogger(__name__)

async def clear_existing_data():
    """Очистка таблиц перед загрузкой новых данных"""
    logger.warning("ОЧИСТКА ТАБЛИЦ: Удаление всех существующих данных...")
    async with get_async_session() as session:
        try:
            await session.execute(delete(SnapshotsOrm))
            logger.info(f"Таблица 'snapshots' очищена")

            await session.execute(delete(VideosOrm))
            logger.info(f"Таблица 'videos' очищена")

            await session.commit()
            logger.warning("Очистка таблиц успешно завершена")
        except Exception as e:
            await session.rollback()
            logger.error(f"Ошибка при очистке таблиц: {e}")
            raise


async def load_videos_from_json(json_file: Path) -> Dict[str, int]:
    """Основная функция загрузки данных из JSON"""
    logger.info(f"Начало загрузки данных из {json_file}")

    if not json_file.exists():
        logger.error(f"Файл не найден: {json_file}")
        return {'videos': 0, 'snapshots': 0, 'errors': 0}

    with open(json_file, 'r', encoding='utf-8') as f:
        raw_data = json.load(f)

    videos_data: List[dict] = raw_data.get("videos", [])
    total_videos = len(videos_data)
    logger.info(f"Найдено {total_videos} видео для обработки")

    stats = {'videos': 0, 'snapshots': 0, 'errors': 0}

    processed_snapshots_count = 0       # Счетчик для отладки

    for index, video_data in enumerate(videos_data, 1):
        video_id = video_data.get('id', f'unknown_{index}')

        if index % 50 == 0 or index == total_videos:
            logger.info(f"Прогресс: обработано {index}/{total_videos} видео")

        try:
            async with get_async_session() as session:
                await _upsert_video(session, video_data)
                stats['videos'] += 1

                snapshots_data = video_data.get("snapshots", [])
                snapshots_count = len(snapshots_data)
                processed_snapshots_count += snapshots_count

                if snapshots_count > 0:
                    snapshot_errors = 0
                    for snapshot_data in snapshots_data:
                        try:
                            await _upsert_snapshot(session, snapshot_data)
                            stats['snapshots'] += 1
                        except Exception as e:
                            logger.error(f"Ошибка снапшота (видео {video_id}): {e}")
                            snapshot_errors += 1

                    if snapshot_errors > 0:
                        stats['errors'] += snapshot_errors
                        logger.warning(f"Видео {video_id}: {snapshot_errors} ошибок снапшотов")

        except Exception as e:
            logger.error(f"Ошибка видео {video_id} (№{index}): {e}")
            stats['errors'] += 1

    # ФИНАЛЬНАЯ СТАТИСТИКА
    logger.info(f"=" * 50)
    logger.info(f"ЗАГРУЗКА ЗАВЕРШЕНА")
    logger.info(f"Видео успешно загружено: {stats['videos']}/{total_videos}")
    logger.info(f"Снапшотов загружено: {stats['snapshots']}")
    logger.info(f"Обнаружено снапшотов в JSON: ~{processed_snapshots_count}")
    logger.info(f"Ошибок: {stats['errors']}")
    logger.info(f"=" * 50)

    return stats


async def _upsert_video(session, video_data: dict):
    """Вставка или обновление видео"""
    video_dict = {
        "video_id": str(video_data["id"]),
        "creator_id": str(video_data["creator_id"]),
        "video_created_at": _parse_datetime(video_data.get("video_created_at")),
        "views_count": int(video_data.get("views_count", 0)),
        "likes_count": int(video_data.get("likes_count", 0)),
        "comments_count": int(video_data.get("comments_count", 0)),
        "reports_count": int(video_data.get("reports_count", 0)),
        "created_at": _parse_datetime(video_data.get("created_at")),
        "updated_at": _parse_datetime(video_data.get("updated_at"))
    }

    stmt = insert(VideosOrm).values(**video_dict)
    stmt = stmt.on_conflict_do_update(
        index_elements=["video_id"],
        set_={
            "views_count": stmt.excluded.views_count,
            "likes_count": stmt.excluded.likes_count,
            "comments_count": stmt.excluded.comments_count,
            "reports_count": stmt.excluded.reports_count,
            "creator_id": stmt.excluded.creator_id,
            "video_created_at": stmt.excluded.video_created_at,
            "updated_at": stmt.excluded.updated_at
        }
    )
    await session.execute(stmt)


async def _upsert_snapshot(session, snapshot_data: dict):
    # ВАЛИДАЦИЯ ОБЯЗАТЕЛЬНЫХ ПОЛЕЙ
    if "video_id" not in snapshot_data:
        raise ValueError("Снапшот не содержит video_id")

    snapshot_dict = {
        "snapshot_id": str(snapshot_data["id"]),
        "video_id": str(snapshot_data["video_id"]),
        "views_count": int(snapshot_data.get("views_count", 0)),
        "likes_count": int(snapshot_data.get("likes_count", 0)),
        "comments_count": int(snapshot_data.get("comments_count", 0)),
        "reports_count": int(snapshot_data.get("reports_count", 0)),
        "delta_views_count": int(snapshot_data.get("delta_views_count", 0)),
        "delta_likes_count": int(snapshot_data.get("delta_likes_count", 0)),
        "delta_comments_count": int(snapshot_data.get("delta_comments_count", 0)),
        "delta_reports_count": int(snapshot_data.get("delta_reports_count", 0)),
        "created_at": _parse_datetime(snapshot_data.get("created_at")),
        "updated_at": _parse_datetime(snapshot_data.get("updated_at"))
    }

    stmt = insert(SnapshotsOrm).values(**snapshot_dict)
    stmt = stmt.on_conflict_do_update(
        index_elements=['snapshot_id'],
        set_={
            'views_count': stmt.excluded.views_count,
            'likes_count': stmt.excluded.likes_count,
            'comments_count': stmt.excluded.comments_count,
            'reports_count': stmt.excluded.reports_count,
            'delta_views_count': stmt.excluded.delta_views_count,
            'delta_likes_count': stmt.excluded.delta_likes_count,
            'delta_comments_count': stmt.excluded.delta_comments_count,
            'delta_reports_count': stmt.excluded.delta_reports_count,
            'updated_at': stmt.excluded.updated_at
        }
    )
    await session.execute(stmt)


def _parse_datetime(dt_str: str) -> datetime:
    if not dt_str:
        return None

    try:
        if '+' in dt_str:
            dt_str = dt_str.split('+')[0]
        elif dt_str.endswith('Z'):
            dt_str = dt_str[:-1]

        return datetime.fromisoformat(dt_str)
    except (ValueError, TypeError):
        logger.warning(f"Не удалось распознать дату: {dt_str}")
        return None


async def main():
    """Основная функция запуска"""
    json_path = Path(r"D:\WrokingProjects\PythonProject\data\videos.json")

    print("=" * 60)
    print("ЗАПУСК ЗАГРУЗЧИКА ДАННЫХ")
    print(f"JSON файл: {json_path}")
    print("=" * 60)

    if not json_path.exists():
        print(f"ОШИБКА: Файл не найден: {json_path}")
        return

    await clear_existing_data()

    print("Начало загрузки данных...")
    stats = await load_videos_from_json(json_path)

    print("\n" + "=" * 60)
    print("ИТОГИ ЗАГРУЗКИ:")
    print(f"  Видео:     {stats['videos']}")
    print(f"  Снапшоты:  {stats['snapshots']}")
    print(f"  Ошибки:    {stats['errors']}")

    if stats['errors'] == 0:
        print("Загрузка успешно завершена!")
    else:
        print(f"Загрузка завершена с {stats['errors']} ошибками")
    print("=" * 60)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('data_loader.log'),
            logging.StreamHandler()
        ]
    )

    asyncio.run(main())