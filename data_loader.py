import asyncio
import json
import os
import sys

import asyncpg
from dotenv import load_dotenv

from logging_config import configure_logger
from utilis import get_naive_utc, run_sql_script

load_dotenv()

logger = configure_logger(__name__)

DATA_FILE = 'videos.json'
DB_URL = os.getenv('DATABASE_URL')


def safe_int(value: any) -> int:
    """Безопасно преобразует значение в целое число, возвращая 0 при ошибке."""
    if value is None:
        return 0
    try:
        return int(str(value).strip())
    except ValueError as error:
        logger.warning(
            f'Не удалось преобразовать значение "{value}" в int: {error}'
        )
        return 0


async def load_data(conn: asyncpg.Connection):
    """Загружает данные из JSON в таблицы"""
    """videos и video_snapshots с помощью copy_records_to_table."""

    if not os.path.exists(DATA_FILE):
        logger.critical(f'Файл данных "{DATA_FILE}" не найден.')
        sys.exit(1)

    logger.info("Начало чтения и обработки JSON данных...")

    with open(DATA_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)

    videos_data = []
    snapshots_data = []

    try:
        for video in data['videos']:
            videos_data.append((
                video['id'],
                video['creator_id'],
                get_naive_utc(video['video_created_at']),
                safe_int(video['views_count']),
                safe_int(video['likes_count']),
                safe_int(video['comments_count']),
                safe_int(video['reports_count']),
            ))

            video_id = video['id']
            for snapshot in video['snapshots']:
                snapshots_data.append((
                    video_id,
                    get_naive_utc(snapshot['created_at']),
                    safe_int(snapshot['views_count']),
                    safe_int(snapshot['delta_views_count']),
                    safe_int(snapshot['likes_count']),
                    safe_int(snapshot['delta_likes_count']),
                    safe_int(snapshot['comments_count']),
                    safe_int(snapshot['delta_comments_count']),
                    safe_int(snapshot['reports_count']),
                    safe_int(snapshot['delta_reports_count']),
                ))
    except KeyError as error:
        logger.critical(f'Ошибка структуры JSON: Отсутствует ключ {error}')
        sys.exit(1)

    try:
        await conn.copy_records_to_table(
            'videos',
            records=videos_data,
            columns=['id', 'creator_id', 'video_created_at', 'views_count',
                     'likes_count', 'comments_count', 'reports_count']
        )
        logger.info(f'Загружено {len(videos_data)} записей в таблицу "videos".')
    except Exception as e:
        logger.error(f'Ошибка при загрузке в таблицу "videos": {e}')
        raise e

    try:
        await conn.copy_records_to_table(
            'video_snapshots',
            records=snapshots_data,
            columns=['video_id', 'created_at', 'views_count',
                     'delta_views_count',
                     'likes_count', 'delta_likes_count', 'comments_count',
                     'delta_comments_count',
                     'reports_count', 'delta_reports_count']
        )
        logger.info(
            f'Загружено {len(snapshots_data)}'
            f'записей в таблицу "video_snapshots".'
        )
    except Exception as e:
        logger.error(f'Ошибка при загрузке в таблицу "video_snapshots": {e}')
        raise e


async def main():
    """Точка входа: проверка токенов, подключение к БД, создание схемы и запуск загрузки."""
    conn = None

    if not DB_URL:
        logger.critical('Ошибка: DATABASE_URL не найдена. Проверьте .env.')
        sys.exit(1)

    try:
        logger.info('Попытка подключения к PostgreSQL...')
        conn = await asyncpg.connect(DB_URL)
        logger.info('Подключение к БД установлено.')

        await run_sql_script(conn, 'schema.sql')
        logger.info('Структура БД "schema.sql" успешно применена.')

        await load_data(conn)

    except Exception as e:

        logger.critical(f'Критическая ошибка при работе с БД: {e}')
        sys.exit(1)

    finally:
        if conn:
            await conn.close()
            logger.info('Соединение с БД закрыто. Загрузка завершена.')

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Загрузчик остановлен вручную.")
        sys.exit(0)
