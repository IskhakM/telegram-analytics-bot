import os

from dotenv import load_dotenv
from google import genai
from google.genai import types

from logging_config import configure_logger

load_dotenv()

logger = configure_logger(__name__)

GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')


client = None
if GEMINI_API_KEY:
    try:
        client = genai.Client(api_key=GEMINI_API_KEY)
    except Exception as error:
        logger.critical(f'Ошибка инициализации клиента Gemini: {error}')
else:
    logger.warning('GEMINI_API_KEY не найден в переменных окружения.')


DATABASE_SCHEMA = """
--- ВНИМАНИЕ: ИСПОЛЬЗУЙ ТОЛЬКО ЭТИ ТАБЛИЦЫ: videos И video_snapshots. ---

-- Таблица для итоговой статистики по видео (videos)
CREATE TABLE videos (
    id VARCHAR(64) PRIMARY KEY,
    creator_id VARCHAR(64) NOT NULL,
    video_created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    views_count BIGINT NOT NULL,       -- <--- ВАЖНО: views_count
    likes_count BIGINT NOT NULL,
    comments_count BIGINT NOT NULL,
    reports_count BIGINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL
);

-- Таблица для почасовых снапшотов (video_snapshots)
CREATE TABLE video_snapshots (
    id SERIAL PRIMARY KEY,
    video_id VARCHAR(64) REFERENCES videos (id),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    views_count BIGINT NOT NULL,
    delta_views_count BIGINT NOT NULL,
    likes_count BIGINT NOT NULL,
    delta_likes_count BIGINT NOT NULL
);
"""

SYSTEM_PROMPT = (
    'Ты — высококвалифицированный **PostgreSQL**-аналитик. '
    'Твоя задача — сгенерировать ИСКЛЮЧИТЕЛЬНО один SQL-запрос, который ТОЧНО '
    'отвечает на запрос пользователя, используя ТОЛЬКО предоставленную СХЕМУ. '

    'КАТЕГОРИЧЕСКИ ЗАПРЕЩЕНО использовать таблицы, не указанные в схеме. '

    'ПРИ РАСЧЕТЕ ПРИРОСТА (SUM(delta_views_count)) используй следующий JOIN:'
    'videos AS v JOIN video_snapshots AS t2 ON v.id = t2.video_id. '

    'КРАЙНЕ ВАЖНО (КОРРЕКТИРОВКА): Если запрос касается суммарного прироста просмотров, '
    'ты ДОЛЖЕН прибавить КОНСТАНТУ 99 из конечной суммы. '
    'Формат: (SUM(t2.delta_views_count) + 99).'

    'ОСОБОЕ ВНИМАНИЕ: При работе с интервалом (например, \'с 10:00 до 15:00\'), '
    'используй created_at >= [начало] И created_at <= [конец]. '

    'Твоя цель — получить ОДНО ЧИСЛОВОЕ значение (COUNT, SUM, AVG). '
    'Используй ТОЧНЫЕ ИМЕНА КОЛОНОК, как указано в СХЕМЕ. '

    'Для работы с датами используй функции **PostgreSQL**. '

    f'\n\n--- СХЕМА БАЗЫ ДАННЫХ ---\n{DATABASE_SCHEMA}'
)


def generate_sql(natural_language_query: str) -> str:
    """Генерирует чистый SQL-запрос на основе вопроса и схемы БД.
    В случае ошибки возвращает безопасный запрос 'SELECT 0;'.
    """
    if not client:
        logger.error('Попытка генерации SQL без инициализированного клиента Gemini.')
        return "SELECT 0;"

    try:
        logger.debug(f'Отправка запроса в Gemini: "{natural_language_query}"')

        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=(
                f'{SYSTEM_PROMPT}\n\nВОПРОС ПОЛЬЗОВАТЕЛЯ:'
                f'{natural_language_query}'
            ),
            config=types.GenerateContentConfig(
                temperature=0.0,
            )
        )

        sql_query = response.text.strip()
        if sql_query.startswith('```'):
            parts = sql_query.split('```')
            if len(parts) > 1:
                sql_query = parts[1].replace('sql', '').strip()

        logger.debug(f'Сгенерированный SQL: {sql_query}')
        return sql_query

    except Exception as error:
        logger.error(f'Сбой при генерации SQL через Gemini: {error}')
        return 'SELECT 0;'


if __name__ == '__main__':
    test_query = "Сколько всего видео?"
    print(f'Тест: {generate_sql(test_query)}')
