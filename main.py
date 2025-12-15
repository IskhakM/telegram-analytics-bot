import asyncio
import os
import sys
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

import asyncpg
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel

from llm_service import generate_sql
from logging_config import configure_logger

load_dotenv()

logger = configure_logger(__name__)

DATABASE_URL: Optional[str] = os.getenv('DATABASE_URL')

if not DATABASE_URL:
    logger.critical(
        'DATABASE_URL environment variable is not set.'
        'Cannot start without database connection.'
    )
    sys.exit(1)

db_pool: Optional[asyncpg.Pool] = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Управление жизненным циклом приложения и пулом соединений PostgreSQL."""
    global db_pool
    logger.info('STARTUP: Создание пула соединений PostgreSQL...')
    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL)
        logger.info('SUCCESS: Пул соединений PostgreSQL создан.')
        yield
    except Exception as error:
        logger.critical(
            f'FATAL ERROR: Не удалось подключиться к PostgreSQL: {error}'
        )
        db_pool = None
        yield
    finally:
        if db_pool:
            await db_pool.close()
            logger.info('SHUTDOWN: Пул соединений PostgreSQL закрыт.')


app = FastAPI(
    title='Text-to-SQL Backend',
    description=('API для преобразования текста в SQL и'
                 'выполнения его в PostgreSQL.'),
    lifespan=lifespan
)


class QueryRequest(BaseModel):
    query: str


class QueryResponse(BaseModel):
    result: int


async def execute_generated_sql(sql_query: str) -> int:
    """Выполняет сгенерированный SELECT-запрос в PostgreSQL, используя пул."""
    global db_pool

    if not db_pool:
        logger.error('Database connection pool is not available. Returning 503.')
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail='Соединение к БД недоступно'
        )

    conn = None
    try:
        conn = await db_pool.acquire()

        result = await conn.fetchval(sql_query)

        if result is None:
            logger.warning(f'SQL query returned NULL/None: {sql_query}')
            return 0

        return int(result)

    except Exception as error:
        logger.error(f'Ошибка выполнения SQL: {error}')
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f'Ошибка выполнения SQL: {error}'
        )
    finally:
        if conn:
            await db_pool.release(conn)


@app.post('/analyze', response_model=QueryResponse)
async def analyze_query(request: QueryRequest):
    """Принимает запрос, генерирует SQL, выполняет его и возвращает числовой результат."""

    logger.debug(f'➡️ Получен запрос пользователя: {request.query}')

    sql_query = await asyncio.to_thread(generate_sql, request.query)

    logger.info(f'⬅️ Сгенерированный SQL: {sql_query}')

    result_num = await execute_generated_sql(sql_query)

    return {'result': result_num}


if __name__ == '__main__':
    logger.info('Starting Uvicorn server...')
    try:
        uvicorn.run(app, host='0.0.0.0', port=8000)
    except KeyboardInterrupt:
        logger.info('Сервер остановлен вручную.')
        sys.exit(0)