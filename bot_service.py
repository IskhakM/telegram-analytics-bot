import os
import sys
import asyncio
import httpx
from aiogram import Bot, Dispatcher, types, Router
from aiogram.filters import CommandStart
from aiogram.client.default import DefaultBotProperties
from dotenv import load_dotenv

from logging_config import configure_logger


load_dotenv()

TELEGRAM_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
BACKEND_URL = os.getenv('BACKEND_URL')

logger = configure_logger(__name__)

router = Router()


def check_tokens():
    tokens = {
        'TELEGRAM_BOT_TOKEN': TELEGRAM_TOKEN,
        'BACKEND_URL': BACKEND_URL,
    }

    missing_tokens = [
        token for token, token_value in tokens.items() if token_value is None
    ]
    for token in missing_tokens:
        logger.critical(
            f'Отсутствует необходимая переменная окружения: {token}'
        )
    return not missing_tokens


async def get_backend_answer(user_query):
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                BACKEND_URL,
                json={'query': user_query},
                timeout=30
            )
        response.raise_for_status()
        return response.json()

    except httpx.ConnectError:
        logger.error(f'Недоступность эндпоинта {BACKEND_URL}')
        raise ConnectionError(f'Бэкенд недоступен: {BACKEND_URL}')

    except httpx.HTTPStatusError as error:
        status_code = error.response.status_code
        
        try:
            detail = error.response.json().get('detail', 'Неизвестная ошибка API')
        except:
            detail = 'Неизвестная ошибка, API вернул не JSON'

        logger.error(f'API вернул ошибку {status_code}: {detail}')
        raise ValueError(f'Ошибка {status_code}: {detail}')

    except Exception as error:
        logger.error(f'Непредвиденная ошибка при запросе к API: {error}')
        raise error


@router.message(CommandStart())
async def command_start_handler(message: types.Message):
    logger.debug(f'Получена команда /start от user_id={message.from_user.id}')
    await message.answer(
        '👋 Привет! Я — Аналитический Бот Text-to-SQL.\n'
        'Спросите меня о статистике видео (просмотры, лайки, прирост).\n'
        'Например: \'Сколько всего видео у креатора 1234?\''
    )


@router.message()
async def analyze_text_query(message: types.Message):
    user_query = message.text

    if user_query.startswith('/'):
        return

    logger.debug(
        f'Получен запрос: \'{user_query}\' от user_id={message.from_user.id}'
    )

    try:
        data = await get_backend_answer(user_query)
        result = data.get('result')

        if result is not None:
            response_text = f'✅ Результат: **{result}**'
            logger.debug(f'Успешный ответ пользователю: {result}')
            await message.answer(response_text)
            
        else:
            logger.warning('Бэкенд вернул пустой результат (None)')
            await message.answer(
                '❌ Не удалось получить числовой результат от бэкенда.'
            )

    except ConnectionError:
        await message.answer(
            '🛑 Ошибка подключения: Сервис аналитики недоступен.\n'
            'Попробуйте позже.'
        )
    except ValueError as error:
        await message.answer(
            f'❌ Ошибка анализа данных.\nПодробности: {error}'
        )

    except Exception as error:
        logger.error(f'Сбой при обработке сообщения: {error}')
        await message.answer(
            'Произошла непредвиденная ошибка при выполнении анализа.'
        )


async def main():
    if not check_tokens():
        sys.exit(1)

    if not BACKEND_URL.startswith('http'):
        logger.critical(
            'BACKEND_URL должен начинаться с \'http://\' или'
            '\'https://\'. Проверьте .env'
        )
        sys.exit(1)

    bot = Bot(
        token=TELEGRAM_TOKEN,
        default=DefaultBotProperties(parse_mode='Markdown')
    )
    dp = Dispatcher()
    dp.include_router(router)

    await bot.delete_webhook(drop_pending_updates=True)

    logger.info('🤖 Бот запущен. Ожидание сообщений...')

    try:
        await dp.start_polling(bot)
    except Exception as error:
        logger.critical(f'Критическая ошибка при запуске поллинга: {error}')


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info('Бот остановлен вручную.')
        sys.exit(0)
