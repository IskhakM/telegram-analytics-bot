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
    """Проверяет доступность необходимых переменных окружения."""
    tokens = {
        'TELEGRAM_BOT_TOKEN': TELEGRAM_TOKEN,
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
    """Отправляет запрос к API-сервису аналитики.
    В случае успешного запроса возвращает словарь с ответом.
    """
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
        logger.error(f'API вернул ошибку: {error.response.status_code}')
        raise ValueError(f'Ошибка API: {error}')

    except Exception as error:
        logger.error(f'Ошибка при запросе к API: {error}')
        raise error


@router.message(CommandStart())
async def command_start_handler(message: types.Message):
    """Ответ на команду /start."""
    logger.debug(f'Получена команда /start от user_id={message.from_user.id}')
    await message.answer(
        "👋 Привет! Я — Аналитический Бот Text-to-SQL.\n"
        "Спросите меня о статистике видео (просмотры, лайки, прирост).\n"
        "Например: 'Сколько всего видео у креатора 1234?'"
    )


@router.message()
async def analyze_text_query(message: types.Message):
    """Обрабатывает входящий текст, отправляет его в бэкенд и возвращает результат."""
    user_query = message.text

    if user_query.startswith('/'):
        return

    logger.debug(f'Получен запрос: "{user_query}" от user_id={message.from_user.id}')
    await message.answer("Анализирую запрос... 🧠")

    try:
        data = await get_backend_answer(user_query)
        result = data.get("result")

        if result is not None:
            response_text = f"✅ Результат: **{result}**"
            logger.debug(f'Успешный ответ пользователю: {result}')
            await message.answer(response_text)
        else:
            logger.warning('Бэкенд вернул пустой результат (None)')
            await message.answer("❌ Не удалось получить числовой результат от бэкенда.")

    except ConnectionError:
        await message.answer(
            "🛑 Ошибка подключения: Сервис аналитики недоступен.\n"
            "Попробуйте позже."
        )
    except Exception as error:
        logger.error(f'Сбой при обработке сообщения: {error}')
        await message.answer("Произошла ошибка при выполнении анализа.")


async def main():
    """Основная логика работы бота."""
    if not check_tokens():
        sys.exit(1)

    bot = Bot(
        token=TELEGRAM_TOKEN,
        default=DefaultBotProperties(parse_mode='Markdown')
    )
    dp = Dispatcher()
    dp.include_router(router)

    await bot.delete_webhook(drop_pending_updates=True)

    logger.info("🤖 Бот запущен. Ожидание сообщений...")

    try:
        await dp.start_polling(bot)
    except Exception as error:
        logger.critical(f'Критическая ошибка при запуске поллинга: {error}')


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен вручную.")
        sys.exit(0)
