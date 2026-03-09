"""Telegram runtime module, called from unified launcher in `bot/main.py`."""

import asyncio
import logging

from aiogram import Bot, Dispatcher, Router
from aiogram.filters import Command
from aiogram.types import BotCommand, Message

from bot.telegram_bot.config import TELEGRAM_BOT_TOKEN_ENV, get_telegram_bot_token
from bot.telegram_bot.link_handler import handle_link_command

logger = logging.getLogger(__name__)
router = Router()


START_TEXT = (
    "Привет! 👋\n"
    "Я Telegram-часть бота.\n\n"
    "Доступные команды:\n"
    "/link <код> — привязать Telegram к Discord аккаунту\n"
    "/helpy — показать список команд"
)

HELPY_TEXT = (
    "📚 Список команд:\n"
    "/start — запуск и краткая справка\n"
    "/link <код> — привязать Telegram к Discord аккаунту\n"
    "/helpy — показать это сообщение"
)


BOT_COMMANDS = [
    BotCommand(command="start", description="Запуск Гуя"),
    BotCommand(command="link", description="Привязать Telegram к Discord аккаунту"),
    BotCommand(command="helpy", description="Список команд"),
]


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


@router.message(Command("start"))
async def start_command(message: Message) -> None:
    await message.answer(START_TEXT)


@router.message(Command("helpy"))
async def helpy_command(message: Message) -> None:
    await message.answer(HELPY_TEXT)


@router.message(Command("link"))
async def link_command(message: Message) -> None:
    text = (message.text or "").strip()
    parts = text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.answer("Использование: /link <код>")
        return

    code = parts[1].strip()
    if message.from_user is None:
        await message.answer("Не удалось определить пользователя Telegram.")
        return

    success, payload = handle_link_command(message.from_user.id, code)
    prefix = "✅" if success else "❌"
    await message.answer(f"{prefix} {payload}")


async def run_polling(token: str) -> None:
    bot = Bot(token=token)
    dp = Dispatcher()
    dp.include_router(router)

    try:
        me = await bot.get_me()
        logger.info("telegram bot started: @%s (id=%s)", me.username, me.id)

        await bot.set_my_commands(BOT_COMMANDS)
        logger.info("telegram commands registered: %s", ", ".join(f"/{c.command}" for c in BOT_COMMANDS))

        # Start clean in polling mode.
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        await bot.session.close()


def main() -> None:
    _configure_logging()
    token = get_telegram_bot_token()
    if not token:
        raise RuntimeError(
            f"Не задана переменная окружения {TELEGRAM_BOT_TOKEN_ENV}. "
            "Добавьте её в Render перед запуском Telegram-процесса."
        )

    asyncio.run(run_polling(token))


if __name__ == "__main__":
    main()
