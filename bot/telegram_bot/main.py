"""Telegram runtime module, called from unified launcher in `bot/main.py`."""

import asyncio
import contextlib
import hashlib
import logging
import os
from pathlib import Path

import fcntl

from aiogram import Bot, Dispatcher
from aiogram.exceptions import TelegramConflictError
from aiogram.types import BotCommand

from bot.telegram_bot.commands import get_commands_router
from bot.telegram_bot.config import TELEGRAM_BOT_TOKEN_ENV, get_telegram_bot_token

logger = logging.getLogger(__name__)


BOT_COMMANDS = [
    BotCommand(command="link", description="Привязать Telegram к Discord аккаунту"),
    BotCommand(command="helpy", description="Список команд"),
]


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


async def run_polling(token: str) -> None:
    token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()[:12]
    lock_path = Path(f"/tmp/bebrobot_telegram_polling_{token_hash}.lock")
    lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)

    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        logger.warning(
            "telegram polling already running (lock=%s), exiting duplicate process",
            lock_path,
        )
        os.close(lock_fd)
        return

    bot = Bot(token=token)
    dp = Dispatcher()
    dp.include_router(get_commands_router())

    try:
        me = await bot.get_me()
        logger.info("telegram bot started: @%s (id=%s)", me.username, me.id)

        await bot.set_my_commands(BOT_COMMANDS)
        logger.info("telegram commands registered: %s", ", ".join(f"/{c.command}" for c in BOT_COMMANDS))

        # Start clean in polling mode.
        await bot.delete_webhook(drop_pending_updates=True)
        try:
            await dp.start_polling(bot)
        except TelegramConflictError:
            logger.error(
                "telegram polling conflict detected: another instance is already consuming updates; "
                "stopping this process to avoid endless retries"
            )
            return
    finally:
        await bot.session.close()
        with contextlib.suppress(OSError):
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            os.close(lock_fd)


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
