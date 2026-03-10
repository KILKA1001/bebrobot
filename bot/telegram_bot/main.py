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
from aiogram.methods import GetUpdates
from aiogram.dispatcher.dispatcher import DEFAULT_BACKOFF_CONFIG
from aiogram.dispatcher.dispatcher import loggers as aiogram_loggers
from aiogram.utils.backoff import Backoff, BackoffConfig
from aiogram.types import BotCommand

from bot.telegram_bot.commands import get_commands_router
from bot.telegram_bot.config import TELEGRAM_BOT_TOKEN_ENV, get_telegram_bot_token

logger = logging.getLogger(__name__)


BOT_COMMANDS = [
    BotCommand(command="register", description="Зарегистрировать общий аккаунт"),
    BotCommand(command="profile", description="Показать профиль общего аккаунта"),
    BotCommand(command="link", description="Привязать Telegram по коду из Discord"),
    BotCommand(command="link_discord", description="Сгенерировать код для привязки Discord"),
    BotCommand(command="helpy", description="Список команд"),
]


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def _patch_aiogram_conflict_behavior() -> None:
    """Stop polling immediately when Telegram reports getUpdates conflict.

    By default aiogram treats all getUpdates errors as transient and retries
    forever with backoff, which creates noisy logs when another bot instance is
    already polling. We keep the default behavior for other errors.
    """

    async def _listen_updates_with_conflict_exit(
        cls,
        bot: Bot,
        polling_timeout: int = 30,
        backoff_config: BackoffConfig = DEFAULT_BACKOFF_CONFIG,
        allowed_updates: list[str] | None = None,
    ):
        backoff = Backoff(config=backoff_config)
        get_updates = GetUpdates(timeout=polling_timeout, allowed_updates=allowed_updates)
        kwargs = {}
        if bot.session.timeout:
            kwargs["request_timeout"] = int(bot.session.timeout + polling_timeout)
        failed = False
        while True:
            try:
                updates = await bot(get_updates, **kwargs)
            except TelegramConflictError:
                aiogram_loggers.dispatcher.error(
                    "Polling stopped due to TelegramConflictError: another getUpdates consumer is active "
                    "(bot id = %d)",
                    bot.id,
                )
                return
            except Exception as e:  # noqa: BLE001
                failed = True
                aiogram_loggers.dispatcher.error("Failed to fetch updates - %s: %s", type(e).__name__, e)
                aiogram_loggers.dispatcher.warning(
                    "Sleep for %f seconds and try again... (tryings = %d, bot id = %d)",
                    backoff.next_delay,
                    backoff.counter,
                    bot.id,
                )
                await backoff.asleep()
                continue

            if failed:
                aiogram_loggers.dispatcher.info(
                    "Connection established (tryings = %d, bot id = %d)",
                    backoff.counter,
                    bot.id,
                )
                backoff.reset()
                failed = False

            for update in updates:
                yield update
                get_updates.offset = update.update_id + 1

    Dispatcher._listen_updates = classmethod(_listen_updates_with_conflict_exit)


async def run_polling(token: str) -> None:
    # Important for BOT_RUNTIME=both where unified launcher calls run_polling()
    # directly and bypasses telegram main().
    _patch_aiogram_conflict_behavior()

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

        # Fast-fail before Dispatcher's internal long backoff loop if another
        # process is already consuming updates for the same bot token.
        # `timeout=0` keeps this check instantaneous.
        try:
            await bot.get_updates(timeout=0, limit=1)
        except TelegramConflictError:
            logger.error(
                "telegram polling preflight failed: another getUpdates consumer is active. "
                "Ensure only one Telegram runtime is running for this token "
                "(for example, only one process with BOT_RUNTIME=telegram/both)."
            )
            return

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
    _patch_aiogram_conflict_behavior()
    token = get_telegram_bot_token()
    if not token:
        raise RuntimeError(
            f"Не задана переменная окружения {TELEGRAM_BOT_TOKEN_ENV}. "
            "Добавьте её в Render перед запуском Telegram-процесса."
        )

    asyncio.run(run_polling(token))


if __name__ == "__main__":
    main()
