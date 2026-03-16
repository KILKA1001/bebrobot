"""Telegram runtime module, called from unified launcher in `bot/main.py`."""

import asyncio
import contextlib
import datetime as dt
import hashlib
import logging
import os
import socket
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


class TelegramPollingLockActiveError(RuntimeError):
    """Raised when another process already owns Telegram polling lock."""


class TelegramPollingAlreadyRunningInProcessError(TelegramPollingLockActiveError):
    """Raised when current process already owns Telegram polling lock."""


def _is_local_process_alive(pid: int) -> bool:
    """Best-effort check for local process existence."""

    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # Process exists but owned by another user/container namespace.
        return True
    return True


def _parse_lock_owner(raw_owner: str) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for part in raw_owner.split():
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        parsed[key.strip()] = value.strip()
    return parsed


BOT_COMMANDS = [
    BotCommand(command="register", description="Зарегистрировать общий аккаунт"),
    BotCommand(command="profile", description="Показать профиль общего аккаунта"),
    BotCommand(command="profile_edit", description="Настройки и редактирование профиля"),
    BotCommand(command="link", description="Привязать Telegram по коду из Discord"),
    BotCommand(command="link_discord", description="Сгенерировать код для привязки Discord"),
    BotCommand(command="points", description="Меню управления баллами"),
    BotCommand(command="balance", description="Показать баланс пользователя"),
    BotCommand(command="tickets", description="Меню управления билетами"),
    BotCommand(command="roles_admin", description="Управление ролями и категориями"),
    BotCommand(command="helpy", description="Список команд"),
    BotCommand(command="guiy", description="Обратиться к Гую (особенно в группе)"),
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

    lock_owner = {
        "pid": os.getpid(),
        "hostname": socket.gethostname(),
        "started_at": dt.datetime.now(dt.UTC).isoformat(),
    }
    owner_line = (
        f"pid={lock_owner['pid']} hostname={lock_owner['hostname']} started_at={lock_owner['started_at']}\n"
    )

    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        existing_owner = "unknown"
        with contextlib.suppress(Exception):
            os.lseek(lock_fd, 0, os.SEEK_SET)
            raw = os.read(lock_fd, 512).decode("utf-8", errors="replace").strip()
            if raw:
                existing_owner = raw

        owner_meta = _parse_lock_owner(existing_owner)
        owner_pid_raw = owner_meta.get("pid", "")
        owner_hostname = owner_meta.get("hostname", "unknown")
        owner_pid = int(owner_pid_raw) if owner_pid_raw.isdigit() else -1
        owner_alive = _is_local_process_alive(owner_pid) if owner_hostname == socket.gethostname() else None

        logger.warning(
            "telegram polling already running (lock=%s, owner=%s), exiting duplicate process",
            lock_path,
            existing_owner,
        )
        current_pid = os.getpid()
        current_hostname = socket.gethostname()
        logger.error(
            "telegram lock diagnostic: current_pid=%s current_hostname=%s owner_pid=%s "
            "owner_hostname=%s owner_alive=%s BOT_RUNTIME=%s",
            current_pid,
            current_hostname,
            owner_pid if owner_pid > 0 else "unknown",
            owner_hostname,
            owner_alive if owner_alive is not None else "unknown",
            (os.getenv("BOT_RUNTIME") or "").strip() or "discord(default)",
        )
        os.close(lock_fd)

        if owner_pid == current_pid and owner_hostname == current_hostname:
            raise TelegramPollingAlreadyRunningInProcessError(
                "telegram polling lock is already owned by current process; "
                "skip duplicate startup"
            )

        raise TelegramPollingLockActiveError(
            f"telegram polling lock is active by another process ({existing_owner})"
        )

    with contextlib.suppress(OSError):
        os.ftruncate(lock_fd, 0)
        os.lseek(lock_fd, 0, os.SEEK_SET)
        os.write(lock_fd, owner_line.encode("utf-8"))
        os.fsync(lock_fd)

    logger.info(
        "telegram polling lock acquired (lock=%s, owner=%s)",
        lock_path,
        owner_line.strip(),
    )

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
            os.ftruncate(lock_fd, 0)
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
