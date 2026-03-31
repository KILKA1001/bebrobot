"""
Назначение: модуль "main" реализует продуктовый контур в зоне Telegram.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Telegram.
"""

import asyncio
import contextlib
import datetime as dt
import hashlib
import logging
import os
import socket
import traceback
from pathlib import Path

import fcntl

from aiogram import Bot, Dispatcher
from aiogram.exceptions import TelegramConflictError
from aiogram.methods import GetUpdates
from aiogram.dispatcher.dispatcher import DEFAULT_BACKOFF_CONFIG
from aiogram.dispatcher.dispatcher import loggers as aiogram_loggers
from aiogram.utils.backoff import Backoff, BackoffConfig
from aiogram.types import BotCommand, BotCommandScopeChat

from bot.telegram_bot.commands import get_commands_router
from bot.telegram_bot.config import TELEGRAM_BOT_TOKEN_ENV, get_telegram_bot_token
from bot.services.guiy_admin_service import resolve_guiy_owner_telegram_ids

logger = logging.getLogger(__name__)
_DISPATCHER: Dispatcher | None = None


class TelegramPollingLockActiveError(RuntimeError):
    """Raised when another process already owns Telegram polling lock."""


class TelegramPollingAlreadyRunningInProcessError(TelegramPollingLockActiveError):
    """Raised when current process already owns Telegram polling lock."""


class TelegramPollingPreflightConflictError(RuntimeError):
    """Raised when preflight getUpdates detects another active consumer."""


class TelegramPollingConflictDetectedError(RuntimeError):
    """Raised when polling detects another active getUpdates consumer."""


class TelegramPollingTransientNetworkError(RuntimeError):
    """Raised when Telegram polling exceeds bounded transient network retries."""


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
    BotCommand(command="roles", description="Каталог ролей и способов получения"),
    BotCommand(command="points", description="Меню управления баллами"),
    BotCommand(command="balance", description="Показать баланс пользователя"),
    BotCommand(command="shop", description="Открыть магазин в личных сообщениях"),
    BotCommand(command="tickets", description="Меню управления билетами"),
    BotCommand(command="roles_admin", description="Управление ролями и категориями (/rolesadmin тоже работает)"),
    BotCommand(command="title", description="Повысить/понизить звание пользователя (суперадмины)"),
    BotCommand(command="helpy", description="Список команд"),
    BotCommand(command="guiy", description="Обратиться к Гую (особенно в группе)"),
]

GUIY_OWNER_COMMANDS = [
    BotCommand(command="guiy_owner", description="Owner-only управление Гуем"),
]
OWNER_PRIVATE_COMMANDS = BOT_COMMANDS + GUIY_OWNER_COMMANDS


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

    max_transient_failures = max(1, int(os.getenv("TELEGRAM_POLLING_MAX_TRANSIENT_FAILURES", "3")))

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
            except TelegramConflictError as exc:
                aiogram_loggers.dispatcher.error(
                    "Polling stopped due to TelegramConflictError: another getUpdates consumer is active "
                    "(bot id = %d)",
                    bot.id,
                )
                raise TelegramPollingConflictDetectedError(
                    "telegram polling conflict detected while fetching updates"
                ) from exc
            except Exception as e:  # noqa: BLE001
                failed = True
                aiogram_loggers.dispatcher.error("Failed to fetch updates - %s: %s", type(e).__name__, e)
                if backoff.counter >= max_transient_failures:
                    aiogram_loggers.dispatcher.error(
                        "Polling stopped after bounded transient retries "
                        "(tryings = %d, bot id = %d, max_failures = %d)",
                        backoff.counter,
                        bot.id,
                        max_transient_failures,
                    )
                    raise TelegramPollingTransientNetworkError(
                        "telegram polling exceeded bounded transient network retries"
                    ) from e
                aiogram_loggers.dispatcher.warning(
                    "Sleep for %f seconds and try again... "
                    "(tryings = %d, bot id = %d, max_failures = %d)",
                    backoff.next_delay,
                    backoff.counter,
                    bot.id,
                    max_transient_failures,
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
    # Important when the unified launcher calls run_polling()
    # directly and bypasses telegram main().
    _patch_aiogram_conflict_behavior()

    current_task = asyncio.current_task()
    startup_context = {
        "correlation_id": f"tg-run-{os.getpid()}-{id(current_task)}",
        "pid": os.getpid(),
        "hostname": socket.gethostname(),
        "task_name": current_task.get_name() if current_task else "unknown",
        "stack": " | ".join(
            line.strip() for line in traceback.format_stack(limit=8) if line.strip()
        ),
    }
    token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()[:12]
    startup_context["token_hash"] = token_hash

    logger.info(
        "telegram polling startup context: correlation_id=%s pid=%s hostname=%s token_hash=%s task=%s stack=%s",
        startup_context["correlation_id"],
        startup_context["pid"],
        startup_context["hostname"],
        startup_context["token_hash"],
        startup_context["task_name"],
        startup_context["stack"],
    )

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

        current_pid = os.getpid()
        current_hostname = socket.gethostname()

        if owner_pid == current_pid and owner_hostname == current_hostname:
            logger.error(
                "telegram polling duplicate startup detected in current process "
                "(lock=%s, owner=%s, correlation_id=%s, pid=%s, hostname=%s, token_hash=%s, task=%s, stack=%s); "
                "stopping duplicate loop",
                lock_path,
                existing_owner,
                startup_context["correlation_id"],
                startup_context["pid"],
                startup_context["hostname"],
                startup_context["token_hash"],
                startup_context["task_name"],
                startup_context["stack"],
            )
            os.close(lock_fd)
            raise TelegramPollingAlreadyRunningInProcessError(
                f"telegram polling already running in current process ({existing_owner})"
            )

        logger.warning(
            "telegram polling already running (lock=%s, owner=%s), exiting duplicate process",
            lock_path,
            existing_owner,
        )
        logger.error(
            "telegram lock diagnostic: current_pid=%s current_hostname=%s owner_pid=%s "
            "owner_hostname=%s owner_alive=%s token_hash=%s",
            current_pid,
            current_hostname,
            owner_pid if owner_pid > 0 else "unknown",
            owner_hostname,
            owner_alive if owner_alive is not None else "unknown",
            startup_context["token_hash"],
        )
        os.close(lock_fd)

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

    bot: Bot | None = None

    try:
        bot = Bot(token=token)

        global _DISPATCHER
        if _DISPATCHER is None:
            _DISPATCHER = Dispatcher()
            try:
                _DISPATCHER.include_router(get_commands_router())
            except Exception:
                logger.exception("telegram dispatcher router attach failed")
                raise

        dp = _DISPATCHER

        me = await bot.get_me()
        logger.info("telegram bot started: @%s (id=%s)", me.username, me.id)

        await bot.set_my_commands(BOT_COMMANDS)
        logger.info("telegram commands registered: %s", ", ".join(f"/{c.command}" for c in BOT_COMMANDS))

        owner_telegram_ids = resolve_guiy_owner_telegram_ids()
        for owner_telegram_id in owner_telegram_ids:
            try:
                await bot.set_my_commands(
                    OWNER_PRIVATE_COMMANDS,
                    scope=BotCommandScopeChat(chat_id=owner_telegram_id),
                )
                logger.info(
                    "telegram guiy owner commands registered owner_telegram_user_id=%s commands=%s",
                    owner_telegram_id,
                    ", ".join(f"/{c.command}" for c in OWNER_PRIVATE_COMMANDS),
                )
            except Exception:
                logger.exception(
                    "telegram guiy owner command registration failed owner_telegram_user_id=%s",
                    owner_telegram_id,
                )
        if not owner_telegram_ids:
            logger.warning("telegram guiy owner commands were not scoped because no owner telegram ids were resolved")

        # Start clean in polling mode.
        await bot.delete_webhook(drop_pending_updates=True)

        # Fast-fail before Dispatcher's internal long backoff loop if another
        # process is already consuming updates for the same bot token.
        # `timeout=0` keeps this check instantaneous.
        try:
            await bot.get_updates(timeout=0, limit=1)
        except TelegramConflictError as exc:
            logger.error(
                "telegram polling preflight failed: another getUpdates consumer is active. "
                "Ensure only one Telegram runtime is running for this token."
            )
            raise TelegramPollingPreflightConflictError(
                "telegram polling preflight conflict: another getUpdates consumer is already active"
            ) from exc

        try:
            await dp.start_polling(bot)
        except TelegramConflictError as exc:
            logger.error(
                "telegram polling conflict detected: another instance is already consuming updates; "
                "stopping this process to avoid endless retries"
            )
            raise TelegramPollingConflictDetectedError(
                "telegram polling conflict detected while running dispatcher polling"
            ) from exc
    finally:
        if bot is not None:
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
            "Укажите её в окружении VPS/сервиса перед запуском Telegram-процесса."
        )

    asyncio.run(run_polling(token))


if __name__ == "__main__":
    main()
