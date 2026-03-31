"""
Назначение: модуль "main" реализует продуктовый контур в зоне общая логика.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: общая логика.
"""

# Core imports
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Системные импорты
import asyncio
import logging
import time
import json
import contextlib
import hashlib
import uuid
import errno
import socket
from aiohttp import ClientConnectionError, ClientError, ServerTimeoutError
from dotenv import load_dotenv

from bot.telegram_bot.config import TELEGRAM_BOT_TOKEN_ENV, get_telegram_bot_token
from bot.services.ai_service import (
    _build_media_input,
    close_shared_http_session,
    generate_guiy_reply,
    init_shared_http_session,
)


load_dotenv()


# Основные импорты Discord
import discord
import pytz
from bot.commands import bot as command_bot
# Локальные импорты
from bot.data import db
import bot.commands.tournament
import bot.commands.maps
from datetime import datetime
from bot.systems import fines_logic
from bot.services.moderation_notifications import ModerationNotificationsService
from bot.systems.profile_titles_logic import (
    handle_member_join_for_profile_titles,
    handle_member_update_for_profile_titles,
    profile_titles_sync_loop,
)
from bot.systems.external_roles_sync_logic import external_roles_sync_loop, schedule_external_roles_sync
import bot.commands.fines
import bot.data.tournament_db as tournament_db
from bot.systems.tournament_logic import BettingView
from bot.systems.interactive_rounds import RoundManagementView
from bot.systems.tournament_logic import create_tournament_logic
from bot.utils import safe_send
from bot.utils.guiy_trigger import is_guiy_name_trigger
from bot.utils.guiy_typing import calculate_typing_delay_seconds
from bot.utils.conversation_activity import should_thread_reply
from bot.telegram_bot.main import (
    TelegramPollingConflictDetectedError,
    TelegramPollingAlreadyRunningInProcessError,
    TelegramPollingLockActiveError,
    TelegramPollingPreflightConflictError,
    TelegramPollingTransientNetworkError,
    run_polling as run_telegram_polling,
)
from bot.utils.discord_http import (
    is_transient_rate_limit_error,
    log_discord_http_exception,
)


# Константы
TIME_FORMAT = "%H:%M (%d.%m.%Y)"
TOP_CHANNEL_ID = int(os.getenv("MONTHLY_TOP_CHANNEL_ID", 0))

# Таймеры удаления сообщений
active_timers = {}

# Prevent duplicate background tasks if on_ready fires multiple times
tasks_started = False
startup_tasks_started = False
commands_synced = False
presence_initialized = False
runtime_views_restored = False
telegram_runtime_started = False
telegram_runtime_guard = asyncio.Lock()

COMMAND_SYNC_STATE_FILE = os.getenv(
    "COMMAND_SYNC_STATE_FILE",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "command_sync_state.json"),
)
COMMAND_SYNC_MIN_INTERVAL = int(os.getenv("COMMAND_SYNC_MIN_INTERVAL", "21600"))
bot = command_bot
db.bot = bot

startup_run_id = uuid.uuid4().hex[:12]
startup_token_hash = "unknown"
_ai_session_hooks_installed = False


def _token_fingerprint(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:12]


def _startup_context(
    *,
    step: str,
    operation_id: str | None = None,
    include_operation_id: bool = True,
    **extra,
) -> dict:
    context = {
        "startup_run_id": startup_run_id,
        "pid": os.getpid(),
        "guild_count": len(getattr(bot, "guilds", []) or []),
        "token_hash": startup_token_hash,
        "step": step,
    }
    if operation_id and include_operation_id:
        context["operation_id"] = operation_id
    context.update(extra)
    return context


def _log_startup_step(level: int, message: str, *, step: str, operation_id: str | None = None, **extra) -> None:
    logging.log(level, "%s | %s", message, _startup_context(step=step, operation_id=operation_id, **extra))


def _install_ai_session_hooks_for_discord() -> None:
    global _ai_session_hooks_installed
    if _ai_session_hooks_installed:
        return

    original_setup_hook = getattr(bot, "setup_hook", None)
    original_close = bot.close

    async def _setup_hook_with_ai_session(*args, **kwargs):
        await init_shared_http_session()
        if original_setup_hook is not None:
            return await original_setup_hook(*args, **kwargs)
        return None

    async def _close_with_ai_session(*args, **kwargs):
        try:
            return await original_close(*args, **kwargs)
        finally:
            await close_shared_http_session()

    bot.setup_hook = _setup_hook_with_ai_session
    bot.close = _close_with_ai_session
    _ai_session_hooks_installed = True


def _create_task_with_startup_logging(
    coro,
    *,
    step: str,
    operation_id: str | None = None,
    startup_burst: bool = False,
):
    async def _runner():
        _log_startup_step(
            logging.INFO,
            "startup background loop entered first pass",
            step=step,
            operation_id=operation_id,
            startup_burst=startup_burst,
        )
        try:
            return await coro
        except discord.HTTPException as exc:
            log_discord_http_exception(
                "startup background loop failed with discord http exception",
                exc,
                stage=step,
                operation_id=operation_id,
                **_startup_context(
                    step=step,
                    operation_id=operation_id,
                    include_operation_id=False,
                    startup_burst=startup_burst,
                ),
            )
            raise
        except Exception:
            logging.exception(
                "startup background loop failed | %s",
                _startup_context(step=step, operation_id=operation_id, startup_burst=startup_burst),
            )
            raise

    _log_startup_step(
        logging.INFO,
        "startup background loop scheduling",
        step=step,
        operation_id=operation_id,
        startup_burst=startup_burst,
    )
    task = asyncio.create_task(_runner(), name=step)
    _log_startup_step(
        logging.INFO,
        "startup background loop scheduled",
        step=step,
        operation_id=operation_id,
        startup_burst=startup_burst,
        task_name=task.get_name(),
    )
    return task


def _reset_discord_http_client_state(reason: str) -> None:
    """Reset discord.py HTTP client internals after transport/session failures."""
    try:
        http_client = getattr(bot, "http", None)
        if not http_client:
            logging.warning("discord http reset skipped: http client is missing reason=%s", reason)
            return

        connector = getattr(http_client, "connector", None)
        connector_closed = bool(connector and getattr(connector, "closed", False))

        session = getattr(http_client, "_HTTPClient__session", None)
        session_closed = bool(session and getattr(session, "closed", False))

        # clear() only сбрасывает ссылки в discord.py и не всегда закрывает
        # aiohttp-сессию после раннего падения login/start, из-за чего в логах
        # появляется "Unclosed client session". Закрываем её явно.
        if session and not session_closed:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                with contextlib.suppress(Exception):
                    asyncio.run(session.close())
            else:
                loop.create_task(session.close())

        with contextlib.suppress(Exception):
            http_client.clear()

        if connector_closed:
            http_client.connector = discord.utils.MISSING

        logging.info(
            "discord http state reset reason=%s session_closed=%s connector_closed=%s",
            reason,
            session_closed,
            connector_closed,
        )
    except Exception:
        logging.exception("discord http state reset failed reason=%s", reason)


def _is_transient_telegram_network_error(exc: BaseException) -> bool:
    """Return True for short-lived Telegram transport/network failures."""

    transient_error_types = (
        asyncio.TimeoutError,
        TimeoutError,
        ConnectionError,
        ClientConnectionError,
        ServerTimeoutError,
        socket.timeout,
    )

    if isinstance(exc, transient_error_types):
        return True

    if isinstance(exc, ClientError):
        return True

    if isinstance(exc, OSError):
        transient_errno = {
            errno.EAI_AGAIN,
            errno.ECONNRESET,
            errno.ECONNABORTED,
            errno.ECONNREFUSED,
            errno.ENETDOWN,
            errno.ENETRESET,
            errno.ENETUNREACH,
            errno.EHOSTDOWN,
            errno.EHOSTUNREACH,
            errno.ETIMEDOUT,
        }
        return exc.errno in transient_errno

    return False


class _SuppressKnownRateLimitWarning(logging.Filter):
    """Filter noisy upstream 429 warnings that we already handle explicitly."""

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        return "API rate limited (HTTP 429)" not in message


def configure_logging() -> None:
    root_logger = logging.getLogger()
    root_logger.addFilter(_SuppressKnownRateLimitWarning())

    # Убираем шумные служебные сообщения библиотек из startup-логов.
    logging.getLogger("discord.client").setLevel(logging.WARNING)
    logging.getLogger("werkzeug").setLevel(logging.WARNING)


async def send_greetings(channel, user_list):
    for user_id in user_list:
        await safe_send(channel, f"Привет, <@{user_id}>!")

async def autosave_task():
    await bot.wait_until_ready()
    autosave_interval_sec = int(os.getenv("SCORES_AUTOSAVE_INTERVAL_SEC", "600"))
    while not bot.is_closed():
        try:
            db.save_all()
        except Exception:
            logging.exception("autosave flush failed")
        await asyncio.sleep(autosave_interval_sec)


def _restore_runtime_views_once() -> None:
    global runtime_views_restored
    if runtime_views_restored:
        logging.info("discord runtime view restoration skipped: already completed in this process")
        return

    active_tournaments = tournament_db.get_active_tournaments()
    logging.info("discord runtime view restoration begin active_tournaments=%s", len(active_tournaments))

    for tour in active_tournaments:
        if not all(key in tour for key in ["id", "size", "type", "announcement_message_id"]):
            logging.warning("discord runtime view restoration skipped malformed tournament payload=%s", tour)
            continue

        try:
            bet_view = BettingView(tour["id"])
            logging.info(
                "discord view registration begin tour_id=%s announcement_message_id=%s | %s",
                tour["id"],
                tour["announcement_message_id"],
                _startup_context(step="betting_view_register", operation_id=f"tournament:{tour['id']}"),
            )
            bot.add_view(bet_view, message_id=tour["announcement_message_id"])
            logging.info(
                "discord view registration complete tour_id=%s announcement_message_id=%s | %s",
                tour["id"],
                tour["announcement_message_id"],
                _startup_context(step="betting_view_register", operation_id=f"tournament:{tour['id']}"),
            )

            status_msg_id = tournament_db.get_status_message_id(tour["id"])
            if status_msg_id:
                logging.info(
                    "discord view registration begin tour_id=%s status_message_id=%s | %s",
                    tour["id"],
                    status_msg_id,
                    _startup_context(step="betting_status_view_register", operation_id=f"tournament:{tour['id']}"),
                )
                bot.add_view(BettingView(tour["id"]), message_id=status_msg_id)
                logging.info(
                    "discord view registration complete tour_id=%s status_message_id=%s | %s",
                    tour["id"],
                    status_msg_id,
                    _startup_context(step="betting_status_view_register", operation_id=f"tournament:{tour['id']}"),
                )
        except Exception:
            logging.exception("discord runtime view restoration failed tour_id=%s", tour.get("id"))

        participants_data = tournament_db.list_participants(tour["id"])
        participants = []
        for p in participants_data:
            if "discord_user_id" in p and p["discord_user_id"]:
                participants.append(p["discord_user_id"])
            elif "player_id" in p and p["player_id"]:
                participants.append(p["player_id"])

        if participants:
            try:
                team_size = 3 if tour.get("type") == "team" else 1
                tournament_logic = create_tournament_logic(
                    participants, team_size=team_size, shuffle=False
                )
                round_management_view = RoundManagementView(tour["id"], tournament_logic)
                logging.info(
                    "discord view registration begin tour_id=%s participants_count=%s | %s",
                    tour["id"],
                    len(participants),
                    _startup_context(step="round_management_view_register", operation_id=f"tournament:{tour['id']}"),
                )
                bot.add_view(round_management_view)
                logging.info(
                    "discord view registration complete tour_id=%s participants_count=%s | %s",
                    tour["id"],
                    len(participants),
                    _startup_context(step="round_management_view_register", operation_id=f"tournament:{tour['id']}"),
                )
            except Exception:
                logging.exception("discord round management view restoration failed tour_id=%s", tour.get("id"))

    runtime_views_restored = True
    logging.info("discord runtime view restoration complete active_tournaments=%s", len(active_tournaments))

@bot.event
async def on_ready():
    print(f'🟢 Бот {bot.user} запущен!')
    print(f'Серверов: {len(bot.guilds)}')

    global tasks_started, startup_tasks_started, commands_synced, presence_initialized
    if not tasks_started:
        tasks_started = True

        _create_task_with_startup_logging(
            fines_logic.check_overdue_fines(bot),
            step="check_overdue_fines_loop",
            operation_id="startup-loop-1",
            startup_burst=True,
        )
        _create_task_with_startup_logging(
            fines_logic.debt_repayment_loop(bot),
            step="debt_repayment_loop",
            operation_id="startup-loop-2",
            startup_burst=True,
        )
        _create_task_with_startup_logging(
            fines_logic.reminder_loop(bot),
            step="reminder_loop",
            operation_id="startup-loop-3",
            startup_burst=True,
        )
        _create_task_with_startup_logging(
            fines_logic.fines_summary_loop(bot),
            step="fines_summary_loop",
            operation_id="startup-loop-4",
            startup_burst=True,
        )
        _create_task_with_startup_logging(
            ModerationNotificationsService.mute_reconciliation_loop(bot),
            step="mute_reconciliation_loop",
            operation_id="startup-loop-4b",
            startup_burst=True,
        )
        from bot.systems.tournament_logic import tournament_reminder_loop, registration_deadline_loop
        _create_task_with_startup_logging(
            tournament_reminder_loop(bot),
            step="tournament_reminder_loop",
            operation_id="startup-loop-5",
            startup_burst=True,
        )
        _create_task_with_startup_logging(
            registration_deadline_loop(bot),
            step="registration_deadline_loop",
            operation_id="startup-loop-6",
            startup_burst=True,
        )
        _create_task_with_startup_logging(
            profile_titles_sync_loop(bot),
            step="profile_titles_sync_loop",
            operation_id="startup-loop-7",
            startup_burst=True,
        )
        _create_task_with_startup_logging(
            external_roles_sync_loop(bot),
            step="external_roles_sync_loop",
            operation_id="startup-loop-8",
            startup_burst=True,
        )

    if not presence_initialized:
        activity = discord.Activity(
            name="Привет! Напиши команду /helpy чтобы увидеть все команды 🧠",
            type=discord.ActivityType.listening
        )
        try:
            _log_startup_step(logging.INFO, "startup step begin", step="change_presence")
            await bot.change_presence(activity=activity)
            presence_initialized = True
            _log_startup_step(logging.INFO, "startup step complete", step="change_presence")
        except discord.HTTPException as exc:
            log_discord_http_exception(
                "discord startup failed to update presence",
                exc,
                stage="on_ready.change_presence",
                **_startup_context(step="change_presence"),
            )
        except Exception:
            logging.exception("discord startup failed to update presence | %s", _startup_context(step="change_presence"))


    if not commands_synced:
        should_sync = should_sync_commands()
        try:
            if should_sync:
                _log_startup_step(logging.INFO, "startup step begin", step="tree_sync", should_sync=should_sync)
                await bot.tree.sync()
                mark_commands_synced()
                _log_startup_step(logging.INFO, "startup step complete", step="tree_sync", should_sync=should_sync)
                print("🔁 Slash-команды синхронизированы")
            else:
                _log_startup_step(logging.INFO, "startup step skipped", step="tree_sync", should_sync=should_sync)
                print("⏭️ Синхронизация slash-команд пропущена (слишком рано после прошлого запуска)")
            commands_synced = True
        except discord.HTTPException as exc:
            log_discord_http_exception(
                "discord startup failed to sync slash commands",
                exc,
                stage="on_ready.tree_sync",
                should_sync=should_sync,
                **_startup_context(step="tree_sync", should_sync=should_sync),
            )
        except Exception:
            logging.exception("❌ Ошибка синхронизации slash-команд | %s", _startup_context(step="tree_sync", should_sync=should_sync))
    
    _restore_runtime_views_once()

    # Не дублируем фоновые задачи при повторном on_ready (reconnect)
    if not startup_tasks_started:
        startup_tasks_started = True
        _create_task_with_startup_logging(
            autosave_task(),
            step="autosave_task",
            operation_id="startup-loop-9",
            startup_burst=True,
        )
        _create_task_with_startup_logging(
            monthly_top_task(),
            step="monthly_top_task",
            operation_id="startup-loop-10",
            startup_burst=True,
        )

    print('--- Ленивый режим загрузки данных активирован ---')
    print("📡 Задачи активированы.")


@bot.event
async def on_member_update(before: discord.Member, after: discord.Member):
    await handle_member_update_for_profile_titles(before, after)

    try:
        before_role_ids = {role.id for role in before.roles if not role.is_default()}
        after_role_ids = {role.id for role in after.roles if not role.is_default()}
        if before_role_ids == after_role_ids:
            return

        from bot.services import AccountsService

        account_id = AccountsService.resolve_account_id("discord", str(after.id))
        if not account_id:
            return
        schedule_external_roles_sync(bot, str(account_id), reason="discord_member_update")
    except Exception:
        logging.exception("external roles sync dispatch failed for member update member_id=%s guild_id=%s", after.id, after.guild.id if after.guild else None)


@bot.event
async def on_member_join(member: discord.Member):
    await handle_member_join_for_profile_titles(member)


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return

    try:
        from bot.services import AccountsService

        AccountsService.persist_identity_lookup_fields(
            "discord",
            str(message.author.id),
            username=getattr(message.author, "name", None),
            display_name=getattr(message.author, "display_name", None),
            global_username=getattr(message.author, "global_name", None),
        )
        logging.info(
            "discord get_context begin channel_id=%s message_id=%s author_id=%s",
            getattr(message.channel, "id", None),
            getattr(message, "id", None),
            getattr(message.author, "id", None),
        )
        ctx = await bot.get_context(message)
        logging.info(
            "discord get_context complete channel_id=%s message_id=%s author_id=%s valid=%s",
            getattr(message.channel, "id", None),
            getattr(message, "id", None),
            getattr(message.author, "id", None),
            getattr(ctx, "valid", False),
        )
        if getattr(ctx, "valid", False):
            await bot.process_commands(message)
            return

        content = (message.content or "").strip()
        media_inputs: list[dict[str, str]] = []
        if getattr(message, "attachments", None):
            for attachment in message.attachments[:3]:
                content_type = str(getattr(attachment, "content_type", "") or "").lower()
                if not content_type.startswith("image/"):
                    logging.info(
                        "discord ai attachment skipped because mime type is not image channel_id=%s author_id=%s attachment_id=%s content_type=%s filename=%s",
                        getattr(message.channel, "id", None),
                        getattr(message.author, "id", None),
                        getattr(attachment, "id", None),
                        content_type,
                        getattr(attachment, "filename", None),
                    )
                    continue
                try:
                    payload = await attachment.read()
                    media_input = _build_media_input(
                        payload=payload,
                        mime_type=content_type,
                        source=f"discord:attachment:{getattr(attachment, 'id', 'unknown')}",
                    )
                    if media_input:
                        media_inputs.append(media_input)
                        logging.info(
                            "discord ai attachment collected channel_id=%s author_id=%s attachment_id=%s filename=%s content_type=%s bytes=%s",
                            getattr(message.channel, "id", None),
                            getattr(message.author, "id", None),
                            getattr(attachment, "id", None),
                            getattr(attachment, "filename", None),
                            content_type,
                            len(payload),
                        )
                except Exception:
                    logging.exception(
                        "discord ai attachment read failed channel_id=%s author_id=%s attachment_id=%s filename=%s",
                        getattr(message.channel, "id", None),
                        getattr(message.author, "id", None),
                        getattr(attachment, "id", None),
                        getattr(attachment, "filename", None),
                    )

        is_reply_to_bot = False
        if message.reference and message.reference.message_id:
            ref_msg = message.reference.resolved
            if ref_msg is None and message.channel:
                try:
                    logging.info(
                        "discord fetch_message begin channel_id=%s reference_message_id=%s",
                        getattr(message.channel, "id", None),
                        message.reference.message_id,
                    )
                    ref_msg = await message.channel.fetch_message(message.reference.message_id)
                    logging.info(
                        "discord fetch_message complete channel_id=%s reference_message_id=%s fetched=%s",
                        getattr(message.channel, "id", None),
                        message.reference.message_id,
                        bool(ref_msg),
                    )
                except Exception:
                    logging.exception(
                        "failed to fetch referenced message channel_id=%s message_id=%s",
                        getattr(message.channel, "id", None),
                        message.reference.message_id,
                    )
            if isinstance(ref_msg, discord.Message) and ref_msg.author and ref_msg.author.id == bot.user.id:
                is_reply_to_bot = True

        is_named = is_guiy_name_trigger(content)
        is_bot_mentioned = bool(bot.user and bot.user in getattr(message, "mentions", []))

        if is_named or is_reply_to_bot or is_bot_mentioned:
            logging.info(
                "discord ai trigger matched guild_id=%s channel_id=%s author_id=%s is_named=%s is_reply_to_bot=%s is_bot_mentioned=%s media_count=%s text=%s",
                getattr(message.guild, "id", None),
                getattr(message.channel, "id", None),
                getattr(message.author, "id", None),
                is_named,
                is_reply_to_bot,
                is_bot_mentioned,
                len(media_inputs),
                content[:160],
            )
            reply = await generate_guiy_reply(
                content,
                provider="discord",
                user_id=getattr(message.author, "id", None),
                conversation_id=getattr(message.channel, "id", None),
                media_inputs=media_inputs,
            )
            if reply:
                typing_delay = calculate_typing_delay_seconds(reply)
                logging.info(
                    "discord ai typing simulation channel_id=%s author_id=%s delay=%ss reply_len=%s",
                    getattr(message.channel, "id", None),
                    getattr(message.author, "id", None),
                    typing_delay,
                    len(reply),
                )
                try:
                    logging.info(
                        "discord typing begin channel_id=%s author_id=%s message_id=%s",
                        getattr(message.channel, "id", None),
                        getattr(message.author, "id", None),
                        getattr(message, "id", None),
                    )
                    async with message.channel.typing():
                        await asyncio.sleep(typing_delay)
                    logging.info(
                        "discord typing complete channel_id=%s author_id=%s message_id=%s",
                        getattr(message.channel, "id", None),
                        getattr(message.author, "id", None),
                        getattr(message, "id", None),
                    )
                except Exception:
                    logging.exception(
                        "discord typing simulation failed channel_id=%s author_id=%s",
                        getattr(message.channel, "id", None),
                        getattr(message.author, "id", None),
                    )
                use_reply_mark = should_thread_reply(
                    f"discord:{getattr(message.channel, 'id', None)}",
                    getattr(message.author, "id", None),
                )
                logging.info(
                    "discord ai reply mode resolved channel_id=%s author_id=%s message_id=%s use_reply_mark=%s",
                    getattr(message.channel, "id", None),
                    getattr(message.author, "id", None),
                    getattr(message, "id", None),
                    use_reply_mark,
                )
                try:
                    if use_reply_mark:
                        logging.info(
                            "discord reply begin channel_id=%s author_id=%s message_id=%s mention_author=%s",
                            getattr(message.channel, "id", None),
                            getattr(message.author, "id", None),
                            getattr(message, "id", None),
                            False,
                        )
                        await message.reply(reply, mention_author=False)
                        logging.info(
                            "discord reply complete channel_id=%s author_id=%s message_id=%s",
                            getattr(message.channel, "id", None),
                            getattr(message.author, "id", None),
                            getattr(message, "id", None),
                        )
                    else:
                        await safe_send(message.channel, reply)
                except Exception:
                    logging.exception(
                        "discord ai failed to send response channel_id=%s author_id=%s message_id=%s use_reply_mark=%s",
                        getattr(message.channel, "id", None),
                        getattr(message.author, "id", None),
                        getattr(message, "id", None),
                        use_reply_mark,
                    )
                    await safe_send(message.channel, reply)
    except Exception:
        logging.exception(
            "discord ai reply failed guild_id=%s channel_id=%s author_id=%s",
            getattr(message.guild, "id", None),
            getattr(message.channel, "id", None),
            getattr(message.author, "id", None),
        )

    await bot.process_commands(message)

async def monthly_top_task():
    await bot.wait_until_ready()
    while not bot.is_closed():
        now = datetime.now(pytz.timezone('Europe/Moscow'))
        if now.day == 1:
            try:
                if db.supabase:
                    check = db.supabase.table("monthly_top_log") \
                        .select("id") \
                        .eq("month", now.month) \
                        .eq("year", now.year) \
                        .execute()
                    if check.data:
                        print("⏳ Топ уже начислен в этом месяце")
                        await asyncio.sleep(3600)
                        continue

                logging.info("discord get_channel begin channel_id=%s", TOP_CHANNEL_ID)
                channel = bot.get_channel(TOP_CHANNEL_ID)
                logging.info(
                    "discord get_channel complete channel_id=%s found=%s",
                    TOP_CHANNEL_ID,
                    isinstance(channel, discord.TextChannel),
                )
                if isinstance(channel, discord.TextChannel):
                    msg = await safe_send(
                        channel,
                        "🔁 Запускаем автоматический топ месяца...",
                    )
                    logging.info(
                        "discord get_context begin channel_id=%s source=monthly_top_task message_exists=%s",
                        getattr(channel, "id", None),
                        bool(msg or channel.last_message),
                    )
                    ctx = await bot.get_context(msg or channel.last_message)
                    logging.info(
                        "discord get_context complete channel_id=%s source=monthly_top_task valid=%s",
                        getattr(channel, "id", None),
                        getattr(ctx, "valid", False),
                    )

                    from bot.systems.core_logic import run_monthly_top
                    await run_monthly_top(ctx, now.month, now.year)

                    logging.info(
                        "legacy monthly fine top flow disabled source=monthly_top_task reason=rep_primary_entrypoint"
                    )
                else:
                    logging.error("❌ Указанный канал недоступен или не текстовый channel_id=%s", TOP_CHANNEL_ID)

            except Exception:
                logging.exception("❌ Ошибка автозапуска топа месяца")

        await asyncio.sleep(3600)


def should_sync_commands() -> bool:
    if os.getenv("FORCE_COMMAND_SYNC", "").lower() in {"1", "true", "yes"}:
        return True

    try:
        with open(COMMAND_SYNC_STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
        last_synced = float(state.get("last_synced", 0))
    except (FileNotFoundError, ValueError, OSError, TypeError):
        return True

    return (time.time() - last_synced) >= max(0, COMMAND_SYNC_MIN_INTERVAL)


def mark_commands_synced() -> None:
    try:
        with open(COMMAND_SYNC_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump({"last_synced": time.time()}, f)
    except OSError as e:
        logging.warning("Не удалось записать состояние синхронизации команд: %s", e)
# Основной запуск

def run_telegram_main(token: str) -> None:
    try:
        async def _run() -> None:
            await init_shared_http_session()
            try:
                await run_telegram_polling(token)
            finally:
                await close_shared_http_session()

        asyncio.run(_run())
    except TelegramPollingAlreadyRunningInProcessError as exc:
        logging.warning("telegram runtime duplicate startup detected; details=%s", exc)
        return
    except Exception:
        logging.exception("telegram polling failed")
        raise


def run_discord_main(token: str) -> None:
    global bot, startup_token_hash

    startup_token_hash = _token_fingerprint(token)
    _install_ai_session_hooks_for_discord()

    try:
        _log_startup_step(
            logging.INFO,
            "startup step begin",
            step="bot.run",
            hostname=os.uname().nodename,
        )
        bot.run(token)
        _log_startup_step(
            logging.INFO,
            "startup step complete",
            step="bot.run",
            hostname=os.uname().nodename,
        )
    except discord.LoginFailure:
        logging.error("discord login failed: invalid DISCORD_TOKEN | %s", _startup_context(step="bot.run"))
        return
    except discord.HTTPException as exc:
        log_discord_http_exception(
            "discord runtime failed during login/start",
            exc,
            stage="run_discord_main.login",
            restart_required=True,
            **_startup_context(step="bot.run"),
        )
        raise
    except Exception as exc:
        error_text = str(exc)
        if "Session is closed" in error_text:
            _reset_discord_http_client_state("run_discord_main.session_closed")
            logging.exception(
                "discord runtime failed because HTTP session was closed; stopping without auto-retry. "
                "Manual dashboard restart is required | %s",
                _startup_context(step="bot.run"),
            )
            raise
        if is_transient_rate_limit_error(exc):
            _reset_discord_http_client_state("run_discord_main.rate_limited")
            logging.exception(
                "discord runtime hit transient/rate-limit error during startup; stopping without auto-retry. "
                "Manual dashboard restart is required | %s",
                _startup_context(step="bot.run"),
            )
            raise
        logging.exception(
            "discord runtime failed during startup; runtime stopped without auto-retry. "
            "Для нового запуска нужен полный рестарт дешборда | %s",
            _startup_context(step="bot.run"),
        )
        raise

async def _run_both_async(discord_token: str, telegram_token: str) -> None:
    await init_shared_http_session()

    async def _run_discord_once() -> None:
        global startup_token_hash
        startup_token_hash = _token_fingerprint(discord_token)
        try:
            _log_startup_step(
                logging.INFO,
                "startup step begin",
                step="bot.start",
                hostname=os.uname().nodename,
            )
            await bot.start(discord_token)
            _log_startup_step(
                logging.INFO,
                "startup step complete",
                step="bot.start",
                hostname=os.uname().nodename,
            )
            logging.error(
                "discord runtime stopped unexpectedly while telegram remains active; stopping Discord without auto-retry. "
                "A full dashboard restart is required to start Discord again"
            )
            raise RuntimeError("discord runtime stopped unexpectedly while telegram remains active")
        except discord.LoginFailure:
            logging.exception(
                "discord login failure; discord runtime stopped while telegram remains active."
            )
            raise
        except asyncio.CancelledError:
            logging.info("discord runtime cancelled")
            raise
        except discord.HTTPException as exc:
            log_discord_http_exception(
                "discord runtime failed; discord runtime stopped while telegram remains active",
                exc,
                stage="run_both.discord",
                restart_required=True,
                **_startup_context(step="bot.start"),
            )
            raise
        except discord.DiscordServerError:
            logging.exception(
                "discord runtime server error; discord runtime stopped while telegram remains active. "
                "A full dashboard restart is required"
            )
            raise
        except Exception as exc:
            error_text = str(exc)
            if "Session is closed" in error_text:
                _reset_discord_http_client_state("run_both.discord.session_closed")
                logging.exception(
                    "discord runtime failed because HTTP session was closed; "
                    "discord runtime stopped while telegram remains active. "
                    "A full dashboard restart is required | %s",
                    _startup_context(step="bot.start"),
                )
                raise
            if is_transient_rate_limit_error(exc):
                _reset_discord_http_client_state("run_both.discord.rate_limited")
                logging.exception(
                    "discord runtime hit transient/rate-limit error; "
                    "discord runtime stopped while telegram remains active. "
                    "A full dashboard restart is required | %s",
                    _startup_context(step="bot.start"),
                )
                raise
            logging.exception(
                "discord runtime fatal error; discord runtime stopped while telegram remains active. "
                "A full dashboard restart is required | %s",
                _startup_context(step="bot.start"),
            )
            raise
        finally:
            with contextlib.suppress(Exception):
                await bot.close()

    async def _run_telegram_with_retries() -> None:
        global telegram_runtime_started
        async with telegram_runtime_guard:
            if telegram_runtime_started:
                logging.warning(
                    "telegram runtime duplicate startup detected; another in-process telegram loop is already active"
                )
                return
            telegram_runtime_started = True

        try:
            logging.info(
                "telegram runtime started token_hash=%s bounded_internal_retries=%s",
                _token_fingerprint(telegram_token),
                max(1, int(os.getenv("TELEGRAM_POLLING_MAX_TRANSIENT_FAILURES", "3"))) - 1,
            )
            await run_telegram_polling(telegram_token)
            logging.error(
                "telegram runtime stopped unexpectedly without exception; "
                "fail-fast shutdown so external supervisor can decide restart policy"
            )
            raise RuntimeError(
                "telegram runtime stopped unexpectedly without exception; process shutdown required"
            )
        except TelegramPollingAlreadyRunningInProcessError as exc:
            logging.warning(
                "telegram runtime duplicate startup detected; no restart. details=%s",
                exc,
            )
            return
        except TelegramPollingLockActiveError as exc:
            logging.error(
                "telegram runtime duplicate startup detected; another process already owns the polling lock. "
                "Fail-fast shutdown required. details=%s",
                exc,
            )
            raise
        except TelegramPollingPreflightConflictError as exc:
            logging.error(
                "telegram runtime duplicate startup detected during preflight getUpdates; "
                "another consumer is active. Fail-fast shutdown required. details=%s",
                exc,
            )
            raise
        except TelegramPollingConflictDetectedError as exc:
            logging.error(
                "telegram runtime duplicate startup detected during active polling; "
                "lost getUpdates ownership. Fail-fast shutdown required. details=%s",
                exc,
            )
            raise
        except TelegramPollingTransientNetworkError as exc:
            logging.exception(
                "telegram runtime exhausted bounded transient polling retries; shutting down process. details=%s",
                exc,
            )
            raise
        except asyncio.CancelledError:
            logging.info("telegram runtime cancelled")
            raise
        except Exception as exc:
            logging.exception(
                "telegram runtime fatal failure; shutting down process transient_network_error=%s error_type=%s",
                _is_transient_telegram_network_error(exc),
                type(exc).__name__,
            )
            raise
        finally:
            async with telegram_runtime_guard:
                telegram_runtime_started = False

    discord_task = asyncio.create_task(_run_discord_once(), name="discord-runtime")
    telegram_task = asyncio.create_task(_run_telegram_with_retries(), name="telegram-runtime")

    pending = {discord_task, telegram_task}
    runtime_errors: dict[str, BaseException] = {}

    try:
        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

            for task in done:
                task_name = task.get_name()

                try:
                    exc = task.exception()
                except asyncio.CancelledError:
                    logging.info("%s cancelled in runtime supervisor", task_name)
                    continue

                if exc is None:
                    logging.warning(
                        "%s stopped; remaining_runtimes=%s",
                        task_name,
                        ", ".join(sorted(other.get_name() for other in pending)) or "none",
                    )
                    continue

                runtime_errors[task_name] = exc
                remaining = ", ".join(sorted(other.get_name() for other in pending)) or "none"

                if task_name == "discord-runtime":
                    logging.error(
                        "discord runtime stopped while telegram remains active. "
                        "Full dashboard restart is required to start Discord again. remaining_runtimes=%s error=%s",
                        remaining,
                        exc,
                    )
                    continue

                logging.error(
                    "%s stopped; remaining_runtimes=%s error=%s",
                    task_name,
                    remaining,
                    exc,
                )

        if runtime_errors and "telegram-runtime" in runtime_errors:
            raise runtime_errors["telegram-runtime"]
    finally:
        await close_shared_http_session()


def run_both_main(discord_token: str, telegram_token: str) -> None:
    asyncio.run(_run_both_async(discord_token, telegram_token))


def main() -> None:
    """Launcher for Discord/Telegram runtimes based on available tokens."""

    load_dotenv()
    configure_logging()

    discord_token = (os.getenv('DISCORD_TOKEN') or '').strip()
    telegram_token = get_telegram_bot_token()

    logging.info(
        "startup token detection: discord_token_present=%s telegram_token_present=%s",
        bool(discord_token),
        bool(telegram_token),
    )

    if discord_token and telegram_token:
        run_both_main(discord_token, telegram_token)
        return
    if discord_token:
        run_discord_main(discord_token)
        return
    if telegram_token:
        run_telegram_main(telegram_token)
        return

    logging.error(
        "No bot tokens configured. Expected environment variables: DISCORD_TOKEN and/or %s",
        TELEGRAM_BOT_TOKEN_ENV,
    )


if __name__ == "__main__":
    main()
