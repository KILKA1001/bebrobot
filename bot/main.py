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
from dotenv import load_dotenv

from bot.telegram_bot.config import TELEGRAM_BOT_TOKEN_ENV, get_telegram_bot_token
from bot.services.ai_service import generate_guiy_reply


load_dotenv()


# Основные импорты Discord
import discord
import pytz
from bot.commands import bot as command_bot
# Локальные импорты
from bot.data import db
from keep_alive import keep_alive
import bot.commands.tournament
import bot.commands.maps
from datetime import datetime
from bot.systems import fines_logic
from bot.systems.profile_titles_logic import profile_titles_sync_loop
from bot.systems.external_roles_sync_logic import external_roles_sync_loop
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
    run_polling as run_telegram_polling,
)
from bot.utils.discord_http import (
    extract_retry_after_seconds,
    is_cloudflare_rate_limited_http_exception,
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
telegram_runtime_started = False
telegram_runtime_guard = asyncio.Lock()

COMMAND_SYNC_STATE_FILE = os.getenv(
    "COMMAND_SYNC_STATE_FILE",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "command_sync_state.json"),
)
COMMAND_SYNC_MIN_INTERVAL = int(os.getenv("COMMAND_SYNC_MIN_INTERVAL", "21600"))
STARTUP_RETRY_STATE_FILE = os.getenv(
    "STARTUP_RETRY_STATE_FILE",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "startup_retry_state.json"),
)

bot = command_bot
db.bot = bot

startup_run_id = uuid.uuid4().hex[:12]
startup_token_hash = "unknown"


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
    while not bot.is_closed():
        db.save_all()
        print("Данные сохранены автоматически.")
        await asyncio.sleep(300)

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
    
    active_tournaments = tournament_db.get_active_tournaments()
    for tour in active_tournaments:
        # Проверяем наличие нужных полей
        if not all(key in tour for key in ["id", "size", "type", "announcement_message_id"]):
            continue

        try:
            # Регистрируем кнопку ставок, чтобы она работала после перезапуска
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

            # если есть отдельное сообщение со статусом — добавляем кнопку и туда
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
        except Exception as e:
            print(f"Ошибка при регистрации кнопок турнира {tour.get('id')}: {e}")

        # Регистрация RoundManagementView
        participants_data = tournament_db.list_participants(tour["id"])
        participants = []
        for p in participants_data:
            if "discord_user_id" in p and p["discord_user_id"]:
                participants.append(p["discord_user_id"])
            elif "player_id" in p and p["player_id"]:
                participants.append(p["player_id"])
                
        if participants:
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

        if is_named or is_reply_to_bot:
            logging.info(
                "discord ai trigger matched guild_id=%s channel_id=%s author_id=%s is_named=%s is_reply_to_bot=%s text=%s",
                getattr(message.guild, "id", None),
                getattr(message.channel, "id", None),
                getattr(message.author, "id", None),
                is_named,
                is_reply_to_bot,
                content[:160],
            )
            reply = await generate_guiy_reply(
                content,
                provider="discord",
                user_id=getattr(message.author, "id", None),
                conversation_id=getattr(message.channel, "id", None),
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

                    # 🔥 Штрафной антибонус для топ-должников
                    from bot.systems.fines_logic import get_fine_leaders
                    top_fines = get_fine_leaders()
                    punishments = [0.01, 0.03, 0.05]

                    for (uid, total), percent in zip(top_fines, punishments):
                        penalty = round(total * percent, 2)
                        db.update_scores(uid, -penalty)
                        db.add_action(
                            user_id=uid,
                            points=-penalty,
                            reason=f"Антибонус за топ штрафников ({int(percent * 100)}%)",
                            author_id=0
                        )

                    db.log_monthly_fine_top(list(zip(top_fines, punishments)))
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


def load_startup_retry_state() -> tuple[float, float]:
    """Load persisted cooldown timestamp and retry delay for startup retries."""
    try:
        with open(STARTUP_RETRY_STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
        return float(state.get("next_retry_at", 0)), float(state.get("retry_delay", 60.0))
    except (FileNotFoundError, ValueError, OSError, TypeError):
        return 0.0, 60.0


def save_startup_retry_state(next_retry_at: float, retry_delay: float) -> None:
    try:
        with open(STARTUP_RETRY_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump({"next_retry_at": next_retry_at, "retry_delay": retry_delay}, f)
    except OSError as e:
        logging.warning("Не удалось записать состояние повторного запуска: %s", e)



def load_next_startup_retry_at() -> float:
    """Backward-compatible shim for older startup code paths."""
    next_retry_at, _ = load_startup_retry_state()
    return next_retry_at


def save_next_startup_retry_at(next_retry_at: float) -> None:
    """Backward-compatible shim for older startup code paths."""
    _, retry_delay = load_startup_retry_state()
    save_startup_retry_state(next_retry_at, retry_delay)
# Основной запуск

def run_telegram_main(token: str) -> None:
    try:
        asyncio.run(run_telegram_polling(token))
    except TelegramPollingAlreadyRunningInProcessError as exc:
        logging.warning("telegram runtime duplicate startup detected; details=%s", exc)
        return
    except Exception:
        logging.exception("telegram polling failed")
        raise


def run_discord_main(token: str) -> None:
    global bot, startup_token_hash

    startup_token_hash = _token_fingerprint(token)

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
        crash_retry_delay = 5.0
        conflict_retry_delay = 15.0
        max_retry_delay = float(os.getenv("BOTH_RUNTIME_MAX_RETRY_DELAY", "300"))
        max_conflict_retry_delay = float(os.getenv("BOTH_RUNTIME_MAX_CONFLICT_RETRY_DELAY", "600"))

        while True:
            async with telegram_runtime_guard:
                if telegram_runtime_started:
                    logging.warning(
                        "telegram runtime duplicate startup detected; another in-process telegram loop is already active"
                    )
                    return
                telegram_runtime_started = True

            try:
                logging.info("telegram runtime started token_hash=%s", _token_fingerprint(telegram_token))
                await run_telegram_polling(telegram_token)
                logging.warning(
                    "telegram runtime stopped without exception; "
                    "treating as graceful stop and restarting in %.1fs",
                    crash_retry_delay,
                )
            except TelegramPollingAlreadyRunningInProcessError as exc:
                logging.warning(
                    "telegram runtime duplicate startup detected; no restart. details=%s",
                    exc,
                )
                return
            except TelegramPollingLockActiveError as exc:
                logging.error(
                    "telegram runtime duplicate startup detected; another process already owns the polling lock. details=%s retry_in=%.1fs",
                    exc,
                    conflict_retry_delay,
                )
                await asyncio.sleep(conflict_retry_delay)
                conflict_retry_delay = min(conflict_retry_delay * 2, max_conflict_retry_delay)
                continue
            except TelegramPollingPreflightConflictError as exc:
                logging.warning(
                    "telegram runtime duplicate startup detected during preflight getUpdates; "
                    "another consumer is active. details=%s retry_in=%.1fs",
                    exc,
                    conflict_retry_delay,
                )
                await asyncio.sleep(conflict_retry_delay)
                conflict_retry_delay = min(conflict_retry_delay * 2, max_conflict_retry_delay)
                continue
            except TelegramPollingConflictDetectedError as exc:
                logging.error(
                    "telegram runtime duplicate startup detected during active polling; "
                    "lost getUpdates ownership. details=%s retry_in=%.1fs",
                    exc,
                    conflict_retry_delay,
                )
                await asyncio.sleep(conflict_retry_delay)
                conflict_retry_delay = min(conflict_retry_delay * 2, max_conflict_retry_delay)
                continue
            except asyncio.CancelledError:
                logging.info("telegram runtime cancelled")
                raise
            except Exception:
                logging.exception(
                    "telegram runtime crashed; retry_in=%.1fs",
                    crash_retry_delay,
                )
                await asyncio.sleep(crash_retry_delay)
                crash_retry_delay = min(crash_retry_delay * 2, max_retry_delay)
                continue
            finally:
                async with telegram_runtime_guard:
                    telegram_runtime_started = False

            await asyncio.sleep(crash_retry_delay)
            crash_retry_delay = min(crash_retry_delay * 2, max_retry_delay)

    discord_task = asyncio.create_task(_run_discord_once(), name="discord-runtime")
    telegram_task = asyncio.create_task(_run_telegram_with_retries(), name="telegram-runtime")

    pending = {discord_task, telegram_task}
    runtime_errors: dict[str, BaseException] = {}

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


def run_both_main(discord_token: str, telegram_token: str) -> None:
    asyncio.run(_run_both_async(discord_token, telegram_token))


def main() -> None:
    """Launcher for Discord/Telegram runtimes based on available tokens."""

    load_dotenv()
    configure_logging()
    keep_alive()

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
