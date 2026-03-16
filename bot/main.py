# Core imports
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Системные импорты
import asyncio
import logging
import time
import random
import json
import contextlib
import re
from dotenv import load_dotenv

from bot.telegram_bot.config import get_telegram_bot_token
from bot.services.ai_service import generate_guiy_reply


def _resolve_runtime() -> str:
    """Resolve runtime mode for unified launcher.

    Runtime mode must be controlled only via BOT_RUNTIME.
    Bot tokens are treated purely as credentials for the selected runtime
    and never as implicit launch switches.
    """

    explicit = (os.getenv("BOT_RUNTIME") or "").strip().lower()
    if explicit:
        if explicit in {"telegram", "tg"}:
            return "telegram"
        if explicit in {"both", "all"}:
            return "both"
        return "discord"

    return "discord"


# Unified runtime bootstrap: allow Telegram mode without importing Discord subsystems.
load_dotenv()
_RUNTIME = _resolve_runtime()
if _RUNTIME == "telegram":
    from bot.telegram_bot.main import main as run_telegram_main

    run_telegram_main()
    raise SystemExit(0)

# Основные импорты Discord (below runtime bootstrap on purpose)
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
    TelegramPollingAlreadyRunningInProcessError,
    TelegramPollingLockActiveError,
    run_polling as run_telegram_polling,
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

    global tasks_started, startup_tasks_started, commands_synced
    if not tasks_started:
        tasks_started = True


        asyncio.create_task(fines_logic.check_overdue_fines(bot))
        asyncio.create_task(fines_logic.debt_repayment_loop(bot))
        asyncio.create_task(fines_logic.reminder_loop(bot))
        asyncio.create_task(fines_logic.fines_summary_loop(bot))
        from bot.systems.tournament_logic import tournament_reminder_loop, registration_deadline_loop
        asyncio.create_task(tournament_reminder_loop(bot))
        asyncio.create_task(registration_deadline_loop(bot))
        asyncio.create_task(profile_titles_sync_loop(bot))
        asyncio.create_task(external_roles_sync_loop(bot))

    activity = discord.Activity(
        name="Привет! Напиши команду /helpy чтобы увидеть все команды 🧠",
        type=discord.ActivityType.listening
    )
    await bot.change_presence(activity=activity)

    if not commands_synced:
        try:
            if should_sync_commands():
                await bot.tree.sync()
                mark_commands_synced()
                print("🔁 Slash-команды синхронизированы")
            else:
                print("⏭️ Синхронизация slash-команд пропущена (слишком рано после прошлого запуска)")
            commands_synced = True
        except Exception as e:
            print(f"❌ Ошибка синхронизации команд: {e}")
    
    active_tournaments = tournament_db.get_active_tournaments()
    for tour in active_tournaments:
        # Проверяем наличие нужных полей
        if not all(key in tour for key in ["id", "size", "type", "announcement_message_id"]):
            continue

        try:
            # Регистрируем кнопку ставок, чтобы она работала после перезапуска
            bet_view = BettingView(tour["id"])
            bot.add_view(bet_view, message_id=tour["announcement_message_id"])

            # если есть отдельное сообщение со статусом — добавляем кнопку и туда
            status_msg_id = tournament_db.get_status_message_id(tour["id"])
            if status_msg_id:
                bot.add_view(BettingView(tour["id"]), message_id=status_msg_id)
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
            bot.add_view(round_management_view)

    # Не дублируем фоновые задачи при повторном on_ready (reconnect)
    if not startup_tasks_started:
        startup_tasks_started = True
        asyncio.create_task(autosave_task())
        asyncio.create_task(monthly_top_task())

    print('--- Ленивый режим загрузки данных активирован ---')
    print("📡 Задачи активированы.")


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return

    try:
        ctx = await bot.get_context(message)
        if getattr(ctx, "valid", False):
            await bot.process_commands(message)
            return

        content = (message.content or "").strip()

        is_reply_to_bot = False
        if message.reference and message.reference.message_id:
            ref_msg = message.reference.resolved
            if ref_msg is None and message.channel:
                try:
                    ref_msg = await message.channel.fetch_message(message.reference.message_id)
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
                    async with message.channel.typing():
                        await asyncio.sleep(typing_delay)
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
                        await message.reply(reply, mention_author=False)
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

                channel = bot.get_channel(TOP_CHANNEL_ID)
                if isinstance(channel, discord.TextChannel):
                    msg = await safe_send(
                        channel,
                        "🔁 Запускаем автоматический топ месяца...",
                    )
                    ctx = await bot.get_context(msg or channel.last_message)

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
                    print("❌ Указанный канал недоступен или не текстовый")

            except Exception as e:
                print(f"❌ Ошибка автозапуска топа месяца: {e}")

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

def run_discord_main():
    global bot
    load_dotenv()
    configure_logging()

    keep_alive()
    TOKEN = (os.getenv('DISCORD_TOKEN') or '').strip()

    if not TOKEN:
        print("❌ Переменная DISCORD_TOKEN не задана.")
        return

    max_retry_delay = float(os.getenv("STARTUP_MAX_RETRY_DELAY", "300"))
    next_retry_at, retry_delay = load_startup_retry_state()
    retry_delay = max(1.0, min(retry_delay, max_retry_delay))



    def normalize_retry_after(parsed_retry: float) -> float:
        """Normalize retry-after to seconds and clamp to configured bounds."""
        # Платформы/прокси иногда возвращают Retry-After в миллисекундах.
        if parsed_retry > max_retry_delay and parsed_retry / 1000 <= max_retry_delay:
            parsed_retry /= 1000
        return max(1.0, min(parsed_retry, max_retry_delay))

    def get_retry_after(exc: discord.HTTPException, default: float) -> float:
        retry_after_attr = getattr(exc, "retry_after", None)
        if isinstance(retry_after_attr, (int, float)):
            return normalize_retry_after(float(retry_after_attr))

        response = getattr(exc, 'response', None)
        if response is not None:
            retry = response.headers.get('Retry-After') or response.headers.get('retry-after')
            if retry:
                retry = retry.strip()
                try:
                    return normalize_retry_after(float(retry))
                except ValueError:
                    pass

        match = re.search(r"retry(?:_|-|\s)after[:]?\s*(\d+(?:\.\d+)?)\s*(ms|milliseconds?|s|sec|seconds?)?", exc.text or "", re.I)
        if match:
            parsed_retry = float(match.group(1))
            unit = (match.group(2) or "").lower()
            if unit.startswith("ms"):
                parsed_retry /= 1000
            return normalize_retry_after(parsed_retry)

        return normalize_retry_after(default)

    def wait_before_retry(base_delay: float) -> float:
        """Sleep before reconnecting and return the next backoff value.

        A small jitter helps avoid synchronized reconnect storms when hosting
        platforms restart multiple workers around the same moment.
        """

        delay = max(1.0, min(base_delay, max_retry_delay))
        next_delay = min(delay * 2, max_retry_delay)
        jittered = delay + random.uniform(0.0, min(5.0, delay * 0.1))
        nonlocal next_retry_at
        next_retry_at = time.time() + jittered
        save_startup_retry_state(next_retry_at, next_delay)
        time.sleep(jittered)
        next_retry_at = 0.0
        save_startup_retry_state(next_retry_at, next_delay)
        return next_delay

    def sleep_if_cooldown_active() -> None:
        nonlocal next_retry_at
        if next_retry_at <= 0:
            return
        remaining = next_retry_at - time.time()
        if remaining <= 0:
            next_retry_at = 0.0
            save_startup_retry_state(next_retry_at, retry_delay)
            return
        remaining = min(remaining, max_retry_delay)
        jittered = remaining + random.uniform(0.0, min(5.0, remaining * 0.1))
        time.sleep(jittered)
        next_retry_at = 0.0
        save_startup_retry_state(next_retry_at, retry_delay)

    while True:
        try:
            sleep_if_cooldown_active()
            bot.run(TOKEN)
            save_startup_retry_state(0.0, 60.0)
            break
        except discord.LoginFailure:
            print("❌ Неверный токен DISCORD_TOKEN. Проверьте переменную окружения на Render.")
            break
        except discord.HTTPException as e:
            if e.status == 429:
                retry_after = get_retry_after(e, retry_delay)
                # Для входа лучше опираться на Retry-After от Discord,
                # чтобы не раздувать задержку экспоненциально между перезапусками.
                retry_delay = wait_before_retry(retry_after)
                continue

            raise
        except Exception as e:
            error_text = str(e)
            if "Session is closed" in error_text:
                retry_delay = wait_before_retry(retry_delay)
                continue
            if "429" in error_text or "rate limit" in error_text.lower():
                retry_delay = wait_before_retry(retry_delay)
                continue
            print("❌ Ошибка при запуске бота:", e)
            import traceback
            traceback.print_exc()
            break

async def _run_both_async(discord_token: str, telegram_token: str) -> None:
    async def _run_discord_with_retries() -> None:
        retry_delay = 5.0
        max_retry_delay = float(os.getenv("BOTH_RUNTIME_MAX_RETRY_DELAY", "300"))

        while True:
            try:
                logging.info("discord runtime starting (both mode)")
                await bot.start(discord_token)
                logging.warning("discord runtime stopped unexpectedly; restarting")
            except discord.LoginFailure:
                logging.exception("discord login failure in both mode; stopping both runtimes")
                raise
            except asyncio.CancelledError:
                logging.info("discord runtime cancelled")
                raise
            except discord.HTTPException as exc:
                if exc.status in {429, 500, 502, 503, 504}:
                    logging.exception(
                        "discord runtime transient HTTP error in both mode (status=%s); retry in %.1fs",
                        exc.status,
                        retry_delay,
                    )
                else:
                    logging.exception("discord runtime unrecoverable HTTP error in both mode")
                    raise
            except discord.DiscordServerError:
                logging.exception(
                    "discord runtime server error in both mode; retry in %.1fs",
                    retry_delay,
                )
            except Exception as exc:
                error_text = str(exc)
                if "Session is closed" in error_text or "429" in error_text or "rate limit" in error_text.lower():
                    logging.exception("discord runtime transient error in both mode; retry in %.1fs", retry_delay)
                else:
                    logging.exception("discord runtime fatal error in both mode")
                    raise

            with contextlib.suppress(Exception):
                await bot.close()

            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, max_retry_delay)

    async def _run_telegram_with_retries() -> None:
        global telegram_runtime_started
        retry_delay = 5.0
        max_retry_delay = float(os.getenv("BOTH_RUNTIME_MAX_RETRY_DELAY", "300"))

        while True:
            async with telegram_runtime_guard:
                if telegram_runtime_started:
                    logging.warning(
                        "telegram runtime duplicate startup prevented in both mode; "
                        "another in-process telegram loop is already active"
                    )
                    return
                telegram_runtime_started = True

            try:
                logging.info("telegram runtime starting (both mode)")
                await run_telegram_polling(telegram_token)
                logging.warning("telegram runtime stopped; restarting")
            except TelegramPollingAlreadyRunningInProcessError as exc:
                logging.warning(
                    "telegram runtime duplicate startup detected in current process (both mode); "
                    "assuming another in-process telegram loop is active and stopping duplicate loop. details=%s",
                    exc,
                )
                return
            except TelegramPollingLockActiveError as exc:
                logging.error(
                    "telegram runtime duplicate process detected in both mode; another process is already polling "
                    "with same token. details=%s Retry in %.1fs",
                    exc,
                    retry_delay,
                )
            except asyncio.CancelledError:
                logging.info("telegram runtime cancelled")
                raise
            except Exception:
                logging.exception(
                    "telegram runtime error in both mode; retry in %.1fs",
                    retry_delay,
                )
            finally:
                async with telegram_runtime_guard:
                    telegram_runtime_started = False

            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, max_retry_delay)

    discord_task = asyncio.create_task(_run_discord_with_retries(), name="discord-runtime")
    telegram_task = asyncio.create_task(_run_telegram_with_retries(), name="telegram-runtime")

    done, pending = await asyncio.wait(
        {discord_task, telegram_task},
        return_when=asyncio.FIRST_EXCEPTION,
    )

    first_exc = None
    for task in done:
        exc = task.exception()
        if exc is not None:
            first_exc = exc
            break

    for task in pending:
        task.cancel()

    if pending:
        await asyncio.gather(*pending, return_exceptions=True)

    if first_exc is not None:
        raise first_exc


def run_both_main() -> None:
    load_dotenv()
    configure_logging()
    keep_alive()

    discord_token = (os.getenv('DISCORD_TOKEN') or '').strip()
    telegram_token = get_telegram_bot_token()
    if not discord_token or not telegram_token:
        print("❌ Для BOT_RUNTIME=both нужны DISCORD_TOKEN и TELEGRAM_BOT_TOKEN.")
        return

    asyncio.run(_run_both_async(discord_token, telegram_token))


def main():
    """Launcher for discord/both modes (telegram-only handled at import bootstrap)."""

    if _RUNTIME == "both":
        run_both_main()
        return

    run_discord_main()


if __name__ == "__main__":
    main()
