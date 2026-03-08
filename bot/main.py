# Core imports
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# Основные импорты Discord
import discord

# Системные импорты
import asyncio
import logging
import time
import re
import random
import json
from dotenv import load_dotenv
import pytz
from bot.commands import bot as command_bot
# Локальные импорты
from bot.data import db
from keep_alive import keep_alive
import bot.commands.tournament
import bot.commands.players
import bot.commands.maps
from datetime import datetime
import importlib
from bot.systems import fines_logic
import bot.commands.fines
import bot.data.tournament_db as tournament_db
from bot.systems.tournament_logic import BettingView
from bot.systems.interactive_rounds import RoundManagementView
from bot.systems.tournament_logic import create_tournament_logic
from bot.utils import safe_send

# Константы
TIME_FORMAT = "%H:%M (%d.%m.%Y)"
TOP_CHANNEL_ID = int(os.getenv("MONTHLY_TOP_CHANNEL_ID", 0))

# Таймеры удаления сообщений
active_timers = {}

# Prevent duplicate background tasks if on_ready fires multiple times
tasks_started = False
startup_tasks_started = False
commands_synced = False

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

def reload_bot():
    module = importlib.import_module('bot.commands')
    module = importlib.reload(module)
    return module.bot

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

        db.load_data()

        asyncio.create_task(fines_logic.check_overdue_fines(bot))
        asyncio.create_task(fines_logic.debt_repayment_loop(bot))
        asyncio.create_task(fines_logic.reminder_loop(bot))
        asyncio.create_task(fines_logic.fines_summary_loop(bot))
        from bot.systems.tournament_logic import tournament_reminder_loop, registration_deadline_loop
        asyncio.create_task(tournament_reminder_loop(bot))
        asyncio.create_task(registration_deadline_loop(bot))

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

    print('--- Данные успешно загружены ---')
    print(f'Пользователей: {len(db.scores)}')
    print(f'Историй действий: {sum(len(v) for v in db.history.values())}')
    print("📡 Задачи активированы.")

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


def load_next_startup_retry_at() -> float:
    """Load persisted cooldown timestamp for startup retries."""
    try:
        with open(STARTUP_RETRY_STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
        return float(state.get("next_retry_at", 0))
    except (FileNotFoundError, ValueError, OSError, TypeError):
        return 0.0


def save_next_startup_retry_at(next_retry_at: float) -> None:
    try:
        with open(STARTUP_RETRY_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump({"next_retry_at": next_retry_at}, f)
    except OSError as e:
        logging.warning("Не удалось записать состояние повторного запуска: %s", e)

# Основной запуск
def main():
    global bot
    load_dotenv()
    keep_alive()
    TOKEN = (os.getenv('DISCORD_TOKEN') or '').strip()

    if not TOKEN:
        print("❌ Переменная DISCORD_TOKEN не задана.")
        return

    retry_delay = 60.0  # seconds
    max_retry_delay = 3600.0
    next_retry_at = load_next_startup_retry_at()

    def get_retry_after(exc: discord.HTTPException, default: float) -> float:
        headers = getattr(exc, 'response', None)
        if headers is not None:
            retry = headers.headers.get('Retry-After') or headers.headers.get('retry-after')
            if retry:
                try:
                    return float(retry)
                except ValueError:
                    pass
        match = re.search(r"retry(?:_|-|\s)after[:]?\s*(\d+(?:\.\d+)?)", exc.text or "", re.I)
        if match:
            try:
                return float(match.group(1))
            except ValueError:
                pass
        return default

    def wait_before_retry(base_delay: float) -> float:
        """Sleep before reconnecting and return the next backoff value.

        A small jitter helps avoid synchronized reconnect storms when hosting
        platforms restart multiple workers around the same moment.
        """

        delay = max(1.0, min(base_delay, max_retry_delay))
        jittered = delay + random.uniform(0.0, min(5.0, delay * 0.1))
        nonlocal next_retry_at
        logging.warning("Retrying bot startup in %.1f seconds", jittered)
        next_retry_at = time.time() + jittered
        save_next_startup_retry_at(next_retry_at)
        time.sleep(jittered)
        next_retry_at = 0.0
        save_next_startup_retry_at(next_retry_at)
        return min(delay * 2, max_retry_delay)

    def sleep_if_cooldown_active() -> None:
        nonlocal next_retry_at
        if next_retry_at <= 0:
            return
        remaining = next_retry_at - time.time()
        if remaining <= 0:
            next_retry_at = 0.0
            save_next_startup_retry_at(next_retry_at)
            return
        jittered = remaining + random.uniform(0.0, min(5.0, remaining * 0.1))
        logging.warning(
            "Startup cooldown active, waiting %.1f seconds before Discord login",
            jittered,
        )
        time.sleep(jittered)
        next_retry_at = 0.0
        save_next_startup_retry_at(next_retry_at)

    while True:
        try:
            sleep_if_cooldown_active()
            bot.run(TOKEN)
            break
        except discord.LoginFailure:
            print("❌ Неверный токен DISCORD_TOKEN. Проверьте переменную окружения на Render.")
            break
        except discord.HTTPException as e:
            if e.status == 429:
                retry_delay = get_retry_after(e, retry_delay)
                logging.warning(
                    "Login rate limited, retrying in %s seconds", retry_delay
                )
                retry_delay = wait_before_retry(retry_delay)
                bot = reload_bot()
                db.bot = bot
                bot.event(on_ready)
                continue
            raise
        except Exception as e:
            if "Session is closed" in str(e):
                logging.warning(
                    "Session closed, retrying in %s seconds", retry_delay
                )
                retry_delay = wait_before_retry(retry_delay)
                bot = reload_bot()
                db.bot = bot
                bot.event(on_ready)
                continue
            print("❌ Ошибка при запуске бота:", e)
            import traceback
            traceback.print_exc()
            break

if __name__ == "__main__":
    main()
