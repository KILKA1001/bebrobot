import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# Основные импорты Discord
import discord

# Системные импорты
import os
import asyncio
from dotenv import load_dotenv
import pytz

# Локальные импорты
from bot.data import db
from keep_alive import keep_alive
from bot.commands import bot as command_bot
from bot.commands import run_monthly_top
from datetime import datetime
from bot.systems import fines_logic
from bot.systems.fines_logic import check_overdue_fines, debt_repayment_loop
from bot.systems.fines_logic import get_fine_leaders
from bot.systems.fines_logic import build_fine_embed
from bot.systems.fines_logic import fines_summary_loop
# Константы
COMMAND_PREFIX = '?'
TIME_FORMAT = "%H:%M (%d.%m.%Y)"
TOP_CHANNEL_ID = int(os.getenv("MONTHLY_TOP_CHANNEL_ID", 0))

# Таймеры удаления сообщений
active_timers = {}

bot = command_bot
db.bot = bot

async def send_greetings(channel, user_list):
    for user_id in user_list:
        await channel.send(f"Привет, <@{user_id}>!")
        await asyncio.sleep(1)

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

    db.load_data()
    
    asyncio.create_task(fines_logic.check_overdue_fines(bot))
    asyncio.create_task(fines_logic.debt_repayment_loop(bot))
    asyncio.create_task(fines_logic.reminder_loop(bot))
    asyncio.create_task(fines_logic.fines_summary_loop(bot))

    activity = discord.Activity(
        name=f"{COMMAND_PREFIX}help",
        type=discord.ActivityType.listening
    )
    await bot.change_presence(activity=activity)

    # 👇 тут будет работать, потому что определена выше
    asyncio.create_task(autosave_task())

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
                    msg = await channel.send("🔁 Запускаем автоматический топ месяца...")
                    ctx = await bot.get_context(msg)

                    from bot.systems.core_logic import run_monthly_top
                    await run_monthly_top(ctx)

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

# Основной запуск
def main():
    load_dotenv()
    keep_alive()
    TOKEN = os.getenv('DISCORD_TOKEN')

    if not TOKEN:
        print("❌ Переменная DISCORD_TOKEN не задана.")
        return

    try:
        bot.run(TOKEN)
    except Exception as e:
        print("❌ Ошибка при запуске бота:", e)
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
