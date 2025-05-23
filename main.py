# Основные импорты Discord
import discord

# Системные импорты
import os
import asyncio
from dotenv import load_dotenv

# Локальные импорты
from data import db
from keep_alive import keep_alive
from commands import bot as command_bot

# Константы
COMMAND_PREFIX = '?'
TIME_FORMAT = "%H:%M (%d.%m.%Y)"

# Таймеры удаления сообщений
active_timers = {}

bot = command_bot

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
