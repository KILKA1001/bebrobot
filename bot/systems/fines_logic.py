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
from collections import defaultdict
from discord import ui

# Локальные импорты
from bot.data import db
from bot.commands import bot as command_bot
from bot.commands import run_monthly_top
from datetime import datetime, timedelta, timezone


# Константы
COMMAND_PREFIX = '?'
TIME_FORMAT = "%H:%M (%d.%m.%Y)"
TOP_CHANNEL_ID = int(os.getenv("MONTHLY_TOP_CHANNEL_ID", 0))

# Таймеры удаления сообщений
active_timers = {}

bot = command_bot
db.bot = bot

def build_fine_embed(fine: dict) -> discord.Embed:
    embed = discord.Embed(
        title=f"📌 Штраф #{fine['id']}",
        description=fine.get("reason", "Без причины"),
        color=discord.Color.red()
    )
    embed.add_field(name="Сумма", value=f"{fine['amount']:.2f} баллов", inline=True)
    paid = fine.get("paid_amount", 0.0)
    embed.add_field(name="Оплачено", value=f"{paid:.2f} / {fine['amount']:.2f}", inline=True)
    status = "✅ Оплачен" if fine.get("is_paid") else "⏳ В ожидании"
    embed.add_field(name="Статус", value=status, inline=False)
    return embed

def build_fine_detail_embed(fine: dict) -> discord.Embed:
    embed = discord.Embed(
        title=f"📋 Детали штрафа #{fine['id']}",
        description=fine.get("reason", "Без причины"),
        color=discord.Color.red()
    )
    # Основная информация
    embed.add_field(name="💰 Сумма", value=f"{fine['amount']:.2f} баллов", inline=True)
    fine_type = fine.get("type", 1)
    type_text = "Обычный (14 дней)" if fine_type == 1 else "Усиленный (30 дней)"
    embed.add_field(name="📝 Тип", value=type_text, inline=True)
    # Статус оплаты
    paid = fine.get("paid_amount", 0.0)
    remaining = fine['amount'] - paid
    embed.add_field(name="💳 Оплата", value=f"{paid:.2f} / {fine['amount']:.2f} баллов", inline=True)
    if remaining > 0:
        embed.add_field(name="💸 Осталось доплатить", value=f"{remaining:.2f} баллов", inline=True)
    # Даты
    created_at = fine.get("created_at", "")[:10] if fine.get("created_at") else "Неизвестно"
    due_date = fine.get("due_date", "")[:10] if fine.get("due_date") else "Неизвестно"
    embed.add_field(name="📅 Создан", value=created_at, inline=True)
    embed.add_field(name="⏰ Срок оплаты", value=due_date, inline=True)
    # Статус
    if fine.get("is_canceled"):
        status = "🚫 Отменён"
        embed.color = discord.Color.orange()
    elif fine.get("is_paid"):
        status = "✅ Оплачен"
        embed.color = discord.Color.green()
    elif fine.get("is_overdue"):
        status = "⚠️ Просрочен"
        embed.color = discord.Color.dark_red()
    else:
        status = "⏳ Ожидает оплаты"
    embed.add_field(name="🔍 Статус", value=status, inline=False)
    # Дополнительная информация
    if fine.get("postponed_until"):
        postponed = fine["postponed_until"][:10]
        embed.add_field(name="📆 Отсрочка до", value=postponed, inline=True)
    if fine.get("was_on_time") is not None:
        on_time_text = "✅ Да" if fine["was_on_time"] else "❌ Нет"
        embed.add_field(name="⏱️ Оплачен вовремя", value=on_time_text, inline=True)
    return embed

class FineView(ui.View):
    def __init__(self, fine: dict):
        super().__init__(timeout=60)
        self.fine = fine
        # здесь можно добавить кнопки оплаты/отсрочки

class FinePaginator:
    def __init__(self, fines: list[dict], per_page: int = 5):
        self.fines = fines
        self.per_page = per_page

    def get_page(self, page: int) -> list[dict]:
        start = (page - 1) * self.per_page
        return self.fines[start:start + self.per_page]

class AllFinesView(ui.View):
    def __init__(self, fines: list[dict], ctx):
        super().__init__(timeout=60)
        self.fines = fines
        self.ctx = ctx
        self.page = 0

    def get_page_embed(self) -> discord.Embed:
        fine = self.fines[self.page]
        return build_fine_embed(fine)

def get_fine_leaders(limit: int = 3) -> list[tuple[int, float]]:
    debt = defaultdict(float)
    for f in db.fines:
        if not f.get("is_paid") and not f.get("is_canceled"):
            owed = f["amount"] - f.get("paid_amount", 0.0)
            debt[f["user_id"]] += owed
    return sorted(debt.items(), key=lambda x: x[1], reverse=True)[:limit]

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
        name="Привет! Напиши команду ?helpy чтобы увидеть все команды 🧠",
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
                    def get_fine_leaders(limit: int = 3) -> list[tuple[int, float]]:
                        """
                        Собирает список пользователей с наибольшей суммой неоплаченных штрафов.
                        Возвращает до `limit` записей вида (user_id, total_debt), отсортированных по убыванию долга.
                        """
                        debt = defaultdict(float)
                        for fine in db.fines:
                            if not fine.get("is_paid") and not fine.get("is_canceled"):
                                paid = fine.get("paid_amount", 0.0)
                                debt[fine["user_id"]] += fine["amount"] - paid

                        # сортируем по сумме долга и возвращаем первые limit элементов
                        top = sorted(debt.items(), key=lambda x: x[1], reverse=True)[:limit]
                        return top
                    # 🔥 Штрафной антибонус для топ-должников
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

        await asyncio.sleep(360)

async def check_overdue_fines(bot):
    while True:
        # реализация проверки просроченных штрафов
        await asyncio.sleep(3600)

async def debt_repayment_loop(bot):
    while True:
        # реализация автоматического напоминания об оплате
        await asyncio.sleep(86400)

async def reminder_loop(bot):
    while True:
        # реализация периодических напоминаний о штрафах
        await asyncio.sleep(86400)

async def fines_summary_loop(bot):
    while True:
        # реализация еженедельных/ежемесячных сводок штрафов
        await asyncio.sleep(604800)
