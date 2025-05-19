# Основные импорты Discord
import discord
from discord.ext import commands

# Системные импорты
import json
import os
from typing import Optional
import asyncio
from datetime import datetime, timezone
import pytz

# Локальные импорты
from data import scores, history, save_data, load_data
import data
from keep_alive import keep_alive
from dotenv import load_dotenv
from roles_and_activities import ACTIVITY_CATEGORIES, ROLE_THRESHOLDS
from history_manager import format_history_embed

# Настройки бота

# Константы
COMMAND_PREFIX = '?'

# Файлы для хранения данных
DATA_FILE = 'scores.json'
HISTORY_FILE = 'history.json'

# Интенты — обязательно message_content=True для команд
intents = discord.Intents.default()
intents.members = True
intents.message_content = True

bot = commands.Bot(command_prefix=COMMAND_PREFIX, intents=intents)


async def update_roles(member: discord.Member):
    user_id = member.id
    user_points = scores.get(user_id, 0)

    user_roles = [role.id for role in member.roles if role.id in ROLE_THRESHOLDS]

    role_to_add_id = None
    for role_id, threshold in sorted(ROLE_THRESHOLDS.items(), key=lambda x: x[1], reverse=True):
        if user_points >= threshold:
            role_to_add_id = role_id
            break

    if role_to_add_id and role_to_add_id not in user_roles:
        role_to_add = member.guild.get_role(role_to_add_id)
        if role_to_add:
            await member.add_roles(role_to_add)

    for role_id in user_roles:
        if role_id != role_to_add_id:
            role_to_remove = member.guild.get_role(role_id)
            if role_to_remove:
                await member.remove_roles(role_to_remove)


@bot.command(name='addpoints')
@commands.has_permissions(administrator=True)
async def add_points(ctx, member: discord.Member, points: str, *, reason: str = 'Без причины'):
    try:
        points_float = float(points.replace(',', '.'))
        user_id = member.id
        scores[user_id] = scores.get(user_id, 0) + points_float
        moscow_tz = pytz.timezone('Europe/Moscow')
        timestamp = datetime.now(moscow_tz).strftime("%H:%M %d-%m-%Y")
        if points_float < 0:
            scores[user_id] = 0
    except ValueError:
        await ctx.send("Ошибка: введите корректное число")
        return

    history.setdefault(user_id, []).append({
        'points': points_float,
        'reason': reason,
        'author_id': ctx.author.id,
        'timestamp': timestamp
    })

    save_data()
    await update_roles(member)

    embed = discord.Embed(
        title="🎉 Баллы начислены!",
        color=discord.Color.green()
    )
    embed.add_field(name="👤 Пользователь:", value=member.mention, inline=False)
    embed.add_field(name="➕ Количество:", value=f"**{points}** баллов", inline=False)
    embed.add_field(name="📝 Причина:", value=reason, inline=False)
    embed.add_field(name="🕒 Время:", value=timestamp, inline=False)
    embed.add_field(name="🎯 Текущий баланс:", value=f"{scores[user_id]} баллов", inline=False)

    await ctx.send(embed=embed)


@bot.command(name='removepoints')
@commands.has_permissions(administrator=True)
async def remove_points(ctx, member: discord.Member, points: str, *, reason: str = 'Без причины'):
    try:
        points_float = float(points.replace(',', '.'))
        
        if points_float < 0:
            await ctx.send("Ошибка: нельзя использовать отрицательные числа в команде removepoints.")
            return
            
        user_id = member.id
        current_points = scores.get(user_id, 0)
        
        # Проверяем, сколько баллов можно реально снять
        actual_points_to_remove = min(points_float, current_points)
        scores[user_id] = current_points - actual_points_to_remove
    except ValueError:
        await ctx.send("Ошибка: введите корректное число")
        return

    moscow_tz = pytz.timezone('Europe/Moscow')
    timestamp = datetime.now(moscow_tz).strftime("%H:%M %d-%m-%Y")
    
    # Записываем в историю реальное количество снятых баллов
    history.setdefault(user_id, []).append({
        'points': -actual_points_to_remove,
        'reason': f"{reason} (запрошено снятие: {points_float} баллов)",
        'author_id': ctx.author.id,
        'timestamp': timestamp
    })

    save_data()
    await update_roles(member)

    embed = discord.Embed(
        title="⚠️ Баллы сняты!",
        color=discord.Color.red()
    )
    embed.add_field(name="👤 Пользователь:", value=member.mention, inline=False)
    embed.add_field(name="➖ Снято баллов:", value=f"**{actual_points_to_remove}** из запрошенных {points_float}", inline=False)
    embed.add_field(name="📝 Причина:", value=reason, inline=False)
    embed.add_field(name="🕒 Время:", value=timestamp, inline=False)
    embed.add_field(name="🎯 Текущий баланс:", value=f"{scores[user_id]} баллов", inline=False)

    await ctx.send(embed=embed)

    embed = discord.Embed(
        title="⚠️ Баллы сняты!",
        color=discord.Color.red()
    )
    embed.add_field(name="👤 Пользователь:", value=member.mention, inline=False)
    embed.add_field(name="➖ Количество:", value=f"**{points}** баллов", inline=False)
    embed.add_field(name="📝 Причина:", value=reason, inline=False)
    embed.add_field(name="🕒 Время:", value=timestamp, inline=False)
    embed.add_field(name="🎯 Текущий баланс:", value=f"{scores[user_id]} баллов", inline=False)

    await ctx.send(embed=embed)

@bot.command(name='points')
async def points(ctx, member: Optional[discord.Member] = None):
    if member is None:
        member = ctx.author
    if member is None:
        await ctx.send("Не удалось определить пользователя. Пожалуйста, попробуйте еще раз.")
        return
    user_id = member.id
    user_points = scores.get(user_id, 0)
    user_roles = [role for role in member.roles if role.id in ROLE_THRESHOLDS]
    role_names = ', '.join(role.name for role in user_roles) if user_roles else 'Нет роли'

    sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    place = None
    for i, (uid, points_val) in enumerate(sorted_scores, start=1):
        if uid == user_id:
            place = i
            break
    place_text = f"{place}" if place else "Не в топе"

    embed = discord.Embed(title=f"Баллы пользователя {member.display_name}", color=discord.Color.blue())
    embed.set_thumbnail(url=member.avatar.url if member.avatar else member.default_avatar.url)
    embed.add_field(name="Баллы", value=f"{user_points}", inline=True)
    embed.add_field(name="Роли", value=role_names, inline=True)
    embed.add_field(name="Место в топе", value=place_text, inline=False)

    await ctx.send(embed=embed)


@bot.command(name='leaderboard')
async def leaderboard(ctx, top: int = 10):
    if not scores:
        await ctx.send("Пока нет данных о баллах.")
        return
    sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top]

    embed = discord.Embed(title=f"Топ {top} лидеров по баллам", color=discord.Color.gold())
    for i, (user_id, points_val) in enumerate(sorted_scores, start=1):
        member = ctx.guild.get_member(user_id)
        if member:
            user_roles = [role for role in member.roles if role.id in ROLE_THRESHOLDS]
            role_names = ', '.join(role.name for role in user_roles) if user_roles else 'Нет роли'
            embed.add_field(name=f"{i}. {member.display_name}", value=f"Баллы: {points_val}\nРоли: {role_names}", inline=False)
        else:
            embed.add_field(name=f"{i}. Пользователь с ID {user_id}", value=f"Баллы: {points_val}", inline=False)
    await ctx.send(embed=embed)

@bot.command(name='history')
async def history_cmd(ctx, member: Optional[discord.Member] = None, page: int = 1):
    if member is None:
        member = ctx.author
    if member is None:
        await ctx.send("Не удалось определить пользователя. Пожалуйста, попробуйте еще раз.")
        return

    user_id = member.id
    entries_per_page = 5

    if user_id not in history or not history[user_id]:
        await ctx.send(f"История начисления баллов для {member.display_name} пуста.")
        return

    total_entries = len(history[user_id])
    total_pages = (total_entries + entries_per_page - 1) // entries_per_page

    if page < 1 or page > total_pages:
        await ctx.send(f"Страница {page} не существует. Доступно всего {total_pages} страниц.")
        return

    start = (page - 1) * entries_per_page
    end = start + entries_per_page
    page_history = history[user_id][start:end]

    embed = format_history_embed(page_history, member.display_name, page, total_entries)
    await ctx.send(embed=embed)

@bot.command(name='roles')
async def roles_list(ctx):
    desc = ""
    for role_id, points_needed in sorted(ROLE_THRESHOLDS.items(), key=lambda x: x[1], reverse=True):
        role = ctx.guild.get_role(role_id)
        if role:
            desc += f"**{role.name}**: {points_needed} баллов\n"
    embed = discord.Embed(title="Роли и стоимость баллов", description=desc, color=discord.Color.purple())
    await ctx.send(embed=embed)


@bot.command(name='helpy')
async def helpy_cmd(ctx):
    help_text = f"""
**Список команд:**

`{COMMAND_PREFIX}addpoints @пользователь <баллы> [причина]` — добавить баллы (только для админов)  
`{COMMAND_PREFIX}removepoints @пользователь <баллы> [причина]` — снять баллы (только для админов)  
`{COMMAND_PREFIX}undo @пользователь <количество>` — отменить последние изменения для пользователя (только для админов) 
`{COMMAND_PREFIX}points [@пользователь]` — показать баллы пользователя (по умолчанию автора)  
`{COMMAND_PREFIX}leaderboard [кол-во]` — показать топ лидеров (по умолчанию 10)  
`{COMMAND_PREFIX}history [@пользователь] [страница]` — история начисления баллов  
`{COMMAND_PREFIX}roles` — показать все роли и их стоимость  
`{COMMAND_PREFIX}activities` — список всех видов деятельности и их стоимость в баллах  
`{COMMAND_PREFIX}helpy` — показать это сообщение  
"""
    await ctx.send(help_text)


@bot.command()
async def ping(ctx):
    await ctx.send('pong')


async def send_greetings(channel, user_list):
    for user_id in user_list:
        await channel.send(f"Привет, <@{user_id}>!")
        await asyncio.sleep(1)


    @bot.event
    async def on_ready():
        load_data()  # Загрузка из файла (data.py)
        print(f'Бот {bot.user} запущен! Команд зарегистрировано: {len(bot.commands)}')
        for cmd in bot.commands:
            print(f"- {cmd.name}")
        bot.loop.create_task(autosave_task())


async def autosave_task():
    await bot.wait_until_ready()
    while not bot.is_closed():
        save_data()
        print("Данные сохранены автоматически.")
        await asyncio.sleep(300)


@bot.command(name='undo')
@commands.has_permissions(administrator=True)
async def undo(ctx, member: discord.Member, count: int = 1):
        user_id = member.id
        user_history = history.get(user_id, [])

        if len(user_history) < count:
            await ctx.send(
                f"❌ Нельзя отменить **{count}** изменений для {member.display_name}, "
                f"так как доступно только **{len(user_history)}** записей."
            )
            return

        undo_entries = []
        for _ in range(count):
            entry = user_history.pop()
            points_val = entry.get("points", 0)
            reason = entry.get("reason", "Без причины")
            scores[user_id] = scores.get(user_id, 0) - points_val
            if scores[user_id] < 0:
                scores[user_id] = 0
            undo_entries.append((points_val, reason))

        if not user_history:
            del history[user_id]

        save_data()
        await update_roles(member)

        embed = discord.Embed(
            title=f"↩️ Отменено {count} изменений для {member.display_name}",
            color=discord.Color.orange()
        )
        for i, (points_val, reason) in enumerate(undo_entries[::-1], start=1):
            sign = "+" if points_val > 0 else ""
            embed.add_field(
                name=f"{i}. {sign}{points_val} баллов",
                value=reason,
                inline=False
            )
        await ctx.send(embed=embed)
        await log_action_cancellation(ctx, member, undo_entries)


async def log_action_cancellation(ctx, member: discord.Member, entries: list):
    channel = discord.utils.get(ctx.guild.channels, name='history-log')
    if not channel:
        return

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    lines = [f"**{ctx.author.display_name}** отменил(а) {len(entries)} изменения для **{member.display_name}** ({member.id}) в {now}:"]
    for i, (points, reason) in enumerate(entries[::-1], start=1):
        sign = "+" if points > 0 else ""
        lines.append(f"{i}. {sign}{points} — {reason}")

    await channel.send("\n".join(lines))

print(bot.all_commands.keys())

print(dir(data))
print(data.scores)

@bot.event
async def on_ready():
    print(f'Logged in as {bot.user}!')

keep_alive()  # Поддерживаем работу через веб-сервер

load_dotenv()  # Загружает переменные из .env файла в окружение

print("TOKEN:", os.getenv("TOKEN"))

TOKEN = os.getenv('DISCORD_TOKEN')

@bot.command(name='activities')
async def activities_cmd(ctx):
    embed = discord.Embed(
        title="📋 Виды помощи клубу",
        description="Список всех видов деятельности и их стоимость в баллах:",
        color=discord.Color.blue()
    )

    for category_name, activities in ACTIVITY_CATEGORIES.items():
        category_text = ""
        for activity_name, info in activities.items():
            category_text += f"**{activity_name}** ({info['points']} баллов)\n"
            category_text += f"↳ {info['description']}\n"
            if 'conditions' in info:
                category_text += "Условия:\n"
                for condition in info['conditions']:
                    category_text += f"• {condition}\n"
            category_text += "\n"

        embed.add_field(
            name=category_name,
            value=category_text,
            inline=False
        )

    await ctx.send(embed=embed)

bot.run(os.getenv("TOKEN"))
