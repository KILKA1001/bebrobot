import discord
from discord.ext import commands
import json
import os
from typing import Optional
import asyncio
from database import db
from datetime import datetime, timezone
from keep_alive import keep_alive
from dotenv import load_dotenv
from supabase import create_client

# Initialize global variables
scores = {}
history = {}

# Константы
COMMAND_PREFIX = '?'

# Роли и их минимальные баллы
ROLE_THRESHOLDS = {
    1212624623548768287: 2000,  # @Бог среди волонтеров
    1105906637824331788: 500,   # @Легендарный среди волонтеров
    1137775519589466203: 140,   # @Мастер волонтер
    1105906455233703989: 30,    # @Хороший Помощник Бебр
    1105906310131744868: 10     # @Новый волонтер
}

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
async def add_points(ctx, member: discord.Member, points: float, *, reason: str = 'Без причины'):
    user_id = member.id
    scores[user_id] = scores.get(user_id, 0) + points
    timestamp = datetime.now(timezone.utc)
    formatted_date = timestamp.strftime('%d.%m.%Y %H:%M:%S UTC')

    history.setdefault(user_id, []).append({
        'points': points,
        'reason': reason,
        'author_id': ctx.author.id,
        'timestamp': timestamp
    })

    await save_data()
    await update_roles(member)

    embed = discord.Embed(
        title="🎉 Баллы начислены!",
        color=discord.Color.green()
    )
    embed.add_field(name="👤 Пользователь:", value=member.mention, inline=False)
    embed.add_field(name="➕ Количество:", value=f"**{points}** баллов", inline=False)
    embed.add_field(name="📝 Причина:", value=reason, inline=False)
    embed.add_field(name="🎯 Текущий баланс:", value=f"{scores[user_id]} баллов", inline=False)
    embed.add_field(name="⏰ Время:", value=formatted_date, inline=False)

    await ctx.send(embed=embed)

@bot.command(name='removepoints')
@commands.has_permissions(administrator=True)
async def remove_points(ctx, member: discord.Member, points: float, *, reason: str = 'Без причины'):
    user_id = member.id
    scores[user_id] = scores.get(user_id, 0) - points
    if scores[user_id] < 0:
        scores[user_id] = 0

    timestamp = datetime.now(timezone.utc)
    formatted_date = timestamp.strftime('%d.%m.%Y %H:%M:%S UTC')

    history.setdefault(user_id, []).append({
        'points': -points,
        'reason': reason,
        'author_id': ctx.author.id,
        'timestamp': timestamp
    })

    await save_data()
    await update_roles(member)

    embed = discord.Embed(
        title="⚠️ Баллы сняты!",
        color=discord.Color.red()
    )
    embed.add_field(name="👤 Пользователь:", value=member.mention, inline=False)
    embed.add_field(name="➖ Количество:", value=f"**{points}** баллов", inline=False)
    embed.add_field(name="📝 Причина:", value=reason, inline=False)
    embed.add_field(name="🎯 Текущий баланс:", value=f"{scores[user_id]} баллов", inline=False)
    embed.add_field(name="⏰ Время:", value=formatted_date, inline=False)

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
    # Проверка: если участник не указан — используем автора команды
    if member is None:
        if not isinstance(ctx.author, discord.Member):
            await ctx.send("Не удалось определить пользователя. Пожалуйста, используйте команду на сервере.")
            return
        member = ctx.author

    user_id = member.id
    entries_per_page = 5

    if user_id not in history or not history[user_id]:
        await ctx.send(f"История начисления баллов для {member.display_name} пуста.")
        return

    total_pages = (len(history[user_id]) + entries_per_page - 1) // entries_per_page
    if page < 1 or page > total_pages:
        await ctx.send(f"Страница {page} не существует. Доступно всего {total_pages} страниц.")
        return

    start = (page - 1) * entries_per_page
    end = start + entries_per_page
    page_history = history[user_id][start:end]

    embed = discord.Embed(
        title=f"История баллов {member.display_name} (страница {page}/{total_pages})",
        color=discord.Color.green()
    )

    for entry in page_history:
        if isinstance(entry, dict):
            points_val = entry.get("points", 0)
            reason = entry.get("reason", "Без причины")
            author_id = entry.get("author_id")
            timestamp = entry.get("timestamp")
            sign = "+" if points_val > 0 else ""
            author_str = f"<@{author_id}>" if author_id else "Неизвестен"
            time_str = f"({timestamp})" if timestamp else ""
            field_value = f"{reason}\nАвтор: {author_str} {time_str}"
        else:
            # Старый формат (points, reason)
            if isinstance(entry, tuple):
                points_val, reason = entry
                sign = "+" if points_val > 0 else ""
            else:
                print(f"Unexpected history entry format: {entry}")
                continue # Skip this entry
            field_value = reason

        embed.add_field(
            name=f"{sign}{points_val} баллов",
            value=field_value,
            inline=False
        )

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
`{COMMAND_PREFIX}points [@пользователь]` — показать баллы пользователя (по умолчанию автора)  
`{COMMAND_PREFIX}leaderboard [кол-во]` — показать топ лидеров (по умолчанию 10)  
`{COMMAND_PREFIX}history [@пользователь] [страница]` — история начисления баллов  
`{COMMAND_PREFIX}roles` — показать все роли и их стоимость  
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
    await load_data()  # Загрузка из Supabase
    print(f'Бот {bot.user} запущен! Команд зарегистрировано: {len(bot.commands)}')
    for cmd in bot.commands:
        print(f"- {cmd.name}")
    bot.loop.create_task(autosave_task())

async def autosave_task():
    await bot.wait_until_ready()
    while not bot.is_closed():
        await save_data()
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

    await save_data()
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

# Load environment variables
load_dotenv()

# Initialize Supabase client
try:
    supabase = create_client(
        os.getenv('SUPABASE_URL'),
        os.getenv('SUPABASE_KEY')
    )
except Exception as e:
    print(f"Error initializing Supabase client: {e}")
    supabase = None

async def save_data():
    try:
        # Сохраняем баллы
        if supabase:
            for user_id, score in scores.items():
                try:
                    supabase.table('points').upsert({
                        'user_id': user_id,
                        'score': score
                    }).execute()
                except Exception as e:
                    print(f"Error upserting points for user {user_id}: {e}")

        # Сохраняем историю
        if supabase:
            for user_id, user_history in history.items():
                for entry in user_history:
                    insert_data = {
                        'user_id': user_id,
                        'points': entry['points'],
                        'reason': entry['reason'],
                        'timestamp': entry.get('timestamp', datetime.now().isoformat())
                    }
                    if entry.get('author_id') is not None:
                        insert_data['author_id'] = int(entry['author_id'])

                    try:
                        res = supabase.table('history').insert(insert_data).execute()
                        print("History insert result:", res)
                    except Exception as e:
                        print(f"Error inserting history for user {user_id}: {e}")

    except Exception as e:
        print(f"Ошибка при сохранении данных: {e}")

async def load_data():
    global scores, history
    try:
        # Загружаем баллы
        if supabase:
            points_response = supabase.table('points').select('*').execute()
            if points_response and points_response.data:
                for record in points_response.data:
                    scores[record['user_id']] = record['score']

        # Загружаем историю
        if supabase:
            history_response = supabase.table('history').select('*').execute()
            if history_response and history_response.data:
                for record in history_response.data:
                    user_id = record['user_id']
                    if user_id not in history:
                        history[user_id] = []
                    history[user_id].append({
                        'points': record['points'],
                        'reason': record['reason'],
                        'author_id': record.get('author_id'),
                        'timestamp': record['timestamp']
                    })

    except Exception as e:
        print(f"Ошибка при загрузке данных: {e}")
        scores = {}
        history = {}

# Start the keep-alive server
keep_alive()

# Run the bot
TOKEN = os.getenv('DISCORD_TOKEN')
if not TOKEN:
    print("Error: DISCORD_TOKEN not found in environment variables")
else:
    bot.run(TOKEN)
