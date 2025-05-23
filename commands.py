import discord
from discord.ext import commands
from typing import Optional
from datetime import datetime, timezone
import pytz
import asyncio
import traceback

from data import db
from history_manager import format_history_embed
from roles_and_activities import ACTIVITY_CATEGORIES, ROLE_THRESHOLDS

# Константы
COMMAND_PREFIX = '?'
TIME_FORMAT = "%H:%M (%d.%m.%Y)"
DATE_FORMAT = "%d-%m-%Y"        # 25-12-2023
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"  # Для сортировки

active_timers = {}

intents = discord.Intents.default()
intents.members = True
intents.message_content = True
bot = commands.Bot(command_prefix=COMMAND_PREFIX, intents=intents)

def format_moscow_time(dt: Optional[datetime] = None) -> str:
    if dt is None:
        dt = datetime.now(timezone.utc)
    return dt.astimezone(pytz.timezone('Europe/Moscow')).strftime(TIME_FORMAT)

async def update_roles(member: discord.Member):
    user_id = member.id
    user_points = db.scores.get(user_id, 0)
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
        current = db.scores.get(user_id, 0)
        db.scores[user_id] = max(current + points_float, 0)
        db.add_action(user_id, points_float, reason, ctx.author.id)
        await update_roles(member)
        embed = discord.Embed(title="🎉 Баллы начислены!", color=discord.Color.green())
        embed.add_field(name="👤 Пользователь:", value=member.mention, inline=False)
        embed.add_field(name="➕ Количество:", value=f"**{points}** баллов", inline=False)
        embed.add_field(name="📝 Причина:", value=reason, inline=False)
        embed.add_field(name="🕒 Время:", value=format_moscow_time(), inline=False)
        embed.add_field(name="🎯 Текущий баланс:", value=f"{db.scores[user_id]} баллов", inline=False)
        await ctx.send(embed=embed)
    except ValueError:
        await ctx.send("Ошибка: введите корректное число")

@bot.command(name='removepoints')
@commands.has_permissions(administrator=True)
async def remove_points(ctx, member: discord.Member, points: str, *, reason: str = 'Без причины'):
    try:
        points_float = float(points.replace(',', '.'))
        if points_float <= 0:
            await ctx.send("❌ Ошибка: введите число больше 0 для снятия баллов.")
            return
        user_id = member.id
        current_points = db.scores.get(user_id, 0)
        if points_float > current_points:
            embed = discord.Embed(title="⚠️ Недостаточно баллов", description=f"У {member.mention} только {current_points} баллов", color=discord.Color.red())
            await ctx.send(embed=embed)
            return
        db.scores[user_id] = current_points - points_float
        db.add_action(user_id, -points_float, reason, ctx.author.id)
        await update_roles(member)
        embed = discord.Embed(title="⚠️ Баллы сняты!", color=discord.Color.red())
        embed.add_field(name="👤 Пользователь:", value=member.mention, inline=False)
        embed.add_field(name="➖ Снято баллов:", value=f"**{points_float}**", inline=False)
        embed.add_field(name="📝 Причина:", value=reason, inline=False)
        embed.add_field(name="🕒 Время:", value=format_moscow_time(), inline=False)
        embed.add_field(name="🎯 Текущий баланс:", value=f"{db.scores[user_id]} баллов", inline=False)
        await ctx.send(embed=embed)
    except ValueError:
        await ctx.send("Ошибка: введите корректное число больше 0")

@bot.command(name='points')
async def points(ctx, member: Optional[discord.Member] = None):
    member = member or ctx.author
    if not member:
        await ctx.send("Не удалось определить пользователя. Пожалуйста, попробуйте еще раз.")
        return
    user_id = member.id
    user_points = db.scores.get(user_id, 0)
    user_roles = [role for role in member.roles if role.id in ROLE_THRESHOLDS]
    role_names = ', '.join(role.name for role in user_roles) if user_roles else 'Нет роли'
    sorted_scores = sorted(db.scores.items(), key=lambda x: x[1], reverse=True)
    place = next((i for i, (uid, _) in enumerate(sorted_scores, 1) if uid == user_id), None)
    embed = discord.Embed(title=f"Баллы пользователя {member.display_name}", color=discord.Color.blue())
    embed.set_thumbnail(url=member.avatar.url if member.avatar else member.default_avatar.url)
    embed.add_field(name="Баллы", value=f"{user_points}", inline=True)
    embed.add_field(name="Роли", value=role_names, inline=True)
    embed.add_field(name="Место в топе", value=f"{place}" if place else "Не в топе", inline=False)
    await ctx.send(embed=embed)

@bot.command(name='leaderboard')
async def leaderboard(ctx, top: int = 10):
    if not db.scores:
        await ctx.send("Пока нет данных о баллах.")
        return
    sorted_scores = sorted(db.scores.items(), key=lambda x: x[1], reverse=True)[:top]
    embed = discord.Embed(title=f"Топ {top} лидеров по баллам", color=discord.Color.gold())
    for i, (user_id, points_val) in enumerate(sorted_scores, start=1):
        member = ctx.guild.get_member(user_id)
        if member:
            user_roles = [role for role in member.roles if role.id in ROLE_THRESHOLDS]
            role_names = ', '.join(role.name for role in user_roles) if user_roles else 'Нет роли'
            embed.add_field(name=f"{i}. {member.display_name}", value=f"Баллы: {points_val}\nРоль: {role_names}", inline=False)
        else:
            embed.add_field(name=f"{i}. <@{user_id}>", value=f"Баллы: {points_val}", inline=False)
    await ctx.send(embed=embed)

@bot.command(name='history')
async def history_cmd(ctx, member: Optional[discord.Member] = None, page: int = 1):
    if member is None:
        member = ctx.author
    if member:
        await render_history(ctx, member, page)
    else:
        await ctx.send("Не удалось определить пользователя.")

class HistoryView(discord.ui.View):
    def __init__(self, member: discord.Member, page: int, total_pages: int):
        super().__init__(timeout=60)
        self.member = member
        self.page = page
        self.total_pages = total_pages

        self.prev_button.disabled = page <= 1
        self.next_button.disabled = page >= total_pages

    @discord.ui.button(label="◀️ Назад", style=discord.ButtonStyle.gray, custom_id="prev")
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        await render_history(interaction, self.member, self.page - 1)

    @discord.ui.button(label="Вперед ▶️", style=discord.ButtonStyle.gray, custom_id="next")
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        await render_history(interaction, self.member, self.page + 1)


async def render_history(ctx_or_interaction, member: discord.Member, page: int):
    try:
        user_id = member.id
        entries_per_page = 5
        user_history = db.history.get(user_id, [])

        if not user_history:
            embed = discord.Embed(
                title="📜 История баллов",
                description="```Записей не найдено```",
                color=discord.Color.orange()
            )
            embed.set_author(name=member.display_name, icon_url=member.avatar.url if member.avatar else member.default_avatar.url)

            if isinstance(ctx_or_interaction, discord.Interaction):
                await ctx_or_interaction.response.send_message(embed=embed, ephemeral=True)
            else:
                await ctx_or_interaction.send(embed=embed)
            return

        total_entries = len(user_history)
        total_pages = max(1, (total_entries + entries_per_page - 1) // entries_per_page)

        if page < 1 or page > total_pages:
            embed = discord.Embed(
                title="⚠️ Ошибка навигации",
                description=f"```Доступно страниц: {total_pages}```",
                color=discord.Color.red()
            )
            if isinstance(ctx_or_interaction, discord.Interaction):
                await ctx_or_interaction.response.send_message(embed=embed, ephemeral=True)
            else:
                await ctx_or_interaction.send(embed=embed)
            return

        start_idx = (page - 1) * entries_per_page
        page_actions = user_history[start_idx:start_idx + entries_per_page]

        embed = discord.Embed(title="📜 История баллов", color=discord.Color.blue())
        embed.set_author(name=member.display_name, icon_url=member.avatar.url if member.avatar else member.default_avatar.url)

        total_points = db.scores.get(user_id, 0)
        embed.add_field(name="💰 Текущий баланс", value=f"```{total_points} баллов```", inline=False)

        for action in page_actions:
            points = action.get('points', 0)
            emoji = "🟢" if points >= 0 else "🔴"
            if action.get('is_undo', False):
                emoji = "⚪"

            timestamp = action.get('timestamp')
            if isinstance(timestamp, str):
                try:
                    dt = datetime.fromisoformat(timestamp)
                    formatted_time = format_moscow_time(dt)
                except ValueError:
                    formatted_time = timestamp
            else:
                formatted_time = format_moscow_time(timestamp) if timestamp else 'N/A'

            author_id = action.get('author_id', 'N/A')
            reason = action.get('reason', 'Не указана')

            field_name = f"{emoji} {formatted_time}"
            field_value = (
                f"```diff\n{'+' if points >= 0 else ''}{points} баллов```\n"
                f"**Причина:** {reason}\n"
                f"**Выдал:** <@{author_id}>"
            )
            embed.add_field(name=field_name, value=field_value, inline=False)

        embed.set_footer(text=f"Страница {page}/{total_pages} • Всего записей: {total_entries}")

        view = HistoryView(member, page, total_pages)

        if isinstance(ctx_or_interaction, discord.Interaction):
            if ctx_or_interaction.response.is_done():
                sent_message = await ctx_or_interaction.edit_original_response(embed=embed, view=view)
            else:
                await ctx_or_interaction.response.send_message(embed=embed, view=view)
                sent_message = await ctx_or_interaction.original_response()
        else:
            sent_message = await ctx_or_interaction.send(embed=embed, view=view)

        if sent_message.id in active_timers:
            active_timers[sent_message.id].cancel()

        async def delete_later(msg: discord.Message):
            try:
                await asyncio.sleep(180)
                await msg.delete()
            except (discord.NotFound, discord.Forbidden):
                pass
            finally:
                active_timers.pop(msg.id, None)

        task = asyncio.create_task(delete_later(sent_message))
        active_timers[sent_message.id] = task

    except Exception as e:
        error_embed = discord.Embed(
            title="⚠️ Ошибка",
            description=f"```{str(e)}```",
            color=discord.Color.red()
        )
        if isinstance(ctx_or_interaction, discord.Interaction):
            await ctx_or_interaction.response.send_message(embed=error_embed, ephemeral=True)
        else:
            await ctx_or_interaction.send(embed=error_embed)
        print(f"Ошибка в render_history: {traceback.format_exc()}")

@bot.command(name='roles')
async def roles_list(ctx):
    desc = ""
    for role_id, points_needed in sorted(ROLE_THRESHOLDS.items(), key=lambda x: x[1], reverse=True):
        role = ctx.guild.get_role(role_id)
        if role:
            desc += f"**{role.name}**: {points_needed} баллов\n"
    embed = discord.Embed(title="Роли и стоимость баллов", description=desc, color=discord.Color.purple())
    await ctx.send(embed=embed)

@bot.command(name='activities')
async def activities_cmd(ctx):
    embed = discord.Embed(title="📋 Виды помощи клубу", description="Список всех видов деятельности и их стоимость в баллах:", color=discord.Color.blue())
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
        embed.add_field(name=category_name, value=category_text, inline=False)
    await ctx.send(embed=embed)

@bot.command(name='undo')
@commands.has_permissions(administrator=True)
async def undo(ctx, member: discord.Member, count: int = 1):
    user_id = member.id
    user_history = db.history.get(user_id, [])
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
        db.scores[user_id] = db.scores.get(user_id, 0) - points_val
        if db.scores[user_id] < 0:
            db.scores[user_id] = 0
        undo_entries.append((points_val, reason))

        from datetime import datetime
        import pytz
        moscow_tz = pytz.timezone('Europe/Moscow')
        timestamp = datetime.now(moscow_tz).strftime("%H:%M %d-%m-%Y")

        user_history.append({
            'points': -points_val,
            'reason': f"Отмена действия: {reason}",
            'author_id': ctx.author.id,
            'timestamp': timestamp,
            'is_undo': True
        })

    if not user_history:
        del db.history[user_id]

    db.save_all()
    await update_roles(member)

    embed = discord.Embed(
        title=f"↩️ Отменено {count} изменений для {member.display_name}",
        color=discord.Color.orange()
    )
    for i, (points_val, reason) in enumerate(undo_entries[::-1], start=1):
        sign = "+" if points_val > 0 else ""
        embed.add_field(name=f"{i}. {sign}{points_val} баллов", value=reason, inline=False)
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
