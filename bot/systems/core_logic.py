import discord
from discord.ext import commands
from typing import Optional
from datetime import datetime, timezone
import pytz
import asyncio
import traceback

from bot.data import db
from bot.utils.history_manager import format_history_embed
from bot.utils.roles_and_activities import ACTIVITY_CATEGORIES, ROLE_THRESHOLDS, display_last_edit_date
from collections import defaultdict
from bot.systems import (
    update_roles,
    render_history,
    log_action_cancellation,
    run_monthly_top,
    tophistory
)
# Константы
COMMAND_PREFIX = '?'
TIME_FORMAT = "%H:%M (%d.%m.%Y)"
DATE_FORMAT = "%d-%m-%Y"        # 25-12-2023
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"  # Для сортировки

active_timers = {}

intents = discord.Intents.default()
intents.members = True
intents.message_content = True
bot = commands.Bot(command_prefix=COMMAND_PREFIX, intents=intents, help_command=None)

def format_moscow_time(dt: Optional[datetime] = None) -> str:
    if dt is None:
        dt = datetime.now(timezone.utc)
    return dt.astimezone(pytz.timezone('Europe/Moscow')).strftime(TIME_FORMAT)

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
    top_bonus_count = 0
    top_bonus_sum = 0.0
    for action in db.history.get(user_id, []):
        if action.get("reason", "").startswith("Бонус за "):
            top_bonus_count += 1
            top_bonus_sum += action.get("points", 0)

    if top_bonus_count:
        embed.add_field(
            name="🏆 Бонусы за топ месяца",
            value=f"{top_bonus_count} наград, {top_bonus_sum:.2f} баллов",
            inline=False
        )
    await ctx.send(embed=embed)

@bot.command(name='leaderboard')
async def leaderboard(ctx, top: int = 10):
    if not db.scores:
        await ctx.send("Пока нет данных о баллах.")
        return

    sorted_scores = sorted(db.scores.items(), key=lambda x: x[1], reverse=True)[:top]
    embed = discord.Embed(title=f"🏆 Топ {top} по баллам", color=discord.Color.gold())
    medals = ["🥇", "🥈", "🥉"]

    for i, (user_id, points_val) in enumerate(sorted_scores, start=1):
        member = ctx.guild.get_member(user_id)
        medal = medals[i - 1] if i <= 3 else f"{i}."
        name = member.display_name if member else f"<@{user_id}>"
        roles = [role.name for role in member.roles if role.id in ROLE_THRESHOLDS] if member else []
        role_str = ', '.join(roles) if roles else 'Нет роли'
        embed.add_field(
            name=f"{medal} {name}",
            value=f"**Баллы:** {points_val:.2f}\n**Роль:** {role_str}",
            inline=False
        )

    await ctx.send(embed=embed)

@bot.command(name='history')
async def history_cmd(ctx, member: Optional[discord.Member] = None, page: int = 1):
    if member is None:
        member = ctx.author
    if member:
        await render_history(ctx, member, page)
    else:
        await ctx.send("Не удалось определить пользователя.")

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
    embed = discord.Embed(
        title="📋 Виды помощи клубу",
        description="Список всех видов деятельности и их стоимость в баллах:",
        color=discord.Color.blue()
    )
    def get_points_word(points):
        if points % 10 == 1 and points % 100 != 11:
            return "балл"
        elif 2 <= points % 10 <= 4 and (points % 100 < 10 or points % 100 >= 20):
            return "балла"
        else:
            return "баллов"

    for category_name, activities in ACTIVITY_CATEGORIES.items():
        category_text = ""
        for activity_name, info in activities.items():
            category_text += f"**{activity_name}** ({info['points']} {get_points_word(info['points'])})\n"
            category_text += f"↳ {info['description']}\n"
            if 'conditions' in info:
                category_text += "Условия:\n"
                for condition in info['conditions']:
                    category_text += f"• {condition}\n"
            category_text += "\n"
        embed.add_field(name=category_name, value=category_text, inline=False)
    embed.set_footer(text=display_last_edit_date())
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
        undo_entries.append((points_val, reason))

        # Запись отмены в базу
        db.add_action(
            user_id=user_id,
            points=-points_val,
            reason=f"Отмена действия: {reason}",
            author_id=ctx.author.id,
            is_undo=True
        )

    if not user_history:
        del db.history[user_id]

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

@bot.command(name='monthlytop')
@commands.has_permissions(administrator=True)
async def monthly_top(ctx):
    await run_monthly_top(ctx)

@bot.command(name='tophistory')
async def tophistory_cmd(ctx, month: Optional[int] = None, year: Optional[int] = None):
    await tophistory(ctx, month, year)

@bot.command(name='helpy')
async def helpy_cmd(ctx):
    embed = discord.Embed(
        title="🛠️ Справочник по командам",
        description="Список всех доступных команд, отсортированных по функциям:",
        color=discord.Color.blue()
    )

    embed.add_field(
        name="⚙️ Админские команды",
        value=(
            "`?addpoints @пользователь <баллы> [причина]` — начислить баллы\n"
            "`?removepoints @пользователь <баллы> [причина]` — снять баллы\n"
            "`?undo @пользователь <кол-во>` — отменить последние действия\n"
            "`?monthlytop` — начислить бонусы за топ месяца\n"
            "`?editfine <id> сумма тип дата причина` — изменить штраф\n"
            "`?cancel_fine <id>` — отменить штраф\n"
            "`?allfines` — все активные штрафы"
        ),
        inline=False
    )

    embed.add_field(
        name="📊 Баллы и рейтинг",
        value=(
            "`?points [@пользователь]` — посмотреть баллы\n"
            "`?leaderboard [кол-во]` — топ по баллам\n"
            "`?history [@пользователь] [страница]` — история действий"
        ),
        inline=False
    )

    embed.add_field(
        name="🏅 Роли и активности",
        value=(
            "`?roles` — список ролей и их требования\n"
            "`?activities` — баллы за виды помощи"
        ),
        inline=False
    )

    embed.add_field(
        name="📆 Топ месяца",
        value=(
            "`?monthlytop` — начислить бонусы (только админы)\n"
            "`?tophistory [месяц] [год]` — история наград за топ"
        ),
        inline=False
    )

    embed.add_field(
        name="📉 Штрафы",
        value=(
            "`?fine @пользователь <сумма> <тип> [причина]` — выдать штраф\n"
            "`?myfines` — ваши штрафы\n"
            "`?finehistory [@пользователь] [страница]` — история штрафов\n"
            "`?finedetails <id>` — подробности штрафа\n"
            "`?topfines` — топ должников"
        ),
        inline=False
    )

    embed.add_field(
        name="🧪 Прочее",
        value="`?ping` — проверка отклика\n`?helpy` — показать это сообщение",
        inline=False
    )

    await ctx.send(embed=embed)

@bot.command()
async def ping(ctx):
    await ctx.send('pong')
