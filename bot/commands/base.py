import os
import discord
from discord.ext import commands
from aiohttp import TraceConfig
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
    render_history,
    log_action_cancellation,
    tophistory
)
from bot.systems.core_logic import (
    update_roles,
    run_monthly_top,
    get_help_embed,
    HelpView,
    LeaderboardView,
    transfer_data_logic,
    build_balance_embed
)
from bot.utils import send_temp
from bot import COMMAND_PREFIX


# Константы
TIME_FORMAT = "%H:%M (%d.%m.%Y)"
DATE_FORMAT = "%d-%m-%Y"        # 25-12-2023
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"  # Для сортировки

active_timers = {}

intents = discord.Intents.default()
intents.members = True
intents.message_content = True

trace_config = TraceConfig()
from bot.utils.api_monitor import monitor

@trace_config.on_request_end.append
async def _trace_request_end(session, ctx, params):
    monitor.record_request(params.response.status)

bot = commands.Bot(
    command_prefix=COMMAND_PREFIX,
    intents=intents,
    help_command=None,
    http_trace=trace_config,
)

def format_moscow_time(dt: Optional[datetime] = None) -> str:
    if dt is None:
        dt = datetime.now(timezone.utc)
    return dt.astimezone(pytz.timezone('Europe/Moscow')).strftime(TIME_FORMAT)

@bot.hybrid_command(
    name='addpoints',
    description='Начислить баллы участнику'
)
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
        await send_temp(ctx, embed=embed, delete_after=None)
    except ValueError:
        await send_temp(ctx, "Ошибка: введите корректное число")

@bot.hybrid_command(
    name='removepoints',
    description='Снять баллы у участника'
)
@commands.has_permissions(administrator=True)
async def remove_points(ctx, member: discord.Member, points: str, *, reason: str = 'Без причины'):
    try:
        points_float = float(points.replace(',', '.'))
        if points_float <= 0:
            await send_temp(ctx, "❌ Ошибка: введите число больше 0 для снятия баллов.")
            return
        user_id = member.id
        current_points = db.scores.get(user_id, 0)
        if points_float > current_points:
            embed = discord.Embed(title="⚠️ Недостаточно баллов", description=f"У {member.mention} только {current_points} баллов", color=discord.Color.red())
            await send_temp(ctx, embed=embed)
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
        await send_temp(ctx, embed=embed)
    except ValueError:
        await send_temp(ctx, "Ошибка: введите корректное число больше 0")

@bot.hybrid_command(
    name='leaderboard',
    description='Показать общий рейтинг по баллам'
)
async def leaderboard(ctx):
    view = LeaderboardView(ctx)
    await send_temp(ctx, embed=view.get_embed(), view=view)

@bot.hybrid_command(
    name='history',
    description='История действий пользователя'
)
async def history_cmd(ctx, member: Optional[discord.Member] = None, page: int = 1):
    if member is None:
        member = ctx.author
    if member:
        await render_history(ctx, member, page)
    else:
        await send_temp(ctx, "Не удалось определить пользователя.")

@bot.hybrid_command(
    name='roles',
    description='Список ролей и стоимость в баллах'
)
async def roles_list(ctx):
    desc = ""
    for role_id, points_needed in sorted(ROLE_THRESHOLDS.items(), key=lambda x: x[1], reverse=True):
        role = ctx.guild.get_role(role_id)
        if role:
            desc += f"**{role.name}**: {points_needed} баллов\n"
    embed = discord.Embed(title="Роли и стоимость баллов", description=desc, color=discord.Color.purple())
    await send_temp(ctx, embed=embed)

@bot.hybrid_command(
    name='activities',
    description='Виды помощи клубу и их стоимость'
)
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
    await send_temp(ctx, embed=embed)


@bot.hybrid_command(
    name='undo',
    description='Отменить последние начисления или списания'
)
@commands.has_permissions(administrator=True)
async def undo(ctx, member: discord.Member, count: int = 1):
    user_id = member.id
    user_history = db.history.get(user_id, [])
    if len(user_history) < count:
        await send_temp(ctx, f"❌ Нельзя отменить **{count}** изменений для {member.display_name}, так как доступно только **{len(user_history)}** записей.")
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
    await send_temp(ctx, embed=embed)
    await log_action_cancellation(ctx, member, undo_entries)

@bot.hybrid_command(
    name='monthlytop',
    description='Запустить начисление топа месяца'
)
@commands.has_permissions(administrator=True)
async def monthly_top(ctx):
    await run_monthly_top(ctx)

@bot.hybrid_command(
    name='tophistory',
    description='История начислений топов месяца'
)
async def tophistory_cmd(ctx, month: Optional[int] = None, year: Optional[int] = None):
    await tophistory(ctx, month, year)

@bot.hybrid_command(
    name='helpy',
    description='Показать список команд'
)
async def helpy_cmd(ctx):
    view = HelpView(ctx.author)
    embed = get_help_embed("points")
    await send_temp(ctx, embed=embed, view=view)

@bot.hybrid_command(description='Проверить работу бота')
async def ping(ctx):
    await send_temp(ctx, 'pong')
    
@bot.hybrid_command(
    name="bank",
    description='Показать баланс клуба'
)
async def bank_balance(ctx):
    total = db.get_bank_balance()
    await send_temp(ctx, f"🏦 Баланс банка: **{total:.2f} баллов**")

@bot.hybrid_command(
    name="bankadd",
    description='Добавить баллы в клубный банк'
)
@commands.has_permissions(administrator=True)
async def bank_add(ctx, amount: float, *, reason: str = "Без причины"):
    if amount <= 0:
        await send_temp(ctx, "❌ Сумма должна быть больше 0")
        return
    db.add_to_bank(amount)
    db.log_bank_income(ctx.author.id, amount, reason)
    await send_temp(ctx, f"✅ Добавлено **{amount:.2f} баллов** в банк. Причина: {reason}")

@bot.hybrid_command(
    name="bankspend",
    description='Потратить баллы из банка'
)
@commands.has_permissions(administrator=True)
async def bank_spend(ctx, amount: float, *, reason: str = "Без причины"):
    if amount <= 0:
        await send_temp(ctx, "❌ Сумма должна быть больше 0")
        return
    success = db.spend_from_bank(amount, ctx.author.id, reason)
    if success:
        await send_temp(ctx, f"💸 Из банка потрачено **{amount:.2f} баллов**. Причина: {reason}")
    else:
        await send_temp(ctx, "❌ Недостаточно средств в банке или ошибка операции")

@bot.hybrid_command(
    name="bankhistory",
    description='История операций клуба'
)
@commands.has_permissions(administrator=True)
async def bank_history(ctx):
    if not db.supabase:
        await send_temp(ctx, "❌ Supabase не инициализирован")
        return

    try:
        result = db.supabase.table("bank_history").select("*").order("timestamp", desc=True).limit(10).execute()
        if not result.data:
            await send_temp(ctx, "📭 История пуста")
            return
        embed = discord.Embed(title="📚 История операций банка", color=discord.Color.teal())
        for entry in result.data:
            user = ctx.guild.get_member(entry["user_id"])
            name = user.display_name if user else f"<@{entry['user_id']}>"
            amt = entry["amount"]
            ts = entry["timestamp"][:19].replace("T", " ")
            embed.add_field(
                name=f"{'➕' if amt > 0 else '➖'} {amt:.2f} баллов • {ts}",
                value=f"👤 {name}\n📝 {entry['reason']}",
                inline=False
            )
        await send_temp(ctx, embed=embed)
    except Exception as e:
        await send_temp(ctx, f"❌ Ошибка получения истории: {str(e)}")

@bot.hybrid_command(
    name="balance",
    description='Показать баланс пользователя'
)
async def balance(ctx, member: discord.Member = None):
    member = member or ctx.author
    embed = build_balance_embed(member)
    await send_temp(ctx, embed=embed)
