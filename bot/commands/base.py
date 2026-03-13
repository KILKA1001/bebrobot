import discord
from discord.ext import commands
from aiohttp import TraceConfig
from typing import Optional
import os
import logging

from bot.data import db
from bot.utils.roles_and_activities import (
    ACTIVITY_CATEGORIES,
    ROLE_THRESHOLDS,
    display_last_edit_date,
)
from bot.systems import render_history, log_action_cancellation, tophistory
from bot.systems.core_logic import (
    update_roles,
    run_monthly_top,
    get_help_embed,
    HelpView,
    LeaderboardView,
    build_balance_embed,
)
from bot.utils import send_temp, format_moscow_time, format_points
from bot.utils.api_monitor import monitor
from bot.services import PointsService, AuthorityService
from bot import COMMAND_PREFIX


# Константы
DATE_FORMAT = "%d-%m-%Y"  # 25-12-2023
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"  # Для сортировки

active_timers = {}

intents = discord.Intents.default()
intents.members = True
intents.message_content = True

trace_config = TraceConfig()
logger = logging.getLogger(__name__)

# Дополнительные роли, которым разрешено начислять и снимать баллы
POINTS_ROLE_IDS = tuple(
    int(r) for r in os.getenv("POINTS_ROLE_IDS", "").split(",") if r
)


@trace_config.on_request_end.append
async def _trace_request_end(session, ctx, params):
    monitor.record_request(params.response.status)


bot = commands.Bot(
    command_prefix=COMMAND_PREFIX,
    intents=intents,
    help_command=None,
    http_trace=trace_config,
)


@bot.before_invoke
async def show_loading_state(ctx: commands.Context):
    """Показывает Discord-индикатор загрузки для slash/hybrid-команд."""
    if not ctx.interaction:
        return
    if ctx.interaction.response.is_done():
        return
    try:
        await ctx.defer()
    except discord.HTTPException:
        pass


def has_points_permission(ctx: commands.Context) -> bool:
    """Check if user can modify points."""
    if ctx.author.guild_permissions.administrator:
        return True
    if any(role.id in POINTS_ROLE_IDS for role in ctx.author.roles):
        return True
    return AuthorityService.has_command_permission("discord", str(ctx.author.id), "points_manage")


async def _check_command_authority(ctx: commands.Context, command_key: str, target: discord.Member | None = None) -> bool:
    if ctx.author.guild_permissions.administrator:
        return True
    if not AuthorityService.has_command_permission("discord", str(ctx.author.id), command_key):
        await send_temp(ctx, "❌ Недостаточно полномочий для этой команды.")
        return False
    if target:
        if target.id == ctx.author.id:
            if not AuthorityService.can_manage_self("discord", str(ctx.author.id)):
                await send_temp(ctx, "❌ Нельзя редактировать себя. Доступно только Главе клуба и Главному вице.")
                return False
        elif not AuthorityService.can_manage_target("discord", str(ctx.author.id), "discord", str(target.id)):
            await send_temp(ctx, "❌ Нельзя выполнять действия над пользователем с равным/более высоким званием.")
            return False
    return True





@bot.hybrid_command(name="addpoints", description="Начислить баллы участнику")
@commands.check(has_points_permission)
async def add_points(
    ctx, member: discord.Member, points: str, *, reason: str = "Без причины"
):
    if not await _check_command_authority(ctx, "points_manage", member):
        return
    try:
        points_float = float(points.replace(",", "."))
        user_id = member.id
        PointsService.add_points(user_id, points_float, reason, ctx.author.id)
        await update_roles(member)
        embed = discord.Embed(
            title="🎉 Баллы начислены!", color=discord.Color.green()
        )
        embed.add_field(
            name="👤 Пользователь:", value=member.mention, inline=False
        )
        embed.add_field(
            name="➕ Количество:", value=f"**{points}** баллов", inline=False
        )
        embed.add_field(name="📝 Причина:", value=reason, inline=False)
        embed.add_field(
            name="🕒 Время:", value=format_moscow_time(), inline=False
        )
        embed.add_field(
            name="🎯 Текущий баланс:",
            value=f"{format_points(db.scores[user_id])} баллов",
            inline=False,
        )
        await send_temp(ctx, embed=embed, delete_after=None)
    except ValueError:
        logger.exception("add_points invalid value author_id=%s target_id=%s points=%s", ctx.author.id, member.id, points)
        await send_temp(ctx, "Ошибка: введите корректное число", delete_after=None)


@bot.hybrid_command(name="removepoints", description="Снять баллы у участника")
@commands.check(has_points_permission)
async def remove_points(
    ctx, member: discord.Member, points: str, *, reason: str = "Без причины"
):
    if not await _check_command_authority(ctx, "points_manage", member):
        return
    try:
        points_float = float(points.replace(",", "."))
        if points_float <= 0:
            await send_temp(
                ctx,
                "❌ Ошибка: введите число больше 0 для снятия баллов.",
                delete_after=None,
            )
            return
        user_id = member.id
        current_points = db.scores.get(user_id, 0)
        if points_float > current_points:
            embed = discord.Embed(
                title="⚠️ Недостаточно баллов",
                description=(
                    f"У {member.mention} только {current_points} баллов"
                ),
                color=discord.Color.red(),
            )
            await send_temp(ctx, embed=embed, delete_after=None)
            return
        PointsService.remove_points(user_id, points_float, reason, ctx.author.id)
        await update_roles(member)
        embed = discord.Embed(
            title="⚠️ Баллы сняты!", color=discord.Color.red()
        )
        embed.add_field(
            name="👤 Пользователь:", value=member.mention, inline=False
        )
        embed.add_field(
            name="➖ Снято баллов:", value=f"**{points_float}**", inline=False
        )
        embed.add_field(name="📝 Причина:", value=reason, inline=False)
        embed.add_field(
            name="🕒 Время:", value=format_moscow_time(), inline=False
        )
        embed.add_field(
            name="🎯 Текущий баланс:",
            value=f"{format_points(db.scores[user_id])} баллов",
            inline=False,
        )
        await send_temp(ctx, embed=embed, delete_after=None)
    except ValueError:
        logger.exception("remove_points invalid value author_id=%s target_id=%s points=%s", ctx.author.id, member.id, points)
        await send_temp(
            ctx, "Ошибка: введите корректное число больше 0", delete_after=None
        )


@bot.hybrid_command(
    name="leaderboard", description="Показать общий рейтинг по баллам"
)
async def leaderboard(ctx):
    view = LeaderboardView(ctx)
    await send_temp(ctx, embed=view.get_embed(), view=view)


@bot.hybrid_command(
    name="history", description="История действий пользователя"
)
async def history_cmd(
    ctx, member: Optional[discord.Member] = None, page: int = 1
):
    if member is None:
        member = ctx.author
    if member:
        await render_history(ctx, member, page)
    else:
        await send_temp(ctx, "Не удалось определить пользователя.")


@bot.hybrid_command(
    name="roles", description="Список ролей и стоимость в баллах"
)
async def roles_list(ctx):
    desc = ""
    for role_id, points_needed in sorted(
        ROLE_THRESHOLDS.items(), key=lambda x: x[1], reverse=True
    ):
        role = ctx.guild.get_role(role_id)
        if role:
            desc += f"**{role.name}**: {points_needed} баллов\n"
    embed = discord.Embed(
        title="Роли и стоимость баллов",
        description=desc,
        color=discord.Color.purple(),
    )
    await send_temp(ctx, embed=embed)


@bot.hybrid_command(
    name="activities", description="Виды помощи клубу и их стоимость"
)
async def activities_cmd(ctx):
    embed = discord.Embed(
        title="📋 Виды помощи клубу",
        description="Список всех видов деятельности и их стоимость в баллах:",
        color=discord.Color.blue(),
    )

    def get_points_word(points):
        if points % 10 == 1 and points % 100 != 11:
            return "балл"
        elif 2 <= points % 10 <= 4 and (
            points % 100 < 10 or points % 100 >= 20
        ):
            return "балла"
        else:
            return "баллов"

    for category_name, activities in ACTIVITY_CATEGORIES.items():
        category_text = ""
        for activity_name, info in activities.items():
            category_text += (
                f"**{activity_name}** "
                f"({info['points']} {get_points_word(info['points'])})\n"
            )
            category_text += f"↳ {info['description']}\n"
            if "conditions" in info:
                category_text += "Условия:\n"
                for condition in info["conditions"]:
                    category_text += f"• {condition}\n"
            category_text += "\n"
        embed.add_field(name=category_name, value=category_text, inline=False)
    embed.set_footer(text=display_last_edit_date())
    await send_temp(ctx, embed=embed)


@bot.hybrid_command(
    name="undo", description="Отменить последние начисления или списания"
)
async def undo(ctx, member: discord.Member, count: int = 1):
    if not await _check_command_authority(ctx, "undo_manage", member):
        return
    user_id = member.id
    user_history = db.history.get(user_id, [])
    if len(user_history) < count:
        await send_temp(
            ctx,
            (
                f"❌ Нельзя отменить **{count}** изменений для "
                f"{member.display_name}, так как доступно только "
                f"**{len(user_history)}** записей."
            ),
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
            is_undo=True,
        )

    if not user_history:
        del db.history[user_id]

    await update_roles(member)

    embed = discord.Embed(
        title=f"↩️ Отменено {count} изменений для {member.display_name}",
        color=discord.Color.orange(),
    )
    for i, (points_val, reason) in enumerate(undo_entries[::-1], start=1):
        sign = "+" if points_val > 0 else ""
        embed.add_field(
            name=f"{i}. {sign}{points_val} баллов", value=reason, inline=False
        )
    await send_temp(ctx, embed=embed)
    await log_action_cancellation(ctx, member, undo_entries)


@bot.hybrid_command(
    name="awardmonthtop", description="Начислить бонусы за выбранный месяц"
)
async def award_monthtop(ctx, month: Optional[int] = None, year: Optional[int] = None):
    if not await _check_command_authority(ctx, "monthtop_manage"):
        return
    await run_monthly_top(ctx, month, year)


@bot.hybrid_command(
    name="tophistory", description="История начислений топов месяца"
)
async def tophistory_cmd(
    ctx, month: Optional[int] = None, year: Optional[int] = None
):
    await tophistory(ctx, month, year)


@bot.hybrid_command(name="helpy", description="Показать список команд")
async def helpy_cmd(ctx):
    view = HelpView(ctx.author)
    embed = get_help_embed("points")
    await send_temp(ctx, embed=embed, view=view)


@bot.hybrid_command(description="Проверить работу бота")
async def ping(ctx):
    await send_temp(ctx, "pong")


@bot.hybrid_command(name="bank", description="Показать баланс клуба")
async def bank_balance(ctx):
    total = db.get_bank_balance()
    await send_temp(ctx, f"🏦 Баланс банка: **{total:.2f} баллов**")


@bot.hybrid_command(
    name="bankadd", description="Добавить баллы в клубный банк"
)
async def bank_add(ctx, amount: float, *, reason: str = "Без причины"):
    if not await _check_command_authority(ctx, "bank_manage"):
        return
    if amount <= 0:
        await send_temp(ctx, "❌ Сумма должна быть больше 0")
        return
    db.add_to_bank(amount)
    db.log_bank_income(ctx.author.id, amount, reason)
    await send_temp(
        ctx, f"✅ Добавлено **{amount:.2f} баллов** в банк. Причина: {reason}"
    )


@bot.hybrid_command(name="bankspend", description="Потратить баллы из банка")
async def bank_spend(ctx, amount: float, *, reason: str = "Без причины"):
    if not await _check_command_authority(ctx, "bank_manage"):
        return
    if amount <= 0:
        await send_temp(ctx, "❌ Сумма должна быть больше 0")
        return
    success = db.spend_from_bank(amount, ctx.author.id, reason)
    if success:
        await send_temp(
            ctx,
            f"💸 Из банка потрачено **{amount:.2f} баллов**. Причина: {reason}",
        )
    else:
        await send_temp(
            ctx, "❌ Недостаточно средств в банке или ошибка операции"
        )


@bot.hybrid_command(name="bankhistory", description="История операций клуба")
async def bank_history(ctx):
    if not await _check_command_authority(ctx, "bank_manage"):
        return
    if not db.supabase:
        await send_temp(ctx, "❌ Supabase не инициализирован")
        return

    try:
        result = (
            db.supabase.table("bank_history")
            .select("*")
            .order("timestamp", desc=True)
            .limit(10)
            .execute()
        )
        if not result.data:
            await send_temp(ctx, "📭 История пуста")
            return
        embed = discord.Embed(
            title="📚 История операций банка", color=discord.Color.teal()
        )
        for entry in result.data:
            user = ctx.guild.get_member(entry["user_id"])
            name = user.display_name if user else f"<@{entry['user_id']}>"
            amt = entry["amount"]
            ts = entry["timestamp"][:19].replace("T", " ")
            embed.add_field(
                name=f"{'➕' if amt > 0 else '➖'} {amt:.2f} баллов • {ts}",
                value=f"👤 {name}\n📝 {entry['reason']}",
                inline=False,
            )
        await send_temp(ctx, embed=embed)
    except Exception as e:
        logger.exception("bank_history failed author_id=%s", ctx.author.id)
        await send_temp(ctx, f"❌ Ошибка получения истории: {str(e)}")


@bot.hybrid_command(name="balance", description="Показать баланс пользователя")
async def balance(ctx, member: discord.Member = None):
    member = member or ctx.author
    embed = build_balance_embed(member)
    await send_temp(ctx, embed=embed)
