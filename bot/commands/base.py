import discord
from discord.ext import commands
from aiohttp import TraceConfig
from typing import Optional
import logging

from bot.data import db
from bot.utils.roles_and_activities import (
    ACTIVITY_CATEGORIES,
    display_last_edit_date,
)
from bot.systems import render_history, log_action_cancellation, tophistory
from bot.systems.core_logic import (
    _get_action_rows_for_account,
    _resolve_account_id_from_discord,
    update_roles,
    run_monthly_top,
    get_help_embed,
    HelpView,
    LeaderboardView,
    build_balance_embed,
)
from bot.legacy_identity_logging import log_legacy_identity_fallback_used
from bot.utils import send_temp
from bot.utils.api_monitor import monitor
from bot.services import AuthorityService, RoleManagementService
from bot.services.role_management_service import USER_ACQUIRE_HINT_PLACEHOLDER
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

ROLE_DESCRIPTION_PLACEHOLDER = "Описание пока не указано администратором"

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
    name="roles", description="Каталог ролей по категориям и способам получения"
)
async def roles_list(ctx):
    try:
        grouped = RoleManagementService.list_public_roles_catalog(
            role_name_resolver=lambda role_id: ctx.guild.get_role(role_id).name if ctx.guild and ctx.guild.get_role(role_id) else None,
            log_context="/roles",
        )
    except Exception:
        logger.exception("roles command failed command=/roles source=discord_user_command guild_id=%s", ctx.guild.id if ctx.guild else None)
        grouped = []

    embed = discord.Embed(
        title="🏅 Каталог ролей",
        description=(
            "Ниже собраны роли по категориям: с описанием, способом получения и практической подсказкой. "
            "Если описание или инструкция ещё не заполнены, бот прямо это покажет."
        ),
        color=discord.Color.purple(),
    )
    for item in grouped:
        lines = []
        for role in item.get("roles", []):
            role_name = str(role.get("name") or "Без названия")
            description = str(role.get("description") or "").strip() or ROLE_DESCRIPTION_PLACEHOLDER
            acquire_method = str(role.get("acquire_method_label") or "Не указан").strip()
            acquire_hint = str(role.get("acquire_hint") or "").strip() or USER_ACQUIRE_HINT_PLACEHOLDER
            lines.append(
                f"**{role_name}**\n"
                f"Описание: {description}\n"
                f"Способ получения: {acquire_method}\n"
                f"Как получить: {acquire_hint}"
            )
        embed.add_field(
            name=str(item.get("category") or "Без категории"),
            value="\n\n".join(lines) if lines else "Пока нет ролей.",
            inline=False,
        )

    if not grouped:
        embed.description = (
            "📭 Каталог ролей пока пуст.\n"
            "Когда администраторы добавят роли, здесь появятся категории, описания и инструкция по получению."
        )

    embed.set_footer(text="Если хочешь получить роль, ориентируйся на блок «Как получить» и при необходимости уточняй условия у администратора.")
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
    account_id = _resolve_account_id_from_discord(user_id, handler="undo")
    if account_id:
        user_history = _get_action_rows_for_account(
            account_id,
            discord_user_id=user_id,
            handler="undo",
        )
    else:
        log_legacy_identity_fallback_used(
            logger,
            module=__name__,
            handler="undo",
            field="discord_user_id",
            action="fallback_to_legacy_history_cache",
            continue_execution=True,
            discord_user_id=user_id,
            recommended_field="account_id",
            developer_hint="temporary compatibility path; resolve account_id before using undo history",
        )
        user_history = list(db.history.get(user_id, []))
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

    if user_id in db.history:
        legacy_history = list(db.history.get(user_id, []))
        if legacy_history:
            db.history[user_id] = legacy_history[:-count]
            if not db.history[user_id]:
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
    embed = build_balance_embed(member, ctx.guild)
    await send_temp(ctx, embed=embed)
