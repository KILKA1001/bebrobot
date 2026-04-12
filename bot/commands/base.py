"""
Назначение: модуль "base" реализует продуктовый контур в зоне Discord.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Discord.
Пользовательский вход: вызовы slash-команд и их базовые сценарии.
"""

import asyncio
import discord
from discord.ext import commands
from aiohttp import TraceConfig
from typing import Any, Optional
import logging

from bot.data import db
from bot.utils.roles_and_activities import (
    ACTIVITY_CATEGORIES,
    display_last_edit_date,
)
from bot.systems import render_history, log_action_cancellation
from bot.systems.core_logic import (
    _get_action_rows_for_account,
    _resolve_account_id_from_discord,
    update_roles,
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
from bot.systems.roles_catalog_shared import (
    ROLES_CATALOG_EMPTY_TEXT,
    ROLES_CATALOG_ERROR_TEXT,
    ROLES_CATALOG_FOOTER_TEXT,
    ROLES_CATALOG_TITLE,
    build_roles_catalog_intro_lines,
    format_roles_catalog_category_title,
    prepare_public_roles_catalog_pages,
    build_role_visual_tags,
)
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
DISCORD_EMBED_DESCRIPTION_LIMIT = 4096
DISCORD_EMBED_FIELD_NAME_LIMIT = 256
DISCORD_EMBED_FIELD_VALUE_LIMIT = 1024
DISCORD_EMBED_FIELD_COUNT_LIMIT = 25

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
    name="top", description="Показать рейтинг по баллам"
)
async def top(ctx):
    try:
        view = LeaderboardView(ctx)
        await send_temp(ctx, embed=view.get_embed(), view=view)
    except Exception:
        logger.exception(
            "leaderboard command failed platform=%s actor_id=%s guild_id=%s mode=%s",
            "discord",
            ctx.author.id if ctx.author else None,
            ctx.guild.id if ctx.guild else None,
            "all",
        )
        await send_temp(ctx, "❌ Не удалось открыть рейтинг. Подробности записаны в консоль.")


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


def _prepare_discord_roles_catalog_pages(guild: discord.Guild | None) -> dict[str, object]:
    try:
        grouped = RoleManagementService.list_public_roles_catalog(
            role_name_resolver=lambda role_id: guild.get_role(role_id).name if guild and guild.get_role(role_id) else None,
            log_context="/roles",
        )
    except Exception:
        logger.exception(
            "roles catalog discord load failed command=/roles source=discord_user_command guild_id=%s",
            guild.id if guild else None,
        )
        return {"status": "error", "pages": [], "message": ROLES_CATALOG_ERROR_TEXT}

    if not grouped:
        return {"status": "empty", "pages": [], "message": ROLES_CATALOG_EMPTY_TEXT}

    return {
        "status": "ok",
        "pages": prepare_public_roles_catalog_pages(grouped, max_roles_per_page=8, log_context="discord:/roles"),
        "message": "",
    }


def _validate_roles_catalog_embed_limits(
    *,
    page_data: dict[str, Any],
    description: str,
    fields: list[tuple[str, str]],
) -> None:
    if len(description) > DISCORD_EMBED_DESCRIPTION_LIMIT:
        logger.error(
            "roles catalog discord embed description exceeds limit page=%s total_pages=%s description_len=%s limit=%s",
            int(page_data.get("page_index") or 0) + 1,
            page_data.get("total_pages"),
            len(description),
            DISCORD_EMBED_DESCRIPTION_LIMIT,
        )
        raise ValueError("roles catalog embed description exceeds limit")

    if len(fields) > DISCORD_EMBED_FIELD_COUNT_LIMIT:
        logger.error(
            "roles catalog discord embed field count exceeds limit page=%s field_count=%s limit=%s",
            int(page_data.get("page_index") or 0) + 1,
            len(fields),
            DISCORD_EMBED_FIELD_COUNT_LIMIT,
        )
        raise ValueError("roles catalog embed field count exceeds limit")

    for field_name, field_value in fields:
        if len(field_name) > DISCORD_EMBED_FIELD_NAME_LIMIT or len(field_value) > DISCORD_EMBED_FIELD_VALUE_LIMIT:
            logger.error(
                "roles catalog discord embed field exceeds limit page=%s field_name_len=%s field_value_len=%s category_count=%s role_count=%s limit_name=%s limit_value=%s",
                int(page_data.get("page_index") or 0) + 1,
                len(field_name),
                len(field_value),
                page_data.get("section_count"),
                page_data.get("role_count"),
                DISCORD_EMBED_FIELD_NAME_LIMIT,
                DISCORD_EMBED_FIELD_VALUE_LIMIT,
            )
            raise ValueError("roles catalog embed field exceeds limit")


def _resolve_discord_role_title(role: dict[str, Any], guild: discord.Guild | None) -> str:
    role_name = str(role.get("name") or "Без названия")
    discord_role_id = str(role.get("discord_role_id") or "").strip()
    if not discord_role_id:
        return role_name
    if not guild:
        logger.info("roles catalog discord role mention fallback: guild missing role_name=%s role_id=%s", role_name, discord_role_id)
        return role_name
    try:
        guild_role = guild.get_role(int(discord_role_id))
    except (TypeError, ValueError):
        logger.warning(
            "roles catalog discord role mention fallback: invalid role id role_name=%s role_id=%s",
            role_name,
            discord_role_id,
        )
        return role_name
    if not guild_role:
        logger.info(
            "roles catalog discord role mention fallback: guild role not found role_name=%s role_id=%s guild_id=%s",
            role_name,
            discord_role_id,
            guild.id,
        )
        return role_name
    return guild_role.mention


def _build_discord_roles_catalog_embed(page_data: dict[str, Any], guild: discord.Guild | None = None) -> discord.Embed:
    current_page = max(int(page_data.get("page_index") or 0) + 1, 1)
    total_pages = max(int(page_data.get("total_pages") or 1), 1)
    description_lines: list[str] = []
    for line in build_roles_catalog_intro_lines(current_page=current_page, total_pages=total_pages):
        if ": " in line:
            label, value = line.split(": ", maxsplit=1)
            description_lines.append(f"**{label}:** {value}")
        else:
            description_lines.append(line)
    description = "\n".join(description_lines)

    fields: list[tuple[str, str]] = []
    for item in page_data.get("sections") or []:
        field_name = format_roles_catalog_category_title(item)
        roles = item.get("items") or []
        if not roles:
            fields.append((field_name, "Пока нет ролей."))
            continue

        lines = []
        for role in roles:
            role_title = _resolve_discord_role_title(role, guild)
            role_description = str(role.get("description") or "").strip() or ROLE_DESCRIPTION_PLACEHOLDER
            acquire_method = str(role.get("acquire_method_label") or "Не указан").strip()
            acquire_hint = str(role.get("acquire_hint") or "").strip() or USER_ACQUIRE_HINT_PLACEHOLDER
            visual_tags = build_role_visual_tags(role)
            lines.append(
                f"**{role_title}**\n"
                f"Метки: `{visual_tags}`\n"
                f"Описание: {role_description}\n"
                f"Способ получения: {acquire_method}\n"
                f"Как получить: {acquire_hint}"
            )
        fields.append((field_name, "\n\n".join(lines)))

    _validate_roles_catalog_embed_limits(page_data=page_data, description=description, fields=fields)

    embed = discord.Embed(
        title=ROLES_CATALOG_TITLE,
        description=description,
        color=discord.Color.purple(),
    )
    for field_name, field_value in fields:
        embed.add_field(name=field_name, value=field_value, inline=False)
    embed.set_footer(text=ROLES_CATALOG_FOOTER_TEXT)
    return embed


def _build_discord_roles_catalog_empty_embed() -> discord.Embed:
    embed = discord.Embed(
        title=ROLES_CATALOG_TITLE,
        description=ROLES_CATALOG_EMPTY_TEXT,
        color=discord.Color.purple(),
    )
    embed.set_footer(text=ROLES_CATALOG_FOOTER_TEXT)
    return embed


class RolesCatalogDiscordView(discord.ui.View):
    def __init__(self, *, author_id: int, guild: discord.Guild | None, page_index: int = 0):
        super().__init__(timeout=300)
        self.author_id = int(author_id)
        self.guild = guild
        self.page_index = max(int(page_index), 0)
        self.total_pages = 1
        self._sync_buttons()

    def _sync_buttons(self) -> None:
        self.page_index = min(max(self.page_index, 0), max(self.total_pages - 1, 0))
        self.prev_button.disabled = self.page_index <= 0
        self.next_button.disabled = self.page_index >= self.total_pages - 1

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            logger.error(
                "roles catalog discord foreign interaction denied owner_id=%s actor_id=%s guild_id=%s page=%s",
                self.author_id,
                interaction.user.id,
                interaction.guild.id if interaction.guild else None,
                self.page_index + 1,
            )
            await interaction.response.send_message("❌ Это меню открыто для другого пользователя.", ephemeral=True)
            return False
        return True

    async def _load_page_embed(self, *, requested_page: int) -> discord.Embed:
        payload = await asyncio.to_thread(_prepare_discord_roles_catalog_pages, self.guild)
        status = str(payload.get("status") or "")
        if status == "error":
            raise RuntimeError(str(payload.get("message") or ROLES_CATALOG_ERROR_TEXT))
        if status == "empty":
            self.total_pages = 1
            self.page_index = 0
            self._sync_buttons()
            return _build_discord_roles_catalog_empty_embed()

        pages = list(payload.get("pages") or [])
        if not pages:
            logger.error(
                "roles catalog discord page build failed empty_pages guild_id=%s requested_page=%s",
                self.guild.id if self.guild else None,
                requested_page + 1,
            )
            raise RuntimeError(ROLES_CATALOG_ERROR_TEXT)

        self.total_pages = len(pages)
        self.page_index = min(max(int(requested_page), 0), len(pages) - 1)
        self._sync_buttons()
        page_data = pages[self.page_index]
        try:
            return _build_discord_roles_catalog_embed(page_data, self.guild)
        except Exception:
            logger.exception(
                "roles catalog discord page build failed guild_id=%s page=%s total_pages=%s category_count=%s role_count=%s",
                self.guild.id if self.guild else None,
                int(page_data.get("page_index") or 0) + 1,
                page_data.get("total_pages"),
                page_data.get("section_count"),
                page_data.get("role_count"),
            )
            raise

    async def _update_message(self, interaction: discord.Interaction, *, requested_page: int, action: str) -> None:
        try:
            embed = await self._load_page_embed(requested_page=requested_page)
            self._sync_buttons()
            await interaction.response.edit_message(embed=embed, view=self)
        except Exception:
            logger.exception(
                "roles catalog discord view update failed guild_id=%s actor_id=%s action=%s requested_page=%s",
                interaction.guild.id if interaction.guild else None,
                interaction.user.id,
                action,
                requested_page + 1,
            )
            if interaction.response.is_done():
                await interaction.followup.send(ROLES_CATALOG_ERROR_TEXT, ephemeral=True)
            else:
                await interaction.response.send_message(ROLES_CATALOG_ERROR_TEXT, ephemeral=True)

    @discord.ui.button(label="⬅️", style=discord.ButtonStyle.secondary)
    async def prev_button(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self._update_message(interaction, requested_page=self.page_index - 1, action="previous")

    @discord.ui.button(label="🔄", style=discord.ButtonStyle.secondary)
    async def refresh_button(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self._update_message(interaction, requested_page=self.page_index, action="refresh")

    @discord.ui.button(label="➡️", style=discord.ButtonStyle.secondary)
    async def next_button(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self._update_message(interaction, requested_page=self.page_index + 1, action="next")


@bot.hybrid_command(
    name="roles", description="Каталог ролей по категориям и способам получения"
)
async def roles_list(ctx):
    payload = await asyncio.to_thread(_prepare_discord_roles_catalog_pages, ctx.guild)
    status = str(payload.get("status") or "")
    if status == "error":
        await send_temp(ctx, ROLES_CATALOG_ERROR_TEXT)
        return
    if status == "empty":
        await send_temp(ctx, embed=_build_discord_roles_catalog_empty_embed())
        return

    pages = list(payload.get("pages") or [])
    if not pages:
        logger.error("roles catalog discord initial render failed empty_pages guild_id=%s", ctx.guild.id if ctx.guild else None)
        await send_temp(ctx, ROLES_CATALOG_ERROR_TEXT)
        return

    try:
        embed = _build_discord_roles_catalog_embed(pages[0], ctx.guild)
    except Exception:
        logger.exception(
            "roles catalog discord initial page build failed guild_id=%s page=%s",
            ctx.guild.id if ctx.guild else None,
            int(pages[0].get("page_index") or 0) + 1,
        )
        await send_temp(ctx, ROLES_CATALOG_ERROR_TEXT)
        return

    view = RolesCatalogDiscordView(author_id=ctx.author.id, guild=ctx.guild, page_index=0)
    view.total_pages = len(pages)
    view._sync_buttons()
    await send_temp(ctx, embed=embed, view=view)


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


@bot.hybrid_command(name="helpy", description="Показать список команд")
async def helpy_cmd(ctx):
    view = HelpView(ctx.author)
    embed = get_help_embed("points", visibility=view.visibility)
    await send_temp(ctx, embed=embed, view=view)


@bot.hybrid_command(description="Проверить работу бота")
async def ping(ctx):
    await send_temp(ctx, "pong")


def _is_dm_context(ctx: commands.Context) -> bool:
    return ctx.guild is None


def _is_super_admin_discord_user(user_id: int) -> bool:
    return AuthorityService.is_super_admin("discord", str(user_id))


def _build_bank_balance_embed(*, actor_id: int | None = None) -> discord.Embed:
    total = db.get_bank_balance()
    embed = discord.Embed(
        title="🏦 Банк клуба",
        color=discord.Color.gold(),
        description=(
            f"Текущий баланс: {total:.2f}\n"
            "Настройки для суперадминов в лс"
        ),
    )
    if actor_id is not None:
        embed.set_footer(text=f"Запросил: {actor_id}")
    return embed


def _load_bank_history_rows(limit: int = 10) -> list[dict[str, Any]]:
    if not db.supabase:
        return []
    result = (
        db.supabase.table("bank_history")
        .select("*")
        .order("timestamp", desc=True)
        .limit(max(1, min(int(limit), 20)))
        .execute()
    )
    return [row for row in (result.data or []) if isinstance(row, dict)]


def _build_bank_history_embed(rows: list[dict[str, Any]]) -> discord.Embed:
    embed = discord.Embed(title="📚 История операций банка", color=discord.Color.teal())
    if not rows:
        embed.description = (
            "Что это: последние операции банка.\n"
            "Что делать сейчас: операций пока нет.\n"
            "Что будет дальше: после пополнения или списания записи появятся в этом списке."
        )
        return embed

    embed.description = (
        "Что это: последние операции банка.\n"
        "Что делать сейчас: проверьте сумму и причину каждой операции.\n"
        "Что будет дальше: новые операции будут добавляться сверху."
    )
    for entry in rows:
        amt = float(entry.get("amount") or 0)
        ts = str(entry.get("timestamp") or "").replace("T", " ")[:19]
        user_id = entry.get("user_id")
        name = f"<@{user_id}>" if user_id is not None else "Неизвестно"
        embed.add_field(
            name=f"{'➕' if amt >= 0 else '➖'} {amt:.2f} баллов • {ts}",
            value=f"👤 {name}\n📝 {entry.get('reason') or 'Без причины'}",
            inline=False,
        )
    return embed


class BankActionModal(discord.ui.Modal):
    def __init__(self, *, actor_id: int, operation: str):
        title = "Пополнение банка" if operation == "add" else "Списание из банка"
        super().__init__(title=title)
        self.actor_id = actor_id
        self.operation = operation
        self.amount = discord.ui.TextInput(
            label="Сумма",
            placeholder="Например: 25.5",
            required=True,
            max_length=32,
        )
        self.reason = discord.ui.TextInput(
            label="Причина",
            placeholder="Коротко опишите причину операции",
            required=True,
            style=discord.TextStyle.paragraph,
            max_length=400,
        )
        self.add_item(self.amount)
        self.add_item(self.reason)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        try:
            if not _is_super_admin_discord_user(interaction.user.id):
                logger.warning(
                    "bank modal denied: super-admin required actor_id=%s operation=%s",
                    interaction.user.id,
                    self.operation,
                )
                await interaction.response.send_message(
                    "❌ Настройка банка доступна только суперадмину в личных сообщениях.",
                    ephemeral=True,
                )
                return
            if interaction.guild is not None:
                logger.warning(
                    "bank modal denied: non-dm context actor_id=%s guild_id=%s operation=%s",
                    interaction.user.id,
                    interaction.guild.id,
                    self.operation,
                )
                await interaction.response.send_message(
                    "❌ Настройка банка доступна только в ЛС с ботом.",
                    ephemeral=True,
                )
                return

            amount = float(str(self.amount.value).replace(",", "."))
            if amount <= 0:
                await interaction.response.send_message("❌ Сумма должна быть больше 0.", ephemeral=True)
                return

            reason = str(self.reason.value).strip()
            if not reason:
                await interaction.response.send_message("❌ Причина обязательна.", ephemeral=True)
                return

            if self.operation == "add":
                ok = db.add_to_bank_with_history(self.actor_id, amount, reason)
                action_line = f"✅ В банк добавлено **{amount:.2f}** баллов.\nПричина: {reason}"
            else:
                ok = db.spend_from_bank(amount, self.actor_id, reason)
                action_line = f"💸 Из банка списано **{amount:.2f}** баллов.\nПричина: {reason}"

            if not ok:
                logger.error(
                    "bank modal operation failed actor_id=%s operation=%s amount=%s reason=%s",
                    self.actor_id,
                    self.operation,
                    amount,
                    reason,
                )
                await interaction.response.send_message(
                    "❌ Операция не выполнена. Проверьте доступ, баланс и логи сервера.",
                    ephemeral=True,
                )
                return

            logger.info(
                "bank modal operation success actor_id=%s operation=%s amount=%s reason=%s",
                self.actor_id,
                self.operation,
                amount,
                reason,
            )
            embed = _build_bank_balance_embed(actor_id=self.actor_id)
            await interaction.response.send_message(action_line, embed=embed, ephemeral=True)
        except ValueError:
            logger.exception(
                "bank modal amount parse failed actor_id=%s operation=%s amount_raw=%s",
                self.actor_id,
                self.operation,
                self.amount.value,
            )
            if interaction.response.is_done():
                await interaction.followup.send("❌ Некорректный формат суммы.", ephemeral=True)
            else:
                await interaction.response.send_message("❌ Некорректный формат суммы.", ephemeral=True)
        except Exception:
            logger.exception(
                "bank modal submit failed actor_id=%s operation=%s",
                self.actor_id,
                self.operation,
            )
            if interaction.response.is_done():
                await interaction.followup.send("❌ Ошибка выполнения банковой операции.", ephemeral=True)
            else:
                await interaction.response.send_message("❌ Ошибка выполнения банковой операции.", ephemeral=True)


class BankSettingsView(discord.ui.View):
    def __init__(self, *, owner_id: int):
        super().__init__(timeout=300)
        self.owner_id = owner_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.owner_id:
            logger.warning(
                "bank settings foreign actor denied owner_id=%s actor_id=%s",
                self.owner_id,
                interaction.user.id,
            )
            await interaction.response.send_message("❌ Эта панель открыта для другого администратора.", ephemeral=True)
            return False
        if interaction.guild is not None:
            logger.warning(
                "bank settings denied non-dm actor_id=%s guild_id=%s",
                interaction.user.id,
                interaction.guild.id,
            )
            await interaction.response.send_message("❌ Настройка банка доступна только в ЛС с ботом.", ephemeral=True)
            return False
        if not _is_super_admin_discord_user(interaction.user.id):
            logger.warning("bank settings denied non-super-admin actor_id=%s", interaction.user.id)
            await interaction.response.send_message(
                "❌ Настройка банка доступна только суперадмину.",
                ephemeral=True,
            )
            return False
        return True

    @discord.ui.button(label="➕ Добавить в банк", style=discord.ButtonStyle.success)
    async def add_button(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interaction.response.send_modal(BankActionModal(actor_id=self.owner_id, operation="add"))

    @discord.ui.button(label="➖ Списать из банка", style=discord.ButtonStyle.danger)
    async def spend_button(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interaction.response.send_modal(BankActionModal(actor_id=self.owner_id, operation="spend"))

    @discord.ui.button(label="📚 История банка", style=discord.ButtonStyle.secondary)
    async def history_button(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        try:
            rows = await asyncio.to_thread(_load_bank_history_rows, 10)
            embed = _build_bank_history_embed(rows)
            await interaction.response.send_message(embed=embed, ephemeral=True)
        except Exception:
            logger.exception("bank settings history render failed actor_id=%s", interaction.user.id)
            if interaction.response.is_done():
                await interaction.followup.send("❌ Не удалось открыть историю банка.", ephemeral=True)
            else:
                await interaction.response.send_message("❌ Не удалось открыть историю банка.", ephemeral=True)


class BankRootView(discord.ui.View):
    def __init__(self, *, owner_id: int):
        super().__init__(timeout=300)
        self.owner_id = owner_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.owner_id:
            logger.warning(
                "bank root foreign actor denied owner_id=%s actor_id=%s",
                self.owner_id,
                interaction.user.id,
            )
            await interaction.response.send_message("❌ Эта панель открыта для другого пользователя.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="⚙️ Настройка банка", style=discord.ButtonStyle.secondary)
    async def open_settings(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if interaction.guild is not None:
            logger.warning(
                "bank settings button hidden path triggered in guild actor_id=%s guild_id=%s",
                interaction.user.id,
                interaction.guild.id,
            )
            await interaction.response.send_message("❌ Настройка банка доступна только в ЛС с ботом.", ephemeral=True)
            return
        if not _is_super_admin_discord_user(interaction.user.id):
            logger.warning("bank settings open denied non-super-admin actor_id=%s", interaction.user.id)
            await interaction.response.send_message("❌ Настройка банка доступна только суперадмину.", ephemeral=True)
            return
        await interaction.response.send_message(
            "Выберите действие с балансом банка. Для каждой операции причина обязательна.",
            view=BankSettingsView(owner_id=self.owner_id),
            ephemeral=True,
        )


@bot.hybrid_command(name="bank", description="Показать баланс клуба")
async def bank_balance(ctx):
    try:
        if _is_dm_context(ctx):
            if not _is_super_admin_discord_user(ctx.author.id):
                logger.warning("bank command denied in dm for non-super-admin actor_id=%s", ctx.author.id)
                await send_temp(
                    ctx,
                    "❌ Экран настройки банка в ЛС доступен только суперадмину.\n"
                    "Что делать сейчас: обратитесь к суперадмину.\n"
                    "Что будет дальше: суперадмин сможет выполнить пополнение или списание через кнопку настройки.",
                )
                return
            show_settings = True
        else:
            logger.info(
                "bank command public-view mode actor_id=%s guild_id=%s",
                ctx.author.id,
                ctx.guild.id if ctx.guild else None,
            )
            show_settings = False

        logger.info("bank command opened actor_id=%s guild_id=%s", ctx.author.id, ctx.guild.id if ctx.guild else None)
        embed = _build_bank_balance_embed(actor_id=ctx.author.id)
        view = BankRootView(owner_id=ctx.author.id) if show_settings else None
        if not show_settings:
            logger.info(
                "bank command rendered without settings button actor_id=%s guild_id=%s reason=%s",
                ctx.author.id,
                ctx.guild.id if ctx.guild else None,
                "non_dm_or_not_super_admin",
            )
        await send_temp(ctx, embed=embed, view=view, delete_after=None)
    except Exception:
        logger.exception("bank command failed actor_id=%s guild_id=%s", ctx.author.id, ctx.guild.id if ctx.guild else None)
        await send_temp(ctx, "❌ Не удалось открыть панель банка. Попробуйте ещё раз позже.")

@bot.hybrid_command(name="balance", description="Показать баланс пользователя")
async def balance(ctx, member: discord.Member = None):
    member = member or ctx.author
    embed = build_balance_embed(member, ctx.guild)
    await send_temp(ctx, embed=embed)
