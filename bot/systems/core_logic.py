"""
Назначение: модуль "core logic" реализует продуктовый контур в зоне общая логика.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: общая логика.
"""

import asyncio
import discord
from dataclasses import dataclass
from typing import Optional
from datetime import datetime, timezone, timedelta
import pytz
import traceback
import logging

from bot.data import db
from bot.legacy_identity_logging import (
    log_identity_resolve_error,
    log_legacy_identity_path_detected,
    log_legacy_schema_fallback,
)
from bot.services import AccountsService, AuthorityService, PointsService
from bot.services.profile_titles import normalize_protected_profile_title
from bot.utils.roles_and_activities import ROLE_THRESHOLDS
from bot.utils import (
    send_temp,
    build_top_embed,
    SafeView,
    safe_send,
    format_moscow_time,
    format_points,
)

active_timers = {}
logger = logging.getLogger(__name__)


def _is_missing_column_error(error: Exception, *, table: str, column: str) -> bool:
    code = str(getattr(error, "code", "") or "").strip()
    if code == "42703":
        return True
    lowered = str(error).lower()
    return f"column {table}.{column} does not exist" in lowered


def _resolve_account_id_from_discord(discord_user_id: int, *, handler: str) -> str | None:
    log_legacy_identity_path_detected(
        logger,
        module=__name__,
        handler=handler,
        field="discord_user_id",
        action="resolve_account_id",
        continue_execution=True,
        provider="discord",
    )
    account_id = AccountsService.resolve_account_id("discord", str(discord_user_id))
    if account_id:
        return str(account_id)
    if hasattr(db, "_inc_metric"):
        db._inc_metric("identity_resolve_errors")
    log_identity_resolve_error(
        logger,
        module=__name__,
        handler=handler,
        field="discord_user_id",
        action="resolve_account_id",
        continue_execution=False,
        provider="discord",
        discord_user_id=discord_user_id,
    )
    return None


def _schedule_soft_identity_refresh_discord(user_obj, *, guild_id: int | None, source_handler: str) -> None:
    async def _runner() -> None:
        try:
            AccountsService.refresh_identity_from_platform_user(
                "discord",
                user_obj,
                source_handler=source_handler,
                guild_id=guild_id,
            )
        except Exception:
            logger.exception(
                "top soft identity refresh failed provider=%s provider_user_id=%s guild_id=%s source_handler=%s",
                "discord",
                getattr(user_obj, "id", None),
                guild_id,
                source_handler,
            )

    try:
        asyncio.get_running_loop().create_task(_runner())
        logger.info(
            "top soft identity refresh launched provider=%s provider_user_id=%s guild_id=%s source_handler=%s",
            "discord",
            getattr(user_obj, "id", None),
            guild_id,
            source_handler,
        )
    except RuntimeError:
        logger.warning(
            "top soft identity refresh skipped provider=%s provider_user_id=%s guild_id=%s source_handler=%s reason=%s",
            "discord",
            getattr(user_obj, "id", None),
            guild_id,
            source_handler,
            "event_loop_unavailable",
        )



def _ensure_core_data_loaded() -> None:
    if hasattr(db, "ensure_core_data_loaded"):
        db.ensure_core_data_loaded()


def _get_score_row_for_account(
    account_id: str,
    *,
    discord_user_id: int | None,
    handler: str,
) -> dict | None:
    if not account_id or not db.supabase:
        return None
    try:
        score_result = (
            db.supabase.table("scores")
            .select("points,tickets_normal,tickets_gold,account_id")
            .eq("account_id", str(account_id))
            .limit(1)
            .execute()
        )
        rows = score_result.data or []
        if rows:
            return rows[0]
    except Exception:
        logger.exception(
            "%s account-first score lookup failed account_id=%s discord_user_id=%s",
            handler,
            account_id,
            discord_user_id,
        )

    if discord_user_id is None:
        return None

    log_legacy_schema_fallback(
        logger,
        module=__name__,
        table="scores",
        field="user_id",
        action="migrate_scores_lookup_to_account_id",
        continue_execution=True,
        handler=handler,
        account_id=account_id,
        discord_user_id=discord_user_id,
        recommended_field="account_id",
        developer_hint="temporary compatibility path; migrate scores rows to scores.account_id",
    )
    try:
        score_result = (
            db.supabase.table("scores")
            .select("points,tickets_normal,tickets_gold,account_id")
            .eq("user_id", str(discord_user_id))
            .limit(1)
            .execute()
        )
        rows = score_result.data or []
        if rows:
            return rows[0]
    except Exception as error:
        if _is_missing_column_error(error, table="scores", column="user_id"):
            logger.warning(
                "%s legacy score fallback skipped because schema has no scores.user_id account_id=%s discord_user_id=%s error=%s",
                handler,
                account_id,
                discord_user_id,
                error,
            )
        else:
            logger.exception(
                "%s legacy score fallback failed account_id=%s discord_user_id=%s",
                handler,
                account_id,
                discord_user_id,
            )
    return None


def _normalize_history_entry(action: dict) -> dict:
    return {
        "points": float(action.get("points") or 0),
        "reason": action.get("reason") or "Не указана",
        "author_account_id": action.get("author_account_id"),
        "timestamp": action.get("timestamp"),
        "is_undo": bool(action.get("is_undo", False)),
    }


def _get_action_rows_for_account(
    account_id: str,
    *,
    discord_user_id: int | None,
    handler: str,
) -> list[dict]:
    _ensure_core_data_loaded()
    action_rows = list(getattr(db.actions, "data", db.actions) or [])
    if not action_rows:
        return []

    account_rows = [
        _normalize_history_entry(action)
        for action in action_rows
        if str(action.get("account_id") or "") == str(account_id)
    ]
    if account_rows:
        return account_rows

    if discord_user_id is None:
        return []

    legacy_rows = [
        _normalize_history_entry(action)
        for action in action_rows
        if str(action.get("user_id") or "") == str(discord_user_id)
    ]
    if legacy_rows:
        log_legacy_schema_fallback(
            logger,
            module=__name__,
            table="actions",
            field="user_id",
            action="migrate_history_lookup_to_actions_account_id",
            continue_execution=True,
            handler=handler,
            account_id=account_id,
            discord_user_id=discord_user_id,
            recommended_field="account_id",
            developer_hint="temporary compatibility path; backfill actions.account_id for history rendering",
        )
        return legacy_rows

    legacy_history = db.history.get(discord_user_id, [])
    if legacy_history:
        log_legacy_schema_fallback(
            logger,
            module=__name__,
            table="history_cache",
            field="user_id",
            action="replace_history_cache_with_account_first_actions",
            continue_execution=True,
            handler=handler,
            account_id=account_id,
            discord_user_id=discord_user_id,
            recommended_field="account_id",
            developer_hint="temporary compatibility path; rebuild history cache from actions.account_id rows",
        )
        return [_normalize_history_entry(action) for action in legacy_history]

    return []


def _get_balance_snapshot(
    account_id: str,
    *,
    discord_user_id: int | None,
    handler: str,
) -> tuple[float, dict]:
    score_row = _get_score_row_for_account(account_id, discord_user_id=discord_user_id, handler=handler) or {}
    if score_row:
        return float(score_row.get("points") or 0), score_row

    history_rows = _get_action_rows_for_account(account_id, discord_user_id=discord_user_id, handler=handler)
    if history_rows:
        return sum(float(row.get("points") or 0) for row in history_rows), score_row

    return 0.0, score_row


def _get_leaderboard_place(
    account_id: str,
    *,
    discord_user_id: int | None,
    handler: str,
) -> int | None:
    if account_id and db.supabase:
        try:
            score_result = (
                db.supabase.table("scores")
                .select("account_id,points")
                .order("points", desc=True)
                .execute()
            )
            score_rows = score_result.data or []
            for index, row in enumerate(score_rows, start=1):
                if str(row.get("account_id") or "") == str(account_id):
                    return index
        except Exception:
            logger.exception(
                "%s leaderboard lookup failed account_id=%s discord_user_id=%s",
                handler,
                account_id,
                discord_user_id,
            )

    if discord_user_id is None:
        return None

    sorted_scores = sorted(db.scores.items(), key=lambda x: x[1], reverse=True)
    place = next((i for i, (uid, _) in enumerate(sorted_scores, 1) if uid == discord_user_id), None)
    if place is not None:
        log_legacy_schema_fallback(
            logger,
            module=__name__,
            table="scores_cache",
            field="user_id",
            action="replace_cached_leaderboard_place_lookup_with_account_id",
            continue_execution=True,
            handler=handler,
            account_id=account_id,
            discord_user_id=discord_user_id,
            recommended_field="account_id",
            developer_hint="temporary compatibility path; derive leaderboard place from scores.account_id rows",
        )
    return place


def _build_missing_account_embed(member: discord.abc.User, *, title: str) -> discord.Embed:
    display_name = getattr(member, "display_name", getattr(member, "name", str(getattr(member, "id", "unknown"))))
    embed = discord.Embed(
        title=title,
        description=(
            "Не удалось найти связанный аккаунт для этого Discord-профиля.\n"
            "Чтобы баланс и история отображались корректно, зарегистрируйте аккаунт "
            "или привяжите его через `/link <код>`."
        ),
        color=discord.Color.orange(),
    )
    embed.set_author(
        name=display_name,
        icon_url=member.avatar.url if getattr(member, "avatar", None) else member.default_avatar.url,
    )
    return embed


async def update_roles(member: discord.Member):
    logger.debug(
        "update_roles skipped: volunteer roles are managed by shop purchases member_id=%s guild_id=%s threshold_roles=%s",
        getattr(member, "id", None),
        getattr(getattr(member, "guild", None), "id", None),
        len(ROLE_THRESHOLDS),
    )


class HistoryView(SafeView):
    def __init__(self, member: discord.Member, page: int, total_pages: int):
        super().__init__(timeout=60)
        self.member = member
        self.page = page
        self.total_pages = total_pages

        self.prev_button.disabled = page <= 1
        self.next_button.disabled = page >= total_pages

    @discord.ui.button(label="◀️ Назад", style=discord.ButtonStyle.gray, custom_id="prev")
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Переход на предыдущую страницу истории."""
        await interaction.response.defer()
        await render_history(interaction, self.member, self.page - 1)

    @discord.ui.button(label="Вперёд ▶️", style=discord.ButtonStyle.gray, custom_id="next")
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Переход на следующую страницу истории."""
        await interaction.response.defer()
        await render_history(interaction, self.member, self.page + 1)


async def render_history(ctx_or_interaction, member: discord.Member, page: int):
    account_id = None
    user_id = member.id
    try:
        entries_per_page = 5
        account_id = _resolve_account_id_from_discord(user_id, handler="render_history")
        if not account_id:
            embed = _build_missing_account_embed(member, title="📜 История баллов")
            if isinstance(ctx_or_interaction, discord.Interaction):
                await ctx_or_interaction.response.send_message(embed=embed, ephemeral=True)
            else:
                await ctx_or_interaction.send(embed=embed)
            return

        user_history = _get_action_rows_for_account(account_id, discord_user_id=user_id, handler="render_history")

        if not user_history:
            embed = discord.Embed(
                title="📜 История баллов",
                description=(
                    "```Записей не найдено```\n"
                    "Когда по аккаунту появятся начисления или списания, они отобразятся здесь."
                ),
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

        total_points, _ = _get_balance_snapshot(account_id, discord_user_id=user_id, handler="render_history")
        embed.add_field(
            name="💰 Текущий баланс",
            value=f"```{format_points(total_points)} баллов```",
            inline=False,
        )

        for action in page_actions:
            points = action.get("points", 0)
            emoji = "🟢" if points >= 0 else "🔴"
            if action.get("is_undo", False):
                emoji = "⚪"

            timestamp = action.get("timestamp")
            if isinstance(timestamp, str):
                try:
                    dt = datetime.fromisoformat(timestamp)
                    formatted_time = format_moscow_time(dt)
                except ValueError:
                    formatted_time = timestamp
            else:
                formatted_time = format_moscow_time(timestamp) if timestamp else "N/A"

            author_account_id = action.get('author_account_id')
            reason = action.get('reason', 'Не указана')
            author_line = (
                f"**Выдал (account_id):** `{author_account_id}`"
                if author_account_id
                else "**Выдал:** общий аккаунт"
            )

            field_name = f"{emoji} {formatted_time}"
            field_value = (
                f"```diff\n{'+' if points >= 0 else ''}{format_points(points)} баллов```\n"
                f"**Причина:** {reason}\n"
                f"{author_line}"
            )
            embed.add_field(name=field_name, value=field_value, inline=False)

        embed.set_footer(text=f"Страница {page}/{total_pages} • Всего записей: {total_entries}")

        view = HistoryView(member, page, total_pages)

        if isinstance(ctx_or_interaction, discord.Interaction):
            if ctx_or_interaction.response.is_done():
                await ctx_or_interaction.edit_original_response(embed=embed, view=view)
            else:
                await ctx_or_interaction.response.send_message(embed=embed, view=view)
                await ctx_or_interaction.original_response()
        else:
            await send_temp(ctx_or_interaction, embed=embed, view=view)

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
        logger.exception(
            "render_history failed discord_user_id=%s account_id=%s traceback=%s",
            user_id,
            account_id,
            traceback.format_exc(),
        )


async def log_action_cancellation(ctx, member: discord.Member, entries: list):
    channel = discord.utils.get(ctx.guild.channels, name='history-log')
    if not channel:
        return

    now = format_moscow_time()
    lines = [
        f"**{ctx.author.display_name}** отменил(а) {len(entries)} изменения для **{member.display_name}** ({member.id}) в {now}:"
    ]
    for i, (points, reason) in enumerate(entries[::-1], start=1):
        sign = "+" if points > 0 else ""
        lines.append(f"{i}. {sign}{format_points(points)} — {reason}")

    await safe_send(channel, "\n".join(lines))


@dataclass(frozen=True)
class HelpVisibilityContext:
    level: int = 0
    titles: tuple[str, ...] = tuple()
    is_administrator: bool = False
    can_use_rep: bool = False


def _normalize_help_titles(titles: tuple[str, ...]) -> set[str]:
    return {str(title).strip().lower() for title in titles}


def _resolve_help_visibility(user: discord.Member | discord.User | None) -> HelpVisibilityContext:
    if user is None:
        return HelpVisibilityContext()

    is_administrator = bool(getattr(getattr(user, "guild_permissions", None), "administrator", False))
    try:
        authority = AuthorityService.resolve_authority("discord", str(user.id))
        can_use_rep = AuthorityService.has_command_permission("discord", str(user.id), "moderation_mute")
    except Exception:
        logger.exception("discord help authority resolve failed actor_id=%s", getattr(user, "id", None))
        return HelpVisibilityContext(is_administrator=is_administrator)

    return HelpVisibilityContext(
        level=authority.level,
        titles=authority.titles,
        is_administrator=is_administrator,
        can_use_rep=can_use_rep,
    )


def _help_can_manage_points(visibility: HelpVisibilityContext) -> bool:
    return visibility.is_administrator or visibility.level >= 80


def _help_can_create_fines(visibility: HelpVisibilityContext) -> bool:
    return visibility.is_administrator or visibility.level >= 30


def _help_can_manage_fines(visibility: HelpVisibilityContext) -> bool:
    return visibility.is_administrator or visibility.level >= 80


def _help_can_manage_bank(visibility: HelpVisibilityContext) -> bool:
    return visibility.is_administrator or visibility.level >= 100


def _help_can_manage_tournaments(visibility: HelpVisibilityContext) -> bool:
    return visibility.is_administrator or visibility.level >= 80


def _help_can_manage_roles_admin(visibility: HelpVisibilityContext) -> bool:
    return visibility.is_administrator or visibility.level >= 80


def _help_can_use_rep(visibility: HelpVisibilityContext) -> bool:
    if getattr(visibility, "is_administrator", False):
        return True
    if hasattr(visibility, "can_use_rep"):
        return bool(getattr(visibility, "can_use_rep", False))
    normalized = {normalize_protected_profile_title(title) for title in getattr(visibility, "titles", tuple()) if str(title).strip()}
    return bool(normalized & {"ветеран города", "младший админ", "вице города", "админ", "главный вице", "глава клуба", "оператор"})


def _help_can_manage_tickets(visibility: HelpVisibilityContext) -> bool:
    if visibility.is_administrator:
        return True
    normalized = {normalize_protected_profile_title(title) for title in visibility.titles if str(title).strip()}
    return bool({"глава клуба", "главный вице"} & normalized) or visibility.level >= 100


def _has_privileged_help_commands(visibility: HelpVisibilityContext) -> bool:
    return any(
        (
            _help_can_manage_points(visibility),
            _help_can_create_fines(visibility),
            _help_can_manage_bank(visibility),
            _help_can_manage_tournaments(visibility),
            _help_can_manage_roles_admin(visibility),
            _help_can_use_rep(visibility),
            _help_can_manage_tickets(visibility),
        )
    )


class HelpView(SafeView):
    def __init__(self, user: discord.Member):
        super().__init__(timeout=120)
        self.user = user
        self.visibility = _resolve_help_visibility(user)
        if not _has_privileged_help_commands(self.visibility):
            self.remove_item(self.admin_category_btn)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user.id

    async def update_embed(self, interaction: discord.Interaction, category: str):
        embed = get_help_embed(category, visibility=self.visibility)
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="📊 Баллы", style=discord.ButtonStyle.blurple, row=0)
    async def points_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.update_embed(interaction, "points")

    @discord.ui.button(label="🏅 Роли", style=discord.ButtonStyle.green, row=0)
    async def roles_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.update_embed(interaction, "roles")

    @discord.ui.button(label="📉 Штрафы", style=discord.ButtonStyle.gray, row=1)
    async def fines_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.update_embed(interaction, "fines")

    @discord.ui.button(label="🧪 Прочее", style=discord.ButtonStyle.secondary, row=1)
    async def misc_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.update_embed(interaction, "misc")

    @discord.ui.button(label="🛡️ Доступные мод-команды", style=discord.ButtonStyle.red, row=1)
    async def admin_category_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not _has_privileged_help_commands(self.visibility):
            await interaction.response.send_message("❌ Для вашего звания дополнительных мод-команд сейчас нет.", ephemeral=True)
            return
        embed = discord.Embed(
            title="🛡️ Команды по вашему званию",
            description="Ниже показаны только те модераторские команды, которые доступны именно вам.",
            color=discord.Color.red(),
        )
        await interaction.response.edit_message(embed=embed, view=AdminCategoryView(self.user, visibility=self.visibility))


def get_help_embed(category: str, visibility: HelpVisibilityContext | None = None) -> discord.Embed:
    visibility = visibility or HelpVisibilityContext()
    embed = discord.Embed(title="🛠️ Справка: категории команд", color=discord.Color.blue())

    if category == "points":
        lines = [
            "`/balance [@пользователь]` — показать текущий баланс.",
            "`/top` — открыть рейтинг по баллам с периодами: все время, месяц, неделя.",
            "`/history [@пользователь] [страница]` — история изменений баллов.",
        ]
        if _help_can_manage_points(visibility):
            lines.extend(
                [
                    "",
                    "**Дополнительно доступно по вашему званию:**",
                    "`/points [@пользователь]` — открыть меню изменения баллов с подсказками по шагам.",
                ]
            )
        embed.title = "📊 Баллы и рейтинг"
        embed.description = "\n".join(lines)
    elif category == "roles":
        lines = [
            "1. `/roles` — открой каталог ролей и прочитай блоки `Способ получения` и `Как получить`.",
            "2. Если у роли указано `выдаёт администратор`, попроси выдачу вручную у админа.",
            "3. Если у роли указано `автоматически`, `за баллы` или похожее условие, выполни его и проверь каталог ещё раз.",
            "4. `/activities` — посмотри, какие активности дают баллы для автоматических ролей и прогресса.",
        ]
        if _help_can_manage_roles_admin(visibility):
            lines.extend(
                [
                    "",
                    "**Дополнительно доступно по вашему званию:**",
                    "`/rolesadmin` — открыть панель управления ролями и категориями.",
                ]
            )
        embed.title = "🏅 Роли и активности"
        embed.description = "\n".join(lines)
    elif category == "fines":
        lines = [
            "`/modstatus` — единый экран чтения: активные наказания, предупреждения, последние кейсы и legacy-штрафы.",
            "Оплата legacy-штрафа запускается кнопкой в `/modstatus` (если штраф не списался автоматически внутри кейса).",
            "Новые кейсы и санкции создаются через `/rep`: выбор цели, выбор нарушения, preview и подтверждение по шагам.",
        ]
        extra_lines = ["", "**Дополнительно доступно по вашему званию:**", "`/modstatus` — показать свои активные наказания, предупреждения, последние кейсы и legacy-штрафы; legacy-штрафы оплачиваются кнопкой прямо в этом экране."]
        if _help_can_create_fines(visibility) or _help_can_use_rep(visibility):
            if _help_can_use_rep(visibility):
                extra_lines.append("`/rep` — модерация по шагам: выбрать нарушителя через reply/mention, выбрать нарушение кнопками и проверить preview с активным наказанием и итогом; себя и равное/старшее звание выбрать нельзя.")
            lines.extend(extra_lines)
        embed.title = "📉 Штрафы и модерация"
        embed.description = "\n".join(lines)
    elif category == "misc":
        lines = [
            "`/ping` — проверить, работает ли бот.",
            "`/helpy` — открыть меню справки.",
            "`/mapinfo id` — информация о карте по ID (ID — последняя цифра в названии карты).",
            "`/jointournament id` — заявиться на турнир.",
            "`/tournamenthistory [n]` — последние турниры.",
        ]
        if _help_can_manage_tournaments(visibility):
            lines.extend(
                [
                    "",
                    "**Дополнительно доступно по вашему званию:**",
                    "`/createtournament` — создать турнир.",
                    "`/managetournament id` — открыть панель управления турниром.",
                ]
            )
        embed.title = "🧪 Прочее"
        embed.description = "\n".join(lines)
    elif category == "admin_points":
        embed.title = "⚙️ Мод-команды: баллы"
        embed.description = (
            "`/addpoints @пользователь сумма [причина]` — начислить баллы.\n"
            "`/removepoints @пользователь сумма [причина]` — снять баллы.\n"
            "`/undo @пользователь [кол-во]` — отменить последние действия.\n"
            "`/points [@пользователь]` — открыть меню изменения баллов с подсказками по шагам."
        )
    elif category == "admin_fines":
        embed.title = "📉 Мод-команды: штрафы и кейсы"
        description = [
            "`/modstatus` — показать активные наказания, предупреждения, последние кейсы и legacy-штрафы; legacy-штрафы оплачиваются кнопкой прямо в этом экране.",
            "Legacy-штрафы оплачиваются внутри `/modstatus`, а их статус и остаток видны прямо в тексте экрана.",
        ]
        if _help_can_use_rep(visibility):
            description.append("`/rep` — модерация по шагам: выбери цель через reply/mention, выбери нарушение кнопками, проверь preview до применения (активные наказания и следующий шаг эскалации); итоговый кейс формируется без ручного выбора наказания, а себя и равное/старшее звание наказать нельзя.")
        description.append("Историю кейсов, активные наказания, предупреждения, снятия и отмен ищите в moderation cases и в `/modstatus`, а не в рейтинге должников.")
        embed.description = "\n".join(description)
    elif category == "admin_bank":
        embed.title = "🏦 Мод-команды: банк"
        embed.description = (
            "`/bank` — открыть единый экран банка с текущим балансом.\n"
            "Кнопка `⚙️ Настройка банка` показывается только суперадмину в ЛС с ботом и открывает действия `добавить`/`списать`/`история` (причина обязательна для изменений)."
        )
    elif category == "admin_tournaments":
        embed.title = "🏟 Мод-команды: турниры"
        embed.description = (
            "`/createtournament` — создать турнир.\n"
            "`/managetournament id` — панель управления (кнопка 👥 покажет участников; `id` — номер турнира)."
        )
    elif category == "admin_tickets":
        embed.title = "🎟️ Мод-команды: билеты"
        embed.description = (
            "`/tickets [@пользователь]` — открыть единое меню управления билетами (выдача/списание через кнопки и форму причины)."
        )
    elif category == "admin_roles":
        embed.title = "🏅 Мод-команды: роли"
        embed.description = "`/rolesadmin` — открыть панель управления ролями и категориями."
    return embed


class AdminCategoryView(SafeView):
    def __init__(self, user: discord.Member, visibility: HelpVisibilityContext | None = None):
        super().__init__(timeout=120)
        self.user = user
        self.visibility = visibility or _resolve_help_visibility(user)

        if not _help_can_manage_points(self.visibility):
            self.remove_item(self.points_admin)
        if not _help_can_create_fines(self.visibility):
            self.remove_item(self.fines_admin)
        if not _help_can_manage_bank(self.visibility):
            self.remove_item(self.bank_admin)
        if not _help_can_manage_tournaments(self.visibility):
            self.remove_item(self.tournaments_admin)
        if not _help_can_manage_tickets(self.visibility):
            self.remove_item(self.tickets_admin)
        if not _help_can_manage_roles_admin(self.visibility):
            self.remove_item(self.roles_admin)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user.id

    async def send_category(self, interaction, category: str):
        embed = get_help_embed(category, visibility=self.visibility)
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="⚙️ Баллы", style=discord.ButtonStyle.blurple, row=0)
    async def points_admin(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.send_category(interaction, "admin_points")

    @discord.ui.button(label="📉 Штрафы", style=discord.ButtonStyle.gray, row=0)
    async def fines_admin(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.send_category(interaction, "admin_fines")

    @discord.ui.button(label="🎟️ Билеты", style=discord.ButtonStyle.blurple, row=0)
    async def tickets_admin(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.send_category(interaction, "admin_tickets")

    @discord.ui.button(label="🏦 Банк", style=discord.ButtonStyle.green, row=1)
    async def bank_admin(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.send_category(interaction, "admin_bank")

    @discord.ui.button(label="🏟 Турниры", style=discord.ButtonStyle.green, row=1)
    async def tournaments_admin(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.send_category(interaction, "admin_tournaments")

    @discord.ui.button(label="🏅 Роли", style=discord.ButtonStyle.gray, row=1)
    async def roles_admin(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.send_category(interaction, "admin_roles")

    @discord.ui.button(label="🔙 Назад", style=discord.ButtonStyle.secondary, row=2)
    async def back_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = get_help_embed("points", visibility=self.visibility)
        await interaction.response.edit_message(embed=embed, view=HelpView(self.user))

class TopView(SafeView):
    def __init__(self, ctx, mode="all", page=1):
        super().__init__(timeout=120)
        self.ctx = ctx
        self.mode = mode
        self.page = page
        self.page_size = 5
        self._resolved_name_cache: dict[int, str] = {}
        self._seen_non_id_names: dict[int, str] = {}
        self.update_embed_data()

    def update_embed_data(self):
        self.entries = PointsService.get_leaderboard_entries(self.mode)

        self.total_pages = max(1, (len(self.entries) + self.page_size - 1) // self.page_size)

    def get_embed(self):
        start = (self.page - 1) * self.page_size
        entries = self.entries[start:start + self.page_size]
        period_label = {
            PointsService.LEADERBOARD_PERIOD_ALL: "Все время",
            PointsService.LEADERBOARD_PERIOD_MONTH: "За месяц",
            PointsService.LEADERBOARD_PERIOD_WEEK: "За неделю",
        }.get(self.mode, "Все время")
        if not entries:
            embed = discord.Embed(
                title="🏆 Топ участников",
                description="Пока нет участников с положительным балансом баллов.",
                color=discord.Color.gold(),
            )
            embed.set_footer(text=f"Страница {self.page}/{self.total_pages} • Период: {period_label}")
            return embed

        formatted = []
        for uid, points in entries:
            member = self.ctx.guild.get_member(uid)
            cached_name = self._resolved_name_cache.get(int(uid))
            name = cached_name or (member.display_name if member else None)
            if name and not str(name).startswith("ID "):
                self._seen_non_id_names[int(uid)] = str(name)
                self._resolved_name_cache[int(uid)] = str(name)
            if not name:
                account_id = None
                try:
                    account_id = AccountsService.resolve_account_id("telegram", str(uid))
                except Exception:
                    logger.exception(
                        "discord top resolve account_id failed platform=%s guild_id=%s user_id=%s",
                        "telegram",
                        self.ctx.guild.id if self.ctx.guild else None,
                        uid,
                    )
                if not account_id:
                    try:
                        account_id = AccountsService.resolve_account_id("discord", str(uid))
                    except Exception:
                        logger.exception(
                            "discord top resolve account_id failed platform=%s guild_id=%s user_id=%s",
                            "discord",
                            self.ctx.guild.id if self.ctx.guild else None,
                            uid,
                        )
                try:
                    identity_name = (
                        AccountsService.get_best_public_name(None, None, account_id=account_id) if account_id else None
                    )
                except Exception:
                    logger.exception(
                        "discord top identity lookup failed platform=%s guild_id=%s user_id=%s",
                        "discord",
                        self.ctx.guild.id if self.ctx.guild else None,
                        uid,
                    )
                    identity_name = None
                if identity_name:
                    name = str(identity_name)
                    self._resolved_name_cache[int(uid)] = name
                    self._seen_non_id_names[int(uid)] = name
                else:
                    if member is not None:
                        _schedule_soft_identity_refresh_discord(
                            member,
                            guild_id=self.ctx.guild.id if self.ctx.guild else None,
                            source_handler="discord.top_render",
                        )
                    previous_name = self._seen_non_id_names.get(int(uid))
                    if previous_name:
                        logger.warning(
                            "top_name_regressed_to_id platform=%s user_id=%s period=%s page=%s guild_id=%s",
                            "discord",
                            uid,
                            self.mode,
                            self.page,
                            self.ctx.guild.id if self.ctx.guild else None,
                        )
                        name = previous_name
                    else:
                        logger.warning(
                            "top name fallback to id platform=%s source_user_id=%s resolved_account_id=%s period=%s page=%s guild_id=%s",
                            "discord",
                            uid,
                            account_id,
                            self.mode,
                            self.page,
                            self.ctx.guild.id if self.ctx.guild else None,
                        )
                        actor_id = getattr(getattr(self, "ctx", None), "author", None)
                        actor_user_id = getattr(actor_id, "id", None)
                        if actor_user_id and AuthorityService.is_super_admin("discord", str(actor_user_id)):
                            logger.info(
                                "top id fallback admin hint platform=%s source_user_id=%s period=%s page=%s guild_id=%s hint=%s",
                                "discord",
                                uid,
                                self.mode,
                                self.page,
                                self.ctx.guild.id if self.ctx.guild else None,
                                "Профиль не привязан или lookup-поля пустые. Проверьте account_identities и обновление identity.",
                            )
                        name = f"ID {uid}"
                    self._resolved_name_cache[int(uid)] = str(name)

            roles = []
            if member:
                roles = [r.name for r in member.roles if r.id in ROLE_THRESHOLDS]
            role_text = f"\nРоль: {', '.join(roles)}" if roles else ""
            formatted.append((name, f"**{format_points(points)}** баллов{role_text}"))

        embed = build_top_embed(
            title="🏆 Топ участников",
            entries=formatted,
            color=discord.Color.gold(),
            footer=f"Страница {self.page}/{self.total_pages} • Период: {period_label}",
            start_index=start + 1,
        )
        return embed

    def _schedule_callback_identity_refresh(self, interaction: discord.Interaction) -> None:
        try:
            if isinstance(interaction.user, discord.Member):
                _schedule_soft_identity_refresh_discord(
                    interaction.user,
                    guild_id=interaction.guild_id,
                    source_handler="discord.top_callback",
                )
        except Exception:
            logger.exception(
                "top callback identity refresh failed platform=%s actor_id=%s guild_id=%s",
                "discord",
                interaction.user.id if interaction.user else None,
                interaction.guild_id,
            )

    @discord.ui.button(label="◀️", style=discord.ButtonStyle.gray)
    async def prev_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            self._schedule_callback_identity_refresh(interaction)
            if self.page > 1:
                self.page -= 1
                await interaction.response.edit_message(embed=self.get_embed(), view=self)
        except Exception:
            logger.exception(
                "top interaction failed platform=%s actor_id=%s guild_id=%s mode=%s action=%s",
                "discord",
                interaction.user.id if interaction.user else None,
                interaction.guild_id,
                self.mode,
                "prev_page",
            )

    @discord.ui.button(label="▶️", style=discord.ButtonStyle.gray)
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            self._schedule_callback_identity_refresh(interaction)
            if self.page < self.total_pages:
                self.page += 1
                await interaction.response.edit_message(embed=self.get_embed(), view=self)
        except Exception:
            logger.exception(
                "top interaction failed platform=%s actor_id=%s guild_id=%s mode=%s action=%s",
                "discord",
                interaction.user.id if interaction.user else None,
                interaction.guild_id,
                self.mode,
                "next_page",
            )

    @discord.ui.button(label="За неделю", style=discord.ButtonStyle.blurple)
    async def mode_week(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            self._schedule_callback_identity_refresh(interaction)
            self.mode = PointsService.LEADERBOARD_PERIOD_WEEK
            self.page = 1
            self.update_embed_data()
            await interaction.response.edit_message(embed=self.get_embed(), view=self)
        except Exception:
            logger.exception(
                "top interaction failed platform=%s actor_id=%s guild_id=%s mode=%s action=%s",
                "discord",
                interaction.user.id if interaction.user else None,
                interaction.guild_id,
                PointsService.LEADERBOARD_PERIOD_WEEK,
                "set_mode",
            )

    @discord.ui.button(label="За месяц", style=discord.ButtonStyle.blurple)
    async def mode_month(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            self._schedule_callback_identity_refresh(interaction)
            self.mode = PointsService.LEADERBOARD_PERIOD_MONTH
            self.page = 1
            self.update_embed_data()
            await interaction.response.edit_message(embed=self.get_embed(), view=self)
        except Exception:
            logger.exception(
                "top interaction failed platform=%s actor_id=%s guild_id=%s mode=%s action=%s",
                "discord",
                interaction.user.id if interaction.user else None,
                interaction.guild_id,
                PointsService.LEADERBOARD_PERIOD_MONTH,
                "set_mode",
            )

    @discord.ui.button(label="Все время", style=discord.ButtonStyle.green)
    async def mode_all(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            self._schedule_callback_identity_refresh(interaction)
            self.mode = PointsService.LEADERBOARD_PERIOD_ALL
            self.page = 1
            self.update_embed_data()
            await interaction.response.edit_message(embed=self.get_embed(), view=self)
        except Exception:
            logger.exception(
                "top interaction failed platform=%s actor_id=%s guild_id=%s mode=%s action=%s",
                "discord",
                interaction.user.id if interaction.user else None,
                interaction.guild_id,
                PointsService.LEADERBOARD_PERIOD_ALL,
                "set_mode",
            )

async def transfer_data_logic(old_id: int, new_id: int) -> discord.Embed:
    success = db.transfer_user_data(old_id, new_id)

    if success:
        embed = discord.Embed(
            title="✅ Данные успешно перенесены",
            color=discord.Color.green()
        )
        embed.add_field(name="📤 От:", value=f"<@{old_id}> (`{old_id}`)", inline=False)
        embed.add_field(name="📥 Кому:", value=f"<@{new_id}> (`{new_id}`)", inline=False)
        embed.set_footer(text="Перенос баллов, билетов и логов")
    else:
        embed = discord.Embed(
            title="❌ Ошибка при переносе данных",
            description="Проверьте корректность ID или повторите позже.",
            color=discord.Color.red()
        )
    return embed

def build_balance_embed(member: discord.abc.User, guild: discord.Guild | None = None) -> discord.Embed:
    user_id = member.id
    account_id = _resolve_account_id_from_discord(user_id, handler="build_balance_embed")

    guild_member = member if isinstance(member, discord.Member) else None
    if guild_member is None and guild is not None:
        try:
            guild_member = guild.get_member(user_id)
        except Exception:
            logger.exception("build_balance_embed failed to resolve guild member user_id=%s guild_id=%s", user_id, getattr(guild, "id", None))

    roles = []
    if guild_member is not None:
        roles = [role for role in guild_member.roles if role.id in ROLE_THRESHOLDS]
    role_names = ', '.join(role.name for role in roles) if roles else 'Нет роли'

    display_name = getattr(member, "display_name", getattr(member, "name", str(user_id)))
    if not account_id:
        embed = _build_missing_account_embed(member, title=f"Баланс пользователя {display_name}")
        embed.add_field(name="🎯 Баллы", value="Недоступно", inline=True)
        embed.add_field(name="🎟 Обычные билеты", value="Недоступно", inline=True)
        embed.add_field(name="🪙 Золотые билеты", value="Недоступно", inline=True)
        embed.add_field(name="🏅 Роли", value=role_names, inline=False)
        embed.add_field(
            name="ℹ️ Что делать",
            value="Используйте `/register_account` или привяжите аккаунт через `/link <код>`, затем повторите команду.",
            inline=False,
        )
        return embed

    points, data = _get_balance_snapshot(account_id, discord_user_id=user_id, handler="build_balance_embed")
    normal = data.get("tickets_normal", 0)
    gold = data.get("tickets_gold", 0)
    place = _get_leaderboard_place(account_id, discord_user_id=user_id, handler="build_balance_embed")

    embed = discord.Embed(
        title=f"Баланс пользователя {display_name}",
        color=discord.Color.blue()
    )
    avatar = getattr(member, "avatar", None)
    default_avatar = getattr(member, "default_avatar", None)
    if avatar:
        embed.set_thumbnail(url=avatar.url)
    elif default_avatar:
        embed.set_thumbnail(url=default_avatar.url)

    embed.add_field(name="🎯 Баллы", value=format_points(points), inline=True)
    embed.add_field(name="🎟 Обычные билеты", value=f"{normal}", inline=True)
    embed.add_field(name="🪙 Золотые билеты", value=f"{gold}", inline=True)
    embed.add_field(name="🏅 Роли", value=role_names, inline=False)
    embed.add_field(name="📊 Место в топе", value=f"{place}" if place else "Не в топе", inline=False)

    return embed
