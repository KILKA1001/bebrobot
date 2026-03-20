import logging
import re
from dataclasses import dataclass
from typing import Any

import discord
from discord import app_commands
from discord.ext import commands

from bot.commands.base import bot
from bot.services import AccountsService, AuthorityService, RoleManagementService
from bot.services.role_management_service import (
    DELETE_ROLE_REASON_DISCORD_MANAGED,
    DELETE_ROLE_REASON_NOT_FOUND,
    PRIVILEGED_DISCORD_ROLE_MESSAGE,
    ROLE_ASSIGNMENT_REASON_PRIVILEGED_DISCORD_ROLE,
)
from bot.utils import send_temp

logger = logging.getLogger(__name__)
_MENTION_RE = re.compile(r"^<@!?(\d+)>$")
_SECTION_LABELS = {
    "categories": "Категории",
    "roles": "Роли",
    "users": "Пользователи",
}


def _role_assignment_error_message(result: dict[str, Any], *, default_message: str) -> str:
    if result.get("reason") == ROLE_ASSIGNMENT_REASON_PRIVILEGED_DISCORD_ROLE:
        return f"❌ {result.get('message') or PRIVILEGED_DISCORD_ROLE_MESSAGE}"
    return default_message


@dataclass(frozen=True)
class RolesAdminVisibilityContext:
    actor_level: int
    actor_titles: tuple[str, ...]
    can_manage_categories: bool
    can_manage_roles: bool
    hidden_sections: tuple[str, ...]


@dataclass
class DiscordUserRoleFlowState:
    actor_id: int
    action: str
    target: dict[str, Any]
    grouped: list[dict[str, Any]]
    current_category: str | None = None
    selected_roles: tuple[str, ...] = tuple()

    def category_names(self) -> list[str]:
        return [str(item.get("category") or "Без категории") for item in self.grouped]

    def roles_for_category(self, category_name: str | None = None) -> list[dict[str, Any]]:
        category_key = str(category_name or self.current_category or "").strip()
        if not category_key:
            return []
        for item in self.grouped:
            if str(item.get("category") or "").strip() == category_key:
                return [
                    role
                    for role in list(item.get("roles") or [])
                    if str(role.get("name") or "").strip()
                ]
        return []

    def with_category(self, category_name: str) -> "DiscordUserRoleFlowState":
        return DiscordUserRoleFlowState(
            actor_id=self.actor_id,
            action=self.action,
            target=self.target,
            grouped=self.grouped,
            current_category=category_name,
            selected_roles=self.selected_roles,
        )

    def with_category_selection(self, category_name: str, selected_in_category: list[str]) -> "DiscordUserRoleFlowState":
        roles_in_category = {
            str(role.get("name") or "").strip()
            for role in self.roles_for_category(category_name)
            if str(role.get("name") or "").strip()
        }
        preserved = [role for role in self.selected_roles if role not in roles_in_category]
        normalized_new: list[str] = []
        seen = set(preserved)
        for role_name in selected_in_category:
            role_key = str(role_name or "").strip()
            if not role_key or role_key in seen:
                continue
            seen.add(role_key)
            normalized_new.append(role_key)
        return DiscordUserRoleFlowState(
            actor_id=self.actor_id,
            action=self.action,
            target=self.target,
            grouped=self.grouped,
            current_category=category_name,
            selected_roles=tuple([*preserved, *normalized_new]),
        )

    def summary_lists(self) -> tuple[list[str], list[str]]:
        normalized = list(dict.fromkeys(role for role in self.selected_roles if str(role or "").strip()))
        if self.action == "revoke":
            return [], normalized
        return normalized, []


def _delete_role_denied_message() -> str:
    return "❌ Эту внешнюю Discord-роль нельзя удалить из каталога. Её можно только переместить или отсортировать."


def _delete_role_not_found_message() -> str:
    return "❌ Роль не найдена в каноническом каталоге `roles`. Обнови список или дождись автосинхронизации Discord-ролей."


def _delete_role_result_message(result: dict[str, Any]) -> str:
    if result.get("reason") == DELETE_ROLE_REASON_DISCORD_MANAGED:
        return _delete_role_denied_message()
    if result.get("reason") == DELETE_ROLE_REASON_NOT_FOUND:
        return _delete_role_not_found_message()
    return "❌ Не удалось удалить роль. Проверь синхронизацию каталога и логи."


def _render_role_source_note() -> str:
    return (
        "Команды `list`, `role_move` и `role_order` сначала пытаются автоматически подтянуть актуальные Discord-роли "
        "в канонический каталог `roles`. Внешние Discord-роли можно перемещать и сортировать, но нельзя удалять. "
        "Если автосинхронизация не успела или упала, обнови список, при необходимости запусти "
        "`/rolesadmin sync_discord_roles` и проверь логи."
    )


def _canonical_role_missing_message() -> str:
    return "❌ Роль не найдена в каталоге `roles`. Запусти `/rolesadmin sync_discord_roles`, затем попробуй ещё раз и проверь логи."


def _render_user_lookup_hint() -> str:
    return (
        "Порядок поиска такой: Telegram ЛС — `@username` / `username`; Telegram группа — reply; "
        "Discord — mention / username / display_name. Если нужен явный провайдер, укажи `ds:username` "
        "или `tg:@username`. ID используй только как резерв."
    )


def _log_role_position_error(
    *,
    actor_id: int | None,
    guild_id: int | None,
    operation: str,
    role_name: str | None,
    category: str | None,
    requested_position: int | None,
    computed_last_position: int | None,
    message: str,
) -> None:
    logger.warning(
        "%s actor_id=%s guild_id=%s operation=%s role_name=%s category=%s requested_position=%s computed_last_position=%s source=%s",
        message,
        actor_id,
        guild_id,
        operation,
        role_name,
        category,
        requested_position,
        computed_last_position,
        "discord_hybrid",
    )


def _build_role_position_preview_embed(
    *,
    title: str,
    role_name: str,
    preview: dict[str, Any],
    role_description: str | None = None,
    role_acquire_hint: str | None = None,
) -> discord.Embed:
    embed = discord.Embed(title=title, color=discord.Color.blurple())
    description_text = str(role_description or "").strip()
    acquire_hint_text = str(role_acquire_hint or "").strip()
    embed.description = (
        f"Роль: **{role_name}**\n"
        f"Категория: **{preview.get('category')}**\n"
        f"Описание: **{description_text or '—'}**\n"
        f"Как получить: **{acquire_hint_text or '—'}**\n"
        f"Расчёт позиции: **{preview.get('position_description')}**\n"
        "Если позицию не указывать в `role_create` или `role_move`, роль будет добавлена последней."
    )
    roles = list(preview.get("current_roles") or [])
    embed.add_field(
        name="Текущий порядок ролей",
        value="\n".join(f"• #{idx}. {item['name']}" for idx, item in enumerate(roles, start=1)) or "• Категория пока пустая.",
        inline=False,
    )
    return embed


def _format_role_line(role: dict[str, Any]) -> str:
    suffix = f" (Discord ID: {role['discord_role_id']})" if role.get("discord_role_id") else ""
    description = str(role.get("description") or "").strip()
    acquire_hint = str(role.get("acquire_hint") or "").strip()
    parts = [f"• {role['name']}{suffix}"]
    if description:
        parts.append(f"описание: {description}")
    if acquire_hint:
        parts.append(f"как получить: {acquire_hint}")
    return " — ".join(parts)


def _catalog_role_exists(role_name: str) -> bool:
    role_key = str(role_name or "").strip()
    if not role_key:
        return False
    return any(item["role"] == role_key for item in RoleManagementService.list_roles_available_for_admin_reorder())


def _role_category_names() -> list[str]:
    return [
        str(item.get("category") or "").strip()
        for item in RoleManagementService.list_roles_grouped()
        if str(item.get("category") or "").strip()
    ]


async def _role_category_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> list[app_commands.Choice[str]]:
    query = str(current or "").strip().lower()
    categories = []
    seen: set[str] = set()
    for category in _role_category_names():
        category_key = category.casefold()
        if category_key in seen:
            continue
        if query and query not in category_key:
            continue
        seen.add(category_key)
        categories.append(app_commands.Choice(name=category[:100], value=category))
        if len(categories) >= 25:
            break
    return categories


def _log_role_create_category_selection(
    *,
    actor_id: int | None,
    guild_id: int | None,
    category: str,
    source: str,
) -> None:
    logger.info(
        "rolesadmin role_create category selected actor_id=%s guild_id=%s category=%s source=%s",
        actor_id,
        guild_id,
        category,
        source,
    )


def _build_user_role_flow_embed(state: DiscordUserRoleFlowState) -> discord.Embed:
    embed = discord.Embed(
        title="🧺 Пакетное управление ролями",
        color=discord.Color.blurple(),
    )
    grant_roles, revoke_roles = state.summary_lists()
    current_category = state.current_category or "не выбрана"
    embed.description = (
        f"Пользователь: **{state.target.get('label') or 'неизвестный пользователь'}**\n"
        f"Действие: **{'выдача' if state.action == 'grant' else 'снятие'} ролей**\n"
        f"Текущая категория: **{current_category}**\n"
        f"Уже выбрано ролей: **{len(state.selected_roles)}**\n\n"
        "Выбор можно продолжать по другим категориям до явного выхода из панели."
    )
    embed.add_field(
        name="Будет выдано",
        value="\n".join(f"• {item}" for item in grant_roles) or "• —",
        inline=False,
    )
    embed.add_field(
        name="Будет снято",
        value="\n".join(f"• {item}" for item in revoke_roles) or "• —",
        inline=False,
    )
    category_roles = state.roles_for_category()
    if category_roles:
        embed.add_field(
            name=f"Роли категории «{current_category}»",
            value="\n".join(
                f"{'✅' if str(role.get('name') or '').strip() in set(state.selected_roles) else '⬜️'} "
                f"{str(role.get('name') or '').strip()}"
                for role in category_roles[:25]
            ),
            inline=False,
        )
    else:
        embed.add_field(
            name="Как пользоваться",
            value=(
                "1. Выберите категорию.\n"
                "2. Отметьте одну или несколько ролей.\n"
                "3. Вернитесь к категориям и продолжайте выбор.\n"
                "4. Нажмите подтверждение, когда пакет готов."
            ),
            inline=False,
        )
    return embed


async def _sync_ctx_discord_roles_catalog(ctx: commands.Context, *, operation: str) -> bool:
    if not ctx.guild:
        return True
    try:
        guild_roles = [
            {"id": str(role.id), "name": role.name, "position": role.position, "guild_id": str(ctx.guild.id)}
            for role in ctx.guild.roles
            if not role.is_default()
        ]
        result = RoleManagementService.sync_discord_guild_roles(guild_roles)
        logger.info(
            "rolesadmin implicit discord catalog sync completed actor_id=%s guild_id=%s operation=%s source=%s roles=%s upserted=%s removed=%s",
            ctx.author.id,
            ctx.guild.id,
            operation,
            "discord_hybrid",
            len(guild_roles),
            result.get("upserted", 0),
            result.get("removed", 0),
        )
        return True
    except Exception:
        logger.exception(
            "rolesadmin implicit discord catalog sync failed actor_id=%s guild_id=%s operation=%s source=%s reason=%s",
            ctx.author.id,
            ctx.guild.id if ctx.guild else None,
            operation,
            "discord_hybrid",
            "exception",
        )
        return False


def _match_discord_member_candidates(guild: discord.Guild | None, raw_target: str) -> list[discord.Member]:
    if not guild:
        return []
    token = str(raw_target or "").strip()
    if not token:
        return []

    mention_match = _MENTION_RE.match(token)
    if mention_match:
        member = guild.get_member(int(mention_match.group(1)))
        return [member] if member else []

    if token.isdigit():
        member = guild.get_member(int(token))
        return [member] if member else []

    normalized = token.lstrip("@").casefold()
    candidates: list[discord.Member] = []
    for member in guild.members:
        candidate_fields = {
            getattr(member, "name", None),
            getattr(member, "display_name", None),
            getattr(member, "global_name", None),
        }
        if any(str(value or "").strip().lstrip("@").casefold() == normalized for value in candidate_fields):
            candidates.append(member)
    return candidates


def _format_discord_candidate(member: discord.Member) -> str:
    global_name = getattr(member, "global_name", None)
    pieces = [
        "discord",
        f"username=@{member.name}",
        f"display={member.display_name}",
        "via=guild_member",
        f"id={member.id}",
        member.mention,
    ]
    if global_name:
        pieces.append(f"global={global_name}")
    return " | ".join(pieces)


def _format_identity_lookup_candidate(candidate: dict[str, Any]) -> str:
    provider = str(candidate.get("provider") or "").strip()
    username = str(candidate.get("username") or "").strip()
    display_name = str(candidate.get("display_name") or "").strip()
    provider_user_id = str(candidate.get("provider_user_id") or "").strip()
    matched_by = str(candidate.get("matched_by") or "").strip()
    pieces = [provider] if provider else []
    if username:
        pieces.append(f"@{username}")
    if display_name:
        pieces.append(display_name)
    if provider_user_id:
        pieces.append(f"id={provider_user_id}")
    if matched_by:
        pieces.append(f"via={matched_by}")
    return " | ".join(pieces)


async def _resolve_discord_target(ctx: commands.Context, raw_target: str, *, operation: str) -> dict[str, Any] | None:
    token = str(raw_target or "").strip()
    actor_id = str(ctx.author.id)
    guild_id = str(ctx.guild.id) if ctx.guild else None
    location = "discord_guild" if ctx.guild else "discord_dm"
    AccountsService.persist_identity_lookup_fields(
        "discord",
        actor_id,
        username=getattr(ctx.author, "name", None),
        display_name=getattr(ctx.author, "display_name", None),
        global_username=getattr(ctx.author, "global_name", None),
    )
    if not token:
        logger.warning(
            "rolesadmin user lookup failed actor_id=%s location=%s guild_id=%s operation=%s lookup_value=%s candidates=%s provider=%s reason=%s",
            actor_id,
            location,
            guild_id,
            operation,
            token,
            0,
            "discord",
            "empty_target",
        )
        await send_temp(ctx, f"❌ Укажи mention, username или display_name. {_render_user_lookup_hint()}")
        return None

    member_candidates = _match_discord_member_candidates(ctx.guild, token)
    if len(member_candidates) > 1:
        logger.warning(
            "rolesadmin user lookup ambiguous actor_id=%s location=%s guild_id=%s operation=%s lookup_value=%s candidates=%s provider=%s reason=%s",
            actor_id,
            location,
            guild_id,
            operation,
            token,
            len(member_candidates),
            "discord",
            "guild_member_ambiguous",
        )
        formatted = "\n".join(f"• {_format_discord_candidate(item)}" for item in member_candidates[:5])
        await send_temp(
            ctx,
            "❌ Найдено несколько пользователей. Уточни через mention, username или display_name:\n"
            f"{formatted}\n\n{_render_user_lookup_hint()}",
        )
        return None

    if len(member_candidates) == 1:
        member = member_candidates[0]
        AccountsService.persist_identity_lookup_fields(
            "discord",
            str(member.id),
            username=getattr(member, "name", None),
            display_name=getattr(member, "display_name", None),
            global_username=getattr(member, "global_name", None),
        )
        return {
            "account_id": AccountsService.resolve_account_id("discord", str(member.id)),
            "provider": "discord",
            "provider_user_id": str(member.id),
            "member": member,
            "label": member.mention,
            "matched_by": "guild_member",
        }

    lookup = AccountsService.resolve_user_lookup(token, default_provider="discord")
    identity_candidates = list(lookup.get("candidates") or [])
    if lookup.get("status") == "multiple":
        logger.warning(
            "rolesadmin user lookup ambiguous actor_id=%s location=%s guild_id=%s operation=%s lookup_value=%s candidates=%s provider=%s reason=%s",
            actor_id,
            location,
            guild_id,
            operation,
            token,
            len(identity_candidates),
            "discord",
            "identity_lookup_ambiguous",
        )
        formatted = "\n".join(
            f"• {_format_identity_lookup_candidate(item)}"
            for item in identity_candidates[:5]
        )
        await send_temp(
            ctx,
            "❌ Найдено несколько пользователей. Уточни mention, provider или более точный username:\n"
            f"{formatted}\n\n{_render_user_lookup_hint()}",
        )
        return None

    if lookup.get("status") == "ok":
        candidate = dict(lookup.get("result") or {})
        provider_user_id = str(candidate["provider_user_id"])
        member = ctx.guild.get_member(int(provider_user_id)) if ctx.guild and provider_user_id.isdigit() else None
        label = member.mention if member else (candidate.get("username") or candidate.get("display_name") or f"{candidate.get('provider')}:{provider_user_id}")
        return {
            "account_id": candidate.get("account_id"),
            "provider": candidate.get("provider"),
            "provider_user_id": provider_user_id,
            "member": member,
            "label": label,
            "matched_by": candidate.get("matched_by"),
        }

    logger.warning(
        "rolesadmin user lookup failed actor_id=%s location=%s guild_id=%s operation=%s lookup_value=%s candidates=%s provider=%s reason=%s",
        actor_id,
        location,
        guild_id,
        operation,
        token,
        len(identity_candidates),
        "discord",
        str(lookup.get("reason") or "not_found"),
    )
    await send_temp(
        ctx,
        "❌ Пользователь не найден в локальном реестре. "
        "Пусть он хотя бы раз напишет боту, выполнит `/register_account`, `/link` или `/profile`, затем попробуйте снова. "
        + _render_user_lookup_hint(),
    )
    return None


def _resolve_rolesadmin_visibility(ctx: commands.Context) -> RolesAdminVisibilityContext:
    actor_id = str(ctx.author.id)
    authority = AuthorityService.resolve_authority("discord", actor_id)
    can_manage_roles = bool(ctx.author.guild_permissions.administrator) or AuthorityService.has_command_permission(
        "discord",
        actor_id,
        "players_manage",
    )
    can_manage_categories = bool(ctx.author.guild_permissions.administrator) or AuthorityService.can_manage_role_categories(
        "discord",
        actor_id,
    )
    hidden_sections = []
    if not can_manage_categories:
        hidden_sections.append("categories")
    if not can_manage_roles:
        hidden_sections.extend(["roles", "users"])
    return RolesAdminVisibilityContext(
        actor_level=authority.level,
        actor_titles=tuple(authority.titles),
        can_manage_categories=can_manage_categories,
        can_manage_roles=can_manage_roles,
        hidden_sections=tuple(hidden_sections),
    )


def _render_hidden_sections_note(hidden_sections: tuple[str, ...]) -> str:
    if not hidden_sections:
        return ""
    return (
        "⚠️ Некоторые кнопки скрыты, потому что у вас нет нужных полномочий.\n"
        f"Скрытые разделы: {', '.join(_SECTION_LABELS.get(section, section) for section in hidden_sections)}.\n\n"
    )


def _log_rolesadmin_navigation(
    *,
    actor_id: int | None,
    visibility: RolesAdminVisibilityContext,
    screen: str,
    guild_id: int | None,
) -> None:
    logger.info(
        "rolesadmin navigation actor_id=%s actor_level=%s actor_titles=%s hidden_sections=%s screen=%s guild_id=%s source=%s",
        actor_id,
        visibility.actor_level,
        list(visibility.actor_titles),
        list(visibility.hidden_sections),
        screen,
        guild_id,
        "discord_hybrid",
    )


def _rolesadmin_help_embed(
    *,
    section: str | None = None,
    visibility: RolesAdminVisibilityContext | None = None,
) -> discord.Embed:
    visibility = visibility or RolesAdminVisibilityContext(0, tuple(), False, False, tuple())
    embed = discord.Embed(title="ℹ️ Что делает /rolesadmin", color=discord.Color.blurple())
    embed.description = (
        "Управление каталогом ролей и ролями пользователей.\n"
        "Навигация разделена на Категории, Роли и Пользователи — как и в Telegram-панели.\n"
        "Внутри каждого раздела показываются только относящиеся к нему действия.\n\n"
        f"{_render_hidden_sections_note(visibility.hidden_sections)}"
        f"{_render_role_source_note()}"
    )
    section_fields = {
        "categories": (
            "Категории",
            "`/rolesadmin category_create <name> [position]` — создать/обновить категорию\n"
            "`/rolesadmin category_order <name> <position>` — изменить порядок категории\n"
            "`/rolesadmin category_delete <name>` — удалить категорию\n"
            "Используй этот раздел, когда меняешь верхний уровень структуры каталога ролей."
        ),
        "roles": (
            "Роли",
            "`/rolesadmin list` — показать роли по категориям (с автосинхронизацией Discord-каталога)\n"
            "`/rolesadmin role_create <category> <name> [description] [acquire_hint] [discord_role] [position]` — создать роль\n"
            "`/rolesadmin role_edit_description <name> <description>` — обновить описание роли\n"
            "`/rolesadmin role_edit_acquire_hint <name> <acquire_hint>` — обновить способ получения роли\n"
            "`/rolesadmin role_move <role_name> <category> [position]` — переместить роль\n"
            "`/rolesadmin role_order <role_name> <category> <position>` — изменить порядок роли\n"
            "`/rolesadmin role_delete <name>` — удалить роль\n"
            "Для `role_create` сначала выбери категорию: в slash-команде у поля category есть autocomplete, а дальше заполняй уже параметры роли.\n"
            "Перед `role_create` / `role_move` / `role_order` бот показывает embed со списком ролей категории и рассчитанной позицией вставки.\n"
            "Описание и способ получения помогают админам и пользователям быстрее понять роль прямо в карточке.\n"
            "Если позицию не указывать в `role_create` или `role_move`, роль добавится последней.\n"
            "Внешние Discord-роли удалять нельзя: их можно только перемещать и сортировать."
        ),
        "users": (
            "Пользователи",
            "`/rolesadmin user_roles <mention|username|display_name>` — посмотреть роли пользователя\n"
            "`/rolesadmin user_grant <mention|username|display_name> [role_name]` — выдать роль или открыть пакетный flow\n"
            "`/rolesadmin user_revoke <mention|username|display_name> [role_name]` — снять роль или открыть пакетный flow\n"
            "Если `role_name` не указывать, откроется embed/view flow: выбор категории, multi-select ролей, возврат к категориям и подтверждение пакета.\n"
            "Порядок подсказок: Telegram ЛС — `@username`/`username`, Telegram группа — reply, Discord — mention/username/display_name, id только как резерв.\n"
            "Если найдено несколько совпадений, бот покажет кандидатов с provider, username, display и matched_by."
        ),
    }
    sections_to_render = [section] if section in section_fields else ["categories", "roles", "users"]
    for section_key in sections_to_render:
        if section_key == "categories" and not visibility.can_manage_categories:
            continue
        if section_key in {"roles", "users"} and not visibility.can_manage_roles:
            continue
        title, value = section_fields[section_key]
        embed.add_field(name=title, value=value, inline=False)
    if not embed.fields:
        embed.add_field(
            name="Недостаточно полномочий",
            value="Сейчас для вас скрыты все разделы управления. Обратитесь к старшему администратору.",
            inline=False,
        )
    return embed


class RolesAdminHelpView(discord.ui.View):
    def __init__(self, *, actor_id: int, visibility: RolesAdminVisibilityContext, guild_id: int | None):
        super().__init__(timeout=300)
        self.actor_id = actor_id
        self.visibility = visibility
        self.guild_id = guild_id
        if visibility.can_manage_categories:
            self.add_item(_RolesAdminSectionButton(section="categories"))
        if visibility.can_manage_roles:
            self.add_item(_RolesAdminSectionButton(section="roles"))
            self.add_item(_RolesAdminSectionButton(section="users"))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.actor_id:
            logger.warning(
                "rolesadmin help denied foreign actor actor_id=%s owner_id=%s custom_id=%s guild_id=%s",
                interaction.user.id,
                self.actor_id,
                interaction.data.get("custom_id") if interaction.data else None,
                self.guild_id,
            )
            await interaction.response.send_message("Эта панель открыта другим администратором.", ephemeral=True)
            return False
        return True

    async def on_timeout(self) -> None:
        for child in self.children:
            child.disabled = True


class _RolesAdminSectionButton(discord.ui.Button):
    def __init__(self, *, section: str):
        super().__init__(
            label=_SECTION_LABELS[section],
            style=discord.ButtonStyle.secondary,
            custom_id=f"rolesadmin_help:{section}",
        )
        self.section = section

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if not isinstance(view, RolesAdminHelpView):
            logger.error("rolesadmin help button view mismatch actor_id=%s section=%s", interaction.user.id, self.section)
            await interaction.response.send_message("❌ Ошибка навигации rolesadmin (смотри логи).", ephemeral=True)
            return
        try:
            _log_rolesadmin_navigation(
                actor_id=interaction.user.id,
                visibility=view.visibility,
                screen=f"help:{self.section}",
                guild_id=view.guild_id,
            )
            await interaction.response.edit_message(
                embed=_rolesadmin_help_embed(section=self.section, visibility=view.visibility),
                view=view,
            )
        except Exception:
            logger.exception(
                "rolesadmin help button failed actor_id=%s section=%s guild_id=%s",
                interaction.user.id,
                self.section,
                view.guild_id,
            )
            if interaction.response.is_done():
                await interaction.followup.send("❌ Ошибка открытия раздела rolesadmin (смотри логи).", ephemeral=True)
            else:
                await interaction.response.send_message("❌ Ошибка открытия раздела rolesadmin (смотри логи).", ephemeral=True)


class _DiscordUserRoleCategorySelect(discord.ui.Select):
    def __init__(self, state: DiscordUserRoleFlowState):
        options = [
            discord.SelectOption(
                label=category[:100],
                value=category,
                default=category == state.current_category,
            )
            for category in state.category_names()[:25]
        ] or [discord.SelectOption(label="Нет категорий", value="__none__")]
        super().__init__(
            placeholder="Выберите категорию",
            min_values=1,
            max_values=1,
            options=options,
            row=0,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if not isinstance(view, DiscordUserRoleFlowView):
            await interaction.response.send_message("❌ Ошибка панели ролей (смотри логи).", ephemeral=True)
            return
        category_name = str(self.values[0] or "").strip()
        if category_name == "__none__":
            await interaction.response.send_message("Категории недоступны.", ephemeral=True)
            return
        view.state = view.state.with_category(category_name)
        view.rebuild()
        await interaction.response.edit_message(embed=_build_user_role_flow_embed(view.state), view=view)


class _DiscordUserRoleMultiSelect(discord.ui.Select):
    def __init__(self, state: DiscordUserRoleFlowState):
        category_roles = state.roles_for_category()
        options = [
            discord.SelectOption(
                label=str(role.get("name") or "").strip()[:100],
                value=str(role.get("name") or "").strip(),
                description=str(role.get("description") or "").strip()[:100] or None,
                default=str(role.get("name") or "").strip() in set(state.selected_roles),
            )
            for role in category_roles[:25]
            if str(role.get("name") or "").strip()
        ]
        if not options:
            options = [discord.SelectOption(label="Сначала выберите категорию", value="__empty__")]
        super().__init__(
            placeholder="Отметьте роли в категории",
            min_values=1 if options[0].value != "__empty__" else 1,
            max_values=max(1, len(options)),
            options=options,
            row=1,
            disabled=options[0].value == "__empty__",
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if not isinstance(view, DiscordUserRoleFlowView):
            await interaction.response.send_message("❌ Ошибка панели ролей (смотри логи).", ephemeral=True)
            return
        if not view.state.current_category:
            await interaction.response.send_message("Сначала выберите категорию.", ephemeral=True)
            return
        view.state = view.state.with_category_selection(view.state.current_category, list(self.values))
        view.rebuild()
        await interaction.response.edit_message(embed=_build_user_role_flow_embed(view.state), view=view)


class _DiscordUserRoleApplyButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Подтвердить пакет", style=discord.ButtonStyle.success, row=2)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if not isinstance(view, DiscordUserRoleFlowView):
            await interaction.response.send_message("❌ Ошибка панели ролей (смотри логи).", ephemeral=True)
            return
        if not view.state.selected_roles:
            await interaction.response.send_message("Сначала выберите хотя бы одну роль.", ephemeral=True)
            return
        grant_roles, revoke_roles = view.state.summary_lists()
        account_id = str(view.state.target.get("account_id") or "").strip()
        result = RoleManagementService.apply_user_role_changes_by_account(
            account_id,
            actor_id=str(interaction.user.id),
            actor_provider="discord",
            actor_user_id=str(interaction.user.id),
            grant_roles=grant_roles,
            revoke_roles=revoke_roles,
        )
        member = view.state.target.get("member")
        if member and interaction.guild:
            successful_roles = [
                *(result.get("grant_success") or []),
                *(result.get("revoke_success") or []),
            ]
            for role_name in successful_roles:
                role_info = RoleManagementService.get_role(role_name) or {}
                discord_role_id = str(role_info.get("discord_role_id") or "").strip()
                if not discord_role_id:
                    continue
                guild_role = interaction.guild.get_role(int(discord_role_id))
                if not guild_role:
                    logger.warning(
                        "rolesadmin flow guild role missing actor_id=%s guild_id=%s target_id=%s role_name=%s role_id=%s",
                        interaction.user.id,
                        interaction.guild.id,
                        getattr(member, "id", None),
                        role_name,
                        discord_role_id,
                    )
                    continue
                try:
                    if role_name in list(result.get("grant_success") or []):
                        await member.add_roles(guild_role, reason=f"rolesadmin batch grant by {interaction.user.id}")
                    if role_name in list(result.get("revoke_success") or []):
                        await member.remove_roles(guild_role, reason=f"rolesadmin batch revoke by {interaction.user.id}")
                except Exception:
                    logger.exception(
                        "rolesadmin flow discord sync failed actor_id=%s guild_id=%s target_id=%s role_name=%s role_id=%s action=%s",
                        interaction.user.id,
                        interaction.guild.id,
                        getattr(member, "id", None),
                        role_name,
                        discord_role_id,
                        "grant" if role_name in list(result.get("grant_success") or []) else "revoke",
                    )
        for child in view.children:
            child.disabled = True
        lines = []
        if result.get("grant_success"):
            lines.append("✅ Выдано: " + ", ".join(result["grant_success"]))
        if result.get("revoke_success"):
            lines.append("✅ Снято: " + ", ".join(result["revoke_success"]))
        if result.get("grant_failed"):
            lines.append("❌ Не выдано: " + ", ".join(result["grant_failed"]))
        if result.get("revoke_failed"):
            lines.append("❌ Не снято: " + ", ".join(result["revoke_failed"]))
        privileged_denied = [*(result.get("grant_denied") or []), *(result.get("revoke_denied") or [])]
        for denied in privileged_denied:
            if denied.get("reason") == ROLE_ASSIGNMENT_REASON_PRIVILEGED_DISCORD_ROLE:
                lines.append(f"❌ {denied.get('message') or PRIVILEGED_DISCORD_ROLE_MESSAGE}")
        if result.get("conflicting_roles"):
            lines.append("⚠️ Пропущены конфликтующие роли: " + ", ".join(result["conflicting_roles"]))
        embed = discord.Embed(
            title="Пакетная операция завершена",
            description="\n".join(lines) or "⚠️ Пакет не применён.",
            color=discord.Color.green() if result.get("ok") else discord.Color.orange(),
        )
        await interaction.response.edit_message(embed=embed, view=view)


class _DiscordUserRoleExitButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Выйти", style=discord.ButtonStyle.secondary, row=2)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if not isinstance(view, DiscordUserRoleFlowView):
            await interaction.response.send_message("❌ Ошибка панели ролей (смотри логи).", ephemeral=True)
            return
        for child in view.children:
            child.disabled = True
        await interaction.response.edit_message(
            embed=discord.Embed(
                title="Панель закрыта",
                description="Чтобы открыть новый пакетный выбор ролей, вызовите `/rolesadmin user_grant` или `/rolesadmin user_revoke` без указания role_name.",
                color=discord.Color.dark_grey(),
            ),
            view=view,
        )


class DiscordUserRoleFlowView(discord.ui.View):
    def __init__(self, state: DiscordUserRoleFlowState):
        super().__init__(timeout=300)
        self.state = state
        self.rebuild()

    def rebuild(self) -> None:
        self.clear_items()
        self.add_item(_DiscordUserRoleCategorySelect(self.state))
        self.add_item(_DiscordUserRoleMultiSelect(self.state))
        self.add_item(_DiscordUserRoleApplyButton())
        self.add_item(_DiscordUserRoleExitButton())

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.state.actor_id:
            await interaction.response.send_message("Эта панель открыта другим администратором.", ephemeral=True)
            return False
        return True

    async def on_timeout(self) -> None:
        for child in self.children:
            child.disabled = True


async def _ensure_roles_admin(ctx: commands.Context) -> bool:
    if ctx.author.guild_permissions.administrator:
        return True
    if not AuthorityService.has_command_permission("discord", str(ctx.author.id), "players_manage"):
        await send_temp(ctx, "❌ Недостаточно полномочий для управления ролями.")
        return False
    return True


async def _ensure_category_manager(ctx: commands.Context) -> bool:
    if ctx.author.guild_permissions.administrator:
        return True
    allowed = AuthorityService.can_manage_role_categories("discord", str(ctx.author.id))
    if not allowed:
        logger.warning(
            "rolesadmin category access denied actor_id=%s guild_id=%s",
            ctx.author.id,
            ctx.guild.id if ctx.guild else None,
        )
        await send_temp(ctx, "❌ Категориями может управлять только Глава клуба или Главный вице.")
        return False
    return True


@bot.hybrid_group(name="rolesadmin", description="Управление ролями и категориями", with_app_command=True)
async def rolesadmin(ctx: commands.Context):
    if ctx.invoked_subcommand is None:
        visibility = _resolve_rolesadmin_visibility(ctx)
        _log_rolesadmin_navigation(
            actor_id=ctx.author.id,
            visibility=visibility,
            screen="help:home",
            guild_id=ctx.guild.id if ctx.guild else None,
        )
        await send_temp(
            ctx,
            embed=_rolesadmin_help_embed(visibility=visibility),
            view=RolesAdminHelpView(
                actor_id=ctx.author.id,
                visibility=visibility,
                guild_id=ctx.guild.id if ctx.guild else None,
            ),
            delete_after=None,
        )


@rolesadmin.command(name="list", description="Показать роли по категориям")
async def rolesadmin_list(ctx: commands.Context):
    if not await _ensure_roles_admin(ctx):
        return

    sync_ok = await _sync_ctx_discord_roles_catalog(ctx, operation="list")
    grouped = RoleManagementService.list_roles_grouped()
    if not grouped:
        await send_temp(ctx, "📭 Список ролей пуст или БД недоступна.")
        return

    embed = discord.Embed(title="🧩 Роли по категориям", color=discord.Color.blurple())
    if not sync_ok:
        embed.description = (
            "⚠️ Автосинхронизация Discord-каталога перед `/rolesadmin list` не удалась. "
            "Ниже показан текущий локальный каталог, он может быть неактуален. "
            "Попробуй ещё раз или запусти `/rolesadmin sync_discord_roles`."
        )
    for item in grouped:
        category = item["category"]
        roles = item["roles"]
        if not roles:
            embed.add_field(name=category, value="—", inline=False)
            continue
        lines = []
        for role in roles:
            lines.append(_format_role_line(role))
        embed.add_field(name=category, value="\n".join(lines), inline=False)
    await send_temp(ctx, embed=embed)


@rolesadmin.command(name="category_create", description="Создать категорию ролей")
async def rolesadmin_category_create(ctx: commands.Context, name: str, position: int = 0):
    if not await _ensure_roles_admin(ctx):
        return
    if not await _ensure_category_manager(ctx):
        return
    if RoleManagementService.create_category(name, position):
        await send_temp(ctx, f"✅ Категория **{name}** создана/обновлена.")
    else:
        await send_temp(ctx, "❌ Не удалось создать категорию (смотри логи).")


@rolesadmin.command(name="category_delete", description="Удалить категорию ролей")
async def rolesadmin_category_delete(ctx: commands.Context, name: str):
    if not await _ensure_roles_admin(ctx):
        return
    if not await _ensure_category_manager(ctx):
        return
    if RoleManagementService.delete_category(name):
        await send_temp(ctx, f"✅ Категория **{name}** удалена. Роли перенесены в 'Без категории'.")
    else:
        await send_temp(ctx, "❌ Не удалось удалить категорию (смотри логи).")


@rolesadmin.command(name="category_order", description="Изменить порядок категории")
async def rolesadmin_category_order(ctx: commands.Context, name: str, position: int):
    if not await _ensure_roles_admin(ctx):
        return
    if not await _ensure_category_manager(ctx):
        return
    if RoleManagementService.create_category(name, position):
        await send_temp(ctx, f"✅ Порядок категории **{name}** обновлён: {position}.")
    else:
        await send_temp(ctx, "❌ Не удалось обновить порядок категории (смотри логи).")


@rolesadmin.command(name="role_create", description="Создать роль в каталоге")
@app_commands.describe(
    category="Сначала выберите категорию роли",
    name="Название новой роли",
    description="Пояснение для пользователей, что делает роль",
    acquire_hint="Как получить роль: турнир, заявка, выдача админа и т.д.",
    discord_role="Связанная Discord-роль, если нужна",
    position="Позиция в категории; если пусто, роль будет добавлена последней",
)
@app_commands.autocomplete(category=_role_category_autocomplete)
async def rolesadmin_role_create(
    ctx: commands.Context,
    category: str,
    name: str,
    description: str | None = None,
    acquire_hint: str | None = None,
    discord_role: discord.Role | None = None,
    position: int | None = None,
):
    if not await _ensure_roles_admin(ctx):
        return
    _log_role_create_category_selection(
        actor_id=ctx.author.id,
        guild_id=ctx.guild.id if ctx.guild else None,
        category=category,
        source="discord_command",
    )
    preview = RoleManagementService.get_category_role_positioning(category, requested_position=position)
    await send_temp(
        ctx,
        embed=_build_role_position_preview_embed(
            title="🧭 Предпросмотр создания роли",
            role_name=name,
            preview=preview,
            role_description=description,
            role_acquire_hint=acquire_hint,
        ),
    )
    if RoleManagementService.create_role(
        name,
        category,
        description=description,
        acquire_hint=acquire_hint,
        position=position,
        discord_role_id=str(discord_role.id) if discord_role else None,
        discord_role_name=discord_role.name if discord_role else None,
        actor_id=str(ctx.author.id),
        operation="role_create",
    ):
        description_note = f" Описание: {description}." if str(description or "").strip() else ""
        acquire_hint_note = f" Как получить: {acquire_hint}." if str(acquire_hint or "").strip() else ""
        await send_temp(ctx, f"✅ Роль **{name}** создана в категории **{category}**.{description_note}{acquire_hint_note}")
    else:
        _log_role_position_error(
            actor_id=ctx.author.id,
            guild_id=ctx.guild.id if ctx.guild else None,
            operation="role_create",
            role_name=name,
            category=category,
            requested_position=position,
            computed_last_position=int(preview.get("computed_last_position", 0)),
            message="rolesadmin role_create failed",
        )
        await send_temp(ctx, "❌ Не удалось создать роль (смотри логи).")


@rolesadmin.command(name="role_edit_description", description="Обновить описание роли")
async def rolesadmin_role_edit_description(ctx: commands.Context, name: str, description: str):
    if not await _ensure_roles_admin(ctx):
        return
    if RoleManagementService.update_role_description(
        name,
        description,
        actor_id=str(ctx.author.id),
        operation="role_edit_description",
    ):
        await send_temp(ctx, f"✅ Описание роли **{name}** обновлено.")
    else:
        await send_temp(ctx, "❌ Не удалось обновить описание роли (смотри логи).")


@rolesadmin.command(name="role_edit_acquire_hint", description="Обновить способ получения роли")
async def rolesadmin_role_edit_acquire_hint(ctx: commands.Context, name: str, acquire_hint: str):
    if not await _ensure_roles_admin(ctx):
        return
    if RoleManagementService.update_role_acquire_hint(
        name,
        acquire_hint,
        actor_id=str(ctx.author.id),
        operation="role_edit_acquire_hint",
    ):
        await send_temp(ctx, f"✅ Способ получения роли **{name}** обновлён.")
    else:
        await send_temp(ctx, "❌ Не удалось обновить способ получения роли (смотри логи).")


@rolesadmin.command(name="role_delete", description="Удалить роль из каталога")
async def rolesadmin_role_delete(ctx: commands.Context, name: str):
    if not await _ensure_roles_admin(ctx):
        return
    result = RoleManagementService.delete_role(
        name,
        actor_id=str(ctx.author.id),
        guild_id=str(ctx.guild.id) if ctx.guild else None,
    )
    if result["ok"]:
        await send_temp(ctx, f"✅ Роль **{name}** удалена.")
    else:
        await send_temp(ctx, _delete_role_result_message(result))


@rolesadmin.command(name="role_move", description="Переместить роль в другую категорию")
async def rolesadmin_role_move(ctx: commands.Context, role_name: str, category: str, position: int | None = None):
    if not await _ensure_roles_admin(ctx):
        return
    sync_ok = await _sync_ctx_discord_roles_catalog(ctx, operation="role_move")
    preview = RoleManagementService.get_category_role_positioning(
        category,
        requested_position=position,
        exclude_role_name=role_name,
    )
    await send_temp(
        ctx,
        embed=_build_role_position_preview_embed(
            title="🧭 Предпросмотр перемещения роли",
            role_name=role_name,
            preview=preview,
        ),
    )
    if not _catalog_role_exists(role_name):
        _log_role_position_error(
            actor_id=ctx.author.id,
            guild_id=ctx.guild.id if ctx.guild else None,
            operation="role_move",
            role_name=role_name,
            category=category,
            requested_position=position,
            computed_last_position=int(preview.get("computed_last_position", 0)),
            message="rolesadmin role_move denied role missing from canonical catalog",
        )
        message = _canonical_role_missing_message()
        if not sync_ok:
            message += " Автосинхронизация тоже не подтвердила каталог — попробуй ещё раз после `/rolesadmin sync_discord_roles`."
        await send_temp(ctx, message)
        return
    if RoleManagementService.move_role(
        role_name,
        category,
        position,
        actor_id=str(ctx.author.id),
        operation="role_move",
    ):
        await send_temp(ctx, f"✅ Роль **{role_name}** перемещена в **{category}**.")
    else:
        _log_role_position_error(
            actor_id=ctx.author.id,
            guild_id=ctx.guild.id if ctx.guild else None,
            operation="role_move",
            role_name=role_name,
            category=category,
            requested_position=position,
            computed_last_position=int(preview.get("computed_last_position", 0)),
            message="rolesadmin role_move failed",
        )
        await send_temp(ctx, "❌ Не удалось переместить роль. Проверь синхронизацию каталога и логи.")


@rolesadmin.command(name="role_order", description="Изменить порядок роли в категории")
async def rolesadmin_role_order(ctx: commands.Context, role_name: str, category: str, position: int):
    if not await _ensure_roles_admin(ctx):
        return
    sync_ok = await _sync_ctx_discord_roles_catalog(ctx, operation="role_order")
    preview = RoleManagementService.get_category_role_positioning(
        category,
        requested_position=position,
        exclude_role_name=role_name,
    )
    await send_temp(
        ctx,
        embed=_build_role_position_preview_embed(
            title="🧭 Предпросмотр порядка роли",
            role_name=role_name,
            preview=preview,
        ),
    )
    if not _catalog_role_exists(role_name):
        _log_role_position_error(
            actor_id=ctx.author.id,
            guild_id=ctx.guild.id if ctx.guild else None,
            operation="role_order",
            role_name=role_name,
            category=category,
            requested_position=position,
            computed_last_position=int(preview.get("computed_last_position", 0)),
            message="rolesadmin role_order denied role missing from canonical catalog",
        )
        message = _canonical_role_missing_message()
        if not sync_ok:
            message += " Автосинхронизация тоже не подтвердила каталог — попробуй ещё раз после `/rolesadmin sync_discord_roles`."
        await send_temp(ctx, message)
        return
    if RoleManagementService.move_role(
        role_name,
        category,
        position,
        actor_id=str(ctx.author.id),
        operation="role_order",
    ):
        await send_temp(ctx, f"✅ Порядок роли **{role_name}** обновлён: категория **{category}**, позиция **{position}**.")
    else:
        _log_role_position_error(
            actor_id=ctx.author.id,
            guild_id=ctx.guild.id if ctx.guild else None,
            operation="role_order",
            role_name=role_name,
            category=category,
            requested_position=position,
            computed_last_position=int(preview.get("computed_last_position", 0)),
            message="rolesadmin role_order failed",
        )
        await send_temp(ctx, "❌ Не удалось обновить порядок роли. Проверь синхронизацию каталога и логи.")


@rolesadmin.command(name="user_roles", description="Посмотреть роли пользователя")
async def rolesadmin_user_roles(ctx: commands.Context, target: str):
    if not await _ensure_roles_admin(ctx):
        return

    resolved = await _resolve_discord_target(ctx, target, operation="user_roles")
    if not resolved:
        return

    roles = RoleManagementService.get_user_roles(str(resolved["provider"]), str(resolved["provider_user_id"]))
    if not roles:
        await send_temp(ctx, f"📭 У пользователя {resolved['label']} нет ролей.")
        return

    lines = []
    for role in roles:
        description = str(role.get("description") or "").strip()
        line = f"• {role['name']} ({role['category']})"
        if description:
            line += f" — {description}"
        lines.append(line)
    await send_temp(ctx, f"🧾 Роли {resolved['label']}:\n" + "\n".join(lines))


@rolesadmin.command(name="user_grant", description="Выдать роль пользователю")
async def rolesadmin_user_grant(ctx: commands.Context, target: str, role_name: str | None = None):
    if not await _ensure_roles_admin(ctx):
        return

    resolved = await _resolve_discord_target(ctx, target, operation="user_grant")
    if not resolved:
        return

    if not role_name:
        grouped = RoleManagementService.list_roles_grouped()
        if not grouped:
            await send_temp(ctx, "📭 Каталог ролей пуст или БД недоступна.")
            return
        state = DiscordUserRoleFlowState(
            actor_id=ctx.author.id,
            action="grant",
            target=resolved,
            grouped=grouped,
            current_category=str(grouped[0].get("category") or ""),
        )
        await send_temp(
            ctx,
            embed=_build_user_role_flow_embed(state),
            view=DiscordUserRoleFlowView(state),
            delete_after=None,
        )
        return

    role_info = RoleManagementService.get_role(role_name)
    result = RoleManagementService.apply_user_role_changes_by_account(
        str(resolved["account_id"]),
        actor_id=str(ctx.author.id),
        actor_provider="discord",
        actor_user_id=str(ctx.author.id),
        grant_roles=[role_name],
    )
    ok = bool(result.get("grant_success"))
    if not ok:
        await send_temp(
            ctx,
            _role_assignment_error_message(result.get("grant_denied", [{}])[0] if result.get("grant_denied") else result, default_message="❌ Не удалось выдать роль в БД (смотри логи)."),
        )
        return

    member = resolved.get("member")
    discord_role_id = str(role_info.get("discord_role_id") or "").strip() if role_info else ""
    if discord_role_id and member and ctx.guild:
        guild_role = ctx.guild.get_role(int(discord_role_id))
        if guild_role:
            try:
                await member.add_roles(guild_role, reason=f"rolesadmin grant by {ctx.author.id}")
            except Exception:
                logger.exception("failed to add discord role member_id=%s role_id=%s", member.id, discord_role_id)
                await send_temp(ctx, "⚠️ Роль в БД выдана, но выдать Discord-роль не удалось (смотри логи).")
                return

    if discord_role_id and not member:
        logger.warning(
            "rolesadmin user_grant missing guild member actor_id=%s guild_id=%s target_id=%s role_name=%s",
            ctx.author.id,
            ctx.guild.id if ctx.guild else None,
            resolved["provider_user_id"],
            role_name,
        )
        await send_temp(ctx, f"⚠️ Роль **{role_name}** выдана в БД пользователю {resolved['label']}, но участник не найден на сервере Discord.")
        return

    await send_temp(ctx, f"✅ Роль **{role_name}** выдана пользователю {resolved['label']}.")


@rolesadmin.command(name="user_revoke", description="Забрать роль у пользователя")
async def rolesadmin_user_revoke(ctx: commands.Context, target: str, role_name: str | None = None):
    if not await _ensure_roles_admin(ctx):
        return

    resolved = await _resolve_discord_target(ctx, target, operation="user_revoke")
    if not resolved:
        return

    if not role_name:
        grouped = RoleManagementService.list_roles_grouped()
        if not grouped:
            await send_temp(ctx, "📭 Каталог ролей пуст или БД недоступна.")
            return
        state = DiscordUserRoleFlowState(
            actor_id=ctx.author.id,
            action="revoke",
            target=resolved,
            grouped=grouped,
            current_category=str(grouped[0].get("category") or ""),
        )
        await send_temp(
            ctx,
            embed=_build_user_role_flow_embed(state),
            view=DiscordUserRoleFlowView(state),
            delete_after=None,
        )
        return

    role_info = RoleManagementService.get_role(role_name)
    result = RoleManagementService.apply_user_role_changes_by_account(
        str(resolved["account_id"]),
        actor_id=str(ctx.author.id),
        actor_provider="discord",
        actor_user_id=str(ctx.author.id),
        revoke_roles=[role_name],
    )
    ok = bool(result.get("revoke_success"))
    if not ok:
        await send_temp(
            ctx,
            _role_assignment_error_message(result.get("revoke_denied", [{}])[0] if result.get("revoke_denied") else result, default_message="❌ Не удалось забрать роль в БД (смотри логи)."),
        )
        return

    member = resolved.get("member")
    discord_role_id = str(role_info.get("discord_role_id") or "").strip() if role_info else ""
    if discord_role_id and member and ctx.guild:
        guild_role = ctx.guild.get_role(int(discord_role_id))
        if guild_role:
            try:
                await member.remove_roles(guild_role, reason=f"rolesadmin revoke by {ctx.author.id}")
            except Exception:
                logger.exception("failed to remove discord role member_id=%s role_id=%s", member.id, discord_role_id)
                await send_temp(ctx, "⚠️ Роль в БД снята, но снять Discord-роль не удалось (смотри логи).")
                return

    if discord_role_id and not member:
        logger.warning(
            "rolesadmin user_revoke missing guild member actor_id=%s guild_id=%s target_id=%s role_name=%s",
            ctx.author.id,
            ctx.guild.id if ctx.guild else None,
            resolved["provider_user_id"],
            role_name,
        )
        await send_temp(ctx, f"⚠️ Роль **{role_name}** снята в БД у {resolved['label']}, но участник не найден на сервере Discord.")
        return

    await send_temp(ctx, f"✅ Роль **{role_name}** снята у {resolved['label']}.")


@rolesadmin.command(name="sync_discord_roles", description="Синхронизировать роли сервера Discord в каталог")
async def rolesadmin_sync_discord_roles(ctx: commands.Context):
    if not await _ensure_roles_admin(ctx):
        return
    if not ctx.guild:
        await send_temp(ctx, "❌ Команда доступна только на сервере.")
        return
    try:
        guild_roles = [
            {"id": str(role.id), "name": role.name, "position": role.position, "guild_id": str(ctx.guild.id)}
            for role in ctx.guild.roles
            if not role.is_default()
        ]
        result = RoleManagementService.sync_discord_guild_roles(guild_roles)
        await send_temp(
            ctx,
            f"✅ Синхронизация ролей завершена. Обновлено: {result.get('upserted', 0)}, удалено устаревших: {result.get('removed', 0)}.",
        )
    except Exception:
        logger.exception("rolesadmin sync_discord_roles failed guild_id=%s actor_id=%s", ctx.guild.id if ctx.guild else None, ctx.author.id)
        await send_temp(ctx, "❌ Не удалось синхронизировать роли Discord (смотри логи).")
