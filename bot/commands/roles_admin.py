import logging
import re
from typing import Any

import discord
from discord.ext import commands

from bot.commands.base import bot
from bot.services import AccountsService, AuthorityService, RoleManagementService
from bot.services.role_management_service import (
    DELETE_ROLE_REASON_DISCORD_MANAGED,
    DELETE_ROLE_REASON_NOT_FOUND,
)
from bot.utils import send_temp

logger = logging.getLogger(__name__)
_MENTION_RE = re.compile(r"^<@!?(\d+)>$")


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


def _rolesadmin_help_embed() -> discord.Embed:
    embed = discord.Embed(title="ℹ️ Что делает /rolesadmin", color=discord.Color.blurple())
    embed.description = (
        "Управление каталогом ролей и ролями пользователей.\n"
        "Все команды доступны только администраторам/модераторам с правами.\n\n"
        f"{_render_role_source_note()}"
    )
    embed.add_field(
        name="Категории",
        value=(
            "`/rolesadmin category_create <name> [position]` — создать/обновить категорию\n"
            "`/rolesadmin category_order <name> <position>` — изменить порядок категории\n"
            "`/rolesadmin category_delete <name>` — удалить категорию"
        ),
        inline=False,
    )
    embed.add_field(
        name="Роли",
        value=(
            "`/rolesadmin list` — показать роли по категориям (с автосинхронизацией Discord-каталога)\n"
            "`/rolesadmin role_create <name> <category> [description] [acquire_hint] [discord_role] [position]` — создать роль\n"
            "`/rolesadmin role_edit_description <name> <description>` — обновить описание роли\n"
            "`/rolesadmin role_edit_acquire_hint <name> <acquire_hint>` — обновить способ получения роли\n"
            "`/rolesadmin role_move <role_name> <category> [position]` — переместить роль\n"
            "`/rolesadmin role_order <role_name> <category> <position>` — изменить порядок роли\n"
            "`/rolesadmin role_delete <name>` — удалить роль\n"
            "Перед `role_create` / `role_move` / `role_order` бот показывает embed со списком ролей категории и рассчитанной позицией вставки.\n"
            "Описание и способ получения помогают админам и пользователям быстрее понять роль прямо в карточке.\n"
            "Если позицию не указывать в `role_create` или `role_move`, роль добавится последней.\n"
            "Внешние Discord-роли удалять нельзя: их можно только перемещать и сортировать."
        ),
        inline=False,
    )
    embed.add_field(
        name="Роли пользователей",
        value=(
            "`/rolesadmin user_roles <mention|username|display_name>` — посмотреть роли пользователя\n"
            "`/rolesadmin user_grant <mention|username|display_name> <role_name>` — выдать роль\n"
            "`/rolesadmin user_revoke <mention|username|display_name> <role_name>` — снять роль\n"
            "Порядок подсказок: Telegram ЛС — `@username`/`username`, Telegram группа — reply, Discord — mention/username/display_name, id только как резерв.\n"
            "Если найдено несколько совпадений, бот покажет кандидатов с provider, username, display и matched_by."
        ),
        inline=False,
    )
    return embed


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
        await send_temp(ctx, embed=_rolesadmin_help_embed())


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
async def rolesadmin_role_create(
    ctx: commands.Context,
    name: str,
    category: str,
    description: str | None = None,
    acquire_hint: str | None = None,
    discord_role: discord.Role | None = None,
    position: int | None = None,
):
    if not await _ensure_roles_admin(ctx):
        return
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
async def rolesadmin_user_grant(ctx: commands.Context, target: str, role_name: str):
    if not await _ensure_roles_admin(ctx):
        return

    resolved = await _resolve_discord_target(ctx, target, operation="user_grant")
    if not resolved:
        return

    role_info = RoleManagementService.get_role(role_name)
    category = role_info.get("category_name") if role_info else None
    ok = RoleManagementService.assign_user_role(
        str(resolved["provider"]),
        str(resolved["provider_user_id"]),
        role_name,
        category=category,
    )
    if not ok:
        await send_temp(ctx, "❌ Не удалось выдать роль в БД (смотри логи).")
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
async def rolesadmin_user_revoke(ctx: commands.Context, target: str, role_name: str):
    if not await _ensure_roles_admin(ctx):
        return

    resolved = await _resolve_discord_target(ctx, target, operation="user_revoke")
    if not resolved:
        return

    role_info = RoleManagementService.get_role(role_name)
    ok = RoleManagementService.revoke_user_role(
        str(resolved["provider"]),
        str(resolved["provider_user_id"]),
        role_name,
    )
    if not ok:
        await send_temp(ctx, "❌ Не удалось забрать роль в БД (смотри логи).")
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
