import logging
import re
from typing import Any

import discord
from discord.ext import commands

from bot.commands.base import bot
from bot.services import AccountsService, AuthorityService, RoleManagementService
from bot.services.role_management_service import DELETE_ROLE_REASON_DISCORD_MANAGED
from bot.utils import send_temp

logger = logging.getLogger(__name__)
_MENTION_RE = re.compile(r"^<@!?(\d+)>$")


def _delete_role_denied_message() -> str:
    return "❌ Эту внешнюю Discord-роль нельзя удалить из каталога. Её можно только переместить или отсортировать."


def _render_role_source_note() -> str:
    return (
        "Для `role_move` и `role_order` доступны только роли из канонического каталога `roles`. "
        "Перед открытием панели или ручной синхронизацией `/rolesadmin sync_discord_roles` Discord-роли "
        "подтягиваются в каталог; если роль ещё не появилась, дождитесь синхронизации и проверьте логи."
    )


def _canonical_role_missing_message() -> str:
    return "❌ Роль не найдена в каталоге `roles`. Запусти `/rolesadmin sync_discord_roles`, затем попробуй ещё раз и проверь логи."


def _render_user_lookup_hint() -> str:
    return (
        "Используй mention или username. Если не сработало — попробуй точный `@username`, "
        "mention пользователя или уточни запрос."
    )


def _catalog_role_exists(role_name: str) -> bool:
    role_key = str(role_name or "").strip()
    if not role_key:
        return False
    return any(item["role"] == role_key for item in RoleManagementService.list_roles_available_for_admin_reorder())


async def _sync_ctx_discord_roles_catalog(ctx: commands.Context) -> None:
    if not ctx.guild:
        return
    try:
        guild_roles = [
            {"id": str(role.id), "name": role.name, "position": role.position, "guild_id": str(ctx.guild.id)}
            for role in ctx.guild.roles
            if not role.is_default()
        ]
        result = RoleManagementService.sync_discord_guild_roles(guild_roles)
        logger.info(
            "rolesadmin implicit discord catalog sync completed actor_id=%s guild_id=%s roles=%s upserted=%s removed=%s",
            ctx.author.id,
            ctx.guild.id,
            len(guild_roles),
            result.get("upserted", 0),
            result.get("removed", 0),
        )
    except Exception:
        logger.exception(
            "rolesadmin implicit discord catalog sync failed actor_id=%s guild_id=%s",
            ctx.author.id,
            ctx.guild.id if ctx.guild else None,
        )


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
    pieces = [member.mention, f"display={member.display_name}", f"name={member.name}"]
    if global_name:
        pieces.append(f"global={global_name}")
    return " | ".join(pieces)


async def _resolve_discord_target(ctx: commands.Context, raw_target: str, *, operation: str) -> dict[str, Any] | None:
    token = str(raw_target or "").strip()
    actor_id = str(ctx.author.id)
    guild_id = str(ctx.guild.id) if ctx.guild else None
    if not token:
        logger.warning(
            "rolesadmin user lookup failed provider=discord actor_id=%s guild_id=%s operation=%s username=%s candidates=%s reason=%s",
            actor_id,
            guild_id,
            operation,
            token,
            0,
            "empty_target",
        )
        await send_temp(ctx, f"❌ Укажи mention или username. {_render_user_lookup_hint()}")
        return None

    member_candidates = _match_discord_member_candidates(ctx.guild, token)
    if len(member_candidates) > 1:
        logger.warning(
            "rolesadmin user lookup ambiguous provider=discord actor_id=%s guild_id=%s operation=%s username=%s candidates=%s reason=%s",
            actor_id,
            guild_id,
            operation,
            token,
            len(member_candidates),
            "guild_member_ambiguous",
        )
        formatted = "\n".join(f"• {_format_discord_candidate(item)}" for item in member_candidates[:5])
        await send_temp(ctx, f"❌ Найдено несколько пользователей. Уточни через mention или более точный username:\n{formatted}")
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
            "provider_user_id": str(member.id),
            "member": member,
            "label": member.mention,
        }

    identity_candidates = AccountsService.find_accounts_by_identity_username("discord", token)
    if len(identity_candidates) > 1:
        logger.warning(
            "rolesadmin user lookup ambiguous provider=discord actor_id=%s guild_id=%s operation=%s username=%s candidates=%s reason=%s",
            actor_id,
            guild_id,
            operation,
            token,
            len(identity_candidates),
            "identity_username_ambiguous",
        )
        formatted = "\n".join(
            f"• {item.get('username') or item.get('display_name') or item.get('provider_user_id')}"
            for item in identity_candidates[:5]
        )
        await send_temp(ctx, f"❌ Найдено несколько пользователей в базе. Уточни через mention или точный username:\n{formatted}")
        return None

    if len(identity_candidates) == 1 and identity_candidates[0].get("provider_user_id"):
        candidate = identity_candidates[0]
        provider_user_id = str(candidate["provider_user_id"])
        member = ctx.guild.get_member(int(provider_user_id)) if ctx.guild and provider_user_id.isdigit() else None
        label = member.mention if member else (candidate.get("username") or candidate.get("display_name") or provider_user_id)
        return {
            "provider_user_id": provider_user_id,
            "member": member,
            "label": label,
        }

    logger.warning(
        "rolesadmin user lookup failed provider=discord actor_id=%s guild_id=%s operation=%s username=%s candidates=%s reason=%s",
        actor_id,
        guild_id,
        operation,
        token,
        len(identity_candidates),
        "not_found",
    )
    await send_temp(ctx, f"❌ Пользователь не найден. {_render_user_lookup_hint()}")
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
            "`/rolesadmin list` — показать роли по категориям\n"
            "`/rolesadmin role_create <name> <category> [discord_role] [position]` — создать роль\n"
            "`/rolesadmin role_move <role_name> <category> [position]` — переместить роль\n"
            "`/rolesadmin role_order <role_name> <category> <position>` — изменить порядок роли\n"
            "`/rolesadmin role_delete <name>` — удалить роль\n"
            "Внешние Discord-роли удалять нельзя: их можно только перемещать и сортировать."
        ),
        inline=False,
    )
    embed.add_field(
        name="Роли пользователей",
        value=(
            "`/rolesadmin user_roles <mention|username>` — посмотреть роли пользователя\n"
            "`/rolesadmin user_grant <mention|username> <role_name>` — выдать роль\n"
            "`/rolesadmin user_revoke <mention|username> <role_name>` — снять роль\n"
            "Если mention не сработал, укажи username. При неоднозначности бот попросит уточнение."
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

    grouped = RoleManagementService.list_roles_grouped()
    if not grouped:
        await send_temp(ctx, "📭 Список ролей пуст или БД недоступна.")
        return

    embed = discord.Embed(title="🧩 Роли по категориям", color=discord.Color.blurple())
    for item in grouped:
        category = item["category"]
        roles = item["roles"]
        if not roles:
            embed.add_field(name=category, value="—", inline=False)
            continue
        lines = []
        for role in roles:
            suffix = f" (Discord ID: {role['discord_role_id']})" if role.get("discord_role_id") else ""
            lines.append(f"• {role['name']}{suffix}")
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
    discord_role: discord.Role | None = None,
    position: int = 0,
):
    if not await _ensure_roles_admin(ctx):
        return
    if RoleManagementService.create_role(
        name,
        category,
        position=position,
        discord_role_id=str(discord_role.id) if discord_role else None,
        discord_role_name=discord_role.name if discord_role else None,
    ):
        await send_temp(ctx, f"✅ Роль **{name}** создана в категории **{category}**.")
    else:
        await send_temp(ctx, "❌ Не удалось создать роль (смотри логи).")


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
    elif result["reason"] == DELETE_ROLE_REASON_DISCORD_MANAGED:
        await send_temp(ctx, _delete_role_denied_message())
    else:
        await send_temp(ctx, "❌ Не удалось удалить роль (смотри логи).")


@rolesadmin.command(name="role_move", description="Переместить роль в другую категорию")
async def rolesadmin_role_move(ctx: commands.Context, role_name: str, category: str, position: int = 0):
    if not await _ensure_roles_admin(ctx):
        return
    await _sync_ctx_discord_roles_catalog(ctx)
    if not _catalog_role_exists(role_name):
        logger.warning(
            "rolesadmin role_move denied role missing from canonical catalog actor_id=%s guild_id=%s role_name=%s category=%s operation=%s source=%s",
            ctx.author.id,
            ctx.guild.id if ctx.guild else None,
            role_name,
            category,
            "role_move",
            "discord_hybrid",
        )
        await send_temp(ctx, _canonical_role_missing_message())
        return
    if RoleManagementService.move_role(role_name, category, position):
        await send_temp(ctx, f"✅ Роль **{role_name}** перемещена в **{category}**.")
    else:
        logger.warning(
            "rolesadmin role_move failed actor_id=%s guild_id=%s role_name=%s category=%s position=%s operation=%s source=%s",
            ctx.author.id,
            ctx.guild.id if ctx.guild else None,
            role_name,
            category,
            position,
            "role_move",
            "discord_hybrid",
        )
        await send_temp(ctx, "❌ Не удалось переместить роль. Проверь синхронизацию каталога и логи.")


@rolesadmin.command(name="role_order", description="Изменить порядок роли в категории")
async def rolesadmin_role_order(ctx: commands.Context, role_name: str, category: str, position: int):
    if not await _ensure_roles_admin(ctx):
        return
    await _sync_ctx_discord_roles_catalog(ctx)
    if not _catalog_role_exists(role_name):
        logger.warning(
            "rolesadmin role_order denied role missing from canonical catalog actor_id=%s guild_id=%s role_name=%s category=%s operation=%s source=%s",
            ctx.author.id,
            ctx.guild.id if ctx.guild else None,
            role_name,
            category,
            "role_order",
            "discord_hybrid",
        )
        await send_temp(ctx, _canonical_role_missing_message())
        return
    if RoleManagementService.move_role(role_name, category, position):
        await send_temp(ctx, f"✅ Порядок роли **{role_name}** обновлён: категория **{category}**, позиция **{position}**.")
    else:
        logger.warning(
            "rolesadmin role_order failed actor_id=%s guild_id=%s role_name=%s category=%s position=%s operation=%s source=%s",
            ctx.author.id,
            ctx.guild.id if ctx.guild else None,
            role_name,
            category,
            position,
            "role_order",
            "discord_hybrid",
        )
        await send_temp(ctx, "❌ Не удалось обновить порядок роли. Проверь синхронизацию каталога и логи.")


@rolesadmin.command(name="user_roles", description="Посмотреть роли пользователя")
async def rolesadmin_user_roles(ctx: commands.Context, target: str):
    if not await _ensure_roles_admin(ctx):
        return

    resolved = await _resolve_discord_target(ctx, target, operation="user_roles")
    if not resolved:
        return

    roles = RoleManagementService.get_user_roles("discord", str(resolved["provider_user_id"]))
    if not roles:
        await send_temp(ctx, f"📭 У пользователя {resolved['label']} нет ролей.")
        return

    lines = [f"• {role['name']} ({role['category']})" for role in roles]
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
    ok = RoleManagementService.assign_user_role("discord", str(resolved["provider_user_id"]), role_name, category=category)
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
    ok = RoleManagementService.revoke_user_role("discord", str(resolved["provider_user_id"]), role_name)
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
