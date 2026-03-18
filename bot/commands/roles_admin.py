import logging

import discord
from discord.ext import commands

from bot.commands.base import bot
from bot.services import AuthorityService, RoleManagementService
from bot.utils import send_temp

logger = logging.getLogger(__name__)


def _render_role_source_note() -> str:
    return (
        "Для `role_move` и `role_order` доступны только роли из канонического каталога `roles`. "
        "Перед открытием панели или ручной синхронизацией `/rolesadmin sync_discord_roles` Discord-роли "
        "подтягиваются в каталог; если роль ещё не появилась, дождитесь синхронизации и проверьте логи."
    )


def _catalog_role_exists(role_name: str) -> bool:
    role_key = str(role_name or "").strip()
    if not role_key:
        return False
    return any(item["role"] == role_key for item in RoleManagementService.list_roles_available_for_admin_reorder())

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
            "`/rolesadmin role_delete <name>` — удалить роль"
        ),
        inline=False,
    )
    embed.add_field(
        name="Роли пользователей",
        value=(
            "`/rolesadmin user_roles <member>` — посмотреть роли пользователя\n"
            "`/rolesadmin user_grant <member> <role_name>` — выдать роль\n"
            "`/rolesadmin user_revoke <member> <role_name>` — снять роль"
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
    if RoleManagementService.delete_role(name):
        await send_temp(ctx, f"✅ Роль **{name}** удалена.")
    else:
        await send_temp(ctx, "❌ Не удалось удалить роль (смотри логи).")


@rolesadmin.command(name="role_move", description="Переместить роль в другую категорию")
async def rolesadmin_role_move(ctx: commands.Context, role_name: str, category: str, position: int = 0):
    if not await _ensure_roles_admin(ctx):
        return
    if not _catalog_role_exists(role_name):
        logger.warning(
            "rolesadmin role_move denied role missing from canonical catalog actor_id=%s guild_id=%s role_name=%s",
            ctx.author.id,
            ctx.guild.id if ctx.guild else None,
            role_name,
        )
        await send_temp(ctx, "❌ Роль не найдена в каталоге `roles`. Запусти `/rolesadmin sync_discord_roles` и проверь логи.")
        return
    if RoleManagementService.move_role(role_name, category, position):
        await send_temp(ctx, f"✅ Роль **{role_name}** перемещена в **{category}**.")
    else:
        await send_temp(ctx, "❌ Не удалось переместить роль (смотри логи).")


@rolesadmin.command(name="role_order", description="Изменить порядок роли в категории")
async def rolesadmin_role_order(ctx: commands.Context, role_name: str, category: str, position: int):
    if not await _ensure_roles_admin(ctx):
        return
    if not _catalog_role_exists(role_name):
        logger.warning(
            "rolesadmin role_order denied role missing from canonical catalog actor_id=%s guild_id=%s role_name=%s",
            ctx.author.id,
            ctx.guild.id if ctx.guild else None,
            role_name,
        )
        await send_temp(ctx, "❌ Роль не найдена в каталоге `roles`. Запусти `/rolesadmin sync_discord_roles` и проверь логи.")
        return
    if RoleManagementService.move_role(role_name, category, position):
        await send_temp(ctx, f"✅ Порядок роли **{role_name}** обновлён: категория **{category}**, позиция **{position}**.")
    else:
        await send_temp(ctx, "❌ Не удалось обновить порядок роли (смотри логи).")


@rolesadmin.command(name="user_roles", description="Посмотреть роли пользователя")
async def rolesadmin_user_roles(ctx: commands.Context, member: discord.Member):
    if not await _ensure_roles_admin(ctx):
        return

    roles = RoleManagementService.get_user_roles("discord", str(member.id))
    if not roles:
        await send_temp(ctx, f"📭 У пользователя {member.mention} нет ролей в account_role_assignments.")
        return

    lines = [f"• {role['name']} ({role['category']})" for role in roles]
    await send_temp(ctx, f"🧾 Роли {member.mention}:\n" + "\n".join(lines))


@rolesadmin.command(name="user_grant", description="Выдать роль пользователю")
async def rolesadmin_user_grant(ctx: commands.Context, member: discord.Member, role_name: str):
    if not await _ensure_roles_admin(ctx):
        return

    role_info = RoleManagementService.get_role(role_name)
    category = role_info.get("category_name") if role_info else None
    ok = RoleManagementService.assign_user_role("discord", str(member.id), role_name, category=category)
    if not ok:
        await send_temp(ctx, "❌ Не удалось выдать роль в БД (смотри логи).")
        return

    discord_role_id = str(role_info.get("discord_role_id") or "").strip() if role_info else ""
    if discord_role_id:
        guild_role = ctx.guild.get_role(int(discord_role_id)) if ctx.guild else None
        if guild_role:
            try:
                await member.add_roles(guild_role, reason=f"rolesadmin grant by {ctx.author.id}")
            except Exception:
                logger.exception("failed to add discord role member_id=%s role_id=%s", member.id, discord_role_id)
                await send_temp(ctx, "⚠️ Роль в БД выдана, но выдать Discord-роль не удалось (смотри логи).")
                return

    await send_temp(ctx, f"✅ Роль **{role_name}** выдана пользователю {member.mention}.")


@rolesadmin.command(name="user_revoke", description="Забрать роль у пользователя")
async def rolesadmin_user_revoke(ctx: commands.Context, member: discord.Member, role_name: str):
    if not await _ensure_roles_admin(ctx):
        return

    role_info = RoleManagementService.get_role(role_name)
    ok = RoleManagementService.revoke_user_role("discord", str(member.id), role_name)
    if not ok:
        await send_temp(ctx, "❌ Не удалось забрать роль в БД (смотри логи).")
        return

    discord_role_id = str(role_info.get("discord_role_id") or "").strip() if role_info else ""
    if discord_role_id:
        guild_role = ctx.guild.get_role(int(discord_role_id)) if ctx.guild else None
        if guild_role:
            try:
                await member.remove_roles(guild_role, reason=f"rolesadmin revoke by {ctx.author.id}")
            except Exception:
                logger.exception("failed to remove discord role member_id=%s role_id=%s", member.id, discord_role_id)
                await send_temp(ctx, "⚠️ Роль в БД снята, но снять Discord-роль не удалось (смотри логи).")
                return

    await send_temp(ctx, f"✅ Роль **{role_name}** снята у {member.mention}.")


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
