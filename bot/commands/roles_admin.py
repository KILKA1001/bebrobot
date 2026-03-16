import logging

import discord
from discord.ext import commands

from bot.commands.base import bot
from bot.services import AuthorityService, RoleManagementService
from bot.utils import send_temp

logger = logging.getLogger(__name__)


async def _ensure_roles_admin(ctx: commands.Context) -> bool:
    if ctx.author.guild_permissions.administrator:
        return True
    if not AuthorityService.has_command_permission("discord", str(ctx.author.id), "players_manage"):
        await send_temp(ctx, "❌ Недостаточно полномочий для управления ролями.")
        return False
    return True


@bot.hybrid_group(name="rolesadmin", description="Управление ролями и категориями", with_app_command=True)
async def rolesadmin(ctx: commands.Context):
    if ctx.invoked_subcommand is None:
        await send_temp(ctx, "Используйте подкоманды: list, category_create, category_delete, role_create, role_delete, role_move, user_roles, user_grant, user_revoke")


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
    if RoleManagementService.create_category(name, position):
        await send_temp(ctx, f"✅ Категория **{name}** создана/обновлена.")
    else:
        await send_temp(ctx, "❌ Не удалось создать категорию (смотри логи).")


@rolesadmin.command(name="category_delete", description="Удалить категорию ролей")
async def rolesadmin_category_delete(ctx: commands.Context, name: str):
    if not await _ensure_roles_admin(ctx):
        return
    if RoleManagementService.delete_category(name):
        await send_temp(ctx, f"✅ Категория **{name}** удалена. Роли перенесены в 'Без категории'.")
    else:
        await send_temp(ctx, "❌ Не удалось удалить категорию (смотри логи).")


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
    if RoleManagementService.move_role(role_name, category, position):
        await send_temp(ctx, f"✅ Роль **{role_name}** перемещена в **{category}**.")
    else:
        await send_temp(ctx, "❌ Не удалось переместить роль (смотри логи).")


@rolesadmin.command(name="user_roles", description="Посмотреть роли пользователя")
async def rolesadmin_user_roles(ctx: commands.Context, member: discord.Member):
    if not await _ensure_roles_admin(ctx):
        return

    roles = RoleManagementService.get_user_roles("discord", str(member.id))
    if not roles:
        await send_temp(ctx, f"📭 У пользователя {member.mention} нет ролей в account_role_assignments.")
        return

    lines = [f"• {role['name']} ({role.get('category') or 'Без категории'})" for role in roles]
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
