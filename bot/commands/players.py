from discord.ext import commands
from typing import Optional

from bot.systems.players_logic import (
    register_player,
    list_players_view,
    edit_player,
    delete_player_cmd,
    list_player_logs_view,
)

from bot.commands.base import bot
from bot.services import AuthorityService
from bot.utils import send_temp

# ─── Регистрация игрока в системе ────────────────────────────────────────────


@bot.hybrid_command(name="register", description="Добавить игрока в систему")
async def register(ctx: commands.Context, nick: str):
    if not AuthorityService.has_command_permission("discord", str(ctx.author.id), "players_manage") and not ctx.author.guild_permissions.administrator:
        await send_temp(ctx, "❌ Недостаточно полномочий для регистрации игроков.")
        return
    """
    /register <nick>
    """
    await register_player(ctx, nick)


# ─── Список игроков ──────────────────────────────────────────────────────────


@bot.hybrid_command(name="listplayers", description="Показать список игроков")
async def listplayers(ctx: commands.Context, page: Optional[int] = 1):
    """
    Показывает постраничный список игроков:
    /listplayers [page]
    """
    try:
        page_num = page if page is not None else 1
    except (TypeError, ValueError):
        page_num = 1
    await list_players_view(ctx, page_num)


# --- Редактирование информации об игроке ---


@bot.hybrid_command(name="editplayer", description="Изменить данные игрока")
async def editplayer(
    ctx: commands.Context, player_id: int, field: str, *, new_value: str
):
    if not AuthorityService.has_command_permission("discord", str(ctx.author.id), "players_manage") and not ctx.author.guild_permissions.administrator:
        await send_temp(ctx, "❌ Недостаточно полномочий для редактирования игроков.")
        return
    """
    Редактирует поле игрока:
    /editplayer <player_id> <nick> <new_value>
    """
    await edit_player(ctx, player_id, field, new_value)


# --- Удаление игрока ---


@bot.hybrid_command(
    name="deleteplayer", description="Удалить игрока из системы"
)
async def deleteplayer(ctx: commands.Context, player_id: int):
    if not AuthorityService.has_command_permission("discord", str(ctx.author.id), "players_manage") and not ctx.author.guild_permissions.administrator:
        await send_temp(ctx, "❌ Недостаточно полномочий для удаления игроков.")
        return
    """
    Удаляет игрока из системы:
    /deleteplayer <player_id>
    """
    await delete_player_cmd(ctx, player_id)




@bot.hybrid_command(name="playerlogs", description="История изменений игрока")
async def playerlogs(
    ctx: commands.Context, player_id: int, page: Optional[int] = 1
):
    if not AuthorityService.has_command_permission("discord", str(ctx.author.id), "players_manage") and not ctx.author.guild_permissions.administrator:
        await send_temp(ctx, "❌ Недостаточно полномочий для просмотра логов игроков.")
        return
    """
    /playerlogs <player_id> [page]
    Показывает историю правок данных игрока.
    """
    try:
        pg = page if page is not None else 1
    except (TypeError, ValueError):
        pg = 1
    await list_player_logs_view(ctx, player_id, pg)
