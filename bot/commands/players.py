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

# ─── Регистрация игрока в системе ────────────────────────────────────────────


@bot.hybrid_command(name="register", description="Добавить игрока в систему")
@commands.has_permissions(administrator=True)
async def register(ctx: commands.Context, nick: str, tg_username: str):
    """
    /register <nick> <@tg_username>
    """
    await register_player(ctx, nick, tg_username)


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
@commands.has_permissions(administrator=True)
async def editplayer(
    ctx: commands.Context, player_id: int, field: str, *, new_value: str
):
    """
    Редактирует поле игрока:
    /editplayer <player_id> <nick|tg_username> <new_value>
    """
    await edit_player(ctx, player_id, field, new_value)


# --- Удаление игрока ---


@bot.hybrid_command(
    name="deleteplayer", description="Удалить игрока из системы"
)
@commands.has_permissions(administrator=True)
async def deleteplayer(ctx: commands.Context, player_id: int):
    """
    Удаляет игрока из системы:
    /deleteplayer <player_id>
    """
    await delete_player_cmd(ctx, player_id)




@bot.hybrid_command(name="playerlogs", description="История изменений игрока")
@commands.has_permissions(administrator=True)
async def playerlogs(
    ctx: commands.Context, player_id: int, page: Optional[int] = 1
):
    """
    /playerlogs <player_id> [page]
    Показывает историю правок данных игрока.
    """
    try:
        pg = page if page is not None else 1
    except (TypeError, ValueError):
        pg = 1
    await list_player_logs_view(ctx, player_id, pg)
