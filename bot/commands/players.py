import discord
from discord.ext import commands
from typing import Optional

from bot.systems.players_logic import (
    register_player,
    list_players_view,
    edit_player,
    delete_player_cmd,
    list_player_logs_view,
    unregister_player
)
from bot.data.players_db import (
    get_player_by_id,
    create_player,
    add_player_to_tournament,
)

from bot.commands.base import bot

# ─── Регистрация игрока в системе ────────────────────────────────────────────

@bot.command(name="register")
@commands.has_permissions(administrator=True)
async def register_player_by_id(
    ctx: commands.Context,
    player_id: int,
    tournament_id: int
) -> None:
    """
    Берёт уже существующего игрока и связывает его с турниром через add_player_to_tournament.
    """
    player = get_player_by_id(player_id)
    if not player:
        await ctx.send("❌ Игрок с таким ID не найден.")
        return

    ok = add_player_to_tournament(player_id, tournament_id)
    if ok:
        await ctx.send(
            f"✅ Игрок #{player_id} (`{player['nick']}`) зарегистрирован в турнире #{tournament_id}."
        )
    else:
        await ctx.send("❌ Не удалось зарегистрировать игрока в турнире.")
# ─── Список игроков ──────────────────────────────────────────────────────────

@bot.command(name="listplayers")
async def listplayers(ctx: commands.Context, page: Optional[int] = 1):
    """
    Показывает постраничный список игроков:
    ?listplayers [page]
    """
    try:
        page_num = page if page is not None else 1
    except (TypeError, ValueError):
        page_num = 1
    await list_players_view(ctx, page_num)

# ─── Редактирование информации об игроке ───────────────────────────────────────

@bot.command(name="editplayer")
@commands.has_permissions(administrator=True)
async def editplayer(ctx: commands.Context, player_id: int, field: str, *, new_value: str):
    """
    Редактирует поле игрока:
    ?editplayer <player_id> <nick|tg_username> <new_value>
    """
    await edit_player(ctx, player_id, field, new_value)

# ─── Удаление игрока ──────────────────────────────────────────────────────────

@bot.command(name="deleteplayer")
@commands.has_permissions(administrator=True)
async def deleteplayer(ctx: commands.Context, player_id: int):
    """
    Удаляет игрока из системы:
    ?deleteplayer <player_id>
    """
    await delete_player_cmd(ctx, player_id)

@bot.command(name="unregister")
@commands.has_permissions(administrator=True)
async def unregister(ctx: commands.Context, player_id: int, tournament_id: int):
    """
    ?unregister <player_id> <tournament_id>
    Убирает игрока из указанного турнира.
    """
    await unregister_player(ctx, player_id, tournament_id)

@bot.command(name="playerlogs")
@commands.has_permissions(administrator=True)
async def playerlogs(ctx: commands.Context, player_id: int, page: Optional[int] = 1):
    """
    ?playerlogs <player_id> [page]
    Показывает историю правок данных игрока.
    """
    try:
        pg = page if page is not None else 1
    except (TypeError, ValueError):
        pg = 1
    await list_player_logs_view(ctx, player_id, pg)
