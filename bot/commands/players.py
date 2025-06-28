import discord
from discord.ext import commands
from typing import Optional

from bot.systems.players_logic import (
    register_player,
    list_players_view,
    edit_player,
    delete_player_cmd,
    list_player_logs_view,
    unregister_player,
    register_player_by_id
)
from bot.data.players_db import (
    get_player_by_id,
    create_player,
    add_player_to_tournament,
)

from bot.commands.base import bot
from bot.utils import send_temp

# ─── Регистрация игрока в системе ────────────────────────────────────────────

@bot.command(name="register")
@commands.has_permissions(administrator=True)
async def register(ctx: commands.Context, *args: str):
    """
    ?register <nick> <@tg_username>
    или
    ?register <player_id> <tournament_id>
    """
    if len(args) != 2:
        await send_temp(
            "❌ Неверный синтаксис. Используйте:\n"
            "`?register <nick> <@tg_username>` — добавить нового игрока\n"
            "`?register <player_id> <tournament_id>` — зарегистрировать существующего в турнире"
        )
        return

    # оба аргумента — числа → регистрация по ID
    if args[0].isdigit() and args[1].isdigit():
        player_id = int(args[0])
        tournament_id = int(args[1])
        await register_player_by_id(ctx, player_id, tournament_id)
        return

    # иначе считаем это ник и Telegram
    nick, tg = args
    await register_player(ctx, nick, tg)
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
