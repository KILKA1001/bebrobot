import discord
from discord.ext import commands
from typing import Optional

# UI-класс и вся бизнес-логика в одном модуле
from bot.systems.tournament_logic import (
    TournamentSetupView,
    join_tournament,
    start_round,
    report_result,
    show_status,
    end_tournament,
    show_history,
    delete_tournament,
    ConfirmDeleteView,
    Tournament,
    handle_jointournament,
    handle_regplayer,
    handle_unregister
)
from bot.data.tournament_db import add_discord_participant as db_add_participant


# Import the bot instance from base.py instead of creating a new one
from bot.commands.base import bot

# В памяти храним экземпляры турниров
active_tournaments: dict[int, Tournament] = {}

@bot.command(name="createtournament")
@commands.has_permissions(administrator=True)
async def createtournament(ctx):
    """Запустить создание нового турнира через мультишаговый UI."""
    view = TournamentSetupView(ctx.author.id)
    await ctx.send(embed=view.initial_embed(), view=view)
    
@bot.command(name="jointournament")
async def jointournament(ctx: commands.Context, tournament_id: int):
    await handle_jointournament(ctx, tournament_id)

@bot.command(name="startround")
@commands.has_permissions(administrator=True)
async def startround(ctx, tournament_id: int):
    """Начать новый раунд турнира."""
    await start_round(ctx, tournament_id)
    
@bot.command(name="reportresult")
async def reportresult(ctx, match_id: int, winner: int):
    """Сообщить результат матча."""
    await report_result(ctx, match_id, winner)

@bot.command(name="tournamentstatus")
async def tournamentstatus(ctx, tournament_id: int, round_number: Optional[int] = None):
    """Показать статус турнира или конкретного раунда."""
    await show_status(ctx, tournament_id, round_number)

@bot.command(name="endtournament")
@commands.has_permissions(administrator=True)
async def endtournament(ctx, tid: int, first: int, second: int, third: Optional[int] = None):
    await end_tournament(ctx, tid, first, second, third)

@bot.command(name="tournamenthistory")
async def tournamenthistory(ctx, limit: int = 10):
    """Показать историю последних турниров."""
    await show_history(ctx, limit)

@bot.command(name="deletetournament")
@commands.has_permissions(administrator=True)
async def deletetournament(ctx, tournament_id: int):
    """Удалить турнир и все связанные с ним записи."""

@bot.command(name="regplayer")
@commands.has_permissions(administrator=True)
async def regplayer(ctx: commands.Context, player_id: int, tournament_id: int):
    await handle_regplayer(ctx, player_id, tournament_id)

@bot.command(name="unregister")
@commands.has_permissions(administrator=True)
async def unregister(ctx: commands.Context, identifier: str, tournament_id: int):
    await handle_unregister(ctx, identifier, tournament_id)
