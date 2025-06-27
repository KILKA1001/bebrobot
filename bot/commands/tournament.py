import discord
from discord.ext import commands
from typing import Optional

# UI-класс и вся бизнес-логика в одном модуле
from bot.systems.tournament_logic import (
    TournamentSetupView,
    join_tournament,
    end_tournament,
    show_history,
    delete_tournament,
    ConfirmDeleteView,
    Tournament,
    handle_jointournament,
    handle_regplayer,
    handle_unregister,
    create_tournament_logic
)
from bot.data.tournament_db import add_discord_participant as db_add_participant
from bot.systems.tournament_logic import delete_tournament as send_delete_confirmation
# Import the bot instance from base.py instead of creating a new one
from bot.commands.base import bot
from bot.systems.interactive_rounds import announce_round_management, RoundManagementView
from bot.systems.tournament_logic import create_tournament_logic
from bot.data.tournament_db import list_participants

# В памяти храним экземпляры турниров
active_tournaments: dict[int, Tournament] = {}

@bot.command(name="createtournament")
@commands.has_permissions(administrator=True)
async def createtournament(ctx):
    """Запустить создание нового турнира через мультишаговый UI."""
    view = TournamentSetupView(ctx.author.id)
    await ctx.send(embed=view.initial_embed(), view=view)

@bot.command(name="managetournament")
@commands.has_permissions(administrator=True)
async def manage_tournament(ctx, tournament_id: int):
    from bot.data.tournament_db import (
        list_participants_full,
        get_tournament_status,
        get_tournament_size
    )
    
    # Получаем данные о турнире
    participants = [p["discord_user_id"] for p in list_participants_full(tournament_id)]
    status = get_tournament_status(tournament_id)
    max_participants = get_tournament_size(tournament_id)
    
    # Создаем embed с информацией
    embed = discord.Embed(
        title=f"⚙ Управление турниром #{tournament_id}",
        color=discord.Color.blue()
    )
    embed.add_field(name="Статус", value=status, inline=True)
    embed.add_field(name="Участники", value=f"{len(participants)}/{max_participants}", inline=True)
    
    # Создаем View с кнопками
    logic = create_tournament_logic(participants)
    view = RoundManagementView(tournament_id, logic)
    
    await ctx.send(embed=embed, view=view)
    
@bot.command(name="jointournament")
async def jointournament(ctx: commands.Context, tournament_id: int):
    await handle_jointournament(ctx, tournament_id)

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
    await send_delete_confirmation(ctx, tournament_id)

@bot.command(name="regplayer")
@commands.has_permissions(administrator=True)
async def regplayer(ctx: commands.Context, player_id: int, tournament_id: int):
    await handle_regplayer(ctx, player_id, tournament_id)

@bot.command(name="tunregister")
@commands.has_permissions(administrator=True)
async def tournament_unregister(ctx: commands.Context, identifier: str, tournament_id: int):
    await handle_unregister(ctx, identifier, tournament_id)

@bot.command(name="tournamentannounce")
@commands.has_permissions(administrator=True)
async def tournament_announce(ctx, tournament_id: int):
    from bot.systems import tournament_logic
    success = await tournament_logic.send_announcement_embed(ctx, tournament_id)
    if not success:
        await ctx.send("❌ Не удалось отправить объявление. Проверь ID турнира.")

@bot.command(name="managerounds")
@commands.has_permissions(administrator=True)
async def managerounds(ctx: commands.Context, tournament_id: int):
    """
    ?managerounds <ID> — открывает интерактивную панель
    управления раундами указанного турнира.
    """
    # 1) подтягиваем участников из БД
    raw = list_participants(tournament_id)
    participants = [
        entry.get("discord_user_id") or entry.get("player_id")
        for entry in raw
    ]

    # 2) создаём объект логики турнира
    logic = create_tournament_logic(participants)

    # 3) запускаем интерактивный View
    await announce_round_management(ctx.channel, tournament_id, logic)