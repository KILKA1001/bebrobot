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
    create_tournament_logic,
    build_tournament_status_embed,
    build_tournament_bracket_embed
)
from bot.data.tournament_db import add_discord_participant as db_add_participant
from bot.systems.tournament_logic import delete_tournament as send_delete_confirmation
# Import the bot instance from base.py instead of creating a new one
from bot.commands.base import bot
from bot.utils import send_temp
from bot.systems.interactive_rounds import RoundManagementView

# Дополнительные структуры для хранения авторов турниров и подтверждений
tournament_admins: dict[int, int] = {}
confirmed_participants: dict[int, set[int]] = {}

# В памяти храним экземпляры турниров
active_tournaments: dict[int, Tournament] = {}

@bot.hybrid_command(
    name="createtournament",
    description='Создать новый турнир'
)
@commands.has_permissions(administrator=True)
async def createtournament(ctx):
    """Запустить создание нового турнира через мультишаговый UI."""
    view = TournamentSetupView(ctx.author.id)
    msg = await send_temp(ctx, embed=view.initial_embed(), view=view)
    view.message = msg

@bot.hybrid_command(
    name="managetournament",
    description='Панель управления турниром'
)
@commands.has_permissions(administrator=True)
async def manage_tournament(ctx, tournament_id: int):
    """Открывает расширенную панель управления турниром.

    `tournament_id` — это номер турнира из базы (смотрите `/tournamenthistory`).
    """
    from bot.data.tournament_db import list_participants_full

    participants = [p["discord_user_id"] for p in list_participants_full(tournament_id)]
    from bot.data.tournament_db import get_tournament_info
    info = get_tournament_info(tournament_id) or {}
    team_size = 3 if info.get("type") == "team" else 1
    logic = create_tournament_logic(participants, team_size=team_size)

    embed = await build_tournament_bracket_embed(tournament_id, ctx.guild)
    if not embed:
        embed = await build_tournament_status_embed(tournament_id)
    if not embed:
        embed = discord.Embed(title=f"⚙ Управление турниром #{tournament_id}", color=discord.Color.blue())

    view = RoundManagementView(tournament_id, logic)
    await send_temp(ctx, embed=embed, view=view)
    
@bot.hybrid_command(
    name="jointournament",
    description='Подать заявку на участие'
)
async def jointournament(ctx: commands.Context, tournament_id: int):
    """Заявиться на участие в турнире по его номеру."""
    await handle_jointournament(ctx, tournament_id)

@bot.hybrid_command(
    name="endtournament",
    description='Завершить турнир и указать призёров'
)
@commands.has_permissions(administrator=True)
async def endtournament(ctx, tid: int, first: int, second: int, third: Optional[int] = None):
    await end_tournament(ctx, tid, first, second, third)

@bot.hybrid_command(
    name="tournamenthistory",
    description='Показать историю турниров'
)
async def tournamenthistory(ctx, limit: int = 10):
    """Показать историю последних турниров."""
    await show_history(ctx, limit)

@bot.hybrid_command(
    name="deletetournament",
    description='Удалить турнир из базы'
)
@commands.has_permissions(administrator=True)
async def deletetournament(ctx, tournament_id: int):
    """Удалить турнир и все связанные с ним записи."""
    await send_delete_confirmation(ctx, tournament_id)

@bot.hybrid_command(
    name="regplayer",
    description='Добавить участника в турнир'
)
@commands.has_permissions(administrator=True)
async def regplayer(ctx: commands.Context, player_id: int, tournament_id: int):
    await handle_regplayer(ctx, player_id, tournament_id)

@bot.hybrid_command(
    name="tunregister",
    description='Убрать участника из турнира'
)
@commands.has_permissions(administrator=True)
async def tournament_unregister(ctx: commands.Context, identifier: str, tournament_id: int):
    await handle_unregister(ctx, identifier, tournament_id)

@bot.hybrid_command(
    name="tournamentannounce",
    description='Отправить объявление о турнире'
)
@commands.has_permissions(administrator=True)
async def tournament_announce(ctx, tournament_id: int):
    from bot.systems import tournament_logic
    success = await tournament_logic.send_announcement_embed(ctx, tournament_id)
    if not success:
        await send_temp(ctx, "❌ Не удалось отправить объявление. Проверь ID турнира.")
