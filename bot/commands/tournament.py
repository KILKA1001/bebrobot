import discord
from discord.ext import commands
from typing import Optional
import os

# UI-класс и вся бизнес-логика в одном модуле
from bot.systems.tournament_logic import (
    TournamentSetupView,
    end_tournament,
    show_history,
    Tournament,
    handle_jointournament,
    build_tournament_status_embed,
    build_tournament_bracket_embed,
    build_tournament_result_embed,
    format_tournament_title,
)
from bot.systems.manage_tournament_view import ManageTournamentView
from bot.systems.tournament_admin_ui import TournamentAdminDashboard
from bot.data.tournament_db import get_tournament_status, get_tournament_info

# Import the bot instance from base.py instead of creating a new one
from bot.commands.base import bot
from bot.utils import send_temp

# Дополнительные структуры для хранения авторов турниров и подтверждений
tournament_admins: dict[int, int] = {}
confirmed_participants: dict[int, set[int]] = {}

# В памяти храним экземпляры турниров
active_tournaments: dict[int, Tournament] = {}

# Роли, которым разрешено создавать и управлять турнирами
TOURNAMENT_ROLE_IDS = tuple(
    int(r) for r in os.getenv("TOURNAMENT_ROLE_IDS", "").split(",") if r
)


def has_tournament_permission(ctx: commands.Context) -> bool:
    if ctx.author.guild_permissions.administrator:
        return True
    return any(role.id in TOURNAMENT_ROLE_IDS for role in ctx.author.roles)


@bot.hybrid_command(
    name="createtournament", description="Создать новый турнир"
)
@commands.check(has_tournament_permission)
async def createtournament(ctx):
    """Запустить создание нового турнира через мультишаговый UI."""
    if ctx.interaction and not ctx.interaction.response.is_done():
        # Acknowledge the interaction to avoid "Unknown interaction" errors
        await ctx.defer()
    view = TournamentSetupView(ctx.author.id)
    msg = await send_temp(ctx, embed=view.initial_embed(), view=view)
    view.message = msg


@bot.hybrid_command(
    name="tournamentadmin", description="Панель управления турнирами"
)
@commands.check(has_tournament_permission)
async def tournamentadmin(ctx: commands.Context):
    """Открывает центральную панель для администраторов турниров."""
    if ctx.interaction and not ctx.interaction.response.is_done():
        await ctx.defer()
    view = TournamentAdminDashboard(ctx)
    embed = discord.Embed(
        title="🎮 Панель турниров", color=discord.Color.blurple()
    )
    await send_temp(ctx, embed=embed, view=view)


@bot.hybrid_command(
    name="managetournament", description="Панель управления турниром"
)
@commands.check(has_tournament_permission)
async def manage_tournament(ctx, tournament_id: int):
    """Открывает расширенную панель управления турниром.

    `tournament_id` — номер турнира из базы
    (смотрите `/tournamenthistory`).
    """
    if ctx.interaction and not ctx.interaction.response.is_done():
        await ctx.defer()
    status = get_tournament_status(tournament_id)
    if status == "finished":
        embed = await build_tournament_result_embed(tournament_id, ctx.guild)
    else:
        embed = await build_tournament_bracket_embed(
            tournament_id, ctx.guild, include_id=True
        )
        if not embed:
            embed = await build_tournament_status_embed(
                tournament_id, include_id=True
            )
    if not embed:
        info = get_tournament_info(tournament_id) or {}
        title = format_tournament_title(
            info.get("name"), info.get("start_time"), tournament_id, include_id=True
        )
        embed = discord.Embed(
            title=f"⚙ Управление турниром {title}",
            color=discord.Color.blue(),
        )

    view = ManageTournamentView(tournament_id, ctx)
    await send_temp(ctx, embed=embed, view=view)


@bot.hybrid_command(
    name="jointournament", description="Подать заявку на участие"
)
async def jointournament(ctx: commands.Context, tournament_id: int):
    """Заявиться на участие в турнире по его номеру."""
    await handle_jointournament(ctx, tournament_id)


async def endtournament(
    ctx, tid: int, first: int, second: int, third: Optional[int] = None
):
    """Завершить турнир и указать призёров."""
    await end_tournament(ctx, tid, first, second, third)


@bot.hybrid_command(
    name="tournamenthistory", description="Показать историю турниров"
)
async def tournamenthistory(ctx, limit: int = 10):
    """Показать историю последних турниров."""
    await show_history(ctx, limit)


