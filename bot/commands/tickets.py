import discord
from discord.ext import commands
from bot.data import db
from bot.systems import tickets_logic

from bot.commands import bot  # используем глобальный экземпляр
from bot.utils import send_temp

@bot.hybrid_command(
    name="addticket",
    description='Выдать билет участнику'
)
@commands.has_permissions(administrator=True)
async def add_ticket(ctx, member: discord.Member, ticket_type: str, amount: int, *, reason: str = "Без причины"):
    embed = await tickets_logic.give_ticket_logic(
        user_id=member.id,
        ticket_type=ticket_type.lower(),
        amount=amount,
        reason=reason,
        author_id=ctx.author.id
    )
    await send_temp(ctx, embed=embed, delete_after=None)

@bot.hybrid_command(
    name="removeticket",
    description='Списать билет у пользователя'
)
@commands.has_permissions(administrator=True)
async def remove_ticket(ctx, member: discord.Member, ticket_type: str, amount: int, *, reason: str = "Без причины"):
    """
    Списать билет у пользователя.
    Пример: /removeticket @user normal 2 За нарушение
    """
    embed = await tickets_logic.remove_ticket_logic(
        user_id=member.id,
        ticket_type=ticket_type.lower(),
        amount=amount,
        reason=reason,
        author_id=ctx.author.id
    )
    await send_temp(ctx, embed=embed)