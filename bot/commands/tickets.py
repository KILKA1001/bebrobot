import discord
from discord.ext import commands
from bot.data import db
from bot.systems import tickets_logic

bot = db.bot  # ���������� ���������� ���������

@bot.command(name="addticket")
@commands.has_permissions(administrator=True)
async def add_ticket(ctx, member: discord.Member, ticket_type: str, amount: int, *, reason: str = "��� �������"):
    embed = await tickets_logic.give_ticket_logic(
        user_id=member.id,
        ticket_type=ticket_type.lower(),
        amount=amount,
        reason=reason,
        author_id=ctx.author.id
    )
    await ctx.send(embed=embed)

@bot.command(name="removeticket")
@commands.has_permissions(administrator=True)
async def remove_ticket(ctx, member: discord.Member, ticket_type: str, amount: int, *, reason: str = "��� �������"):
    """
    ������� ����� � ������������.
    ������: ?removeticket @user normal 2 �� ���������
    """
    embed = await tickets_logic.remove_ticket_logic(
        user_id=member.id,
        ticket_type=ticket_type.lower(),
        amount=amount,
        reason=reason,
        author_id=ctx.author.id
    )
    await ctx.send(embed=embed)
