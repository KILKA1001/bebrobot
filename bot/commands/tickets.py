import discord
from discord.ext import commands
from bot.systems import tickets_logic

from bot.commands import bot  # используем глобальный экземпляр
from bot.utils import send_temp
from bot.services import AuthorityService


@bot.hybrid_command(name="addticket", description="Выдать билет участнику")
async def add_ticket(
    ctx,
    member: discord.Member,
    ticket_type: str,
    amount: int,
    *,
    reason: str = "Без причины"
):
    if not AuthorityService.has_command_permission("discord", str(ctx.author.id), "tickets_manage") and not ctx.author.guild_permissions.administrator:
        await send_temp(ctx, "❌ Недостаточно полномочий для выдачи билетов.")
        return
    if member.id == ctx.author.id:
        if not AuthorityService.can_manage_self("discord", str(ctx.author.id)):
            await send_temp(ctx, "❌ Нельзя редактировать себе билеты. Доступно только Главе клуба и Главному вице.")
            return
    elif not AuthorityService.can_manage_target("discord", str(ctx.author.id), "discord", str(member.id)):
        await send_temp(ctx, "❌ Нельзя выдавать билеты пользователю с равным/более высоким званием.")
        return
    embed = await tickets_logic.give_ticket_logic(
        user_id=member.id,
        ticket_type=ticket_type.lower(),
        amount=amount,
        reason=reason,
        author_id=ctx.author.id,
    )
    await send_temp(ctx, embed=embed, delete_after=None)


@bot.hybrid_command(
    name="removeticket", description="Списать билет у пользователя"
)
async def remove_ticket(
    ctx,
    member: discord.Member,
    ticket_type: str,
    amount: int,
    *,
    reason: str = "Без причины"
):
    """
    Списать билет у пользователя.
    Пример: /removeticket @user normal 2 За нарушение
    """
    if not AuthorityService.has_command_permission("discord", str(ctx.author.id), "tickets_manage") and not ctx.author.guild_permissions.administrator:
        await send_temp(ctx, "❌ Недостаточно полномочий для списания билетов.")
        return
    if member.id == ctx.author.id:
        if not AuthorityService.can_manage_self("discord", str(ctx.author.id)):
            await send_temp(ctx, "❌ Нельзя редактировать себе билеты. Доступно только Главе клуба и Главному вице.")
            return
    elif not AuthorityService.can_manage_target("discord", str(ctx.author.id), "discord", str(member.id)):
        await send_temp(ctx, "❌ Нельзя списывать билеты у пользователя с равным/более высоким званием.")
        return
    embed = await tickets_logic.remove_ticket_logic(
        user_id=member.id,
        ticket_type=ticket_type.lower(),
        amount=amount,
        reason=reason,
        author_id=ctx.author.id,
    )
    await send_temp(ctx, embed=embed)
