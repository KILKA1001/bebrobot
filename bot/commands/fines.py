import discord
from discord.ext import commands
from datetime import datetime, timedelta, timezone
from bot.data import db

ALLOWED_ROLES = []  # 👉 сюда можно вписать ID ролей, кому разрешено выдавать штрафы

def has_permission(ctx):
    if ctx.author.guild_permissions.administrator:
        return True
    return any(role.id in ALLOWED_ROLES for role in ctx.author.roles)

@commands.command(name="fine")
async def fine(ctx, member: discord.Member, amount: str, fine_type: int, *, reason: str = "Без причины"):
    if not has_permission(ctx):
        await ctx.send("❌ У вас нет прав для назначения штрафов.")
        return

    try:
        amount_value = float(amount.replace(',', '.'))
        if amount_value <= 0:
            raise ValueError

        if fine_type not in (1, 2):
            await ctx.send("❌ Тип штрафа должен быть 1 (обычный) или 2 (усиленный).")
            return

        due_date = datetime.now(timezone.utc) + timedelta(days=14 if fine_type == 1 else 30)

        fine = db.add_fine(
            user_id=member.id,
            author_id=ctx.author.id,
            amount=amount_value,
            fine_type=fine_type,
            reason=reason,
            due_date=due_date
        )

        if fine:
            embed = discord.Embed(
                title="📌 Назначен штраф",
                description=f"{member.mention}, вам назначен штраф.",
                color=discord.Color.red()
            )
            embed.add_field(name="Сумма", value=f"{amount_value:.2f} баллов", inline=True)
            embed.add_field(name="Тип", value=f"{'Обычный (14 дней)' if fine_type == 1 else 'Усиленный (30 дней)'}", inline=True)
            embed.add_field(name="Причина", value=reason, inline=False)
            embed.add_field(name="Срок оплаты", value=due_date.strftime("%d.%m.%Y"), inline=True)
            embed.set_footer(text=f"ID штрафа: {fine['id']}")

            await ctx.send(embed=embed)
            try:
                await member.send(embed=embed)
            except discord.Forbidden:
                await ctx.send(f"⚠️ Не удалось отправить сообщение в ЛС {member.mention}")

        else:
            await ctx.send("❌ Не удалось создать штраф.")

    except ValueError:
        await ctx.send("❌ Введите корректную сумму.")
