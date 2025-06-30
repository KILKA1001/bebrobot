import discord
from discord.ext import commands
from datetime import datetime, timedelta, timezone
from typing import Optional

from bot.commands.base import bot
from bot.utils import send_temp, build_top_embed

from bot.data import db
from bot.systems.fines_logic import (
    build_fine_embed,
    build_fine_detail_embed,
    FineView,
    FinePaginator,
    AllFinesView,
    get_fine_leaders
)
ALLOWED_ROLES = []  # 👉 сюда можно вписать ID ролей, кому разрешено выдавать штрафы

def has_permission(ctx):
    if ctx.author.guild_permissions.administrator:
        return True
    return any(role.id in ALLOWED_ROLES for role in ctx.author.roles)

@bot.hybrid_command(name="fine")
async def fine(ctx, member: discord.Member, amount: str, fine_type: int, *, reason: str = "Без причины"):
    if not has_permission(ctx):
        await send_temp(ctx, "❌ У вас нет прав для назначения штрафов.")
        return

    try:
        amount_value = float(amount.replace(',', '.'))
        if amount_value <= 0:
            raise ValueError

        if fine_type not in (1, 2):
            await send_temp(ctx, "❌ Тип штрафа должен быть 1 (обычный) или 2 (усиленный).")
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
                description=(
                    f"{member.mention}, вам назначен штраф.\n\n"
                    f"ℹ️ Чтобы просмотреть и оплатить его, используйте команду `/myfines`"
                ),
                color=discord.Color.red()
            )
            embed.add_field(name="Сумма", value=f"{amount_value:.2f} баллов", inline=True)
            embed.add_field(name="Тип", value=f"{'Обычный (14 дней)' if fine_type == 1 else 'Усиленный (30 дней)'}", inline=True)
            embed.add_field(name="Причина", value=reason, inline=False)
            embed.add_field(name="Срок оплаты", value=due_date.strftime("%d.%m.%Y"), inline=True)
            embed.set_footer(text=f"ID штрафа: {fine['id']}")

            await send_temp(ctx, embed=embed)
            try:
                await member.send(embed=embed)
            except discord.Forbidden:
                await send_temp(ctx, f"⚠️ Не удалось отправить сообщение в ЛС {member.mention}")

        else:
            await send_temp(ctx, "❌ Не удалось создать штраф.")

    except ValueError:
        await send_temp(ctx, "❌ Введите корректную сумму.")

@bot.hybrid_command(name="myfines")
async def myfines(ctx):
    user_id = ctx.author.id
    fines = db.get_user_fines(user_id)

    if not fines:
        await send_temp(ctx, "✅ У вас нет активных штрафов!")
        return

    for fine in fines:
        embed = build_fine_embed(fine)
        view = FineView(fine)
        await send_temp(ctx, embed=embed, view=view)

@bot.hybrid_command(name="allfines")
@commands.has_permissions(administrator=True)
async def all_fines(ctx):
    fines = [f for f in db.fines if not f.get("is_paid") and not f.get("is_canceled")]

    if not fines:
        await send_temp(ctx, "✅ Нет активных штрафов.")
        return

    view = AllFinesView(fines, ctx)
    await send_temp(ctx, embed=view.get_page_embed(), view=view)

@bot.hybrid_command(name="finedetails")
async def finedetails(ctx, fine_id: int):
    fine = db.get_fine_by_id(fine_id)
    if not fine:
        await send_temp(ctx, "❌ Штраф не найден.")
        return

    is_admin = ctx.author.guild_permissions.administrator
    if fine["user_id"] != ctx.author.id and not is_admin:
        await send_temp(ctx, "❌ Вы не можете просматривать чужие штрафы.")
        return

    embed = build_fine_detail_embed(fine)
    await send_temp(ctx, embed=embed)

@bot.hybrid_command(name="editfine")
@commands.has_permissions(administrator=True)
async def editfine(ctx, fine_id: int, amount: float, fine_type: int, due_date_str: str, *, reason: str):
    fine = db.get_fine_by_id(fine_id)
    if not fine:
        await send_temp(ctx, "❌ Штраф не найден.")
        return

    try:
        # Европейский формат: ДД.ММ.ГГГГ
        due_date = datetime.strptime(due_date_str, "%d.%m.%Y").replace(tzinfo=timezone.utc)
    except ValueError:
        await send_temp(ctx, "❌ Неверный формат даты. Используйте ДД.ММ.ГГГГ.")
        return

    fine["amount"] = amount
    fine["type"] = fine_type
    fine["reason"] = reason
    fine["due_date"] = due_date.isoformat()

    if not db.supabase:
        await send_temp(ctx, "❌ Supabase не инициализирован.")
        return

    db.supabase.table("fines").update({
        "amount": amount,
        "type": fine_type,
        "reason": reason,
        "due_date": due_date.isoformat()
    }).eq("id", fine_id).execute()

    await send_temp(ctx, f"✏️ Штраф #{fine_id} успешно обновлён.")

@bot.hybrid_command(name="cancel_fine")
@commands.has_permissions(administrator=True)
async def cancel_fine(ctx, fine_id: int):
    fine = db.get_fine_by_id(fine_id)
    if not fine:
        await send_temp(ctx, "❌ Штраф не найден.")
        return

    if fine.get("is_canceled"):
        await send_temp(ctx, "⚠️ Этот штраф уже отменён.")
        return

    fine["is_canceled"] = True

    if not db.supabase:
        await send_temp(ctx, "❌ Supabase не инициализирован.")
        return

    db.supabase.table("fines").update({
        "is_canceled": True
    }).eq("id", fine_id).execute()

    db.add_action(
        user_id=fine["user_id"],
        points=0,
        reason=f"Отмена штрафа ID #{fine_id}",
        author_id=ctx.author.id
    )

    await send_temp(ctx, f"❌ Штраф #{fine_id} успешно отменён.")

@bot.hybrid_command(name="finehistory")
async def finehistory(ctx, member: Optional[discord.Member] = None, page: int = 1):
    member = member or ctx.author
    if not member:
        await send_temp(ctx, "❌ Пользователь не найден.")
        return

    if member.id != ctx.author.id and not ctx.author.guild_permissions.administrator:
        await send_temp(ctx, "❌ Вы не можете просматривать чужую историю штрафов.")
        return

    fines = [f for f in db.fines if f["user_id"] == member.id]
    if not fines:
        await send_temp(ctx, "📭 У пользователя нет штрафов.")
        return

    fines_per_page = 5
    total_pages = max(1, (len(fines) + fines_per_page - 1) // fines_per_page)

    if page < 1 or page > total_pages:
        await send_temp(ctx, f"❌ Недопустимая страница. Всего страниц: {total_pages}")
        return

    embed = discord.Embed(
        title=f"📚 История штрафов — {member.display_name}",
        color=discord.Color.teal()
    )
    start = (page - 1) * fines_per_page
    for fine in fines[start:start + fines_per_page]:
        status = "✅ Оплачен" if fine.get("is_paid") else "❌ Не оплачен"
        if fine.get("is_canceled"):
            status = "🚫 Отменён"
        if fine.get("is_overdue"):
            status += " + ⚠️ Просрочен"
        due = fine.get("due_date", "")[:10]
        embed.add_field(
            name=f"#{fine['id']} • {fine['amount']} баллов ({status})",
            value=f"📅 До: {due}\n📝 {fine['reason']}",
            inline=False
        )

    embed.set_footer(text=f"Страница {page}/{total_pages}")
    await send_temp(ctx, embed=embed)

@bot.hybrid_command(name="topfines")
async def topfines(ctx):
    top = get_fine_leaders()
    if not top:
        await send_temp(ctx, "📭 Нет должников.")
        return

    formatted = []
    for uid, amount in top:
        member = ctx.guild.get_member(uid)
        name = member.display_name if member else f"<@{uid}>"
        formatted.append((name, f"💰 Задолженность: {amount:.2f} баллов"))

    embed = build_top_embed(
        title="📉 Топ по задолженности",
        entries=formatted,
        color=discord.Color.red(),
    )
    await send_temp(ctx, embed=embed)
