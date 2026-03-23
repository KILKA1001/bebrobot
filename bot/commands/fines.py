import discord
from discord.ext import commands
from datetime import datetime, timedelta, timezone
from typing import Optional

from bot.commands.base import bot
from bot.utils import send_temp, safe_send, format_moscow_date
import os
import logging

from bot.data import db
from bot.services import FinesService, AuthorityService
from bot.systems.fines_logic import (
    build_fine_embed,
    build_fine_detail_embed,
    FineView,
    AllFinesView,
)

logger = logging.getLogger(__name__)

FINE_ROLE_IDS = tuple(
    int(r) for r in os.getenv("FINE_ROLE_IDS", "").split(",") if r
)


def has_permission(ctx):
    if ctx.author.guild_permissions.administrator:
        return True
    if any(role.id in FINE_ROLE_IDS for role in ctx.author.roles):
        return True
    return AuthorityService.has_command_permission("discord", str(ctx.author.id), "fine_create")


def has_manage_permission(ctx):
    if ctx.author.guild_permissions.administrator:
        return True
    return AuthorityService.has_command_permission("discord", str(ctx.author.id), "fine_manage")


@bot.hybrid_command(name="fine", description="Назначить штраф пользователю")
async def fine(
    ctx,
    member: discord.Member,
    amount: str,
    fine_type: int,
    *,
    reason: str = "Без причины",
):
    if not has_permission(ctx):
        await send_temp(ctx, "❌ У вас нет прав для назначения штрафов.")
        return
    if member.id != ctx.author.id and not AuthorityService.can_manage_target("discord", str(ctx.author.id), "discord", str(member.id)):
        await send_temp(ctx, "❌ Нельзя назначать штраф пользователю с равным/более высоким званием.")
        return

    try:
        amount_value = float(amount.replace(",", "."))
        if amount_value <= 0:
            raise ValueError

        if fine_type not in (1, 2):
            await send_temp(
                ctx, "❌ Тип штрафа должен быть 1 (обычный) или 2 (усиленный)."
            )
            return

        due_date = datetime.now(timezone.utc) + timedelta(
            days=14 if fine_type == 1 else 30
        )

        fine = FinesService.create_fine(
            discord_user_id=member.id,
            author_id=ctx.author.id,
            amount=amount_value,
            fine_type=fine_type,
            reason=reason,
            due_date=due_date,
        )

        if fine:
            embed = discord.Embed(
                title="📌 Назначен штраф",
                description=(
                    f"{member.mention}, вам назначен штраф.\n\n"
                    "ℹ️ Чтобы просмотреть и оплатить его, "
                    "используйте команду `/myfines`"
                ),
                color=discord.Color.red(),
            )
            embed.add_field(
                name="Сумма", value=f"{amount_value:.2f} баллов", inline=True
            )
            embed.add_field(
                name="Тип",
                value=(
                    "Обычный (14 дней)"
                    if fine_type == 1
                    else "Усиленный (30 дней)"
                ),
                inline=True,
            )
            embed.add_field(name="Причина", value=reason, inline=False)
            embed.add_field(
                name="Срок оплаты",
                value=format_moscow_date(due_date),
                inline=True,
            )
            embed.set_footer(text=f"ID штрафа: {fine['id']}")

            await send_temp(ctx, embed=embed, delete_after=None)
            try:
                await safe_send(member, embed=embed)
            except discord.Forbidden:
                await send_temp(
                    ctx,
                    f"⚠️ Не удалось отправить сообщение в ЛС {member.mention}",
                )

        else:
            await send_temp(ctx, "❌ Не удалось создать штраф.")

    except ValueError:
        await send_temp(ctx, "❌ Введите корректную сумму.")


@bot.hybrid_command(
    name="myfines", description="Посмотреть и оплатить свои legacy-штрафы"
)
async def myfines(ctx):
    user_id = ctx.author.id
    fines = FinesService.get_user_fines(user_id)

    if not fines:
        await send_temp(ctx, "✅ У вас нет активных legacy-штрафов. Для новой модерации используйте `/rep`.")
        return

    for fine in fines:
        embed = build_fine_embed(fine)
        view = FineView(fine)
        await send_temp(ctx, embed=embed, view=view)


@bot.hybrid_command(
    name="allfines", description="Список всех неоплаченных штрафов"
)
async def all_fines(ctx):
    if not has_manage_permission(ctx):
        await send_temp(ctx, "❌ Недостаточно полномочий для просмотра всех штрафов.")
        return
    fines = [
        f
        for f in db.fines
        if not f.get("is_paid") and not f.get("is_canceled")
    ]

    if not fines:
        await send_temp(ctx, "✅ Нет активных штрафов.")
        return

    view = AllFinesView(fines, ctx)
    await send_temp(ctx, embed=view.get_page_embed(), view=view)


@bot.hybrid_command(name="finedetails", description="Подробности legacy-штрафа по ID")
async def finedetails(ctx, fine_id: int):
    fine = db.get_fine_by_id(fine_id)
    if not fine:
        await send_temp(ctx, "❌ Штраф не найден.")
        return

    is_admin = ctx.author.guild_permissions.administrator or has_manage_permission(ctx)
    target_user_id = db._get_discord_user_for_account_id(fine.get("account_id"))
    if target_user_id is None:
        logger.error("finedetails: unresolved target user for fine_id=%s account_id=%s", fine_id, fine.get("account_id"))
        await send_temp(ctx, "❌ Не удалось определить владельца штрафа.")
        return
    if target_user_id != ctx.author.id and not is_admin:
        await send_temp(ctx, "❌ Вы не можете просматривать чужие штрафы.")
        return

    embed = build_fine_detail_embed(fine)
    await send_temp(ctx, embed=embed)


@bot.hybrid_command(name="editfine", description="Изменить параметры штрафа")
async def editfine(
    ctx,
    fine_id: int,
    amount: float,
    fine_type: int,
    due_date_str: str,
    *,
    reason: str,
):
    if not has_manage_permission(ctx):
        await send_temp(ctx, "❌ Недостаточно полномочий для редактирования штрафов.")
        return

    fine = db.get_fine_by_id(fine_id)
    if not fine:
        await send_temp(ctx, "❌ Штраф не найден.")
        return

    try:
        # Европейский формат: ДД.ММ.ГГГГ
        due_date = datetime.strptime(due_date_str, "%d.%m.%Y").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        await send_temp(
            ctx, "❌ Неверный формат даты. Используйте ДД.ММ.ГГГГ."
        )
        return

    fine["amount"] = amount
    fine["type"] = fine_type
    fine["reason"] = reason
    fine["due_date"] = due_date.isoformat()

    if not db.supabase:
        await send_temp(ctx, "❌ Supabase не инициализирован.")
        return

    db.supabase.table("fines").update(
        {
            "amount": amount,
            "type": fine_type,
            "reason": reason,
            "due_date": due_date.isoformat(),
        }
    ).eq("id", fine_id).execute()

    await send_temp(ctx, f"✏️ Штраф #{fine_id} успешно обновлён.")


@bot.hybrid_command(name="cancel_fine", description="Отменить штраф по ID")
async def cancel_fine(ctx, fine_id: int):
    if not has_manage_permission(ctx):
        await send_temp(ctx, "❌ Недостаточно полномочий для отмены штрафов.")
        return

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

    db.supabase.table("fines").update({"is_canceled": True}).eq(
        "id", fine_id
    ).execute()

    target_user_id = db._get_discord_user_for_account_id(fine.get("account_id"))
    if target_user_id is None:
        logger.error("cancel_fine: unresolved target user for fine_id=%s account_id=%s", fine_id, fine.get("account_id"))
    else:
        db.add_action(
            user_id=target_user_id,
            points=0,
            reason=f"Отмена штрафа ID #{fine_id}",
            author_id=ctx.author.id,
        )

    await send_temp(ctx, f"❌ Штраф #{fine_id} успешно отменён.")


@bot.hybrid_command(
    name="finehistory", description="История legacy-штрафов пользователя"
)
async def finehistory(
    ctx, member: Optional[discord.Member] = None, page: int = 1
):
    member = member or ctx.author
    if not member:
        await send_temp(ctx, "❌ Пользователь не найден.")
        return

    if (
        member.id != ctx.author.id
        and not (ctx.author.guild_permissions.administrator or has_manage_permission(ctx))
    ):
        await send_temp(
            ctx, "❌ Вы не можете просматривать чужую историю штрафов."
        )
        return

    member_account_id = db._get_account_id_for_discord_user(member.id)
    if not member_account_id:
        logger.warning("finehistory: no account_id for member_id=%s", member.id)
        await send_temp(ctx, "📭 У пользователя нет штрафов.")
        return
    fines = [f for f in db.fines if f.get("account_id") == member_account_id]
    if not fines:
        await send_temp(ctx, "📭 У пользователя нет штрафов.")
        return

    fines_per_page = 5
    total_pages = max(1, (len(fines) + fines_per_page - 1) // fines_per_page)

    if page < 1 or page > total_pages:
        await send_temp(
            ctx, f"❌ Недопустимая страница. Всего страниц: {total_pages}"
        )
        return

    embed = discord.Embed(
        title=f"📚 История legacy-штрафов — {member.display_name}",
        description=(
            "Это переходный экран для старых денежных штрафов. "
            "Рейтинг должников больше не используется; для новой модерации открывайте `/rep`."
        ),
        color=discord.Color.teal(),
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
            inline=False,
        )

    embed.set_footer(text=f"Legacy-данные переходного периода • Страница {page}/{total_pages}")
    await send_temp(ctx, embed=embed)


@bot.hybrid_command(
    name="topfines", description="Legacy-режим: перенаправление со старого штрафного топа"
)
async def topfines(ctx):
    logger.warning(
        "legacy topfines command invoked actor_id=%s guild_id=%s reason=topfines_retired",
        getattr(getattr(ctx, "author", None), "id", None),
        getattr(getattr(ctx, "guild", None), "id", None),
    )
    embed = discord.Embed(
        title="🧭 /topfines больше не используется",
        description=(
            "Рейтинг должников и штрафной monthly-top выведены из использования. "
            "Теперь модерация ведётся через кейсы, а основной сценарий начинается с `/rep`, а не с `/topfines`."
        ),
        color=discord.Color.orange(),
    )
    embed.add_field(
        name="Что использовать вместо этого",
        value=(
            "• `/rep` — открыть кейс модерации, увидеть автонаказание, предупреждения, активное наказание и следующий шаг эскалации.\n"
            "• `/myfines` — посмотреть свои активные legacy-денежные штрафы.\n"
            "• `/finehistory [@пользователь] [страница]` — открыть историю legacy-штрафов на переходный период.\n"
            "• В кейсе /rep отдельно видно, был ли списан штраф в банк, какое наказание активно сейчас и что будет дальше по эскалации."
        ),
        inline=False,
    )
    embed.add_field(
        name="Что происходит прямо сейчас",
        value=(
            "• Старая механика больше не участвует в новой продуктовой логике.\n"
            "• Активные legacy-штрафы пока остаются отдельным переходным экраном.\n"
            "• История кейсов и нарушений сейчас смотрится через `/rep` и журнал модерации; позже она будет вынесена в отдельный экран."
        ),
        inline=False,
    )
    embed.set_footer(text="Используйте `/rep`. Legacy-вызов записан в лог, чтобы можно было добить оставшиеся ссылки на /topfines.")
    await send_temp(ctx, embed=embed)
