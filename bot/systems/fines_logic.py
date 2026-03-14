import discord
from discord.ui import Button
from bot.utils import (
    SafeView,
    safe_send,
    format_moscow_date,
    safe_defer,
    safe_response_send,
    safe_followup_send,
)
from datetime import datetime, timezone, timedelta
from typing import List
from bot.data import db
from collections import defaultdict
import asyncio
import os
import logging

latest_report_message_id = None
logger = logging.getLogger(__name__)

# Статус штрафа
def get_fine_status(fine: dict) -> str:
    if fine.get("is_canceled"):
        return "🚫 Отменён"
    if fine.get("is_paid"):
        return "✅ Оплачен"
    if fine.get("is_overdue"):
        return "⚠️ Просрочен"
    return "⏳ Активен"


def format_fine_due_date(fine: dict) -> str:
    raw = fine.get("due_date")
    if not isinstance(raw, str):
        return "N/A"
    try:
        dt = datetime.fromisoformat(raw)
        return format_moscow_date(dt)
    except Exception:
        return raw


def build_fine_embed(fine: dict) -> discord.Embed:
    embed = discord.Embed(title=f"📌 Штраф ID #{fine['id']}", color=discord.Color.orange())
    embed.add_field(name="💰 Сумма", value=f"{fine['amount']} баллов", inline=True)
    embed.add_field(name="📄 Осталось оплатить", value=f"{fine['amount'] - fine.get('paid_amount', 0):.2f} баллов", inline=True)
    embed.add_field(name="📅 Срок", value=format_fine_due_date(fine), inline=True)
    embed.add_field(name="🌿 Тип", value=f"{'Обычный' if fine['type'] == 1 else 'Усиленный'}", inline=True)
    embed.add_field(name="📍 Статус", value=get_fine_status(fine), inline=True)
    embed.add_field(name="📝 Причина", value=fine['reason'], inline=False)
    return embed


def build_fine_detail_embed(fine: dict) -> discord.Embed:
    embed = build_fine_embed(fine)
    embed.title = f"ℹ️ Подробности штрафа #{fine['id']}"
    author_account_id = fine.get('author_account_id')
    author_id = db._get_discord_user_for_account_id(author_account_id) if author_account_id else None
    author_display = f'<@{author_id}>' if author_id else (author_account_id or 'unknown')
    embed.set_footer(text=f"Назначен: {fine['created_at'][:10]} | Автор: {author_display}")
    return embed


class FineView(SafeView):
    def __init__(self, fine: dict):
        super().__init__(timeout=120)
        self.fine = fine

    @discord.ui.button(label="💸 Оплатить", style=discord.ButtonStyle.green)
    async def pay(self, interaction: discord.Interaction, button: Button):
        await safe_response_send(interaction, 
            f"💰 Выберите сумму для оплаты штрафа #{self.fine['id']}",
            view=PaymentMenuView(self.fine),
            ephemeral=True
        )

    @discord.ui.button(label="📅 Отсрочка", style=discord.ButtonStyle.blurple)
    async def postpone(self, interaction: discord.Interaction, button: Button):
        await safe_defer(interaction, ephemeral=True)
        member = interaction.guild.get_member(interaction.user.id) if interaction.guild else None
        is_admin = member.guild_permissions.administrator if member else False

        if not is_admin and not db.can_postpone(interaction.user.id):
            await safe_followup_send(interaction, "❌ Отсрочка уже использована за последние 2 месяца", ephemeral=True)
            return

        success = db.apply_postponement(self.fine['id'], days=7)
        if success:
            self.fine['due_date'] = (datetime.fromisoformat(self.fine['due_date']) + timedelta(days=7)).isoformat()
            self.fine['postponed_until'] = datetime.now(timezone.utc).isoformat()
            await safe_followup_send(interaction, f"📅 Срок штрафа #{self.fine['id']} продлён на 7 дней", ephemeral=True)
        else:
            await safe_followup_send(interaction, "❌ Ошибка при отсрочке", ephemeral=True)

    @discord.ui.button(label="ℹ️ Подробно", style=discord.ButtonStyle.gray)
    async def details(self, interaction: discord.Interaction, button: Button):
        embed = build_fine_detail_embed(self.fine)
        await safe_response_send(interaction, embed=embed, ephemeral=True)

async def process_payment(interaction: discord.Interaction, fine: dict, percent: float):
    user_id = interaction.user.id
    account_id = db._get_account_id_for_discord_user(user_id)
    if not account_id:
        logger.error("process_payment: unresolved account_id for user_id=%s", user_id)
        await safe_followup_send(interaction, "❌ Не удалось определить ваш аккаунт.", ephemeral=True)
        return
    user_points = db.scores.get(user_id, 0)
    amount_remaining = fine['amount'] - fine.get('paid_amount', 0)
    to_pay = round(amount_remaining * percent, 2)

    if user_points < to_pay:
        await safe_followup_send(interaction, f"❌ У вас недостаточно баллов для оплаты {to_pay} баллов.", ephemeral=True)
        return

    if not db.supabase:
        await safe_followup_send(interaction, "❌ Supabase не инициализирован.", ephemeral=True)
        return

    success = db.record_payment_by_account(account_id=account_id, fine_id=fine['id'], amount=to_pay, author_account_id=account_id)
    if not success:
        await safe_followup_send(interaction, "❌ Ошибка при записи оплаты.", ephemeral=True)
        return

    fine['paid_amount'] = round(fine.get('paid_amount', 0) + to_pay, 2)
    if fine['paid_amount'] >= fine['amount']:
        fine['is_paid'] = True

    if db.supabase:
        db.supabase.table("fines").update({
            "paid_amount": fine['paid_amount'],
            "is_paid": fine.get('is_paid', False)
        }).eq("id", fine['id']).execute()

    await safe_followup_send(interaction, f"✅ Вы оплатили {to_pay} баллов штрафа #{fine['id']}", ephemeral=True)



class PaymentMenuView(SafeView):
    def __init__(self, fine: dict):
        super().__init__(timeout=90)
        self.fine = fine

    @discord.ui.button(label="📏 100%", style=discord.ButtonStyle.green)
    async def pay_100(self, interaction: discord.Interaction, button: Button):
        await safe_defer(interaction, ephemeral=True)
        await process_payment(interaction, self.fine, 1.0)

    @discord.ui.button(label="🌑 50%", style=discord.ButtonStyle.blurple)
    async def pay_50(self, interaction: discord.Interaction, button: Button):
        await safe_defer(interaction, ephemeral=True)
        await process_payment(interaction, self.fine, 0.5)

    @discord.ui.button(label="🌒 25%", style=discord.ButtonStyle.gray)
    async def pay_25(self, interaction: discord.Interaction, button: Button):
        await safe_defer(interaction, ephemeral=True)
        await process_payment(interaction, self.fine, 0.25)

    @discord.ui.button(label="✏️ Своя сумма", style=discord.ButtonStyle.secondary)
    async def pay_custom(self, interaction: discord.Interaction, button: Button):
        await safe_response_send(interaction, "✏️ Введите сумму в чат (30 сек)", ephemeral=True)

        def check(m):
            return m.author.id == interaction.user.id and m.channel == interaction.channel

        try:
            msg = await interaction.client.wait_for("message", timeout=30.0, check=check)
            amount = float(msg.content.strip().replace(",", "."))
            remaining = self.fine["amount"] - self.fine.get("paid_amount", 0)

            if amount <= 0 or amount > remaining:
                await safe_followup_send(interaction, f"❌ От 0 до {remaining:.2f} баллов", ephemeral=True)
                return

            if db.scores.get(interaction.user.id, 0) < amount:
                await safe_followup_send(interaction, "❌ Недостаточно баллов", ephemeral=True)
                return

            caller_account_id = db._get_account_id_for_discord_user(interaction.user.id)
            if not caller_account_id:
                logger.error("custom payment: unresolved account_id user_id=%s", interaction.user.id)
                await safe_followup_send(interaction, "❌ Не удалось определить ваш аккаунт.", ephemeral=True)
                return
            success = db.record_payment_by_account(caller_account_id, self.fine["id"], amount, caller_account_id)
            if success:
                updated = db.get_fine_by_id(self.fine["id"])
                if updated:
                    self.fine.update(updated)
                await safe_followup_send(interaction, f"✅ Оплачено {amount:.2f} баллов", ephemeral=True)
            else:
                await safe_followup_send(interaction, "❌ Ошибка при оплате", ephemeral=True)

        except asyncio.TimeoutError:
            await safe_followup_send(interaction, "⌛ Время истекло", ephemeral=True)
        except ValueError:
            await safe_followup_send(interaction, "❌ Неверное число", ephemeral=True)


def get_fine_leaders():
    user_totals = defaultdict(float)
    for fine in db.fines:
        if not fine.get("is_paid") and not fine.get("is_canceled"):
            rest = fine["amount"] - fine.get("paid_amount", 0)
            account_id = fine.get("account_id")
            if not account_id:
                logger.warning("fine leader skip: fine_id=%s without account_id", fine.get("id"))
                continue
            user_totals[account_id] += rest
    return sorted(user_totals.items(), key=lambda x: x[1], reverse=True)[:3]


class FinePaginator:
    def __init__(self, fines: List[dict], per_page: int = 5):
        self.fines = fines
        self.per_page = per_page
        self.total_pages = max(1, (len(fines) + per_page - 1) // per_page)

    def get_page(self, page: int) -> List[dict]:
        start = (page - 1) * self.per_page
        end = start + self.per_page
        return self.fines[start:end]


class AllFinesView(SafeView):
    def __init__(self, fines, ctx, per_page=5):
        super().__init__(timeout=60)
        self.ctx = ctx
        self.fines = fines
        self.page = 1
        self.per_page = per_page
        self.total_pages = max(1, (len(fines) + per_page - 1) // per_page)

    def get_page_embed(self):
        page_fines = self.fines[(self.page - 1)*self.per_page : self.page*self.per_page]
        total = sum(f["amount"] - f.get("paid_amount", 0) for f in self.fines)
        embed = discord.Embed(
            title=f"📊 Штрафы — страница {self.page}/{self.total_pages}",
            description=f"Общий долг: **{total:.2f}** баллов",
            color=discord.Color.orange()
        )
        for fine in page_fines:
            account_id = fine.get("account_id")
            uid = db._get_discord_user_for_account_id(account_id) if account_id else None
            user = self.ctx.guild.get_member(uid) if uid else None
            name = user.display_name if user else (f"<@{uid}>" if uid else f"account:{account_id}")
            rest = fine["amount"] - fine.get("paid_amount", 0)
            due_raw = fine.get("due_date")
            if isinstance(due_raw, str):
                try:
                    due = format_moscow_date(datetime.fromisoformat(due_raw))
                except Exception:
                    due = due_raw[:10]
            else:
                due = "N/A"
            status = "⚠️ Просрочен" if fine.get("is_overdue") else "⏳ Активен"
            embed.add_field(
                name=f"#{fine['id']} • {name}",
                value=f"💰 {fine['amount']} → Осталось: **{rest:.2f}**\n📅 До: {due} • {status}",
                inline=False
            )
        return embed

    @discord.ui.button(label="◀️ Назад", style=discord.ButtonStyle.gray)
    async def prev_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.page > 1:
            self.page -= 1
        await interaction.response.edit_message(embed=self.get_page_embed(), view=self)

    @discord.ui.button(label="Вперёд ▶️", style=discord.ButtonStyle.gray)
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.page < self.total_pages:
            self.page += 1
        await interaction.response.edit_message(embed=self.get_page_embed(), view=self)

def calculate_penalty(fine: dict) -> float:
    try:
        if not fine.get("is_overdue") or fine.get("is_paid"):
            return 0.0

        due_raw = fine.get("due_date")
        if not isinstance(due_raw, str):
            return 0.0
        due_date = datetime.fromisoformat(due_raw)

        now = datetime.now(timezone.utc)
        overdue_days = (now - due_date).days
        if overdue_days <= 0:
            return 0.0

        rate = 0.01 if fine["type"] == 1 else 0.05
        max_daily = 1.5
        base = fine["amount"] - fine.get("paid_amount", 0)

        total_penalty = 0.0
        for day in range(overdue_days):
            daily = min(base * rate, max_daily)
            total_penalty += daily

        return round(total_penalty, 2)

    except Exception as e:
        logger.exception("Ошибка расчёта пени")
        return 0.0

# 💳 Задолженность из штрафа

def create_debt_from_fine(fine: dict) -> dict:
    try:
        base_due = fine['amount'] - fine.get('paid_amount', 0)
        penalty = calculate_penalty(fine)
        total_debt = round(base_due + penalty, 2)

        return {
            "account_id": fine.get("account_id"),
            "fine_id": fine['id'],
            "amount_due": base_due,
            "penalty": penalty,
            "total_due": total_debt,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "last_attempt": None,
            "is_resolved": False
        }
    except Exception as e:
        logger.exception("Ошибка при создании задолженности")
        return {}

# ⏰ Проверка просроченных
async def check_overdue_fines(bot):
    await bot.wait_until_ready()
    now = datetime.now(timezone.utc)
    for fine in db.fines:
        if fine.get("is_paid") or fine.get("is_canceled") or fine.get("is_overdue"):
            continue
        due_raw = fine.get("due_date")
        if not isinstance(due_raw, str):
            continue
        try:
            due_date = datetime.fromisoformat(due_raw)
            if now > due_date:
                db.mark_overdue(fine)
        except Exception:
            continue

# 📆 Ежедневное удержание по задолженностям
async def debt_repayment_loop(bot):
    await bot.wait_until_ready()
    while True:
        now = datetime.now(timezone.utc)
        for fine in db.fines:
            if not fine.get("is_overdue") or fine.get("is_paid") or fine.get("is_canceled"):
                continue

            due_raw = fine.get("due_date")
            if not isinstance(due_raw, str):
                continue
            due_date = datetime.fromisoformat(due_raw)

            if (now - due_date).days < 10:
                continue

            debt = create_debt_from_fine(fine)
            account_id = debt.get("account_id")
            if not account_id:
                logger.warning("debt repayment skip: fine_id=%s without account_id", fine.get("id"))
                continue
            user_id = db._get_discord_user_for_account_id(account_id)
            if user_id is None:
                logger.warning("debt repayment skip: fine_id=%s unresolved discord user for account_id=%s", fine.get("id"), account_id)
                continue
            available = db.scores.get(user_id, 0)

            if available > 0:
                to_deduct = min(available, debt["total_due"])
                db.update_scores(user_id, -to_deduct)
                author_user_id = db._get_discord_user_for_account_id(fine.get("author_account_id")) or 0
                db.add_action(user_id, -to_deduct, f"Погашение долга по штрафу ID #{debt['fine_id']}", author_user_id)
                reason = str(fine.get("reason", ""))
                if "test" not in reason.lower():
                    db.add_to_bank(to_deduct)

                fine['paid_amount'] = round(fine.get('paid_amount', 0) + to_deduct, 2)
                if fine['paid_amount'] >= fine['amount']:
                    fine['is_paid'] = True

                if db.supabase:
                    db.supabase.table("fines").update({
                        "paid_amount": fine['paid_amount'],
                        "is_paid": fine['is_paid']
                    }).eq("id", fine['id']).execute()
        await asyncio.sleep(86400)

# 🔔 Напоминания перед сроком
async def remind_fines(bot):
    await bot.wait_until_ready()
    now = datetime.now(timezone.utc)
    for fine in db.fines:
        if fine.get("is_paid") or fine.get("is_canceled"):
            continue
        due_raw = fine.get("due_date")
        if not isinstance(due_raw, str):
            continue
        try:
            due_date = datetime.fromisoformat(due_raw)
            delta = (due_date - now).days
            if 0 < delta <= 3:
                account_id = fine.get("account_id")
                if not account_id:
                    logger.warning("remind_fines skip: fine_id=%s without account_id", fine.get("id"))
                    continue
                target_user_id = db._get_discord_user_for_account_id(account_id)
                if target_user_id is None:
                    logger.warning("remind_fines skip: unresolved discord user for account_id=%s fine_id=%s", account_id, fine.get("id"))
                    continue
                user = discord.utils.get(bot.get_all_members(), id=target_user_id)
                if user:
                    try:
                        await safe_send(
                            user,
                            f"⏰ Напоминание: штраф #{fine['id']} нужно оплатить до {format_moscow_date(due_date)} (через {delta} дн.)",
                        )
                    except discord.Forbidden:
                        continue
        except Exception:
            continue

async def reminder_loop(bot):
    await bot.wait_until_ready()
    while True:
        await remind_fines(bot)
        await asyncio.sleep(86400)

# 📊 Сводка по штрафам в канал
async def fines_summary_report(bot):
    global latest_report_message_id

    await bot.wait_until_ready()
    channel_id = int(os.getenv("FINE_REPORT_CHANNEL_ID", 0))
    if not channel_id:
        logger.error("❌ FINE_REPORT_CHANNEL_ID не задан")
        return

    channel = bot.get_channel(channel_id)
    if not channel or not isinstance(channel, discord.TextChannel):
        logger.error("❌ Указанный канал не найден или не текстовый")
        return

    if latest_report_message_id:
        try:
            msg = await channel.fetch_message(latest_report_message_id)
            await msg.delete()
        except Exception:
            pass

    active = [f for f in db.fines if not f.get("is_paid") and not f.get("is_canceled")]
    overdue = [f for f in active if f.get("is_overdue")]
    total_sum = sum(f["amount"] - f.get("paid_amount", 0) for f in active)
    bank = db.get_bank_balance()

    now = format_moscow_date()

    embed = discord.Embed(title=f"📢 Актуальная сводка по штрафам на {now}", color=discord.Color.orange())
    embed.add_field(name="📋 Активных штрафов", value=str(len(active)), inline=True)
    embed.add_field(name="⚠️ Просроченных", value=str(len(overdue)), inline=True)
    embed.add_field(name="💰 Общая сумма долга", value=f"{total_sum:.2f} баллов", inline=False)
    embed.add_field(name="🏦 Баланс Банка Бебр", value=f"{bank:.2f} баллов", inline=False)
    embed.set_footer(text="Следующее обновление — через 2 дня")

    msg = await safe_send(channel, embed=embed)
    latest_report_message_id = msg.id if msg else None

async def fines_summary_loop(bot):
    while True:
        await fines_summary_report(bot)
        await asyncio.sleep(172800)
