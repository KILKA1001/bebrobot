import discord
from discord.ui import View, Button
from datetime import datetime, timezone, timedelta
from typing import List
from bot.data import db
import asyncio
import os
import pytz

latest_report_message_id = None  # глобальная переменная

# 💡 Получить статус штрафа
def get_fine_status(fine: dict) -> str:
    if fine.get("is_canceled"):
        return "🚫 Отменён"
    if fine.get("is_paid"):
        return "✅ Оплачен"
    if fine.get("is_overdue"):
        return "⚠️ Просрочен"
    return "⏳ Активен"


# 💡 Сформатировать дату
def format_fine_due_date(fine: dict) -> str:
    raw = fine.get("due_date")
    if not isinstance(raw, str):
        return "N/A"
    try:
        dt = datetime.fromisoformat(raw)
        return dt.astimezone(timezone.utc).strftime("%d.%m.%Y")
    except Exception:
        return raw

# 📋 Embed краткой информации о штрафе
def build_fine_embed(fine: dict) -> discord.Embed:
    embed = discord.Embed(title=f"📌 Штраф ID #{fine['id']}", color=discord.Color.orange())
    embed.add_field(name="💰 Сумма", value=f"{fine['amount']} баллов", inline=True)
    embed.add_field(name="📤 Осталось оплатить", value=f"{fine['amount'] - fine.get('paid_amount', 0):.2f} баллов", inline=True)
    embed.add_field(name="📅 Срок", value=format_fine_due_date(fine), inline=True)
    embed.add_field(name="🏷️ Тип", value=f"{'Обычный' if fine['type'] == 1 else 'Усиленный'}", inline=True)
    embed.add_field(name="📍 Статус", value=get_fine_status(fine), inline=True)
    embed.add_field(name="📝 Причина", value=fine['reason'], inline=False)
    return embed


# 📋 Embed подробной информации
def build_fine_detail_embed(fine: dict) -> discord.Embed:
    embed = build_fine_embed(fine)
    embed.title = f"ℹ️ Подробности штрафа #{fine['id']}"
    embed.set_footer(text=f"Назначен: {fine['created_at'][:10]} | Автор: <@{fine['author_id']}>")
    return embed


# 🎛️ View с кнопками под каждым штрафом
class FineView(View):
    def __init__(self, fine: dict):
        super().__init__(timeout=120)
        self.fine = fine


    @discord.ui.button(label="💸 Оплатить", style=discord.ButtonStyle.green, custom_id="pay")
    async def pay(self, interaction: discord.Interaction, button: Button):
        await interaction.response.send_message(
            f"💰 Выберите сумму оплаты штрафа #{self.fine['id']}",
            view=PaymentMenuView(self.fine),
            ephemeral=True
        )

    @discord.ui.button(label="📅 Отсрочка", style=discord.ButtonStyle.blurple, custom_id="postpone")
    async def postpone(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer(ephemeral=True)
        guild = interaction.guild
        member = guild.get_member(interaction.user.id) if guild else None
        is_admin = member.guild_permissions.administrator if member else False


        can_user = db.can_postpone(interaction.user.id)
        if not is_admin and not can_user:
            await interaction.followup.send("❌ Вы уже использовали отсрочку за последние 2 месяца.", ephemeral=True)
            return

        success = db.apply_postponement(self.fine['id'], days=7)
        if success:
            self.fine['due_date'] = (datetime.fromisoformat(self.fine['due_date']) + timedelta(days=7)).isoformat()
            self.fine['postponed_until'] = datetime.now(timezone.utc).isoformat()
            await interaction.followup.send(f"📅 Срок оплаты штрафа #{self.fine['id']} продлён на 7 дней.", ephemeral=True)
        else:
            await interaction.followup.send("❌ Не удалось продлить срок штрафа.", ephemeral=True)

    @discord.ui.button(label="ℹ️ Подробнее", style=discord.ButtonStyle.gray, custom_id="details")
    async def details(self, interaction: discord.Interaction, button: Button):
        embed = build_fine_detail_embed(self.fine)
        await interaction.response.send_message(embed=embed, ephemeral=True)


# 🔄 Пагинация списка штрафов
class FinePaginator:
    def __init__(self, fines: List[dict], per_page: int = 5):
        self.fines = fines
        self.per_page = per_page
        self.total_pages = max(1, (len(fines) + per_page - 1) // per_page)

    def get_page(self, page: int) -> List[dict]:
        start = (page - 1) * self.per_page
        end = start + self.per_page
        return self.fines[start:end]


# 💸 Меню выбора суммы оплаты
class PaymentMenuView(View):
    def __init__(self, fine: dict):
        super().__init__(timeout=90)
        self.fine = fine

    @discord.ui.button(label="💯 100%", style=discord.ButtonStyle.green, custom_id="pay_100")
    async def pay_100(self, interaction: discord.Interaction, button: Button):
        await process_payment(interaction, self.fine, 1.0)

    @discord.ui.button(label="🌓 50%", style=discord.ButtonStyle.blurple, custom_id="pay_50")
    async def pay_50(self, interaction: discord.Interaction, button: Button):
        await process_payment(interaction, self.fine, 0.5)

    @discord.ui.button(label="🌗 25%", style=discord.ButtonStyle.gray, custom_id="pay_25")
    async def pay_25(self, interaction: discord.Interaction, button: Button):
        await process_payment(interaction, self.fine, 0.25)

    @discord.ui.button(label="✏️ Своя сумма", style=discord.ButtonStyle.secondary, custom_id="pay_custom")
    async def pay_custom(self, interaction: discord.Interaction, button: Button):
        await interaction.response.send_message("✏️ Введите сумму для оплаты в чат (в баллах, например `12.5`). Ожидание 30 сек...", ephemeral=True)

        def check(m):
            return m.author.id == interaction.user.id and m.channel == interaction.channel

        try:
            message = await interaction.client.wait_for("message", timeout=30.0, check=check)
            amount_str = message.content.strip().replace(",", ".")
            amount = float(amount_str)

            max_amount = self.fine["amount"] - self.fine.get("paid_amount", 0)
            if amount <= 0 or amount > max_amount:
                await interaction.followup.send(f"❌ Сумма должна быть от 0 до {max_amount:.2f} баллов.", ephemeral=True)
                return

            user_points = db.scores.get(interaction.user.id, 0)
            if user_points < amount:
                await interaction.followup.send(f"❌ У вас недостаточно баллов. У вас: {user_points:.2f} баллов.", ephemeral=True)
                return

            success = db.record_payment(
                user_id=interaction.user.id,
                fine_id=self.fine["id"],
                amount=amount,
                author_id=interaction.user.id
            )

            if success:
                self.fine["paid_amount"] = round(self.fine.get("paid_amount", 0) + amount, 2)
                if self.fine["paid_amount"] >= self.fine["amount"]:
                    self.fine["is_paid"] = True
                await interaction.followup.send(f"✅ Вы оплатили {amount:.2f} баллов штрафа #{self.fine['id']}", ephemeral=True)
            else:
                await interaction.followup.send("❌ Ошибка при записи оплаты.", ephemeral=True)

        except asyncio.TimeoutError:
            await interaction.followup.send("⌛ Время ожидания истекло.", ephemeral=True)
        except ValueError:
            await interaction.followup.send("❌ Введите корректное число.", ephemeral=True)



async def process_payment(interaction: discord.Interaction, fine: dict, percent: float):
    await interaction.response.defer(ephemeral=True)
    user_id = interaction.user.id
    user_points = db.scores.get(user_id, 0)
    amount_remaining = fine['amount'] - fine.get('paid_amount', 0)
    to_pay = round(amount_remaining * percent, 2)

    if user_points < to_pay:
        await interaction.followup.send(f"❌ У вас недостаточно баллов для оплаты {to_pay} баллов.", ephemeral=True)
        return

    if not db.supabase:
        await interaction.followup.send("❌ Supabase не инициализирован.", ephemeral=True)
        return

    success = db.record_payment(user_id=user_id, fine_id=fine['id'], amount=to_pay, author_id=interaction.user.id)
    if not success:
        await interaction.followup.send("❌ Ошибка при записи оплаты.", ephemeral=True)
        return

    fine['paid_amount'] = round(fine.get('paid_amount', 0) + to_pay, 2)
    if fine['paid_amount'] >= fine['amount']:
        fine['is_paid'] = True

    await interaction.followup.send(f"✅ Вы оплатили {to_pay} баллов штрафа #{fine['id']}", ephemeral=True)


        # 📐 Расчет пени по просроченному штрафу
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
        print(f"Ошибка расчета пени: {e}")
        return 0.0

# 💣 Создание задолженности на основе штрафа
def create_debt_from_fine(fine: dict) -> dict:
    try:
        base_due = fine['amount'] - fine.get('paid_amount', 0)
        penalty = calculate_penalty(fine)
        total_debt = round(base_due + penalty, 2)

        return {
            "user_id": fine['user_id'],
            "fine_id": fine['id'],
            "amount_due": base_due,
            "penalty": penalty,
            "total_due": total_debt,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "last_attempt": None,
            "is_resolved": False
        }
    except Exception as e:
        print(f"Ошибка при создании задолженности: {e}")
        return {}


    # ⏰ Проверка просроченных штрафов
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

    # 🔁 Ежедневное удержание баллов с должников
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
            user_id = debt["user_id"]
            available = db.scores.get(user_id, 0)

            if available > 0:
                to_deduct = min(available, debt["total_due"])
                db.update_scores(user_id, -to_deduct)
                db.add_action(user_id, -to_deduct, f"Погашение долга по штрафу ID #{debt['fine_id']}", fine["author_id"])
                db.add_to_bank(to_deduct)

                fine['paid_amount'] = round(fine.get('paid_amount', 0) + to_deduct, 2)
                if fine['paid_amount'] >= fine['amount']:
                    fine['is_paid'] = True
                    print(f"✅ Штраф #{fine['id']} полностью закрыт через задолженность")

                if not db.supabase:
                    print("❌ Supabase не инициализирован (при обновлении штрафа)")
                    continue

                assert db.supabase is not None

                db.supabase.table("fines").update({
                    "paid_amount": fine['paid_amount'],
                    "is_paid": fine['is_paid']
                }).eq("id", fine['id']).execute()


        await asyncio.sleep(86400)

class AllFinesView(discord.ui.View):
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
            title=f"📊 Активные штрафы — страница {self.page}/{self.total_pages}",
            description=f"Общая сумма задолженности: **{total:.2f}** баллов",
            color=discord.Color.orange()
        )
        for fine in page_fines:
            user = self.ctx.guild.get_member(fine["user_id"])
            name = user.mention if user else f"<@{fine['user_id']}>"
            rest = fine["amount"] - fine.get("paid_amount", 0)
            due = fine.get("due_date", "N/A")[:10]
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

# 🔔 Ежедневные напоминания за 3 дня до срока
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
                user = discord.utils.get(bot.get_all_members(), id=fine["user_id"])
                if user:
                    try:
                        await user.send(
                            f"⏰ Напоминание: штраф #{fine['id']} должен быть оплачен до {due_date.strftime('%d.%m.%Y')}. Осталось {delta} дней."
                        )
                        channel_id = int(os.getenv("FINE_ALERT_CHANNEL_ID", 0))
                        if channel_id:
                            channel = bot.get_channel(channel_id)
                            if channel and isinstance(channel, discord.TextChannel):
                                await channel.send(
                                    f"📢 Напоминание: {user.mention}, вам нужно оплатить штраф #{fine['id']} до {due_date.strftime('%d.%m.%Y')} (через {delta} дн.)"
                                )
                    except discord.Forbidden:
                        continue
        except Exception:
            continue

# 🔄 Цикл напоминаний
async def reminder_loop(bot):
    await bot.wait_until_ready()
    while True:
        await remind_fines(bot)
        await asyncio.sleep(86400)

def get_fine_leaders():
    from collections import defaultdict
    user_totals = defaultdict(float)
    for fine in db.fines:
        if not fine.get("is_paid") and not fine.get("is_canceled"):
            rest = fine["amount"] - fine.get("paid_amount", 0)
            user_totals[fine["user_id"]] += rest
    top = sorted(user_totals.items(), key=lambda x: x[1], reverse=True)[:3]
    return top

async def fines_summary_report(bot):
    global latest_report_message_id

    await bot.wait_until_ready()
    channel_id = int(os.getenv("FINE_REPORT_CHANNEL_ID", 0))
    if not channel_id:
        print("❌ FINE_REPORT_CHANNEL_ID не задан")
        return

    channel = bot.get_channel(channel_id)
    if not channel or not isinstance(channel, discord.TextChannel):
        print("❌ Указанный канал не найден или не текстовый")
        return

    # Удаляем предыдущее сообщение, если оно было
    if latest_report_message_id:
        try:
            msg = await channel.fetch_message(latest_report_message_id)
            await msg.delete()
        except Exception:
            pass

    # Статистика
    active = [f for f in db.fines if not f.get("is_paid") and not f.get("is_canceled")]
    overdue = [f for f in active if f.get("is_overdue")]
    total_sum = sum(f["amount"] - f.get("paid_amount", 0) for f in active)
    bank = db.get_bank_balance()

    now = datetime.now(pytz.timezone("Europe/Moscow")).strftime("%d.%m.%Y")

    embed = discord.Embed(
        title=f"📢 Актуальная сводка по штрафам на {now}",
        color=discord.Color.orange()
    )
    embed.add_field(name="📋 Активных штрафов", value=str(len(active)), inline=True)
    embed.add_field(name="⚠️ Просроченных", value=str(len(overdue)), inline=True)
    embed.add_field(name="💰 Общая сумма долга", value=f"{total_sum:.2f} баллов", inline=False)
    embed.add_field(name="🏦 Баланс Банка Бебр", value=f"{bank:.2f} баллов", inline=False)
    embed.set_footer(text="Следующее обновление — через 2 дня")

    msg = await channel.send(embed=embed)
    latest_report_message_id = msg.id

async def fines_summary_loop(bot):
    while True:
        await fines_summary_report(bot)
        await asyncio.sleep(172800)  # 2 дня в секундах
