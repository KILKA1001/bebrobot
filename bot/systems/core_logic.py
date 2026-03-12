import discord
from typing import Optional
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import pytz
import traceback
import logging

from bot.data import db
from bot.utils.roles_and_activities import ROLE_THRESHOLDS
from bot.utils import (
    send_temp,
    build_top_embed,
    SafeView,
    safe_send,
    format_moscow_time,
    format_points,
)

active_timers = {}
  
async def update_roles(member: discord.Member):
    user_id = member.id
    user_points = db.scores.get(user_id, 0)
    threshold_role_ids = set(ROLE_THRESHOLDS)

    target_role_id = None
    for role_id, threshold in sorted(ROLE_THRESHOLDS.items(), key=lambda x: x[1], reverse=True):
        if user_points >= threshold:
            target_role_id = role_id
            break

    desired_roles = [role for role in member.roles if role.id not in threshold_role_ids]
    if target_role_id:
        target_role = member.guild.get_role(target_role_id)
        if target_role:
            desired_roles.append(target_role)

    current_role_ids = {role.id for role in member.roles}
    desired_role_ids = {role.id for role in desired_roles}
    if current_role_ids == desired_role_ids:
        return

    try:
        await member.edit(roles=desired_roles, reason="Обновление роли по баллам")
    except (discord.Forbidden, discord.HTTPException) as exc:
        logging.warning("Не удалось обновить роли пользователя %s: %s", user_id, exc)


class HistoryView(SafeView):
    def __init__(self, member: discord.Member, page: int, total_pages: int):
        super().__init__(timeout=60)
        self.member = member
        self.page = page
        self.total_pages = total_pages

        self.prev_button.disabled = page <= 1
        self.next_button.disabled = page >= total_pages

    @discord.ui.button(label="◀️ Назад", style=discord.ButtonStyle.gray, custom_id="prev")
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Переход на предыдущую страницу истории."""
        await interaction.response.defer()
        await render_history(interaction, self.member, self.page - 1)

    @discord.ui.button(label="Вперёд ▶️", style=discord.ButtonStyle.gray, custom_id="next")
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Переход на следующую страницу истории."""
        await interaction.response.defer()
        await render_history(interaction, self.member, self.page + 1)


async def render_history(ctx_or_interaction, member: discord.Member, page: int):
    try:
        user_id = member.id
        entries_per_page = 5
        user_history = db.history.get(user_id, [])

        if not user_history:
            embed = discord.Embed(
                title="📜 История баллов",
                description="```Записей не найдено```",
                color=discord.Color.orange()
            )
            embed.set_author(name=member.display_name, icon_url=member.avatar.url if member.avatar else member.default_avatar.url)

            if isinstance(ctx_or_interaction, discord.Interaction):
                await ctx_or_interaction.response.send_message(embed=embed, ephemeral=True)
            else:
                await ctx_or_interaction.send(embed=embed)
            return

        total_entries = len(user_history)
        total_pages = max(1, (total_entries + entries_per_page - 1) // entries_per_page)

        if page < 1 or page > total_pages:
            embed = discord.Embed(
                title="⚠️ Ошибка навигации",
                description=f"```Доступно страниц: {total_pages}```",
                color=discord.Color.red()
            )
            if isinstance(ctx_or_interaction, discord.Interaction):
                await ctx_or_interaction.response.send_message(embed=embed, ephemeral=True)
            else:
                await ctx_or_interaction.send(embed=embed)
            return

        start_idx = (page - 1) * entries_per_page
        page_actions = user_history[start_idx:start_idx + entries_per_page]

        embed = discord.Embed(title="📜 История баллов", color=discord.Color.blue())
        embed.set_author(name=member.display_name, icon_url=member.avatar.url if member.avatar else member.default_avatar.url)

        total_points = db.scores.get(user_id, 0)
        embed.add_field(
            name="💰 Текущий баланс",
            value=f"```{format_points(total_points)} баллов```",
            inline=False,
        )

        for action in page_actions:
            points = action.get("points", 0)
            emoji = "🟢" if points >= 0 else "🔴"
            if action.get("is_undo", False):
                emoji = "⚪"

            timestamp = action.get("timestamp")
            if isinstance(timestamp, str):
                try:
                    dt = datetime.fromisoformat(timestamp)
                    formatted_time = format_moscow_time(dt)
                except ValueError:
                    formatted_time = timestamp
            else:
                formatted_time = format_moscow_time(timestamp) if timestamp else "N/A"

            author_id = action.get('author_id', 'N/A')
            reason = action.get('reason', 'Не указана')

            field_name = f"{emoji} {formatted_time}"
            field_value = (
                f"```diff\n{'+' if points >= 0 else ''}{format_points(points)} баллов```\n"
                f"**Причина:** {reason}\n"
                f"**Выдал:** <@{author_id}>"
            )
            embed.add_field(name=field_name, value=field_value, inline=False)

        embed.set_footer(text=f"Страница {page}/{total_pages} • Всего записей: {total_entries}")

        view = HistoryView(member, page, total_pages)

        if isinstance(ctx_or_interaction, discord.Interaction):
            if ctx_or_interaction.response.is_done():
                await ctx_or_interaction.edit_original_response(embed=embed, view=view)
            else:
                await ctx_or_interaction.response.send_message(embed=embed, view=view)
                await ctx_or_interaction.original_response()
        else:
            await send_temp(ctx_or_interaction, embed=embed, view=view)

    except Exception as e:
        error_embed = discord.Embed(
            title="⚠️ Ошибка",
            description=f"```{str(e)}```",
            color=discord.Color.red()
        )
        if isinstance(ctx_or_interaction, discord.Interaction):
            await ctx_or_interaction.response.send_message(embed=error_embed, ephemeral=True)
        else:
            await ctx_or_interaction.send(embed=error_embed)
        print(f"Ошибка в render_history: {traceback.format_exc()}")


async def log_action_cancellation(ctx, member: discord.Member, entries: list):
    channel = discord.utils.get(ctx.guild.channels, name='history-log')
    if not channel:
        return

    now = format_moscow_time()
    lines = [
        f"**{ctx.author.display_name}** отменил(а) {len(entries)} изменения для **{member.display_name}** ({member.id}) в {now}:"
    ]
    for i, (points, reason) in enumerate(entries[::-1], start=1):
        sign = "+" if points > 0 else ""
        lines.append(f"{i}. {sign}{format_points(points)} — {reason}")

    await safe_send(channel, "\n".join(lines))


async def run_monthly_top(ctx, month: Optional[int] = None, year: Optional[int] = None):
    """Award monthly top bonuses.

    Parameters
    ----------
    ctx : commands.Context
        Command context.
    month : Optional[int], optional
        Month number to calculate results for. Defaults to current month.
    year : Optional[int], optional
        Year number to calculate results for. Defaults to current year.
    """
    now = datetime.now(pytz.timezone('Europe/Moscow'))
    current_month = month or now.month
    current_year = year or now.year
    from collections import defaultdict
    monthly_scores = defaultdict(float)
    for action in db.actions:
        if action.get('is_undo'):
            continue
        timestamp = action.get('timestamp')
        if isinstance(timestamp, str):
            try:
                dt = datetime.fromisoformat(timestamp)
            except ValueError:
                continue
            if dt.month == current_month and dt.year == current_year:
                uid = int(action['user_id'])
                monthly_scores[uid] += float(action['points'])
    if not monthly_scores:
        await send_temp(ctx, "❌ Нет данных о баллах за этот месяц.")
        return

    top_users = sorted(monthly_scores.items(), key=lambda x: x[1], reverse=True)[:3]
    percentages = [0.125, 0.075, 0.05]

    entries_to_log = []
    formatted = []

    for i, (uid, score) in enumerate(top_users):
        percent = percentages[i]
        bonus = round(score * percent, 2)
        db.add_action(uid, bonus, f"Бонус за {i + 1} место ({score} баллов)", ctx.author.id)
        member = ctx.guild.get_member(uid)
        name = member.display_name if member else f"<@{uid}>"

        formatted.append(
            (name, f"{i + 1} место\nЗаработано: {score:.2f} баллов\nБонус: +{bonus:.2f} баллов")
        )
        entries_to_log.append((uid, score, percent))

    db.log_monthly_top(entries_to_log, current_month, current_year)
    embed = build_top_embed("🏆 Топ месяца", formatted, color=discord.Color.gold())
    await send_temp(ctx, embed=embed)


async def tophistory(ctx, month: Optional[int] = None, year: Optional[int] = None):
    now = datetime.now()
    month = month or now.month
    year = year or now.year

    if not db.supabase:
        await send_temp(ctx, "❌ Supabase не инициализирован.")
        return

    try:
        response = db.supabase \
            .table("monthly_top_log") \
            .select("*") \
            .eq("month", month) \
            .eq("year", year) \
            .order("place") \
            .execute()

        entries = response.data
        if not entries:
            await send_temp(ctx, f"📭 Нет записей за {month:02d}.{year}")
            return

        formatted = []
        for entry in entries:
            uid = entry['user_id']
            place = entry['place']
            bonus = entry['bonus']
            member = ctx.guild.get_member(uid)
            name = member.display_name if member else f"<@{uid}>"
            formatted.append((name, f"{place} место • +{bonus} баллов"))

        embed = build_top_embed(
            title=f"📅 История топа — {month:02d}.{year}",
            entries=formatted,
            color=discord.Color.green(),
        )
        await send_temp(ctx, embed=embed)

    except Exception as e:
        await send_temp(ctx, f"❌ Ошибка при получении данных: {e}")

class HelpView(SafeView):
    def __init__(self, user: discord.Member):
        super().__init__(timeout=120)
        self.user = user

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user.id

    async def update_embed(self, interaction: discord.Interaction, category: str):
        embed = get_help_embed(category)
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="📊 Баллы", style=discord.ButtonStyle.blurple, row=0)
    async def points_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.update_embed(interaction, "points")

    @discord.ui.button(label="🏅 Роли", style=discord.ButtonStyle.green, row=0)
    async def roles_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.update_embed(interaction, "roles")

    @discord.ui.button(label="📉 Штрафы", style=discord.ButtonStyle.gray, row=1)
    async def fines_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.update_embed(interaction, "fines")

    @discord.ui.button(label="🧪 Прочее", style=discord.ButtonStyle.secondary, row=1)
    async def misc_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.update_embed(interaction, "misc")

    @discord.ui.button(label="🛡️ Админ-панель", style=discord.ButtonStyle.red, row=1)
    async def admin_category_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ Только для администраторов", ephemeral=True)
            return
        embed = discord.Embed(title="🛡️ Админ-панель", description="Выберите категорию:", color=discord.Color.red())
        await interaction.response.edit_message(embed=embed, view=AdminCategoryView(self.user))

def get_help_embed(category: str) -> discord.Embed:
    embed = discord.Embed(title="🛠️ Справка: категории команд", color=discord.Color.blue())

    if category == "points":
        embed.title = "📊 Баллы и рейтинг"
        embed.description = (
            "`/balance [@пользователь]` — показать текущий баланс\n"
            "`/leaderboard` — топ пользователей по баллам\n"
            "`/history [@пользователь] [страница]` — история изменений баллов"
        )
    elif category == "roles":
        embed.title = "🏅 Роли и активности"
        embed.description = (
            "`/roles` — список ролей и стоимость\n"
            "`/activities` — виды деятельности и их баллы"
        )
    elif category == "fines":
        embed.title = "📉 Штрафы"
        embed.description = (
            "`/myfines` — ваши активные штрафы\n"
            "`/finehistory [@пользователь] [страница]` — история штрафов\n"
            "`/finedetails ID` — детали конкретного штрафа"
        )
    elif category == "misc":
        embed.title = "🧪 Прочее"
        embed.description = (
            "`/ping` — проверить, работает ли бот\n"
            "`/helpy` — открыть меню справки\n"
            "`/tophistory [месяц] [год]` — история топов месяца\n"
            "`/mapinfo id` — информация о карте по ID (ID — последняя цифра в названии карты)\n"
            "`/jointournament id` — заявиться на турнир\n"
            "`/tournamenthistory [n]` — последние турниры"
        )
    elif category == "admin_points":
        embed.title = "⚙️ Админ: Баллы и билеты"
        embed.description = (
            "`/addpoints @пользователь сумма [причина]` — начислить баллы\n"
            "`/removepoints @пользователь сумма [причина]` — снять баллы\n"
            "`/undo @пользователь [кол-во]` — отменить последние действия\n"
            "`/awardmonthtop [месяц] [год]` — бонусы за топ месяца\n"
            "`/addticket @пользователь тип кол-во [причина]` — выдать билет\n"
            "`/removeticket @пользователь тип кол-во [причина]` — списать билет"
        )
    elif category == "admin_fines":
        embed.title = "📉 Админ: Управление штрафами"
        embed.description = (
            "`/fine @пользователь сумма тип [причина]` — выдать штраф (тип: 1 — обычный, 2 — усиленный)\n"
            "`/editfine ID сумма тип дата причина` — изменить параметры штрафа (дата в формате ДД.ММ.ГГГГ)\n"
            "`/cancel_fine ID` — отменить штраф\n"
            "`/topfines` — список топ-должников по сумме штрафов\n"
            "`/allfines` — список всех активных штрафов"
        )
    elif category == "admin_bank":
        embed.title = "🏦 Админ: Управление банком"
        embed.description = (
            "`/bank` — баланс банка\n"
            "`/bankadd сумма причина` — добавить баллы в банк\n"
            "`/bankspend сумма причина` — потратить баллы из банка\n"
            "`/bankhistory` — история операций"
        )
    elif category == "admin_tournaments":
        embed.title = "🏟 Админ: Турниры"
        embed.description = (
            "`/createtournament` — создать турнир\n"
            "`/managetournament id` — панель управления (кнопка 👥 покажет участников; `id` — номер турнира)"
        )
    return embed

class AdminCategoryView(SafeView):
    def __init__(self, user: discord.Member):
        super().__init__(timeout=120)
        self.user = user

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user.id

    async def send_category(self, interaction, category: str):
        embed = get_help_embed(category)
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="⚙️ Баллы", style=discord.ButtonStyle.blurple, row=0)
    async def points_admin(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.send_category(interaction, "admin_points")

    @discord.ui.button(label="📉 Штрафы", style=discord.ButtonStyle.gray, row=0)
    async def fines_admin(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.send_category(interaction, "admin_fines")

    @discord.ui.button(label="🏦 Банк", style=discord.ButtonStyle.green, row=0)
    async def bank_admin(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.send_category(interaction, "admin_bank")

    @discord.ui.button(label="🏟 Турниры", style=discord.ButtonStyle.green, row=0)
    async def tournaments_admin(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.send_category(interaction, "admin_tournaments")

    @discord.ui.button(label="🔙 Назад", style=discord.ButtonStyle.secondary, row=1)
    async def back_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = get_help_embed("points")
        await interaction.response.edit_message(embed=embed, view=HelpView(self.user))

class LeaderboardView(SafeView):
    def __init__(self, ctx, mode="all", page=1):
        super().__init__(timeout=120)
        self.ctx = ctx
        self.mode = mode
        self.page = page
        self.page_size = 5
        self.update_embed_data()

    def update_embed_data(self):
        if self.mode == "week":
            self.entries = self.get_scores_by_range(days=7)
        elif self.mode == "month":
            self.entries = self.get_scores_by_range(days=30)
        else:
            self.entries = sorted(db.scores.items(), key=lambda x: x[1], reverse=True)

        self.total_pages = max(1, (len(self.entries) + self.page_size - 1) // self.page_size)

    def get_scores_by_range(self, days):
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=days)
        temp_scores = defaultdict(float)
        for entry in db.actions:
            if entry.get("is_undo"):
                continue
            ts = entry.get("timestamp")
            if not ts:
                continue  # Пропускаем пустые timestamp
            if isinstance(ts, str):
                try:
                    ts = datetime.fromisoformat(ts)
                except Exception:
                    continue
            if not ts or not isinstance(ts, datetime):
                continue  # Пропускаем если не удалось распарсить
            if ts >= cutoff:
                temp_scores[int(entry["user_id"])] += float(entry["points"])
        return sorted(temp_scores.items(), key=lambda x: x[1], reverse=True)

    def get_embed(self):
        start = (self.page - 1) * self.page_size
        entries = self.entries[start:start + self.page_size]

        if not entries:
            embed = discord.Embed(
                title="🏆 Топ участников",
                description="Нет данных для отображения.",
                color=discord.Color.gold(),
            )
            embed.set_footer(text=f"Страница {self.page}/{self.total_pages} • Режим: {self.mode}")
            return embed

        formatted = []
        for uid, points in entries:
            member = self.ctx.guild.get_member(uid)
            name = member.display_name if member else f"<@{uid}>"

            roles = []
            if member:
                roles = [r.name for r in member.roles if r.id in ROLE_THRESHOLDS]
            role_text = f"\nРоль: {', '.join(roles)}" if roles else ""
            formatted.append((name, f"**{format_points(points)}** баллов{role_text}"))

        footer = f"Страница {self.page}/{self.total_pages} • Режим: {self.mode}"
        return build_top_embed(
            title="🏆 Топ участников",
            entries=formatted,
            color=discord.Color.gold(),
            footer=footer,
            start_index=start + 1,
        )

    @discord.ui.button(label="◀️", style=discord.ButtonStyle.gray)
    async def prev_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.page > 1:
            self.page -= 1
            await interaction.response.edit_message(embed=self.get_embed(), view=self)

    @discord.ui.button(label="▶️", style=discord.ButtonStyle.gray)
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.page < self.total_pages:
            self.page += 1
            await interaction.response.edit_message(embed=self.get_embed(), view=self)

    @discord.ui.button(label="Неделя", style=discord.ButtonStyle.blurple)
    async def mode_week(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.mode = "week"
        self.page = 1
        self.update_embed_data()
        await interaction.response.edit_message(embed=self.get_embed(), view=self)

    @discord.ui.button(label="Месяц", style=discord.ButtonStyle.blurple)
    async def mode_month(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.mode = "month"
        self.page = 1
        self.update_embed_data()
        await interaction.response.edit_message(embed=self.get_embed(), view=self)

    @discord.ui.button(label="Все время", style=discord.ButtonStyle.green)
    async def mode_all(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.mode = "all"
        self.page = 1
        self.update_embed_data()
        await interaction.response.edit_message(embed=self.get_embed(), view=self)

async def transfer_data_logic(old_id: int, new_id: int) -> discord.Embed:
    success = db.transfer_user_data(old_id, new_id)

    if success:
        embed = discord.Embed(
            title="✅ Данные успешно перенесены",
            color=discord.Color.green()
        )
        embed.add_field(name="📤 От:", value=f"<@{old_id}> (`{old_id}`)", inline=False)
        embed.add_field(name="📥 Кому:", value=f"<@{new_id}> (`{new_id}`)", inline=False)
        embed.set_footer(text="Перенос баллов, билетов и логов")
    else:
        embed = discord.Embed(
            title="❌ Ошибка при переносе данных",
            description="Проверьте корректность ID или повторите позже.",
            color=discord.Color.red()
        )
    return embed

def build_balance_embed(member: discord.Member) -> discord.Embed:
    user_id = member.id
    points = db.scores.get(user_id, 0)
    roles = [role for role in member.roles if role.id in ROLE_THRESHOLDS]
    role_names = ', '.join(role.name for role in roles) if roles else 'Нет роли'

    sorted_scores = sorted(db.scores.items(), key=lambda x: x[1], reverse=True)
    place = next((i for i, (uid, _) in enumerate(sorted_scores, 1) if uid == user_id), None)

    # Загружаем билеты
    try:
        result = db.supabase.table("scores").select("tickets_normal, tickets_gold").eq("user_id", user_id).single().execute()
        data = result.data or {}
    except Exception:
        data = {}

    normal = data.get("tickets_normal", 0)
    gold = data.get("tickets_gold", 0)

    embed = discord.Embed(
        title=f"Баланс пользователя {member.display_name}",
        color=discord.Color.blue()
    )
    embed.set_thumbnail(url=member.avatar.url if member.avatar else member.default_avatar.url)

    embed.add_field(name="🎯 Баллы", value=format_points(points), inline=True)
    embed.add_field(name="🎟 Обычные билеты", value=f"{normal}", inline=True)
    embed.add_field(name="🪙 Золотые билеты", value=f"{gold}", inline=True)
    embed.add_field(name="🏅 Роли", value=role_names, inline=False)
    embed.add_field(name="📊 Место в топе", value=f"{place}" if place else "Не в топе", inline=False)

    # ➕ Добавим бонусы за топ месяца
    top_bonus_count = 0
    top_bonus_sum = 0.0
    for action in db.history.get(user_id, []):
        if action.get("reason", "").startswith("Бонус за "):
            top_bonus_count += 1
            top_bonus_sum += action.get("points", 0)

    if top_bonus_count:
        embed.add_field(
            name="🏆 Бонусы за топ месяца",
            value=f"{top_bonus_count} наград, {top_bonus_sum:.2f} баллов",
            inline=False
        )

    return embed
