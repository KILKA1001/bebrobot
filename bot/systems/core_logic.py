import discord
from discord.ext import commands
from typing import Optional
from datetime import datetime, timezone
import pytz
import asyncio
import traceback

from bot.data import db
from bot.utils.roles_and_activities import ROLE_THRESHOLDS
from bot.utils.history_manager import format_history_embed

TIME_FORMAT = "%H:%M (%d.%m.%Y)"
active_timers = {}

def format_moscow_time(dt: Optional[datetime] = None) -> str:
  if dt is None:
      dt = datetime.now(timezone.utc)
  return dt.astimezone(pytz.timezone('Europe/Moscow')).strftime(TIME_FORMAT)
  
async def update_roles(member: discord.Member):
    user_id = member.id
    user_points = db.scores.get(user_id, 0)
    user_roles = [role.id for role in member.roles if role.id in ROLE_THRESHOLDS]

    role_to_add_id = None
    for role_id, threshold in sorted(ROLE_THRESHOLDS.items(), key=lambda x: x[1], reverse=True):
        if user_points >= threshold:
            role_to_add_id = role_id
            break

    if role_to_add_id and role_to_add_id not in user_roles:
        role_to_add = member.guild.get_role(role_to_add_id)
        if role_to_add:
            await member.add_roles(role_to_add)

    for role_id in user_roles:
        if role_id != role_to_add_id:
            role_to_remove = member.guild.get_role(role_id)
            if role_to_remove:
                await member.remove_roles(role_to_remove)


class HistoryView(discord.ui.View):
    def __init__(self, member: discord.Member, page: int, total_pages: int):
        super().__init__(timeout=60)
        self.member = member
        self.page = page
        self.total_pages = total_pages

        self.prev_button.disabled = page <= 1
        self.next_button.disabled = page >= total_pages

    @discord.ui.button(label="◀️ Назад", style=discord.ButtonStyle.gray, custom_id="prev")
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        await render_history(interaction, self.member, self.page - 1)

    @discord.ui.button(label="Вперед ▶️", style=discord.ButtonStyle.gray, custom_id="next")
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
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
        embed.add_field(name="💰 Текущий баланс", value=f"```{total_points} баллов```", inline=False)

        for action in page_actions:
            points = action.get('points', 0)
            emoji = "🟢" if points >= 0 else "🔴"
            if action.get('is_undo', False):
                emoji = "⚪"

            timestamp = action.get('timestamp')
            if isinstance(timestamp, str):
                try:
                    dt = datetime.fromisoformat(timestamp)
                    formatted_time = dt.astimezone(pytz.timezone('Europe/Moscow')).strftime("%H:%M (%d.%m.%Y)")
                except ValueError:
                    formatted_time = timestamp
            else:
                formatted_time = timestamp.strftime("%H:%M (%d.%m.%Y)") if timestamp else 'N/A'

            author_id = action.get('author_id', 'N/A')
            reason = action.get('reason', 'Не указана')

            field_name = f"{emoji} {formatted_time}"
            field_value = (
                f"```diff\n{'+' if points >= 0 else ''}{points} баллов```\n"
                f"**Причина:** {reason}\n"
                f"**Выдал:** <@{author_id}>"
            )
            embed.add_field(name=field_name, value=field_value, inline=False)

        embed.set_footer(text=f"Страница {page}/{total_pages} • Всего записей: {total_entries}")

        view = HistoryView(member, page, total_pages)

        if isinstance(ctx_or_interaction, discord.Interaction):
            if ctx_or_interaction.response.is_done():
                sent_message = await ctx_or_interaction.edit_original_response(embed=embed, view=view)
            else:
                await ctx_or_interaction.response.send_message(embed=embed, view=view)
                sent_message = await ctx_or_interaction.original_response()
        else:
            sent_message = await ctx_or_interaction.send(embed=embed, view=view)

        async def delete_later(msg: discord.Message):
            try:
                await asyncio.sleep(180)
                await msg.delete()
            except (discord.NotFound, discord.Forbidden):
                pass

        asyncio.create_task(delete_later(sent_message))

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

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    lines = [f"**{ctx.author.display_name}** отменил(а) {len(entries)} изменения для **{member.display_name}** ({member.id}) в {now}:"]
    for i, (points, reason) in enumerate(entries[::-1], start=1):
        sign = "+" if points > 0 else ""
        lines.append(f"{i}. {sign}{points} — {reason}")

    await channel.send("\n".join(lines))


async def run_monthly_top(ctx):
    now = datetime.now(pytz.timezone('Europe/Moscow'))
    current_month = now.month
    current_year = now.year
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
        await ctx.send("❌ Нет данных о баллах за этот месяц.")
        return

    top_users = sorted(monthly_scores.items(), key=lambda x: x[1], reverse=True)[:3]
    percentages = [0.125, 0.075, 0.05]
    descriptions = ["🥇 1 место", "🥈 2 место", "🥉 3 место"]

    entries_to_log = []
    embed = discord.Embed(title="🏆 Топ месяца", color=discord.Color.gold())

    for i, (uid, score) in enumerate(top_users):
        percent = percentages[i]
        bonus = round(score * percent, 2)
        db.add_action(uid, bonus, f"Бонус за {descriptions[i]} ({score} баллов)", ctx.author.id)
        member = ctx.guild.get_member(uid)
        name = member.display_name if member else f"<@{uid}>"



        embed.add_field(
            name=f"{descriptions[i]} — {name}",
            value=f"Заработано: {score:.2f} баллов\nБонус: +{bonus:.2f} баллов",
            inline=False
        )
        
        entries_to_log.append((uid, score, percent))

    db.log_monthly_top(entries_to_log)
    await ctx.send(embed=embed)


async def tophistory(ctx, month: Optional[int] = None, year: Optional[int] = None):
    now = datetime.now()
    month = month or now.month
    year = year or now.year

    if not db.supabase:
        await ctx.send("❌ Supabase не инициализирован.")
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
            await ctx.send(f"📭 Нет записей за {month:02d}.{year}")
            return

        embed = discord.Embed(
            title=f"📅 История топа — {month:02d}.{year}",
            color=discord.Color.green()
        )
        for entry in entries:
            uid = entry['user_id']
            place = entry['place']
            bonus = entry['bonus']
            medal = "🥇" if place == 1 else "🥈" if place == 2 else "🥉"
            embed.add_field(
                name=f"{medal} Место {place}",
                value=f"<@{uid}> — +{bonus} баллов",
                inline=False
            )
        await ctx.send(embed=embed)

    except Exception as e:
        await ctx.send(f"❌ Ошибка при получении данных: {e}")
