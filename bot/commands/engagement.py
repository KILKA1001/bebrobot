import logging
import asyncio

import discord
from discord.ext import commands

from bot.commands import bot
from bot.services import AccountsService, AuthorityService, PointsService, TicketsService
from bot.utils import send_temp, safe_defer, safe_edit_original_response

logger = logging.getLogger(__name__)
PROCESSING_TEXT = "⏳ Обрабатываю…"
ROLE_UPDATE_TEXT = "🛠️ Сохраняю изменение роли…"


def _can_manage_tickets(authority) -> bool:
    normalized = {str(title).strip().lower() for title in authority.titles}
    if "глава клуба" in normalized or "главный вице" in normalized:
        return True
    return authority.level >= 100


def _can_manage_points(authority) -> bool:
    return authority.level >= 80


def _can_manage_own_engagement(authority) -> bool:
    normalized = {str(title).strip().lower() for title in authority.titles}
    return bool(normalized & {"глава клуба", "главный вице"})


def _score_snapshot(account_id: str) -> tuple[float, int, int]:
    from bot.data import db

    if not db.supabase:
        return 0.0, 0, 0
    try:
        row = (
            db.supabase.table("scores")
            .select("points,tickets_normal,tickets_gold")
            .eq("account_id", str(account_id))
            .limit(1)
            .execute()
        )
        if row.data:
            data = row.data[0]
            return float(data.get("points") or 0), int(data.get("tickets_normal") or 0), int(data.get("tickets_gold") or 0)
    except Exception:
        logger.exception("discord engagement snapshot failed account_id=%s", account_id)
    return 0.0, 0, 0


class PointsActionModal(discord.ui.Modal):
    def __init__(self, *, target: discord.Member, actor_id: int, operation: str):
        title = "Начисление баллов" if operation == "add" else "Списание баллов"
        super().__init__(title=title)
        self.target = target
        self.actor_id = actor_id
        self.operation = operation

        self.amount = discord.ui.TextInput(label="Количество", placeholder="Например: 10.5", required=True, max_length=32)
        self.reason = discord.ui.TextInput(label="Причина", required=True, style=discord.TextStyle.paragraph, max_length=400)
        self.add_item(self.amount)
        self.add_item(self.reason)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            amount = float(str(self.amount.value).replace(",", "."))
            if amount <= 0:
                await interaction.response.send_message("❌ Количество баллов должно быть больше 0.", ephemeral=True)
                return
            reason = str(self.reason.value).strip()
            if not reason:
                await interaction.response.send_message("❌ Причина обязательна.", ephemeral=True)
                return

            await safe_defer(interaction, ephemeral=True)
            await safe_edit_original_response(interaction, content=PROCESSING_TEXT)

            if self.operation == "add":
                ok = await asyncio.to_thread(
                    PointsService.add_points_by_identity,
                    "discord",
                    str(self.target.id),
                    amount,
                    reason,
                    self.actor_id,
                )
                action_text = "начислены"
            else:
                ok = await asyncio.to_thread(
                    PointsService.remove_points_by_identity,
                    "discord",
                    str(self.target.id),
                    amount,
                    reason,
                    self.actor_id,
                )
                action_text = "списаны"
            if not ok:
                logger.error(
                    "points modal action failed actor_id=%s target_id=%s operation=%s",
                    self.actor_id,
                    self.target.id,
                    self.operation,
                )
                await interaction.followup.send(
                    "❌ Не удалось обновить баллы. Проверьте привязку аккаунтов.",
                    ephemeral=True,
                )
                return

            await interaction.followup.send(
                f"✅ Баллы успешно {action_text}: {amount:.2f}. Причина: {reason}",
                ephemeral=True,
            )
        except ValueError:
            logger.exception(
                "points modal value parse failed actor_id=%s target_id=%s value=%s",
                self.actor_id,
                self.target.id,
                self.amount.value,
            )
            if interaction.response.is_done():
                await interaction.followup.send("❌ Ошибка формата количества.", ephemeral=True)
            else:
                await interaction.response.send_message("❌ Ошибка формата количества.", ephemeral=True)
        except Exception:
            logger.exception(
                "points modal submit failed actor_id=%s target_id=%s",
                self.actor_id,
                self.target.id,
            )
            if interaction.response.is_done():
                await interaction.followup.send("❌ Ошибка выполнения операции.", ephemeral=True)
            else:
                await interaction.response.send_message("❌ Ошибка выполнения операции.", ephemeral=True)


class TicketsActionModal(discord.ui.Modal):
    def __init__(self, *, target: discord.Member, actor_id: int, operation: str):
        title_map = {
            "add_normal": "Выдача обычного билета",
            "remove_normal": "Списание обычного билета",
            "add_gold": "Выдача золотого билета",
            "remove_gold": "Списание золотого билета",
        }
        super().__init__(title=title_map[operation])
        self.target = target
        self.actor_id = actor_id
        self.operation = operation

        self.reason = discord.ui.TextInput(label="Причина", required=True, style=discord.TextStyle.paragraph, max_length=400)
        self.add_item(self.reason)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            reason = str(self.reason.value).strip()
            if not reason:
                await interaction.response.send_message("❌ Причина обязательна.", ephemeral=True)
                return

            await safe_defer(interaction, ephemeral=True)
            await safe_edit_original_response(interaction, content=ROLE_UPDATE_TEXT)

            mapping = {
                "add_normal": ("normal", True),
                "remove_normal": ("normal", False),
                "add_gold": ("gold", True),
                "remove_gold": ("gold", False),
            }
            ticket_type, is_add = mapping[self.operation]

            if is_add:
                ok = await asyncio.to_thread(
                    TicketsService.give_ticket_by_identity,
                    "discord",
                    str(self.target.id),
                    ticket_type,
                    1,
                    reason,
                    self.actor_id,
                )
                verb = "начислены"
            else:
                ok = await asyncio.to_thread(
                    TicketsService.remove_ticket_by_identity,
                    "discord",
                    str(self.target.id),
                    ticket_type,
                    1,
                    reason,
                    self.actor_id,
                )
                verb = "списаны"
            if not ok:
                logger.error(
                    "tickets modal action failed actor_id=%s target_id=%s operation=%s",
                    self.actor_id,
                    self.target.id,
                    self.operation,
                )
                await interaction.followup.send(
                    "❌ Не удалось обновить билеты. Проверьте привязку аккаунтов.",
                    ephemeral=True,
                )
                return

            await interaction.followup.send(
                f"✅ Билеты успешно {verb}: 1. Причина: {reason}",
                ephemeral=True,
            )
        except Exception:
            logger.exception(
                "tickets modal submit failed actor_id=%s target_id=%s operation=%s",
                self.actor_id,
                self.target.id,
                self.operation,
            )
            if interaction.response.is_done():
                await interaction.followup.send("❌ Ошибка выполнения операции.", ephemeral=True)
            else:
                await interaction.response.send_message("❌ Ошибка выполнения операции.", ephemeral=True)


class EngagementMenuView(discord.ui.View):
    def __init__(self, *, target: discord.Member, actor_id: int, domain: str):
        super().__init__(timeout=300)
        self.target = target
        self.actor_id = actor_id
        self.domain = domain

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.actor_id:
            await interaction.response.send_message("Чушка, не суй свой пятак в чужой пердак", ephemeral=True)
            logger.warning(
                "discord engagement button denied foreign actor actor_id=%s owner_id=%s custom_id=%s",
                interaction.user.id,
                self.actor_id,
                interaction.data.get("custom_id") if interaction.data else None,
            )
            return False
        return True

    @discord.ui.button(label="ℹ️ Что делает команда", style=discord.ButtonStyle.secondary)
    async def help_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.domain == "points":
            text = "Изменение баллов пользователя. Для каждого изменения причина обязательна."
        else:
            text = "Выдача/списание билетов по одному за действие. Причина обязательна."
        await interaction.response.send_message(text, ephemeral=True)

    @discord.ui.button(label="➕ Начислить баллы", style=discord.ButtonStyle.success)
    async def points_add(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.domain != "points":
            await interaction.response.send_message("❌ Действие недоступно в этом меню.", ephemeral=True)
            return
        await interaction.response.send_modal(PointsActionModal(target=self.target, actor_id=self.actor_id, operation="add"))

    @discord.ui.button(label="➖ Снять баллы", style=discord.ButtonStyle.danger)
    async def points_remove(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.domain != "points":
            await interaction.response.send_message("❌ Действие недоступно в этом меню.", ephemeral=True)
            return
        await interaction.response.send_modal(PointsActionModal(target=self.target, actor_id=self.actor_id, operation="remove"))

    @discord.ui.button(label="🎟️ + Обычные", style=discord.ButtonStyle.success)
    async def tickets_add_normal(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.domain != "tickets":
            await interaction.response.send_message("❌ Действие недоступно в этом меню.", ephemeral=True)
            return
        await interaction.response.send_modal(TicketsActionModal(target=self.target, actor_id=self.actor_id, operation="add_normal"))

    @discord.ui.button(label="🎟️ - Обычные", style=discord.ButtonStyle.danger)
    async def tickets_remove_normal(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.domain != "tickets":
            await interaction.response.send_message("❌ Действие недоступно в этом меню.", ephemeral=True)
            return
        await interaction.response.send_modal(TicketsActionModal(target=self.target, actor_id=self.actor_id, operation="remove_normal"))

    @discord.ui.button(label="🪙 + Золотые", style=discord.ButtonStyle.success)
    async def tickets_add_gold(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.domain != "tickets":
            await interaction.response.send_message("❌ Действие недоступно в этом меню.", ephemeral=True)
            return
        await interaction.response.send_modal(TicketsActionModal(target=self.target, actor_id=self.actor_id, operation="add_gold"))

    @discord.ui.button(label="🪙 - Золотые", style=discord.ButtonStyle.danger)
    async def tickets_remove_gold(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.domain != "tickets":
            await interaction.response.send_message("❌ Действие недоступно в этом меню.", ephemeral=True)
            return
        await interaction.response.send_modal(TicketsActionModal(target=self.target, actor_id=self.actor_id, operation="remove_gold"))

    async def on_timeout(self) -> None:
        for child in self.children:
            child.disabled = True

    def sync_buttons(self):
        for child in self.children:
            if not isinstance(child, discord.ui.Button):
                continue
            if self.domain == "points":
                child.disabled = child.label in {"🎟️ + Обычные", "🎟️ - Обычные", "🪙 + Золотые", "🪙 - Золотые"}
            else:
                child.disabled = child.label in {"➕ Начислить баллы", "➖ Снять баллы"}


async def _resolve_target_member(ctx: commands.Context, member: discord.Member | None) -> discord.Member | None:
    target = member or ctx.author
    if isinstance(target, discord.Member):
        return target
    if ctx.guild is None:
        return None
    return ctx.guild.get_member(ctx.author.id)


@bot.hybrid_command(name="points", description="Меню управления баллами")
async def points_menu(ctx, member: discord.Member = None):
    try:
        target = await _resolve_target_member(ctx, member)
        if target is None:
            await send_temp(ctx, "❌ Команда доступна только на сервере Discord.")
            return

        authority = AuthorityService.resolve_authority("discord", str(ctx.author.id))
        if not _can_manage_points(authority):
            await send_temp(ctx, "Недоступно по вашему званию.")
            return

        if target.id == ctx.author.id:
            if not _can_manage_own_engagement(authority):
                logger.warning("discord points menu self-edit denied actor_id=%s", ctx.author.id)
                await send_temp(ctx, "❌ Нельзя редактировать себя. Доступно только Главе клуба и Главному вице.")
                return
        elif not AuthorityService.can_manage_target("discord", str(ctx.author.id), "discord", str(target.id)):
            await send_temp(ctx, "❌ Нельзя взаимодействовать с пользователем с равным/более высоким званием.")
            return

        profile = AccountsService.get_profile("discord", str(target.id))
        if not profile:
            await send_temp(ctx, "❌ Целевой пользователь не зарегистрирован в системе.")
            return

        points, tickets_normal, tickets_gold = await asyncio.to_thread(_score_snapshot, profile["account_id"])
        embed = discord.Embed(title="🎛️ Меню баллов", color=discord.Color.blue())
        embed.add_field(name="Пользователь", value=target.mention, inline=False)
        embed.add_field(name="Текущий баланс", value=f"**{points:.2f}**", inline=False)
        embed.add_field(name="Билеты", value=f"🎟️ {tickets_normal} / 🪙 {tickets_gold}", inline=False)
        embed.description = "Выберите действие. Для любого изменения причина обязательна."

        view = EngagementMenuView(target=target, actor_id=ctx.author.id, domain="points")
        view.sync_buttons()
        await send_temp(ctx, embed=embed, view=view, delete_after=None)
    except Exception:
        logger.exception("discord points menu command failed actor_id=%s", ctx.author.id)
        await send_temp(ctx, "❌ Ошибка открытия меню баллов.")


@bot.hybrid_command(name="tickets", description="Меню управления билетами")
async def tickets_menu(ctx, member: discord.Member = None):
    try:
        target = await _resolve_target_member(ctx, member)
        if target is None:
            await send_temp(ctx, "❌ Команда доступна только на сервере Discord.")
            return

        authority = AuthorityService.resolve_authority("discord", str(ctx.author.id))
        if not _can_manage_tickets(authority):
            await send_temp(ctx, "Недоступно по вашему званию.")
            return

        if target.id == ctx.author.id:
            if not _can_manage_own_engagement(authority):
                logger.warning("discord tickets menu self-edit denied actor_id=%s", ctx.author.id)
                await send_temp(ctx, "❌ Нельзя редактировать себя. Доступно только Главе клуба и Главному вице.")
                return
        elif not AuthorityService.can_manage_target("discord", str(ctx.author.id), "discord", str(target.id)):
            await send_temp(ctx, "❌ Нельзя взаимодействовать с пользователем с равным/более высоким званием.")
            return

        profile = AccountsService.get_profile("discord", str(target.id))
        if not profile:
            await send_temp(ctx, "❌ Целевой пользователь не зарегистрирован в системе.")
            return

        points, tickets_normal, tickets_gold = await asyncio.to_thread(_score_snapshot, profile["account_id"])
        embed = discord.Embed(title="🎟️ Меню билетов", color=discord.Color.blue())
        embed.add_field(name="Пользователь", value=target.mention, inline=False)
        embed.add_field(name="Баллы", value=f"**{points:.2f}**", inline=False)
        embed.add_field(name="Текущие билеты", value=f"🎟️ **{tickets_normal}** / 🪙 **{tickets_gold}**", inline=False)
        embed.description = "Выберите действие. Для любого изменения причина обязательна."

        view = EngagementMenuView(target=target, actor_id=ctx.author.id, domain="tickets")
        view.sync_buttons()
        await send_temp(ctx, embed=embed, view=view, delete_after=None)
    except Exception:
        logger.exception("discord tickets menu command failed actor_id=%s", ctx.author.id)
        await send_temp(ctx, "❌ Ошибка открытия меню билетов.")
