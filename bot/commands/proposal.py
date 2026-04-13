"""
Назначение: модуль "proposal" реализует продуктовый контур в зоне Discord.
Ответственность: единый сценарий предложений Совету в рамках одной команды.
Где используется: Discord.
Пользовательский вход: команда /proposal и связанный пользовательский сценарий.
"""

from __future__ import annotations

import logging

import discord
from discord.ext import commands

from bot.commands.base import bot
from bot.services.council_feedback_service import CouncilFeedbackService
from bot.services.proposal_ui_texts import (
    render_archive_empty_text,
    render_archive_lines,
    render_confirmation_prompt,
    render_help_text,
    render_menu_overview,
    render_status_text,
    render_submit_success_text,
)

logger = logging.getLogger(__name__)


class ProposalSubmitModal(discord.ui.Modal, title="Подать предложение"):
    proposal_title = discord.ui.TextInput(label="Заголовок", max_length=140, required=True)
    proposal_text = discord.ui.TextInput(
        label="Текст предложения",
        style=discord.TextStyle.paragraph,
        max_length=1000,
        required=True,
    )

    def __init__(self, view: "ProposalRootView"):
        super().__init__()
        self.root_view = view

    async def on_submit(self, interaction: discord.Interaction) -> None:
        try:
            self.root_view.pending_title = str(self.proposal_title.value or "").strip()
            self.root_view.pending_text = str(self.proposal_text.value or "").strip()
            await interaction.response.send_message(
                embed=self.root_view.build_confirmation_embed(),
                view=ProposalConfirmView(self.root_view),
                ephemeral=True,
            )
        except Exception:
            logger.exception("discord proposal modal submit failed actor_id=%s", getattr(interaction.user, "id", None))
            if interaction.response.is_done():
                await interaction.followup.send("❌ Не удалось подготовить подтверждение. Попробуйте ещё раз.", ephemeral=True)
            else:
                await interaction.response.send_message("❌ Не удалось подготовить подтверждение. Попробуйте ещё раз.", ephemeral=True)


class ProposalConfirmView(discord.ui.View):
    def __init__(self, root_view: "ProposalRootView"):
        super().__init__(timeout=300)
        self.root_view = root_view

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.root_view.actor_id:
            await interaction.response.send_message("❌ Это окно подтверждения открыто для другого пользователя.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="✅ Отправить", style=discord.ButtonStyle.success)
    async def confirm_submit(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        try:
            result = CouncilFeedbackService.submit_proposal(
                provider="discord",
                provider_user_id=str(interaction.user.id),
                title=self.root_view.pending_title,
                proposal_text=self.root_view.pending_text,
            )
            if not result.get("ok"):
                await interaction.response.edit_message(
                    content=str(result.get("message") or "Не удалось отправить предложение."),
                    embed=None,
                    view=None,
                )
                return
            await interaction.response.edit_message(
                embed=discord.Embed(
                    title="✅ Предложение отправлено",
                    description=render_submit_success_text(
                        proposal_id=result.get("proposal_id"),
                        status_label=result.get("status_label"),
                    ),
                    color=discord.Color.green(),
                ),
                view=None,
            )
        except Exception:
            logger.exception("discord proposal confirm failed actor_id=%s", getattr(interaction.user, "id", None))
            await interaction.response.edit_message(content="❌ Ошибка отправки. Попробуйте ещё раз.", embed=None, view=None)

    @discord.ui.button(label="✏️ Изменить", style=discord.ButtonStyle.secondary)
    async def edit_submit(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interaction.response.send_modal(ProposalSubmitModal(self.root_view))

    @discord.ui.button(label="↩️ В меню", style=discord.ButtonStyle.secondary)
    async def back_to_menu(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interaction.response.edit_message(embed=self.root_view.build_root_embed(), view=self.root_view)


class ProposalRootView(discord.ui.View):
    def __init__(self, actor_id: int):
        super().__init__(timeout=600)
        self.actor_id = actor_id
        self.pending_title: str = ""
        self.pending_text: str = ""

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.actor_id:
            await interaction.response.send_message("❌ Это меню открыто для другого пользователя.", ephemeral=True)
            return False
        return True

    def build_root_embed(self) -> discord.Embed:
        return discord.Embed(
            title="🗂 Меню предложений",
            description=render_menu_overview(),
            color=discord.Color.blurple(),
        )

    def build_confirmation_embed(self) -> discord.Embed:
        return discord.Embed(
            title="📨 Подтверждение отправки",
            description=render_confirmation_prompt(),
            color=discord.Color.gold(),
        ).add_field(name="Заголовок", value=self.pending_title or "—", inline=False).add_field(name="Текст", value=self.pending_text or "—", inline=False)

    @discord.ui.button(label="📝 Подать предложение", style=discord.ButtonStyle.primary)
    async def open_form(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interaction.response.send_modal(ProposalSubmitModal(self))

    @discord.ui.button(label="📍 Статус", style=discord.ButtonStyle.secondary)
    async def show_status(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        try:
            payload = CouncilFeedbackService.get_latest_status(provider="discord", provider_user_id=str(interaction.user.id))
            if not payload.get("ok"):
                await interaction.response.send_message(str(payload.get("message") or "Не удалось загрузить статус."), ephemeral=True)
                return
            if not payload.get("has_data"):
                await interaction.response.send_message(str(payload.get("message")), ephemeral=True)
                return
            await interaction.response.send_message(
                embed=discord.Embed(
                    title="📍 Текущий статус",
                    description=render_status_text(
                        proposal_id=payload.get("proposal_id"),
                        title=payload.get("title"),
                        status_label=payload.get("status_label"),
                        updated_at=payload.get("updated_at"),
                    ),
                    color=discord.Color.blue(),
                ),
                ephemeral=True,
            )
        except Exception:
            logger.exception("discord proposal status failed actor_id=%s", getattr(interaction.user, "id", None))
            await interaction.response.send_message("❌ Не удалось открыть статус. Попробуйте позже.", ephemeral=True)

    @discord.ui.button(label="📚 Архив решений", style=discord.ButtonStyle.secondary)
    async def show_archive(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        try:
            rows = CouncilFeedbackService.get_decisions_archive(limit=5)
            if not rows:
                await interaction.response.send_message(render_archive_empty_text(), ephemeral=True)
                return
            lines = render_archive_lines(rows, text_limit=160)
            await interaction.response.send_message(
                embed=discord.Embed(title="📚 Архив решений", description="\n".join(lines), color=discord.Color.dark_teal()),
                ephemeral=True,
            )
        except Exception:
            logger.exception("discord proposal archive failed actor_id=%s", getattr(interaction.user, "id", None))
            await interaction.response.send_message("❌ Не удалось открыть архив. Попробуйте позже.", ephemeral=True)

    @discord.ui.button(label="❓ Помощь", style=discord.ButtonStyle.secondary)
    async def show_help(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interaction.response.send_message(render_help_text(), ephemeral=True)


@bot.hybrid_command(name="proposal", description="Единое меню подачи предложений в Совет")
async def proposal(ctx: commands.Context) -> None:
    try:
        if not ctx.author:
            await ctx.reply("❌ Не удалось определить пользователя.", mention_author=False)
            return
        view = ProposalRootView(actor_id=ctx.author.id)
        await ctx.reply(embed=view.build_root_embed(), view=view, mention_author=False)
    except Exception:
        logger.exception("discord proposal command failed actor_id=%s", getattr(getattr(ctx, "author", None), "id", None))
        await ctx.reply("❌ Не удалось открыть меню предложений.", mention_author=False)
