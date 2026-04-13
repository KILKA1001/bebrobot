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
from bot.services.council_system_events_service import CouncilSystemEventsService
from bot.services.proposal_ui_texts import (
    ARCHIVE_PERIOD_LABELS,
    ARCHIVE_STATUS_LABELS,
    ARCHIVE_TYPE_LABELS,
    render_archive_empty_text,
    render_archive_filters_text,
    render_archive_lines,
    render_confirmation_prompt,
    render_help_text,
    render_menu_overview,
    render_status_text,
    render_submit_success_text,
)

logger = logging.getLogger(__name__)


def _log_alias_event(*, operation: str, user_id: int | None, result: str) -> None:
    logger.info(
        "proposal alias event operation=%s platform=discord user_id=%s result=%s",
        operation,
        user_id,
        result,
    )


_log_alias_event(operation="command_alias_register:council", user_id=None, result="ok")


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
        self.archive_period_code: str = "90d"
        self.archive_status_code: str = "all"
        self.archive_question_type_code: str = "all"

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.actor_id:
            await interaction.response.send_message("❌ Это меню открыто для другого пользователя.", ephemeral=True)
            return False
        return True

    def build_root_embed(self) -> discord.Embed:
        return discord.Embed(
            title="🗂 Меню предложений",
            description=(
                render_menu_overview()
                + "\n\n"
                "📝 «Подать предложение» — начать новый вопрос для Совета.\n"
                "📍 «Статус» — проверить текущий этап по вашему последнему вопросу.\n"
                "📚 «Архив решений» — открыть уже завершённые решения Совета.\n"
                "❓ «Помощь» — посмотреть короткую пошаговую инструкцию."
            ),
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
                logger.error(
                    "discord proposal status not ok actor_id=%s message=%s",
                    getattr(interaction.user, "id", None),
                    payload.get("message"),
                )
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
            rows = CouncilFeedbackService.get_decisions_archive(
                limit=5,
                period_code=self.archive_period_code,
                status_code=self.archive_status_code,
                question_type_code=self.archive_question_type_code,
            )
            if not rows:
                await interaction.response.send_message(
                    render_archive_empty_text() + "\n\n" + render_archive_filters_text(
                        period_code=self.archive_period_code,
                        status_code=self.archive_status_code,
                        question_type_code=self.archive_question_type_code,
                    ),
                    view=ProposalArchiveFilterView(self),
                    ephemeral=True,
                )
                return
            lines = render_archive_lines(rows, text_limit=160)
            await interaction.response.send_message(
                embed=discord.Embed(
                    title="📚 Архив решений",
                    description=render_archive_filters_text(
                        period_code=self.archive_period_code,
                        status_code=self.archive_status_code,
                        question_type_code=self.archive_question_type_code,
                    )
                    + "\n\n"
                    + "\n".join(lines),
                    color=discord.Color.dark_teal(),
                ),
                view=ProposalArchiveFilterView(self),
                ephemeral=True,
            )
        except Exception:
            logger.exception("discord proposal archive failed actor_id=%s", getattr(interaction.user, "id", None))
            await interaction.response.send_message("❌ Не удалось открыть архив. Попробуйте позже.", ephemeral=True)

    @discord.ui.button(label="❓ Помощь", style=discord.ButtonStyle.secondary)
    async def show_help(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interaction.response.send_message(render_help_text(), ephemeral=True)


class ProposalArchiveFilterView(discord.ui.View):
    def __init__(self, root_view: ProposalRootView):
        super().__init__(timeout=600)
        self.root_view = root_view
        self._sync_labels()

    def _sync_labels(self) -> None:
        self.period_button.label = f"🗓 Период: {ARCHIVE_PERIOD_LABELS.get(self.root_view.archive_period_code, '90 дней')}"
        self.status_button.label = f"📌 Статус: {ARCHIVE_STATUS_LABELS.get(self.root_view.archive_status_code, 'Все статусы')}"
        self.type_button.label = f"🧩 Тип: {ARCHIVE_TYPE_LABELS.get(self.root_view.archive_question_type_code, 'Все типы')}"

    async def _refresh_archive_message(self, interaction: discord.Interaction) -> None:
        rows = CouncilFeedbackService.get_decisions_archive(
            limit=5,
            period_code=self.root_view.archive_period_code,
            status_code=self.root_view.archive_status_code,
            question_type_code=self.root_view.archive_question_type_code,
        )
        if not rows:
            await interaction.response.edit_message(
                content=render_archive_empty_text()
                + "\n\n"
                + render_archive_filters_text(
                    period_code=self.root_view.archive_period_code,
                    status_code=self.root_view.archive_status_code,
                    question_type_code=self.root_view.archive_question_type_code,
                ),
                embed=None,
                view=self,
            )
            return
        lines = render_archive_lines(rows, text_limit=160)
        await interaction.response.edit_message(
            content=None,
            embed=discord.Embed(
                title="📚 Архив решений",
                description=render_archive_filters_text(
                    period_code=self.root_view.archive_period_code,
                    status_code=self.root_view.archive_status_code,
                    question_type_code=self.root_view.archive_question_type_code,
                )
                + "\n\n"
                + "\n".join(lines),
                color=discord.Color.dark_teal(),
            ),
            view=self,
        )

    @discord.ui.button(label="🗓 Период", style=discord.ButtonStyle.secondary)
    async def period_button(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        chain = ["30d", "90d", "365d", "all"]
        current = self.root_view.archive_period_code
        next_index = (chain.index(current) + 1) % len(chain) if current in chain else 0
        self.root_view.archive_period_code = chain[next_index]
        self._sync_labels()
        await self._refresh_archive_message(interaction)

    @discord.ui.button(label="📌 Статус", style=discord.ButtonStyle.secondary)
    async def status_button(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        chain = ["all", "accepted", "rejected", "pending"]
        current = self.root_view.archive_status_code
        next_index = (chain.index(current) + 1) % len(chain) if current in chain else 0
        self.root_view.archive_status_code = chain[next_index]
        self._sync_labels()
        await self._refresh_archive_message(interaction)

    @discord.ui.button(label="🧩 Тип", style=discord.ButtonStyle.secondary)
    async def type_button(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        chain = ["all", "general", "election", "other"]
        current = self.root_view.archive_question_type_code
        next_index = (chain.index(current) + 1) % len(chain) if current in chain else 0
        self.root_view.archive_question_type_code = chain[next_index]
        self._sync_labels()
        await self._refresh_archive_message(interaction)

@bot.hybrid_command(name="proposal", aliases=["council"], description="Единое меню подачи предложений в Совет")
async def proposal(ctx: commands.Context) -> None:
    actor_id = getattr(getattr(ctx, "author", None), "id", None)
    operation = f"command_invoke:{getattr(ctx, 'invoked_with', 'proposal')}"
    try:
        if not ctx.author:
            _log_alias_event(operation=operation, user_id=actor_id, result="failed_no_user")
            await ctx.reply("❌ Не удалось определить пользователя.", mention_author=False)
            return
        view = ProposalRootView(actor_id=ctx.author.id)
        await ctx.reply(embed=view.build_root_embed(), view=view, mention_author=False)
        _log_alias_event(operation=operation, user_id=actor_id, result="ok")
    except Exception:
        _log_alias_event(operation=operation, user_id=actor_id, result="error")
        logger.exception("discord proposal command failed actor_id=%s", actor_id)
        await ctx.reply("❌ Не удалось открыть меню предложений.", mention_author=False)


@bot.hybrid_command(name="proposal_system_channel", description="Настройка канала системных событий Совета (только суперадмин)")
async def proposal_system_channel(ctx: commands.Context, action: str = "show") -> None:
    try:
        if not ctx.author:
            await ctx.reply("❌ Не удалось определить пользователя.", mention_author=False)
            return
        normalized_action = str(action or "show").strip().lower()
        provider_user_id = str(ctx.author.id)
        if normalized_action == "show":
            current = CouncilSystemEventsService.get_channel("discord")
            if not current:
                await ctx.reply(
                    "ℹ️ Канал системных событий Совета пока не настроен.\n"
                    "Суперадмин может выполнить: `/proposal_system_channel set_here` в нужном канале.",
                    mention_author=False,
                )
                return
            await ctx.reply(f"✅ Сейчас выбран канал `{current}` для системных событий Совета.", mention_author=False)
            return
        if normalized_action == "set_here":
            channel_id = str(getattr(ctx.channel, "id", "") or "").strip()
            result = CouncilSystemEventsService.set_channel(
                provider="discord",
                actor_user_id=provider_user_id,
                destination_id=f"{getattr(ctx.guild, 'id', '')}:{channel_id}" if channel_id else "",
            )
            await ctx.reply(str(result.get("message") or ("✅ Канал системных событий Совета сохранён." if result.get("ok") else "❌ Не удалось сохранить канал.")), mention_author=False)
            return
        if normalized_action == "clear":
            result = CouncilSystemEventsService.set_channel(
                provider="discord",
                actor_user_id=provider_user_id,
                destination_id="",
            )
            await ctx.reply(str(result.get("message") or ("✅ Канал системных событий Совета очищен." if result.get("ok") else "❌ Не удалось очистить канал.")), mention_author=False)
            return
        await ctx.reply(
            "❌ Неизвестное действие. Доступно: `show`, `set_here`, `clear`.\n"
            "Пример: `/proposal_system_channel set_here` в нужном канале.",
            mention_author=False,
        )
    except Exception:
        logger.exception(
            "discord proposal system channel command failed actor_id=%s action=%s",
            getattr(getattr(ctx, "author", None), "id", None),
            action,
        )
        await ctx.reply("❌ Ошибка настройки канала. Подробности в логах.", mention_author=False)
