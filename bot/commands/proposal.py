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
from bot.services.authority_service import AuthorityService
from bot.services.council_feedback_service import CouncilFeedbackService
from bot.services.council_system_events_service import CouncilSystemEventsService
from bot.services.guiy_publish_destinations_service import GuiyPublishDestination, GuiyPublishDestinationsService
from bot.services.proposal_ui_texts import (
    ARCHIVE_PERIOD_LABELS,
    ARCHIVE_STATUS_LABELS,
    ARCHIVE_TYPE_LABELS,
    PROPOSAL_ADMIN_ACTION_BY_CODE,
    PROPOSAL_ADMIN_SECTION_BY_CODE,
    PROPOSAL_ADMIN_SECTIONS,
    render_admin_action_result,
    render_admin_confirm_text,
    render_admin_root_text,
    render_admin_section_text,
    render_archive_empty_text,
    render_archive_filters_text,
    render_archive_lines,
    render_confirmation_prompt,
    render_help_text,
    render_menu_action_explanations,
    render_menu_overview,
    render_status_text,
    render_submit_success_text,
)

logger = logging.getLogger(__name__)
_DISCORD_EVENTS_DESTINATIONS_PAGE_SIZE = 20


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
                + render_menu_action_explanations()
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

    @discord.ui.button(label="⚙️ Настройки Совета", style=discord.ButtonStyle.secondary)
    async def admin_settings(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        try:
            if not AuthorityService.is_super_admin("discord", str(interaction.user.id)):
                await interaction.response.send_message("❌ Действие доступно только суперадмину.", ephemeral=True)
                return
            view = ProposalAdminSettingsView(actor_id=self.actor_id)
            await interaction.response.send_message(embed=view.build_embed(), view=view, ephemeral=True)
        except Exception:
            logger.exception("discord proposal admin settings open failed actor_id=%s", getattr(interaction.user, "id", None))
            await interaction.response.send_message("❌ Не удалось открыть настройки Совета.", ephemeral=True)


class ProposalAdminSettingsView(discord.ui.View):
    def __init__(self, actor_id: int):
        super().__init__(timeout=300)
        self.actor_id = actor_id
        self.current_section_code: str | None = None
        self.pending_confirm_action_code: str | None = None
        self.events_picker_active: bool = False
        self.events_confirm_active: bool = False
        self.events_page: int = 0
        self.events_destinations: list[GuiyPublishDestination] = []
        self.events_selected_destination_id: str | None = None
        self.events_selected_label: str | None = None
        self._rebuild_items()

    def _rebuild_items(self) -> None:
        self.clear_items()
        if self.events_picker_active:
            select = _AdminEventsDestinationSelect(self)
            if not self.events_destinations:
                select.disabled = True
            self.add_item(select)
            page, total_pages, _items = self._events_page_items()
            if page > 0:
                self.add_item(_AdminEventsPageButton(self, label="⬅️", page_delta=-1))
            if page + 1 < total_pages:
                self.add_item(_AdminEventsPageButton(self, label="➡️", page_delta=1))
            self.add_item(_AdminEventsCancelButton(self))
            return
        if self.events_confirm_active:
            self.add_item(_AdminEventsSaveButton(self))
            self.add_item(_AdminEventsCancelButton(self))
            return
        if self.pending_confirm_action_code:
            self.add_item(_AdminConfirmExecuteButton(self))
            self.add_item(_AdminConfirmCancelButton(self))
            self.add_item(_AdminBackToRootButton(self))
            return
        if self.current_section_code:
            section = PROPOSAL_ADMIN_SECTION_BY_CODE.get(self.current_section_code)
            if section:
                for action in section.actions[:5]:
                    self.add_item(_AdminActionButton(self, action.code, action.title))
            self.add_item(_AdminBackToRootButton(self))
            return
        for section in PROPOSAL_ADMIN_SECTIONS[:5]:
            self.add_item(_AdminSectionButton(self, section.code, section.title))

    def _events_page_items(self) -> tuple[int, int, list[GuiyPublishDestination]]:
        total_pages = max((len(self.events_destinations) - 1) // _DISCORD_EVENTS_DESTINATIONS_PAGE_SIZE + 1, 1)
        self.events_page = min(max(self.events_page, 0), total_pages - 1)
        start = self.events_page * _DISCORD_EVENTS_DESTINATIONS_PAGE_SIZE
        return self.events_page, total_pages, self.events_destinations[start : start + _DISCORD_EVENTS_DESTINATIONS_PAGE_SIZE]

    def open_events_picker(self) -> None:
        self.events_destinations = GuiyPublishDestinationsService.list_discord_destinations(bot)
        self.events_picker_active = True
        self.events_confirm_active = False
        self.events_page = 0
        self.events_selected_destination_id = None
        self.events_selected_label = None
        self.pending_confirm_action_code = None
        self._rebuild_items()

    def close_events_picker(self) -> None:
        self.events_picker_active = False
        self.events_confirm_active = False
        self.events_page = 0
        self.events_destinations = []
        self.events_selected_destination_id = None
        self.events_selected_label = None
        self._rebuild_items()

    def build_embed(self, *, result_text: str | None = None) -> discord.Embed:
        if self.events_picker_active:
            page, total_pages, _items = self._events_page_items()
            if not self.events_destinations:
                description = (
                    "Сейчас нет доступных текстовых каналов, куда бот может отправлять сообщения.\n"
                    "Проверьте, что бот добавлен на сервер и имеет право отправки."
                )
            else:
                description = (
                    "Выберите канал из списка, куда бот будет отправлять системные события Совета.\n"
                    f"Страница: **{page + 1}/{total_pages}**"
                )
            return discord.Embed(title="⚙️ Канал и чат уведомлений", description=description, color=discord.Color.dark_gold())
        if self.events_confirm_active:
            description = (
                f"Вы выбрали: **{self.events_selected_label or '—'}**\n"
                "После сохранения системные события Совета будут отправляться сюда."
            )
            return discord.Embed(title="⚙️ Подтверждение канала", description=description, color=discord.Color.orange())
        if self.pending_confirm_action_code:
            description = render_admin_confirm_text(self.pending_confirm_action_code).replace("<b>", "**").replace("</b>", "**")
            return discord.Embed(title="⚠️ Подтверждение", description=description, color=discord.Color.orange())
        if self.current_section_code:
            section = PROPOSAL_ADMIN_SECTION_BY_CODE.get(self.current_section_code)
            title = f"⚙️ {section.title}" if section else "⚙️ Админ-меню Совета"
            description = render_admin_section_text(self.current_section_code).replace("<b>", "**").replace("</b>", "**")
            if result_text:
                description += f"\n\n{result_text}"
            return discord.Embed(title=title, description=description, color=discord.Color.dark_gold())
        description = render_admin_root_text().replace("<b>", "**").replace("</b>", "**")
        if result_text:
            description += f"\n\n{result_text}"
        return discord.Embed(title="⚙️ Админ-меню Совета", description=description, color=discord.Color.dark_gold())

    async def run_action(self, interaction: discord.Interaction, action_code: str) -> str:
        if action_code == "events_show_channel":
            current = CouncilSystemEventsService.get_channel("discord")
            CouncilSystemEventsService.record_admin_action(
                provider="discord",
                actor_user_id=str(self.actor_id),
                action=action_code,
                destination_id=str(getattr(interaction.channel, "id", "") or "") or None,
                target_object=current or None,
                status="success",
                reason="channel_shown",
            )
            status_text = (
                f"✅ Сейчас выбран канал `{current}` для системных уведомлений Совета."
                if current
                else "ℹ️ Канал системных уведомлений Совета пока не настроен."
            )
            return render_admin_action_result(action_code, custom_result=status_text)
        if action_code == "events_set_channel_here":
            self.open_events_picker()
            return ""
        if action_code == "events_clear_channel":
            result = CouncilSystemEventsService.set_channel(
                provider="discord",
                actor_user_id=str(self.actor_id),
                destination_id="",
            )
            CouncilSystemEventsService.record_admin_action(
                provider="discord",
                actor_user_id=str(self.actor_id),
                action=action_code,
                destination_id=None,
                target_object="system_event_channel",
                status="success" if result.get("ok") else "failed",
                reason=str(result.get("reason") or ("channel_cleared" if result.get("ok") else "clear_channel_failed")),
            )
            return render_admin_action_result(
                action_code,
                custom_result=str(result.get("message") or ("✅ Канал уведомлений очищен." if result.get("ok") else "❌ Не удалось очистить канал уведомлений.")),
            )
        CouncilSystemEventsService.record_admin_action(
            provider="discord",
            actor_user_id=str(self.actor_id),
            action=action_code,
            destination_id=str(getattr(interaction.channel, "id", "") or "") or None,
            target_object="proposal_admin_action",
            status="success",
            reason="executed",
        )
        return render_admin_action_result(action_code)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.actor_id:
            CouncilSystemEventsService.record_admin_action(
                provider="discord",
                actor_user_id=str(getattr(interaction.user, "id", "") or ""),
                action="admin_settings_interaction_check",
                destination_id=str(getattr(interaction.channel, "id", "") or "") or None,
                target_object="proposal_admin_menu",
                status="denied",
                reason="forbidden_not_owner",
            )
            await interaction.response.send_message("❌ Это меню открыто для другого пользователя.", ephemeral=True)
            return False
        if not AuthorityService.is_super_admin("discord", str(interaction.user.id)):
            CouncilSystemEventsService.record_admin_action(
                provider="discord",
                actor_user_id=str(getattr(interaction.user, "id", "") or ""),
                action="admin_settings_interaction_check",
                destination_id=str(getattr(interaction.channel, "id", "") or "") or None,
                target_object="proposal_admin_menu",
                status="denied",
                reason="forbidden",
            )
            await interaction.response.send_message("❌ Действие доступно только суперадмину.", ephemeral=True)
            return False
        return True


class _AdminSectionButton(discord.ui.Button["ProposalAdminSettingsView"]):
    def __init__(self, view: ProposalAdminSettingsView, section_code: str, title: str):
        super().__init__(label=f"📂 {title}", style=discord.ButtonStyle.secondary)
        self._owner_view = view
        self._section_code = section_code

    async def callback(self, interaction: discord.Interaction) -> None:
        self._owner_view.current_section_code = self._section_code
        self._owner_view.pending_confirm_action_code = None
        self._owner_view._rebuild_items()
        await interaction.response.edit_message(embed=self._owner_view.build_embed(), view=self._owner_view)


class _AdminActionButton(discord.ui.Button["ProposalAdminSettingsView"]):
    def __init__(self, view: ProposalAdminSettingsView, action_code: str, title: str):
        super().__init__(label=f"➡️ {title}", style=discord.ButtonStyle.primary)
        self._owner_view = view
        self._action_code = action_code

    async def callback(self, interaction: discord.Interaction) -> None:
        action = PROPOSAL_ADMIN_ACTION_BY_CODE.get(self._action_code)
        if not action:
            CouncilSystemEventsService.record_admin_action(
                provider="discord",
                actor_user_id=str(getattr(interaction.user, "id", "") or ""),
                action=self._action_code,
                destination_id=str(getattr(interaction.channel, "id", "") or "") or None,
                target_object="proposal_admin_action",
                status="denied",
                reason="validation_action_not_found",
            )
            await interaction.response.send_message("❌ Действие не найдено.", ephemeral=True)
            return
        if action.requires_confirmation:
            self._owner_view.pending_confirm_action_code = self._action_code
            self._owner_view._rebuild_items()
            await interaction.response.edit_message(embed=self._owner_view.build_embed(), view=self._owner_view)
            return
        try:
            result_text = await self._owner_view.run_action(interaction, self._action_code)
            self._owner_view.pending_confirm_action_code = None
            self._owner_view._rebuild_items()
            await interaction.response.edit_message(
                embed=self._owner_view.build_embed(result_text=result_text),
                view=self._owner_view,
            )
        except Exception:
            CouncilSystemEventsService.record_admin_action(
                provider="discord",
                actor_user_id=str(getattr(interaction.user, "id", "") or ""),
                action=self._action_code,
                destination_id=str(getattr(interaction.channel, "id", "") or "") or None,
                target_object="proposal_admin_action",
                status="failed",
                reason="unexpected_error",
            )
            logger.exception(
                "discord proposal admin action failed actor_id=%s action=%s",
                getattr(interaction.user, "id", None),
                self._action_code,
            )
            await interaction.response.send_message("❌ Не удалось выполнить действие. Попробуйте снова.", ephemeral=True)


class _AdminEventsDestinationSelect(discord.ui.Select):
    def __init__(self, view: ProposalAdminSettingsView):
        self._owner_view = view
        _page, _total_pages, items = view._events_page_items()
        options: list[discord.SelectOption] = []
        for item in items:
            options.append(
                discord.SelectOption(
                    label=item.title[:100],
                    description=item.subtitle[:100] if item.subtitle else None,
                    value=item.destination_id,
                )
            )
        if not options:
            options = [discord.SelectOption(label="Нет доступных каналов", value="__empty__", description="Проверьте права бота")]
        super().__init__(placeholder="Выберите канал", min_values=1, max_values=1, options=options, row=0)

    async def callback(self, interaction: discord.Interaction) -> None:
        selected_id = str(self.values[0] if self.values else "").strip()
        if selected_id == "__empty__":
            CouncilSystemEventsService.record_admin_action(
                provider="discord",
                actor_user_id=str(getattr(interaction.user, "id", "") or ""),
                action="events_choose",
                destination_id=None,
                target_object="system_event_channel",
                status="denied",
                reason="validation_no_destinations",
            )
            await interaction.response.send_message("❌ Сейчас нет доступных каналов для выбора.", ephemeral=True)
            return
        selected = next((item for item in self._owner_view.events_destinations if item.destination_id == selected_id), None)
        if selected is None:
            logger.warning(
                "discord proposal events destination no longer available actor_id=%s destination_id=%s",
                getattr(interaction.user, "id", None),
                selected_id,
            )
            await interaction.response.send_message("❌ Этот канал больше недоступен. Выберите другой.", ephemeral=True)
            return
        self._owner_view.events_selected_destination_id = selected.destination_id
        self._owner_view.events_selected_label = selected.display_label
        self._owner_view.events_picker_active = False
        self._owner_view.events_confirm_active = True
        self._owner_view._rebuild_items()
        await interaction.response.edit_message(embed=self._owner_view.build_embed(), view=self._owner_view)


class _AdminEventsPageButton(discord.ui.Button["ProposalAdminSettingsView"]):
    def __init__(self, view: ProposalAdminSettingsView, *, label: str, page_delta: int):
        super().__init__(label=label, style=discord.ButtonStyle.secondary, row=1)
        self._owner_view = view
        self._page_delta = page_delta

    async def callback(self, interaction: discord.Interaction) -> None:
        self._owner_view.events_page += self._page_delta
        self._owner_view._rebuild_items()
        await interaction.response.edit_message(embed=self._owner_view.build_embed(), view=self._owner_view)


class _AdminEventsSaveButton(discord.ui.Button["ProposalAdminSettingsView"]):
    def __init__(self, view: ProposalAdminSettingsView):
        super().__init__(label="✅ Сохранить", style=discord.ButtonStyle.success)
        self._owner_view = view

    async def callback(self, interaction: discord.Interaction) -> None:
        destination_id = str(self._owner_view.events_selected_destination_id or "").strip()
        if not destination_id:
            CouncilSystemEventsService.record_admin_action(
                provider="discord",
                actor_user_id=str(getattr(interaction.user, "id", "") or ""),
                action="events_save",
                destination_id=None,
                target_object="system_event_channel",
                status="denied",
                reason="validation_destination_not_selected",
            )
            await interaction.response.send_message("❌ Сначала выберите канал.", ephemeral=True)
            return
        result = CouncilSystemEventsService.set_channel(
            provider="discord",
            actor_user_id=str(self._owner_view.actor_id),
            destination_id=destination_id,
        )
        CouncilSystemEventsService.record_admin_action(
            provider="discord",
            actor_user_id=str(self._owner_view.actor_id),
            action="events_set_channel_here",
            destination_id=destination_id,
            target_object="system_event_channel",
            status="success" if result.get("ok") else "failed",
            reason=str(result.get("reason") or ("channel_saved" if result.get("ok") else "set_channel_failed")),
        )
        if not result.get("ok"):
            logger.error(
                "discord proposal events save failed actor_id=%s destination_id=%s message=%s",
                self._owner_view.actor_id,
                destination_id,
                result.get("message"),
            )
        self._owner_view.close_events_picker()
        await interaction.response.edit_message(
            embed=self._owner_view.build_embed(
                result_text=render_admin_action_result(
                    "events_set_channel_here",
                    custom_result=str(result.get("message") or ("✅ Канал уведомлений сохранён." if result.get("ok") else "❌ Не удалось сохранить канал уведомлений.")),
                )
            ),
            view=self._owner_view,
        )


class _AdminEventsCancelButton(discord.ui.Button["ProposalAdminSettingsView"]):
    def __init__(self, view: ProposalAdminSettingsView):
        super().__init__(label="↩️ Отмена", style=discord.ButtonStyle.secondary, row=1)
        self._owner_view = view

    async def callback(self, interaction: discord.Interaction) -> None:
        self._owner_view.close_events_picker()
        self._owner_view.current_section_code = "events"
        await interaction.response.edit_message(embed=self._owner_view.build_embed(), view=self._owner_view)


class _AdminConfirmExecuteButton(discord.ui.Button["ProposalAdminSettingsView"]):
    def __init__(self, view: ProposalAdminSettingsView):
        super().__init__(label="✅ Подтвердить", style=discord.ButtonStyle.danger)
        self._owner_view = view

    async def callback(self, interaction: discord.Interaction) -> None:
        action_code = self._owner_view.pending_confirm_action_code
        if not action_code:
            CouncilSystemEventsService.record_admin_action(
                provider="discord",
                actor_user_id=str(getattr(interaction.user, "id", "") or ""),
                action="admin_confirm",
                destination_id=str(getattr(interaction.channel, "id", "") or "") or None,
                target_object="proposal_admin_confirm",
                status="denied",
                reason="validation_confirmation_expired",
            )
            await interaction.response.send_message("❌ Подтверждение устарело.", ephemeral=True)
            return
        try:
            result_text = await self._owner_view.run_action(interaction, action_code)
            self._owner_view.pending_confirm_action_code = None
            self._owner_view._rebuild_items()
            await interaction.response.edit_message(
                embed=self._owner_view.build_embed(result_text=result_text),
                view=self._owner_view,
            )
        except Exception:
            CouncilSystemEventsService.record_admin_action(
                provider="discord",
                actor_user_id=str(getattr(interaction.user, "id", "") or ""),
                action=action_code,
                destination_id=str(getattr(interaction.channel, "id", "") or "") or None,
                target_object="proposal_admin_confirm",
                status="failed",
                reason="unexpected_error",
            )
            logger.exception(
                "discord proposal admin confirm failed actor_id=%s action=%s",
                getattr(interaction.user, "id", None),
                action_code,
            )
            await interaction.response.send_message("❌ Не удалось подтвердить действие. Попробуйте снова.", ephemeral=True)


class _AdminConfirmCancelButton(discord.ui.Button["ProposalAdminSettingsView"]):
    def __init__(self, view: ProposalAdminSettingsView):
        super().__init__(label="↩️ Отмена", style=discord.ButtonStyle.secondary)
        self._owner_view = view

    async def callback(self, interaction: discord.Interaction) -> None:
        self._owner_view.pending_confirm_action_code = None
        self._owner_view._rebuild_items()
        await interaction.response.edit_message(embed=self._owner_view.build_embed(), view=self._owner_view)


class _AdminBackToRootButton(discord.ui.Button["ProposalAdminSettingsView"]):
    def __init__(self, view: ProposalAdminSettingsView):
        super().__init__(label="↩️ К разделам", style=discord.ButtonStyle.secondary)
        self._owner_view = view

    async def callback(self, interaction: discord.Interaction) -> None:
        self._owner_view.current_section_code = None
        self._owner_view.pending_confirm_action_code = None
        self._owner_view._rebuild_items()
        await interaction.response.edit_message(embed=self._owner_view.build_embed(), view=self._owner_view)


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
