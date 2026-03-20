import logging

import discord

from bot.commands.base import bot
from bot.services import AccountsService
from bot.services.guiy_admin_service import (
    GUIY_OWNER_DENIED_MESSAGE,
    GUIY_OWNER_REPLY_REQUIRED_MESSAGE,
    GUIY_OWNER_USAGE_TEXT,
    parse_guiy_owner_profile_payload,
)
from bot.services.guiy_owner_flow_service import (
    GUIY_OWNER_ACTION_SPECS,
    GUIY_OWNER_PROFILE_FIELDS,
    execute_guiy_owner_flow,
    get_guiy_owner_action_spec,
    get_guiy_owner_profile_field_spec,
    parse_guiy_owner_text_command,
    resolve_guiy_profile_catalog,
)
from bot.services.guiy_publish_destinations_service import (
    GuiyPublishDestination,
    GuiyPublishDestinationsService,
)
from bot.utils import send_temp
from bot.utils.safe_view import SafeView

logger = logging.getLogger(__name__)
_DISCORD_DESTINATIONS_PAGE_SIZE = 20


def _persist_discord_identity(user: discord.abc.User | None) -> None:
    if not user or getattr(user, "bot", False):
        return
    AccountsService.persist_identity_lookup_fields(
        "discord",
        str(user.id),
        username=getattr(user, "name", None),
        display_name=getattr(user, "display_name", None),
        global_username=getattr(user, "global_name", None),
    )


def _log_guiy_owner_info(
    *,
    actor_user_id,
    selected_action: str,
    target_chat_or_guild,
    target_message_id,
    guiy_account_id,
    message: str,
) -> None:
    logger.info(
        "%s provider=%s actor_user_id=%s selected_action=%s target_chat_or_guild=%s target_message_id=%s guiy_account_id=%s",
        message,
        "discord",
        actor_user_id,
        selected_action,
        target_chat_or_guild,
        target_message_id,
        guiy_account_id,
    )


def _log_guiy_owner_warning(
    *,
    actor_user_id,
    selected_action: str,
    target_chat_or_guild,
    target_message_id,
    guiy_account_id,
    message: str,
) -> None:
    logger.warning(
        "%s provider=%s actor_user_id=%s selected_action=%s target_chat_or_guild=%s target_message_id=%s guiy_account_id=%s",
        message,
        "discord",
        actor_user_id,
        selected_action,
        target_chat_or_guild,
        target_message_id,
        guiy_account_id,
    )


async def _resolve_reply_message(ctx) -> discord.Message | None:
    reference = getattr(getattr(ctx, "message", None), "reference", None)
    if not reference or not reference.message_id or not getattr(ctx, "channel", None):
        return None
    resolved = getattr(reference, "resolved", None)
    if isinstance(resolved, discord.Message):
        return resolved
    try:
        return await ctx.channel.fetch_message(reference.message_id)
    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
        logger.exception(
            "discord guiy owner failed to fetch reply target provider=%s actor_user_id=%s selected_action=%s target_chat_or_guild=%s target_message_id=%s guiy_account_id=%s",
            "discord",
            getattr(ctx.author, "id", None),
            "reply",
            getattr(ctx.channel, "id", None),
            getattr(reference, "message_id", None),
            None,
        )
        return None


class GuiyOwnerVisibleRoleButton(discord.ui.Button):
    def __init__(self, role_name: str, selected: bool, index: int):
        super().__init__(
            label=(f"✅ {role_name}" if selected else role_name)[:80],
            style=discord.ButtonStyle.success if selected else discord.ButtonStyle.secondary,
            row=index // 2,
        )
        self.role_name = role_name

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if not isinstance(view, GuiyOwnerVisibleRolesView):
            await interaction.response.send_message("❌ Ошибка интерфейса выбора ролей.", ephemeral=True)
            return
        await view.toggle_role(interaction, self.role_name)


class GuiyOwnerVisibleRolesView(SafeView):
    def __init__(self, actor_id: int, bot_user_id: str, role_catalog: list[dict[str, str]], selected_roles: list[str], target_message_id: int | None):
        super().__init__(timeout=300)
        self.actor_id = actor_id
        self.bot_user_id = bot_user_id
        self.role_catalog = [item for item in role_catalog if str(item.get("role") or "").strip()]
        self.page = 0
        allowed_roles = {str(item.get("role") or "").strip() for item in self.role_catalog}
        self.selected_roles = [role_name for role_name in selected_roles if role_name in allowed_roles][
            : AccountsService.MAX_VISIBLE_PROFILE_ROLES
        ]
        self.target_message_id = target_message_id
        self._rebuild_buttons()

    def _get_page_items(self) -> tuple[int, int, list[dict[str, str]]]:
        total_pages = max((len(self.role_catalog) - 1) // 8 + 1, 1)
        self.page = min(max(self.page, 0), total_pages - 1)
        start = self.page * 8
        return self.page, total_pages, self.role_catalog[start : start + 8]

    def _content_text(self) -> str:
        page, total_pages, _ = self._get_page_items()
        selected_text = ", ".join(self.selected_roles) if self.selected_roles else "—"
        return (
            "🏅 Отображаемые роли\n"
            f"{GUIY_OWNER_PROFILE_FIELDS['visible_roles'].instruction}\n"
            "Нажимайте на роли ниже и затем подтвердите сохранение.\n"
            f"Страница: {page + 1}/{total_pages}\n"
            f"Выбрано ({len(self.selected_roles)}/{AccountsService.MAX_VISIBLE_PROFILE_ROLES}): {selected_text}"
        )

    def _rebuild_buttons(self) -> None:
        self.clear_items()
        page, total_pages, items = self._get_page_items()
        for idx, item in enumerate(items):
            role_name = str(item.get("role") or "").strip()
            self.add_item(GuiyOwnerVisibleRoleButton(role_name, role_name in self.selected_roles, idx))

        if page > 0:
            prev_button = discord.ui.Button(label="⬅️", style=discord.ButtonStyle.secondary, row=4)
            prev_button.callback = self._prev_callback
            self.add_item(prev_button)
        if page + 1 < total_pages:
            next_button = discord.ui.Button(label="➡️", style=discord.ButtonStyle.secondary, row=4)
            next_button.callback = self._next_callback
            self.add_item(next_button)
        save_button = discord.ui.Button(label="💾 Сохранить", style=discord.ButtonStyle.primary, row=4)
        save_button.callback = self._save_callback
        self.add_item(save_button)
        clear_button = discord.ui.Button(label="🧹 Очистить", style=discord.ButtonStyle.danger, row=4)
        clear_button.callback = self._clear_callback
        self.add_item(clear_button)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.actor_id:
            await interaction.response.send_message("❌ Это меню не для вас.", ephemeral=True)
            return False
        return True

    async def toggle_role(self, interaction: discord.Interaction, role_name: str) -> None:
        if role_name in self.selected_roles:
            self.selected_roles = [item for item in self.selected_roles if item != role_name]
        else:
            if len(self.selected_roles) >= AccountsService.MAX_VISIBLE_PROFILE_ROLES:
                await interaction.response.send_message(
                    f"❌ Можно выбрать не более {AccountsService.MAX_VISIBLE_PROFILE_ROLES} ролей.",
                    ephemeral=True,
                )
                return
            self.selected_roles.append(role_name)
        self._rebuild_buttons()
        await interaction.response.edit_message(content=self._content_text(), view=self)

    async def _prev_callback(self, interaction: discord.Interaction) -> None:
        self.page -= 1
        self._rebuild_buttons()
        await interaction.response.edit_message(content=self._content_text(), view=self)

    async def _next_callback(self, interaction: discord.Interaction) -> None:
        self.page += 1
        self._rebuild_buttons()
        await interaction.response.edit_message(content=self._content_text(), view=self)

    async def _save_callback(self, interaction: discord.Interaction):
        try:
            result = execute_guiy_owner_flow(
                provider="discord",
                actor_user_id=interaction.user.id,
                bot_user_id=self.bot_user_id,
                selected_action="profile_update",
                field_name="visible_roles",
                payload=", ".join(self.selected_roles),
                target_message_id=self.target_message_id,
            )
            _log_guiy_owner_info(
                actor_user_id=interaction.user.id,
                selected_action="profile_update",
                target_chat_or_guild=getattr(interaction.guild, "id", None) or getattr(interaction.channel, "id", None),
                target_message_id=self.target_message_id,
                guiy_account_id=result.guiy_account_id,
                message="discord guiy owner visible roles saved",
            )
            await interaction.response.edit_message(content=result.message, view=None)
        except Exception:
            logger.exception(
                "discord guiy owner visible roles save failed provider=%s actor_user_id=%s selected_action=%s target_chat_or_guild=%s target_message_id=%s guiy_account_id=%s",
                "discord",
                getattr(interaction.user, "id", None),
                "profile_update",
                getattr(interaction.guild, "id", None) or getattr(interaction.channel, "id", None),
                self.target_message_id,
                None,
            )
            await interaction.response.send_message("❌ Не удалось сохранить роли.", ephemeral=True)

    async def _clear_callback(self, interaction: discord.Interaction) -> None:
        self.selected_roles = []
        self._rebuild_buttons()
        await interaction.response.edit_message(content=self._content_text(), view=self)


class GuiyOwnerMessageModal(discord.ui.Modal):
    def __init__(
        self,
        *,
        actor_id: int,
        bot_user_id: str,
        selected_action: str,
        target_message_id: int | None,
        reply_author_user_id: str | None,
        target_destination_id: str | None = None,
        target_destination_label: str | None = None,
    ):
        title = get_guiy_owner_action_spec(selected_action).title if get_guiy_owner_action_spec(selected_action) else "Guiy Owner"
        super().__init__(title=title)
        self.actor_id = actor_id
        self.bot_user_id = bot_user_id
        self.selected_action = selected_action
        self.target_message_id = target_message_id
        self.reply_author_user_id = reply_author_user_id
        self.target_destination_id = target_destination_id
        self.target_destination_label = target_destination_label
        action_spec = get_guiy_owner_action_spec(selected_action)
        self.text_input = discord.ui.TextInput(
            label=action_spec.title if action_spec else "Текст",
            style=discord.TextStyle.paragraph,
            max_length=2000,
            placeholder=action_spec.instruction[:100] if action_spec else "Введите текст",
            required=True,
        )
        self.add_item(self.text_input)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            result = execute_guiy_owner_flow(
                provider="discord",
                actor_user_id=interaction.user.id,
                bot_user_id=self.bot_user_id,
                selected_action=self.selected_action,
                payload=str(self.text_input.value or "").strip(),
                reply_author_user_id=self.reply_author_user_id,
                target_message_id=self.target_message_id,
            )
            _log_guiy_owner_info(
                actor_user_id=interaction.user.id,
                selected_action=self.selected_action,
                target_chat_or_guild=getattr(interaction.guild, "id", None) or getattr(interaction.channel, "id", None),
                target_message_id=self.target_message_id,
                guiy_account_id=result.guiy_account_id,
                message="discord guiy owner message modal submitted",
            )
            if not result.ok:
                await interaction.response.send_message(result.message, ephemeral=True)
                return
            if self.selected_action == "say":
                is_writable, reason, channel, destination = GuiyPublishDestinationsService.discord_destination_is_writable(
                    interaction.client,
                    self.target_destination_id,
                )
                if not is_writable or channel is None:
                    _log_guiy_owner_warning(
                        actor_user_id=interaction.user.id,
                        selected_action=self.selected_action,
                        target_chat_or_guild=self.target_destination_id,
                        target_message_id=self.target_message_id,
                        guiy_account_id=result.guiy_account_id,
                        message=f"discord guiy owner send blocked reason={reason}",
                    )
                    await interaction.response.send_message(
                        "❌ Не удалось отправить сообщение от Гуя: канал больше недоступен или у бота нет прав писать туда.\n"
                        f"Выбранное место: {self.target_destination_label or getattr(destination, 'display_label', 'неизвестно')}.",
                        ephemeral=True,
                    )
                    return
                try:
                    await channel.send(result.outbound_text)
                except (discord.Forbidden, discord.NotFound, discord.HTTPException):
                    _log_guiy_owner_warning(
                        actor_user_id=interaction.user.id,
                        selected_action=self.selected_action,
                        target_chat_or_guild=self.target_destination_id,
                        target_message_id=self.target_message_id,
                        guiy_account_id=result.guiy_account_id,
                        message="discord guiy owner send failed after destination selection",
                    )
                    logger.exception(
                        "discord guiy owner send failed provider=%s actor_user_id=%s selected_action=%s target_chat_or_guild=%s target_message_id=%s guiy_account_id=%s",
                        "discord",
                        getattr(interaction.user, "id", None),
                        self.selected_action,
                        self.target_destination_id,
                        self.target_message_id,
                        result.guiy_account_id,
                    )
                    await interaction.response.send_message(
                        "❌ Не удалось отправить сообщение от Гуя: канал удалён, недоступен или у бота пропали права.\n"
                        f"Выбранное место: {self.target_destination_label or getattr(destination, 'display_label', 'неизвестно')}.",
                        ephemeral=True,
                    )
                    return
                await interaction.response.send_message(
                    "✅ Сообщение отправлено. "
                    f"Гуй отправил сообщение сюда: {self.target_destination_label or getattr(destination, 'display_label', 'неизвестно')}.",
                    ephemeral=True,
                )
                return
            if self.selected_action == "reply":
                target_message = None
                if interaction.channel and self.target_message_id:
                    target_message = await interaction.channel.fetch_message(int(self.target_message_id))
                if not target_message:
                    await interaction.response.send_message(GUIY_OWNER_REPLY_REQUIRED_MESSAGE, ephemeral=True)
                    return
                await target_message.reply(result.outbound_text, mention_author=False)
                await interaction.response.send_message(
                    "✅ Ответ отправлен. Что изменилось: Гуй ответил в выбранной ветке диалога.",
                    ephemeral=True,
                )
                return
            await interaction.response.send_message(result.message, ephemeral=True)
        except Exception:
            logger.exception(
                "discord guiy owner modal failed provider=%s actor_user_id=%s selected_action=%s target_chat_or_guild=%s target_message_id=%s guiy_account_id=%s",
                "discord",
                getattr(interaction.user, "id", None),
                self.selected_action,
                getattr(interaction.guild, "id", None) or getattr(interaction.channel, "id", None),
                self.target_message_id,
                None,
            )
            if interaction.response.is_done():
                await interaction.followup.send("❌ Не удалось выполнить действие. Попробуйте позже.", ephemeral=True)
            else:
                await interaction.response.send_message("❌ Не удалось выполнить действие. Попробуйте позже.", ephemeral=True)


class GuiyOwnerDestinationSelect(discord.ui.Select):
    def __init__(self, view: "GuiyOwnerDestinationView"):
        self.destination_view = view
        options = self.destination_view.build_options()
        super().__init__(
            placeholder="Выберите канал для публикации",
            min_values=1,
            max_values=1,
            options=options,
            row=0,
        )

    async def callback(self, interaction: discord.Interaction):
        await self.destination_view.select_destination(interaction, self.values[0])


class GuiyOwnerDestinationView(SafeView):
    def __init__(
        self,
        *,
        actor_id: int,
        bot_user_id: str,
        bot_client,
        target_message_id: int | None,
        reply_author_user_id: str | None,
    ):
        super().__init__(timeout=300)
        self.actor_id = actor_id
        self.bot_user_id = bot_user_id
        self.bot_client = bot_client
        self.target_message_id = target_message_id
        self.reply_author_user_id = reply_author_user_id
        self.destinations = GuiyPublishDestinationsService.list_discord_destinations(bot_client)
        self.page = 0
        self.selected_destination_id: str | None = None
        self.selected_destination_label: str | None = None
        self._rebuild_items()

    def _get_page_items(self) -> tuple[int, int, list[GuiyPublishDestination]]:
        total_pages = max((len(self.destinations) - 1) // _DISCORD_DESTINATIONS_PAGE_SIZE + 1, 1)
        self.page = min(max(self.page, 0), total_pages - 1)
        start = self.page * _DISCORD_DESTINATIONS_PAGE_SIZE
        return self.page, total_pages, self.destinations[start : start + _DISCORD_DESTINATIONS_PAGE_SIZE]

    def build_options(self) -> list[discord.SelectOption]:
        _page, _total_pages, items = self._get_page_items()
        options: list[discord.SelectOption] = []
        for item in items:
            options.append(
                discord.SelectOption(
                    label=item.title[:100],
                    description=item.subtitle[:100] if item.subtitle else None,
                    value=item.destination_id,
                    default=item.destination_id == self.selected_destination_id,
                )
            )
        if not options:
            options.append(
                discord.SelectOption(
                    label="Нет доступных каналов",
                    description="Проверьте права бота и повторите позже",
                    value="__empty__",
                )
            )
        return options

    def content_text(self) -> str:
        _page, total_pages, _items = self._get_page_items()
        if not self.destinations:
            return (
                "📍 **Куда писать?**\n"
                "Сейчас у бота нет ни одного доступного канала для отправки. "
                "Проверьте, что бот состоит в нужном сервере и имеет право писать в текстовые каналы."
            )
        if self.selected_destination_label:
            return (
                "📍 **Куда писать?**\n"
                f"Гуй отправит сообщение сюда: **{self.selected_destination_label}**.\n"
                f"Страница: **{self.page + 1}/{total_pages}**\n"
                "Нажмите **Ввести текст**, чтобы открыть форму отправки."
            )
        return (
            "📍 **Куда писать?**\n"
            "Выберите канал, куда Гуй отправит новое сообщение. "
            f"Страница: **{self.page + 1}/{total_pages}**"
        )

    def _rebuild_items(self) -> None:
        self.clear_items()
        select = GuiyOwnerDestinationSelect(self)
        if not self.destinations:
            select.disabled = True
        self.add_item(select)
        page, total_pages, _items = self._get_page_items()
        if page > 0:
            prev_button = discord.ui.Button(label="⬅️", style=discord.ButtonStyle.secondary, row=1)
            prev_button.callback = self._prev_callback
            self.add_item(prev_button)
        if page + 1 < total_pages:
            next_button = discord.ui.Button(label="➡️", style=discord.ButtonStyle.secondary, row=1)
            next_button.callback = self._next_callback
            self.add_item(next_button)
        confirm_button = discord.ui.Button(label="✍️ Ввести текст", style=discord.ButtonStyle.primary, row=1)
        confirm_button.disabled = not bool(self.selected_destination_id)
        confirm_button.callback = self._confirm_callback
        self.add_item(confirm_button)
        cancel_button = discord.ui.Button(label="Отмена", style=discord.ButtonStyle.danger, row=1)
        cancel_button.callback = self._cancel_callback
        self.add_item(cancel_button)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.actor_id:
            await interaction.response.send_message("❌ Это меню owner-управления не для вас.", ephemeral=True)
            return False
        return True

    async def select_destination(self, interaction: discord.Interaction, destination_id: str) -> None:
        if destination_id == "__empty__":
            await interaction.response.send_message("❌ Нет доступных каналов для выбора.", ephemeral=True)
            return
        selected = next((item for item in self.destinations if item.destination_id == destination_id), None)
        if selected is None:
            _log_guiy_owner_warning(
                actor_user_id=interaction.user.id,
                selected_action="say",
                target_chat_or_guild=destination_id,
                target_message_id=self.target_message_id,
                guiy_account_id=None,
                message="discord guiy owner selected destination disappeared",
            )
            await interaction.response.send_message("❌ Этот канал больше недоступен. Выберите другой.", ephemeral=True)
            return
        self.selected_destination_id = selected.destination_id
        self.selected_destination_label = selected.display_label
        _log_guiy_owner_info(
            actor_user_id=interaction.user.id,
            selected_action="say",
            target_chat_or_guild=selected.destination_id,
            target_message_id=self.target_message_id,
            guiy_account_id=None,
            message="discord guiy owner destination selected",
        )
        self._rebuild_items()
        await interaction.response.edit_message(content=self.content_text(), view=self)

    async def _prev_callback(self, interaction: discord.Interaction) -> None:
        self.page -= 1
        self._rebuild_items()
        await interaction.response.edit_message(content=self.content_text(), view=self)

    async def _next_callback(self, interaction: discord.Interaction) -> None:
        self.page += 1
        self._rebuild_items()
        await interaction.response.edit_message(content=self.content_text(), view=self)

    async def _confirm_callback(self, interaction: discord.Interaction) -> None:
        if not self.selected_destination_id:
            await interaction.response.send_message("❌ Сначала выберите канал.", ephemeral=True)
            return
        await interaction.response.send_modal(
            GuiyOwnerMessageModal(
                actor_id=self.actor_id,
                bot_user_id=self.bot_user_id,
                selected_action="say",
                target_message_id=self.target_message_id,
                reply_author_user_id=self.reply_author_user_id,
                target_destination_id=self.selected_destination_id,
                target_destination_label=self.selected_destination_label,
            )
        )

    async def _cancel_callback(self, interaction: discord.Interaction) -> None:
        _log_guiy_owner_info(
            actor_user_id=interaction.user.id,
            selected_action="cancel",
            target_chat_or_guild=self.selected_destination_id,
            target_message_id=self.target_message_id,
            guiy_account_id=None,
            message="discord guiy owner destination picker canceled",
        )
        await interaction.response.edit_message(
            content="✅ Owner-сценарий отменён. Ничего не изменилось, меню можно открыть снова командой /guiy_owner.",
            view=None,
        )


class GuiyOwnerProfileFieldModal(discord.ui.Modal):
    def __init__(self, *, actor_id: int, bot_user_id: str, field_name: str, target_message_id: int | None):
        field_spec = get_guiy_owner_profile_field_spec(field_name)
        super().__init__(title=field_spec.title if field_spec else "Профиль Гуя")
        self.actor_id = actor_id
        self.bot_user_id = bot_user_id
        self.field_name = field_name
        self.target_message_id = target_message_id
        self.value_input = discord.ui.TextInput(
            label=field_spec.title if field_spec else "Значение",
            style=discord.TextStyle.paragraph,
            max_length=field_spec.max_length if field_spec else 255,
            required=False,
            placeholder=(field_spec.instruction[:100] if field_spec else "Введите значение") + " | Для очистки оставьте пусто.",
        )
        self.add_item(self.value_input)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            value = str(self.value_input.value or "").strip()
            result = execute_guiy_owner_flow(
                provider="discord",
                actor_user_id=interaction.user.id,
                bot_user_id=self.bot_user_id,
                selected_action="profile_update",
                field_name=self.field_name,
                payload=value,
                target_message_id=self.target_message_id,
            )
            _log_guiy_owner_info(
                actor_user_id=interaction.user.id,
                selected_action="profile_update",
                target_chat_or_guild=getattr(interaction.guild, "id", None) or getattr(interaction.channel, "id", None),
                target_message_id=self.target_message_id,
                guiy_account_id=result.guiy_account_id,
                message="discord guiy owner profile modal submitted",
            )
            await interaction.response.send_message(result.message, ephemeral=True)
        except Exception:
            logger.exception(
                "discord guiy owner profile modal failed provider=%s actor_user_id=%s selected_action=%s target_chat_or_guild=%s target_message_id=%s guiy_account_id=%s",
                "discord",
                getattr(interaction.user, "id", None),
                "profile_update",
                getattr(interaction.guild, "id", None) or getattr(interaction.channel, "id", None),
                self.target_message_id,
                None,
            )
            if interaction.response.is_done():
                await interaction.followup.send("❌ Не удалось обновить профиль Гуя.", ephemeral=True)
            else:
                await interaction.response.send_message("❌ Не удалось обновить профиль Гуя.", ephemeral=True)


class GuiyOwnerProfileView(SafeView):
    def __init__(self, *, actor_id: int, bot_user_id: str, target_message_id: int | None):
        super().__init__(timeout=300)
        self.actor_id = actor_id
        self.bot_user_id = bot_user_id
        self.target_message_id = target_message_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.actor_id:
            await interaction.response.send_message("❌ Это меню owner-управления не для вас.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Никнейм", style=discord.ButtonStyle.primary)
    async def nickname(self, interaction: discord.Interaction, _button: discord.ui.Button):
        _log_guiy_owner_info(
            actor_user_id=interaction.user.id,
            selected_action="profile_update",
            target_chat_or_guild=getattr(interaction.guild, "id", None) or getattr(interaction.channel, "id", None),
            target_message_id=self.target_message_id,
            guiy_account_id=None,
            message="discord guiy owner profile field opened field=custom_nick",
        )
        await interaction.response.send_modal(
            GuiyOwnerProfileFieldModal(
                actor_id=self.actor_id,
                bot_user_id=self.bot_user_id,
                field_name="custom_nick",
                target_message_id=self.target_message_id,
            )
        )

    @discord.ui.button(label="Описание", style=discord.ButtonStyle.secondary)
    async def description(self, interaction: discord.Interaction, _button: discord.ui.Button):
        _log_guiy_owner_info(
            actor_user_id=interaction.user.id,
            selected_action="profile_update",
            target_chat_or_guild=getattr(interaction.guild, "id", None) or getattr(interaction.channel, "id", None),
            target_message_id=self.target_message_id,
            guiy_account_id=None,
            message="discord guiy owner profile field opened field=description",
        )
        await interaction.response.send_modal(
            GuiyOwnerProfileFieldModal(
                actor_id=self.actor_id,
                bot_user_id=self.bot_user_id,
                field_name="description",
                target_message_id=self.target_message_id,
            )
        )

    @discord.ui.button(label="Null's ID", style=discord.ButtonStyle.secondary)
    async def nulls_id(self, interaction: discord.Interaction, _button: discord.ui.Button):
        _log_guiy_owner_info(
            actor_user_id=interaction.user.id,
            selected_action="profile_update",
            target_chat_or_guild=getattr(interaction.guild, "id", None) or getattr(interaction.channel, "id", None),
            target_message_id=self.target_message_id,
            guiy_account_id=None,
            message="discord guiy owner profile field opened field=nulls_brawl_id",
        )
        await interaction.response.send_modal(
            GuiyOwnerProfileFieldModal(
                actor_id=self.actor_id,
                bot_user_id=self.bot_user_id,
                field_name="nulls_brawl_id",
                target_message_id=self.target_message_id,
            )
        )

    @discord.ui.button(label="Отображаемые роли", style=discord.ButtonStyle.secondary)
    async def visible_roles(self, interaction: discord.Interaction, _button: discord.ui.Button):
        try:
            profile, catalog, selected_roles = resolve_guiy_profile_catalog(
                provider="discord",
                bot_user_id=self.bot_user_id,
                display_name=getattr(getattr(interaction.client, "user", None), "display_name", None),
            )
            guiy_account_id = profile.get("account_id") if isinstance(profile, dict) else None
            if not catalog:
                _log_guiy_owner_warning(
                    actor_user_id=interaction.user.id,
                    selected_action="profile_update",
                    target_chat_or_guild=getattr(interaction.guild, "id", None) or getattr(interaction.channel, "id", None),
                    target_message_id=self.target_message_id,
                    guiy_account_id=guiy_account_id,
                    message="discord guiy owner visible roles catalog is empty",
                )
                await interaction.response.send_message(
                    "❌ Для Гуя пока нет доступных ролей. Сначала зарегистрируйте профиль и проверьте /profile_roles.",
                    ephemeral=True,
                )
                return
            view = GuiyOwnerVisibleRolesView(self.actor_id, str(self.bot_user_id), catalog, selected_roles, self.target_message_id)
            await interaction.response.send_message(view._content_text(), view=view, ephemeral=True)
        except Exception:
            logger.exception(
                "discord guiy owner visible roles open failed provider=%s actor_user_id=%s selected_action=%s target_chat_or_guild=%s target_message_id=%s guiy_account_id=%s",
                "discord",
                getattr(interaction.user, "id", None),
                "profile_update",
                getattr(interaction.guild, "id", None) or getattr(interaction.channel, "id", None),
                self.target_message_id,
                None,
            )
            await interaction.response.send_message("❌ Не удалось открыть выбор ролей.", ephemeral=True)


class GuiyOwnerActionsView(SafeView):
    def __init__(self, *, actor_id: int, bot_user_id: str, target_message_id: int | None, reply_author_user_id: str | None):
        super().__init__(timeout=300)
        self.actor_id = actor_id
        self.bot_user_id = bot_user_id
        self.target_message_id = target_message_id
        self.reply_author_user_id = reply_author_user_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.actor_id:
            await interaction.response.send_message("❌ Это меню owner-управления не для вас.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Написать от Гуя", style=discord.ButtonStyle.primary)
    async def say(self, interaction: discord.Interaction, _button: discord.ui.Button):
        _log_guiy_owner_info(
            actor_user_id=interaction.user.id,
            selected_action="say",
            target_chat_or_guild=getattr(interaction.guild, "id", None) or getattr(interaction.channel, "id", None),
            target_message_id=self.target_message_id,
            guiy_account_id=None,
            message="discord guiy owner destination picker opened",
        )
        view = GuiyOwnerDestinationView(
            actor_id=self.actor_id,
            bot_user_id=self.bot_user_id,
            bot_client=interaction.client,
            target_message_id=self.target_message_id,
            reply_author_user_id=self.reply_author_user_id,
        )
        if not view.destinations:
            _log_guiy_owner_warning(
                actor_user_id=interaction.user.id,
                selected_action="say",
                target_chat_or_guild=getattr(interaction.guild, "id", None) or getattr(interaction.channel, "id", None),
                target_message_id=self.target_message_id,
                guiy_account_id=None,
                message="discord guiy owner destination list is empty",
            )
        await interaction.response.send_message(view.content_text(), view=view, ephemeral=True)

    @discord.ui.button(label="Ответить от Гуя", style=discord.ButtonStyle.primary)
    async def reply(self, interaction: discord.Interaction, _button: discord.ui.Button):
        spec = GUIY_OWNER_ACTION_SPECS["reply"]
        if self.target_message_id is None:
            _log_guiy_owner_warning(
                actor_user_id=interaction.user.id,
                selected_action="reply",
                target_chat_or_guild=getattr(interaction.guild, "id", None) or getattr(interaction.channel, "id", None),
                target_message_id=self.target_message_id,
                guiy_account_id=None,
                message="discord guiy owner reply requested without reply context",
            )
            await interaction.response.send_message(
                f"ℹ️ {spec.title}\n{spec.instruction}\n\nСейчас ничего не изменится: запустите /guiy_owner ответом на сообщение Гуя и повторите действие.",
                ephemeral=True,
            )
            return
        _log_guiy_owner_info(
            actor_user_id=interaction.user.id,
            selected_action="reply",
            target_chat_or_guild=getattr(interaction.guild, "id", None) or getattr(interaction.channel, "id", None),
            target_message_id=self.target_message_id,
            guiy_account_id=None,
            message="discord guiy owner action selected",
        )
        await interaction.response.send_modal(
            GuiyOwnerMessageModal(
                actor_id=self.actor_id,
                bot_user_id=self.bot_user_id,
                selected_action="reply",
                target_message_id=self.target_message_id,
                reply_author_user_id=self.reply_author_user_id,
            )
        )

    @discord.ui.button(label="Профиль Гуя", style=discord.ButtonStyle.secondary)
    async def profile(self, interaction: discord.Interaction, _button: discord.ui.Button):
        _log_guiy_owner_info(
            actor_user_id=interaction.user.id,
            selected_action="profile",
            target_chat_or_guild=getattr(interaction.guild, "id", None) or getattr(interaction.channel, "id", None),
            target_message_id=self.target_message_id,
            guiy_account_id=None,
            message="discord guiy owner profile menu opened",
        )
        embed = discord.Embed(
            title="Профиль Гуя",
            description=(
                f"{GUIY_OWNER_ACTION_SPECS['profile'].instruction}\n\n"
                "Выберите поле ниже. Для текстовых полей откроется modal, а для ролей — picker."
            ),
            color=discord.Color.blurple(),
        )
        await interaction.response.send_message(
            embed=embed,
            view=GuiyOwnerProfileView(actor_id=self.actor_id, bot_user_id=self.bot_user_id, target_message_id=self.target_message_id),
            ephemeral=True,
        )

    @discord.ui.button(label="Зарегистрировать профиль Гуя", style=discord.ButtonStyle.success)
    async def register(self, interaction: discord.Interaction, _button: discord.ui.Button):
        try:
            result = execute_guiy_owner_flow(
                provider="discord",
                actor_user_id=interaction.user.id,
                bot_user_id=self.bot_user_id,
                selected_action="register_profile",
                target_message_id=self.target_message_id,
                reply_author_user_id=self.reply_author_user_id,
            )
            _log_guiy_owner_info(
                actor_user_id=interaction.user.id,
                selected_action="register_profile",
                target_chat_or_guild=getattr(interaction.guild, "id", None) or getattr(interaction.channel, "id", None),
                target_message_id=self.target_message_id,
                guiy_account_id=result.guiy_account_id,
                message="discord guiy owner register action handled",
            )
            await interaction.response.send_message(result.message, ephemeral=True)
        except Exception:
            logger.exception(
                "discord guiy owner register failed provider=%s actor_user_id=%s selected_action=%s target_chat_or_guild=%s target_message_id=%s guiy_account_id=%s",
                "discord",
                getattr(interaction.user, "id", None),
                "register_profile",
                getattr(interaction.guild, "id", None) or getattr(interaction.channel, "id", None),
                self.target_message_id,
                None,
            )
            await interaction.response.send_message("❌ Не удалось зарегистрировать профиль Гуя.", ephemeral=True)

    @discord.ui.button(label="Отмена", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, _button: discord.ui.Button):
        _log_guiy_owner_info(
            actor_user_id=interaction.user.id,
            selected_action="cancel",
            target_chat_or_guild=getattr(interaction.guild, "id", None) or getattr(interaction.channel, "id", None),
            target_message_id=self.target_message_id,
            guiy_account_id=None,
            message="discord guiy owner flow canceled",
        )
        await interaction.response.send_message(
            "✅ Owner-сценарий отменён. Ничего не изменилось, меню можно открыть снова командой /guiy_owner.",
            ephemeral=True,
        )


async def _run_text_fallback(ctx, action: str, payload: str):
    reply_message = await _resolve_reply_message(ctx)
    _persist_discord_identity(reply_message.author if reply_message else None)
    target_message_id = getattr(reply_message, "id", None)
    reply_author_user_id = getattr(getattr(reply_message, "author", None), "id", None)

    bot_user = getattr(ctx.bot, "user", None)
    bot_user_id = getattr(bot_user, "id", None)
    if action == "profile":
        field_name, field_value = parse_guiy_owner_profile_payload(payload)
        if not field_name:
            await send_temp(ctx, GUIY_OWNER_USAGE_TEXT, delete_after=None)
            return
        result = execute_guiy_owner_flow(
            provider="discord",
            actor_user_id=getattr(ctx.author, "id", None),
            bot_user_id=bot_user_id,
            selected_action="profile_update",
            field_name=field_name,
            payload=field_value or "",
            target_message_id=target_message_id,
            reply_author_user_id=reply_author_user_id,
        )
    else:
        result = execute_guiy_owner_flow(
            provider="discord",
            actor_user_id=getattr(ctx.author, "id", None),
            bot_user_id=bot_user_id,
            selected_action=action,
            payload=payload,
            target_message_id=target_message_id,
            reply_author_user_id=reply_author_user_id,
        )

    _log_guiy_owner_info(
        actor_user_id=getattr(ctx.author, "id", None),
        selected_action=action,
        target_chat_or_guild=getattr(ctx.guild, "id", None) or getattr(ctx.channel, "id", None),
        target_message_id=target_message_id,
        guiy_account_id=result.guiy_account_id,
        message="discord guiy owner fallback handled",
    )
    if not result.ok:
        await send_temp(ctx, result.message, delete_after=None)
        return
    if action == "say":
        await send_temp(ctx, result.outbound_text, delete_after=None)
        return
    if action == "reply" and reply_message is not None:
        await reply_message.reply(result.outbound_text, mention_author=False)
        return
    await send_temp(ctx, result.message, delete_after=None)


@bot.command(name="guiy_owner", hidden=True)
async def guiy_owner(ctx, action: str = "", *, payload: str = ""):
    _persist_discord_identity(ctx.author)
    requested_action, requested_payload = parse_guiy_owner_text_command(f"{action} {payload}" if action else payload)
    if action:
        requested_action = str(action or "").strip().lower()
        requested_payload = str(payload or "").strip()

    if requested_action in {"say", "reply", "profile"}:
        await _run_text_fallback(ctx, requested_action, requested_payload)
        return

    reply_message = await _resolve_reply_message(ctx)
    _persist_discord_identity(reply_message.author if reply_message else None)
    embed = discord.Embed(
        title="Owner-управление Гуем",
        description=(
            "Выберите действие кнопками ниже. После каждого выбора бот коротко объяснит следующий шаг и что изменится после подтверждения.\n\n"
            f"• {GUIY_OWNER_ACTION_SPECS['say'].title} — выбрать канал и отправить туда новое сообщение от лица Гуя.\n"
            f"• {GUIY_OWNER_ACTION_SPECS['reply'].title} — ответить от лица Гуя на выбранное сообщение.\n"
            f"• {GUIY_OWNER_ACTION_SPECS['profile'].title} — открыть поля профиля Гуя.\n"
            f"• {GUIY_OWNER_ACTION_SPECS['register_profile'].title} — создать профиль Гуя, если его ещё нет."
        ),
        color=discord.Color.blue(),
    )
    await send_temp(
        ctx,
        embed=embed,
        view=GuiyOwnerActionsView(
            actor_id=ctx.author.id,
            bot_user_id=str(getattr(getattr(ctx.bot, 'user', None), 'id', '')),
            target_message_id=getattr(reply_message, "id", None),
            reply_author_user_id=str(getattr(getattr(reply_message, "author", None), "id", "")) or None,
        ),
        delete_after=None,
    )
