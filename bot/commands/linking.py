import logging

import discord

from bot.commands.base import bot
from bot.services import AccountsService
from bot.systems.linking_logic import (
    consume_discord_link_code,
    issue_discord_telegram_link_code,
    register_discord_account,
)
from bot.utils import send_temp

logger = logging.getLogger(__name__)
MAX_ROLE_PICKER_PAGE_SIZE = 8


def _is_private_context(ctx) -> bool:
    return getattr(ctx, "guild", None) is None


class ProfileEditModal(discord.ui.Modal):
    def __init__(
        self,
        field_name: str,
        title_text: str,
        placeholder: str,
        max_length: int,
        default_value: str | None = None,
    ):
        super().__init__(title=f"Изменить: {title_text}")
        self.field_name = field_name
        self.field_label = title_text
        self.value_input = discord.ui.TextInput(
            label=title_text,
            style=discord.TextStyle.paragraph,
            max_length=max_length,
            required=False,
            placeholder=placeholder,
            default=default_value,
        )
        self.add_item(self.value_input)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            value = str(self.value_input.value or "").strip()
            success, payload = AccountsService.update_profile_field(
                "discord",
                str(interaction.user.id),
                self.field_name,
                value,
            )
            prefix = "✅" if success else "❌"
            await interaction.response.send_message(f"{prefix} {payload}", ephemeral=True)
        except Exception:
            logger.exception(
                "discord profile edit modal submit failed user_id=%s field=%s",
                getattr(interaction.user, "id", None),
                self.field_name,
            )
            if interaction.response.is_done():
                await interaction.followup.send("❌ Не удалось сохранить изменения профиля.", ephemeral=True)
            else:
                await interaction.response.send_message("❌ Не удалось сохранить изменения профиля.", ephemeral=True)



class VisibleRoleToggleButton(discord.ui.Button):
    def __init__(self, role_name: str, category_name: str, selected: bool, index: int):
        super().__init__(
            label=(f"✅ {role_name}" if selected else role_name)[:80],
            style=discord.ButtonStyle.success if selected else discord.ButtonStyle.secondary,
            row=index // 2,
        )
        self.role_name = role_name
        self.category_name = category_name

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if not isinstance(view, VisibleRolesPickerView):
            await interaction.response.send_message("❌ Ошибка интерфейса выбора ролей.", ephemeral=True)
            return
        await view.toggle_role(interaction, self.role_name)


class VisibleRolesPickerView(discord.ui.View):
    def __init__(self, user_id: int, role_catalog: list[dict[str, str]], selected_roles: list[str]):
        super().__init__(timeout=300)
        self.user_id = user_id
        self.role_catalog = [item for item in role_catalog if str(item.get("role") or "").strip()]
        self.page = 0
        allowed_roles = {str(item.get("role") or "").strip() for item in self.role_catalog}
        self.selected_roles = [
            role_name for role_name in selected_roles if role_name in allowed_roles
        ][: AccountsService.MAX_VISIBLE_PROFILE_ROLES]
        self._rebuild_buttons()

    def _get_page_items(self) -> tuple[int, int, list[dict[str, str]]]:
        total_pages = max((len(self.role_catalog) - 1) // MAX_ROLE_PICKER_PAGE_SIZE + 1, 1)
        self.page = min(max(self.page, 0), total_pages - 1)
        start = self.page * MAX_ROLE_PICKER_PAGE_SIZE
        return self.page, total_pages, self.role_catalog[start : start + MAX_ROLE_PICKER_PAGE_SIZE]

    def _content_text(self) -> str:
        page, total_pages, _ = self._get_page_items()
        selected_text = ", ".join(self.selected_roles) if self.selected_roles else "—"
        return (
            "🏅 Выбор отображаемых ролей\n"
            "Роли отсортированы по категориям. Листайте страницы и выбирайте до 3 ролей.\n"
            f"Страница: {page + 1}/{total_pages}\n"
            f"Выбрано ({len(self.selected_roles)}/{AccountsService.MAX_VISIBLE_PROFILE_ROLES}): {selected_text}"
        )


    def _rebuild_buttons(self):
        self.clear_items()
        page, total_pages, page_items = self._get_page_items()
        for idx, item in enumerate(page_items):
            role_name = str(item.get("role") or "").strip()
            category_name = str(item.get("category") or "Без категории").strip() or "Без категории"
            self.add_item(VisibleRoleToggleButton(role_name, category_name, role_name in self.selected_roles, idx))

        if page > 0:
            prev_button = discord.ui.Button(label="⬅️", style=discord.ButtonStyle.secondary, custom_id="visible_roles_prev", row=4)
            prev_button.callback = self._prev_callback
            self.add_item(prev_button)

        if page + 1 < total_pages:
            next_button = discord.ui.Button(label="➡️", style=discord.ButtonStyle.secondary, custom_id="visible_roles_next", row=4)
            next_button.callback = self._next_callback
            self.add_item(next_button)

        save_button = discord.ui.Button(label="💾 Сохранить", style=discord.ButtonStyle.primary, custom_id="visible_roles_save", row=4)
        save_button.callback = self._save_callback
        self.add_item(save_button)
        clear_button = discord.ui.Button(label="🧹 Очистить", style=discord.ButtonStyle.danger, custom_id="visible_roles_clear", row=4)
        clear_button.callback = self._clear_callback
        self.add_item(clear_button)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ Это меню не для вас.", ephemeral=True)
            return False
        return True

    async def toggle_role(self, interaction: discord.Interaction, role_name: str):
        try:
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
        except Exception:
            logger.exception("discord visible role toggle failed user_id=%s role=%s", interaction.user.id, role_name)
            await interaction.response.send_message("❌ Не удалось переключить роль.", ephemeral=True)

    async def _prev_callback(self, interaction: discord.Interaction):
        self.page -= 1
        self._rebuild_buttons()
        await interaction.response.edit_message(content=self._content_text(), view=self)

    async def _next_callback(self, interaction: discord.Interaction):
        self.page += 1
        self._rebuild_buttons()
        await interaction.response.edit_message(content=self._content_text(), view=self)

    async def _save_callback(self, interaction: discord.Interaction):
        try:
            value = ", ".join(self.selected_roles)
            success, payload = AccountsService.update_profile_field("discord", str(interaction.user.id), "visible_roles", value)
            prefix = "✅" if success else "❌"
            await interaction.response.edit_message(content=f"{prefix} {payload}", view=None)
        except Exception:
            logger.exception("discord visible roles save failed user_id=%s", interaction.user.id)
            await interaction.response.send_message("❌ Не удалось сохранить роли.", ephemeral=True)

    async def _clear_callback(self, interaction: discord.Interaction):
        try:
            self.selected_roles = []
            self._rebuild_buttons()
            await interaction.response.edit_message(content=self._content_text(), view=self)
        except Exception:
            logger.exception("discord visible roles clear failed user_id=%s", interaction.user.id)
            await interaction.response.send_message("❌ Не удалось очистить выбор.", ephemeral=True)


class ProfileEditView(discord.ui.View):
    def __init__(self, user_id: int):
        super().__init__(timeout=300)
        self.user_id = user_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ Редактировать можно только свой профиль.", ephemeral=True)
            return False
        if interaction.guild is not None:
            await interaction.response.send_message("❌ Редактирование профиля доступно только в ЛС с ботом.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="✏️ Никнейм", style=discord.ButtonStyle.primary)
    async def edit_nickname(self, interaction: discord.Interaction, _button: discord.ui.Button):
        await interaction.response.send_modal(
            ProfileEditModal("custom_nick", "Никнейм", "Например: Bebra Hero", max_length=32)
        )

    @discord.ui.button(label="📝 Описание", style=discord.ButtonStyle.secondary)
    async def edit_description(self, interaction: discord.Interaction, _button: discord.ui.Button):
        await interaction.response.send_modal(
            ProfileEditModal("description", "Описание", "Коротко расскажи о себе", max_length=100)
        )

    @discord.ui.button(label="🆔 Null's ID", style=discord.ButtonStyle.secondary)
    async def edit_nulls_id(self, interaction: discord.Interaction, _button: discord.ui.Button):
        await interaction.response.send_modal(
            ProfileEditModal("nulls_brawl_id", "Null's Brawl ID", "Например: #ABCD123", max_length=32)
        )

    @discord.ui.button(label="🏅 Отображаемые роли", style=discord.ButtonStyle.secondary)
    async def edit_visible_roles(self, interaction: discord.Interaction, _button: discord.ui.Button):
        try:
            display_name = getattr(interaction.user, "display_name", None) or getattr(interaction.user, "name", None)
            profile = AccountsService.get_profile("discord", str(interaction.user.id), display_name=display_name) or {}
            roles_by_category = profile.get("roles_by_category") or {}
            role_catalog: list[dict[str, str]] = []
            for category_name in sorted(roles_by_category.keys(), key=lambda value: str(value).lower()):
                role_names = sorted(
                    {
                        str(role_name).strip()
                        for role_name in (roles_by_category.get(category_name) or [])
                        if str(role_name).strip()
                    },
                    key=lambda value: value.lower(),
                )
                for role_name in role_names:
                    role_catalog.append({"category": str(category_name).strip() or "Без категории", "role": role_name})
            visible_roles = [str(name).strip() for name in profile.get("visible_roles", []) if str(name).strip()]
            if not role_catalog:
                await interaction.response.send_message(
                    "❌ Нет доступных ролей для выбора. Проверьте /profile_roles.",
                    ephemeral=True,
                )
                return

            view = VisibleRolesPickerView(interaction.user.id, role_catalog, visible_roles)
            await interaction.response.send_message(
                view._content_text(),
                view=view,
                ephemeral=True,
            )
        except Exception:
            logger.exception("discord visible roles picker open failed user_id=%s", getattr(interaction.user, "id", None))
            await interaction.response.send_message("❌ Не удалось открыть выбор ролей.", ephemeral=True)


@bot.hybrid_command(name="register_account", description="Зарегистрировать общий аккаунт")
async def register_account(ctx):
    success, payload = register_discord_account(ctx.author.id)
    prefix = "✅" if success else "❌"
    await send_temp(ctx, f"{prefix} {payload}", delete_after=None)


@bot.hybrid_command(name="link_telegram", description="Сгенерировать код для привязки Telegram аккаунта")
async def link_telegram(ctx):
    if not _is_private_context(ctx):
        await send_temp(ctx, "❌ Команда привязки доступна только в личных сообщениях с ботом.", delete_after=None)
        return

    success, payload = issue_discord_telegram_link_code(ctx.author.id)
    if not success:
        await send_temp(ctx, f"❌ {payload}", delete_after=None)
        return

    await send_temp(
        ctx,
        (
            "🔗 Код привязки Telegram сгенерирован.\n"
            f"Код: `{payload}`\n"
            f"Срок действия: {AccountsService.LINK_TTL_MINUTES} минут.\n"
            "Используйте в Telegram: `/link <код>`"
        ),
        delete_after=None,
    )


@bot.hybrid_command(name="link", description="Привязать Discord к аккаунту по коду из Telegram")
async def link(ctx, code: str):
    if not _is_private_context(ctx):
        await send_temp(ctx, "❌ Команда привязки доступна только в личных сообщениях с ботом.", delete_after=None)
        return

    success, payload = consume_discord_link_code(ctx.author.id, code)
    prefix = "✅" if success else "❌"
    await send_temp(ctx, f"{prefix} {payload}", delete_after=None)


@bot.hybrid_command(name="profile", description="Показать профиль общего аккаунта")
async def profile(ctx):
    target_user = ctx.author
    reference = getattr(ctx.message, "reference", None)
    if reference and reference.message_id and ctx.guild:
        try:
            referenced_message = await ctx.channel.fetch_message(reference.message_id)
            if referenced_message and referenced_message.author:
                target_user = referenced_message.author
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            pass

    display_name = getattr(target_user, "display_name", None) or getattr(target_user, "name", None)
    data = AccountsService.get_profile("discord", str(target_user.id), display_name=display_name)
    if not data:
        await send_temp(ctx, "❌ Профиль не найден. Сначала выполните `/register_account`.", delete_after=None)
        return

    platform_target_name = display_name
    title_text = data["custom_nick"]
    if platform_target_name and platform_target_name != data["custom_nick"]:
        title_text = f"{title_text} ({platform_target_name})"

    embed = discord.Embed(title=f"👤 {title_text}", color=discord.Color.blurple())
    embed.add_field(
        name="**Общая информация**",
        value=(
            f"Звания: {data['titles_text']}\n"
            f"Айди в Null's Brawl: `{data['nulls_brawl_id']}` ({data['nulls_status']})\n"
            f"Баллы: {data['points']}"
        ),
        inline=False,
    )
    embed.add_field(name="**Описание**", value=data["description"][:100], inline=False)
    embed.add_field(
        name="**Дополнительная информация**",
        value=(
            f"🔗 TG ↔ DC: **{data['link_status']}**"
        ),
        inline=False,
    )
    visible_roles = data.get("visible_roles") or []
    roles_text = "\n".join(f"• {role_name}" for role_name in visible_roles) if visible_roles else "Нет назначенных ролей"
    embed.add_field(name="**Роли**", value=roles_text[:1024], inline=False)
    thumbnail_url = None
    if getattr(target_user, "avatar", None):
        thumbnail_url = target_user.display_avatar.url
    elif getattr(ctx.bot, "user", None) and getattr(ctx.bot.user, "display_avatar", None):
        thumbnail_url = ctx.bot.user.display_avatar.url

    if thumbnail_url:
        embed.set_thumbnail(url=thumbnail_url)

    await send_temp(ctx, embed=embed, delete_after=None)


@bot.hybrid_command(name="profile_roles", description="Показать все роли профиля по категориям")
async def profile_roles(ctx):
    target_user = ctx.author
    reference = getattr(ctx.message, "reference", None)
    if reference and reference.message_id and ctx.guild:
        try:
            referenced_message = await ctx.channel.fetch_message(reference.message_id)
            if referenced_message and referenced_message.author:
                target_user = referenced_message.author
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            pass

    display_name = getattr(target_user, "display_name", None) or getattr(target_user, "name", None)
    data = AccountsService.get_profile("discord", str(target_user.id), display_name=display_name)
    if not data:
        await send_temp(ctx, "❌ Профиль не найден. Сначала выполните `/register_account`.", delete_after=None)
        return

    roles_by_category = data.get("roles_by_category") or {}
    embed = discord.Embed(title="🏅 Роли пользователя", color=discord.Color.green())
    if not roles_by_category:
        embed.description = "Нет назначенных ролей"
    else:
        for category_name in sorted(roles_by_category.keys()):
            role_names = roles_by_category.get(category_name) or []
            if not role_names:
                continue
            embed.add_field(name=category_name, value="\n".join(f"• {name}" for name in role_names)[:1024], inline=False)

    await send_temp(ctx, embed=embed, delete_after=None)


@bot.hybrid_command(name="profile_edit", description="Настройки и редактирование своего профиля")
async def profile_edit(ctx):
    if not _is_private_context(ctx):
        await send_temp(ctx, "❌ Редактирование профиля доступно только в личных сообщениях с ботом.", delete_after=None)
        return

    embed = discord.Embed(
        title="⚙️ Настройки профиля",
        description="Выберите, какое поле хотите изменить. Роли можно выбрать кнопками.",
        color=discord.Color.blue(),
    )
    await send_temp(ctx, embed=embed, view=ProfileEditView(ctx.author.id), delete_after=None)
