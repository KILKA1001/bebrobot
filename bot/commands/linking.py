import logging

import discord

from bot.commands.base import bot
from bot.services import AccountsService, AuthorityService, ExternalRolesSyncService
from bot.systems.linking_logic import (
    consume_discord_link_code,
    issue_discord_telegram_link_code,
    register_discord_account,
)
from bot.utils import send_temp

logger = logging.getLogger(__name__)


def _is_private_context(ctx) -> bool:
    return getattr(ctx, "guild", None) is None


class ProfileEditModal(discord.ui.Modal):
    def __init__(self, field_name: str, title_text: str, placeholder: str, max_length: int):
        super().__init__(title=f"Изменить: {title_text}")
        self.field_name = field_name
        self.field_label = title_text
        self.value_input = discord.ui.TextInput(
            label=title_text,
            style=discord.TextStyle.paragraph,
            max_length=max_length,
            required=False,
            placeholder=placeholder,
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




class ForceRoleSyncView(discord.ui.View):
    def __init__(self, actor_user_id: int, account_id: str):
        super().__init__(timeout=300)
        self.actor_user_id = actor_user_id
        self.account_id = account_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.actor_user_id:
            await interaction.response.send_message("❌ Обновить роли может только инициатор команды.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="🔄 Обновить внешние роли", style=discord.ButtonStyle.secondary)
    async def force_sync(self, interaction: discord.Interaction, _button: discord.ui.Button):
        try:
            changed = ExternalRolesSyncService.sync_account_by_account_id(interaction.client, self.account_id)
            text = "✅ Синхронизация ролей выполнена." if changed else "✅ Синхронизация выполнена, изменений нет."
            await interaction.response.send_message(text, ephemeral=True)
        except Exception:
            logger.exception(
                "discord force role sync failed actor_user_id=%s account_id=%s",
                getattr(interaction.user, "id", None),
                self.account_id,
            )
            await interaction.response.send_message("❌ Ошибка синхронизации ролей.", ephemeral=True)
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

    embed = discord.Embed(title=f"👤 {data['custom_nick']}", color=discord.Color.blurple())
    embed.add_field(
        name="**Общая информация**",
        value=(
            f"Звания: {data['titles_text']}\n"
            f"Айди в Null's Brawl: `{data['nulls_brawl_id']}`\n"
            f"Баллы: {data['points']}"
        ),
        inline=False,
    )
    embed.add_field(name="**Описание**", value=data["description"][:100], inline=False)
    external_roles_last_synced_at = data.get("external_roles_last_synced_at") or "—"
    embed.add_field(
        name="**Дополнительная информация**",
        value=(
            f"🔗 TG ↔ DC: **{data['link_status']}**\n"
            f"🛡️ Null's Brawl: **{data['nulls_status']}**\n"
            f"🕒 Последний sync ролей: **{external_roles_last_synced_at}**"
        ),
        inline=False,
    )
    roles = data.get("roles") or []
    if roles:
        roles_text = "\n".join(
            f"• {item.get('name', 'unknown')} ({item.get('source', 'unknown')}) | {item.get('origin_label') or '—'} | {item.get('synced_at') or '—'}"
            for item in roles
        )
    else:
        roles_text = "Нет назначенных ролей"
    embed.add_field(name="**Роли**", value=roles_text[:1024], inline=False)
    thumbnail_url = None
    if getattr(target_user, "avatar", None):
        thumbnail_url = target_user.display_avatar.url
    elif getattr(ctx.bot, "user", None) and getattr(ctx.bot.user, "display_avatar", None):
        thumbnail_url = ctx.bot.user.display_avatar.url

    if thumbnail_url:
        embed.set_thumbnail(url=thumbnail_url)

    view = None
    if AuthorityService.can_manage_self("discord", str(ctx.author.id)):
        view = ForceRoleSyncView(ctx.author.id, data["account_id"])
    await send_temp(ctx, embed=embed, view=view, delete_after=None)


@bot.hybrid_command(name="profile_edit", description="Настройки и редактирование своего профиля")
async def profile_edit(ctx):
    if not _is_private_context(ctx):
        await send_temp(ctx, "❌ Редактирование профиля доступно только в личных сообщениях с ботом.", delete_after=None)
        return

    embed = discord.Embed(
        title="⚙️ Настройки профиля",
        description="Выберите, какое поле хотите изменить.",
        color=discord.Color.blue(),
    )
    await send_temp(ctx, embed=embed, view=ProfileEditView(ctx.author.id), delete_after=None)
