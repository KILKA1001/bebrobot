import discord

from bot.commands.base import bot
from bot.services import AccountsService
from bot.systems.linking_logic import (
    consume_discord_link_code,
    issue_discord_telegram_link_code,
    register_discord_account,
)
from bot.utils import send_temp


@bot.hybrid_command(name="register_account", description="Зарегистрировать общий аккаунт")
async def register_account(ctx):
    success, payload = register_discord_account(ctx.author.id)
    prefix = "✅" if success else "❌"
    await send_temp(ctx, f"{prefix} {payload}", delete_after=None)


@bot.hybrid_command(name="link_telegram", description="Сгенерировать код для привязки Telegram аккаунта")
async def link_telegram(ctx):
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

    embed = discord.Embed(title=f"👤 {display_name}", color=discord.Color.blurple())
    embed.add_field(
        name="**Общая информация**",
        value=(
            "Звания: *скоро будет*\n"
            f"Айди в Null's Brawl: `{data['nulls_brawl_id']}`\n"
            f"Баллы: {data['points']}"
        ),
        inline=False,
    )
    embed.add_field(name="**Описание**", value=data["description"][:100], inline=False)
    embed.add_field(
        name="**Дополнительная информация**",
        value=(
            f"🔗 TG ↔ DC: **{data['link_status']}**\n"
            f"🛡️ Null's Brawl: **{data['nulls_status']}**"
        ),
        inline=False,
    )
    thumbnail_url = None
    if getattr(target_user, "avatar", None):
        thumbnail_url = target_user.display_avatar.url
    elif getattr(ctx.bot, "user", None) and getattr(ctx.bot.user, "display_avatar", None):
        thumbnail_url = ctx.bot.user.display_avatar.url

    if thumbnail_url:
        embed.set_thumbnail(url=thumbnail_url)

    await send_temp(ctx, embed=embed, delete_after=None)
