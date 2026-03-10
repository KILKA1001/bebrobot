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
    data = AccountsService.get_profile("discord", str(ctx.author.id), display_name=ctx.author.display_name)
    if not data:
        await send_temp(ctx, "❌ Профиль не найден. Сначала выполните `/register_account`.", delete_after=None)
        return

    embed = discord.Embed(title=f"👤 {ctx.author.display_name}", color=discord.Color.blurple())
    embed.description = data["description"][:100]
    embed.add_field(name="Пользователь Discord", value=ctx.author.mention, inline=False)
    embed.add_field(
        name="Статусы",
        value=(
            f"🔗 TG ↔ DC: **{data['link_status']}**\n"
            f"🛡️ Null's Brawl: **{data['nulls_status']}**"
        ),
        inline=False,
    )
    embed.add_field(name="Айди в Null's Brawl", value=f"`{data['nulls_brawl_id']}`", inline=False)
    thumbnail_url = None
    if getattr(ctx.author, "avatar", None):
        thumbnail_url = ctx.author.display_avatar.url
    elif getattr(ctx.bot, "user", None) and getattr(ctx.bot.user, "display_avatar", None):
        thumbnail_url = ctx.bot.user.display_avatar.url

    if thumbnail_url:
        embed.set_thumbnail(url=thumbnail_url)

    await send_temp(ctx, embed=embed, delete_after=None)
