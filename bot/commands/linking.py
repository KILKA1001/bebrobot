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

    embed = discord.Embed(title=f"👤 {data['custom_nick']}", color=discord.Color.blurple())
    embed.add_field(name="ID общего аккаунта", value=f"`{data['account_id']}`", inline=False)
    embed.add_field(name="Discord", value=f"`{data['discord_id'] or 'не привязан'}`", inline=True)
    embed.add_field(name="Telegram", value=f"`{data['telegram_id'] or 'не привязан'}`", inline=True)
    embed.add_field(name="Айди из NULS (заглушка)", value=f"`{data['nulls_id']}`", inline=False)
    embed.add_field(name="Описание", value=data["description"][:100], inline=False)
    embed.add_field(
        name="Статусы",
        value=(
            f"🔗 TG ↔ DC: **{data['link_status']}**\n"
            f"🛡️ NULS: **{data['nulls_status']}**"
        ),
        inline=False,
    )
    if ctx.author.display_avatar:
        embed.set_thumbnail(url=ctx.author.display_avatar.url)

    await send_temp(ctx, embed=embed, delete_after=None)
