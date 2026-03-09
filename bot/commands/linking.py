from bot.commands.base import bot
from bot.services import AccountsService
from bot.systems.linking_logic import issue_discord_telegram_link_code
from bot.utils import send_temp


@bot.hybrid_command(
    name="link_telegram",
    description="Сгенерировать код для привязки Telegram аккаунта",
)
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
