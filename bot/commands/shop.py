import logging
import os

import discord

from bot.commands.base import bot
from bot.systems.shop_logic import build_shop_prompt_text, check_shop_profile_access
from bot.utils import send_temp

logger = logging.getLogger(__name__)

SHOP_OPEN_PROMPT_TEXT = "Откройте магазин в личных сообщениях, я уже отправил вам инструкцию."
DM_FALLBACK_TEXT = (
    "❌ Не удалось отправить инструкцию в личные сообщения.\n"
    "Откройте ЛС с ботом: нажмите на профиль бота → Message, включите личные сообщения для сервера и снова выполните /shop."
)
SHOP_BUTTON_TEXT = "Открыть магазин"
SHOP_DEEPLINK_HINT = "Откройте ЛС с ботом и выполните /shop"
SHOP_URL = os.getenv("SHOP_URL", "").strip()


class ShopOpenView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=600)
        if SHOP_URL:
            self.add_item(discord.ui.Button(label=SHOP_BUTTON_TEXT, style=discord.ButtonStyle.link, url=SHOP_URL))


def _shop_dm_content() -> str:
    return build_shop_prompt_text().replace("<b>", "**").replace("</b>", "**")


def _extract_dm_failure_code(error: Exception) -> str:
    if isinstance(error, discord.Forbidden):
        return "dm_closed"
    if isinstance(error, discord.HTTPException):
        if error.status == 403:
            return "forbidden"
        return f"http_{error.status}"
    return "dm_failed"


@bot.hybrid_command(name="shop", description="Открыть магазин (в личных сообщениях)")
async def shop(ctx):
    source = "dm" if getattr(ctx, "guild", None) is None else "group"
    actor_id = getattr(getattr(ctx, "author", None), "id", None)
    logger.info(
        "shop flow step=received provider=discord source=%s actor_user_id=%s guild_id=%s channel_id=%s",
        source,
        actor_id,
        getattr(getattr(ctx, "guild", None), "id", None),
        getattr(getattr(ctx, "channel", None), "id", None),
    )

    profile_check = check_shop_profile_access("discord", actor_id, register_command="/register_account")
    if not profile_check.ok:
        await send_temp(ctx, profile_check.user_message or "Сначала создайте профиль и повторите команду /shop.", delete_after=None)
        return

    dm_content = _shop_dm_content()
    dm_view = ShopOpenView()

    if source == "dm":
        await send_temp(ctx, dm_content, view=dm_view, delete_after=None)
        logger.info("shop flow step=completed provider=discord source=dm actor_user_id=%s dm_sent=true reason=ok", actor_id)
        return

    await send_temp(ctx, SHOP_OPEN_PROMPT_TEXT, delete_after=None)
    logger.info("shop flow step=group_notice_sent provider=discord source=group actor_user_id=%s", actor_id)

    try:
        await ctx.author.send(dm_content, view=dm_view)
        logger.info("shop flow step=dm_attempt provider=discord source=group actor_user_id=%s dm_sent=true reason=ok", actor_id)
    except Exception as error:  # noqa: BLE001
        reason = _extract_dm_failure_code(error)
        logger.warning(
            "shop flow step=dm_attempt provider=discord source=group actor_user_id=%s dm_sent=false reason=%s error=%s",
            actor_id,
            reason,
            error,
        )
        await send_temp(ctx, DM_FALLBACK_TEXT, delete_after=None)
