import logging

import discord

from bot.commands.base import bot
from bot.services import AccountsService
from bot.services.guiy_admin_service import (
    GUIY_OWNER_DENIED_MESSAGE,
    GUIY_OWNER_REPLY_REQUIRED_MESSAGE,
    GUIY_OWNER_USAGE_TEXT,
    authorize_guiy_owner_action,
    parse_guiy_owner_profile_payload,
    resolve_guiy_target_account,
)
from bot.utils import send_temp

logger = logging.getLogger(__name__)


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


def _parse_action_and_payload(action: str | None, payload: str | None) -> tuple[str, str]:
    normalized_action = str(action or "").strip().lower()
    normalized_payload = str(payload or "").strip()
    return normalized_action, normalized_payload


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
            "discord guiy owner failed to fetch reply target channel_id=%s message_id=%s actor_user_id=%s",
            getattr(ctx.channel, "id", None),
            reference.message_id,
            getattr(ctx.author, "id", None),
        )
        return None


@bot.command(name="guiy_owner", hidden=True)
async def guiy_owner(ctx, action: str = "", *, payload: str = ""):
    _persist_discord_identity(ctx.author)
    requested_action, requested_payload = _parse_action_and_payload(action, payload)
    if requested_action not in {"say", "reply", "profile"}:
        await send_temp(ctx, GUIY_OWNER_USAGE_TEXT, delete_after=None)
        return
    if requested_action in {"say", "reply"} and not requested_payload:
        await send_temp(ctx, GUIY_OWNER_USAGE_TEXT, delete_after=None)
        return

    reply_message = await _resolve_reply_message(ctx)
    _persist_discord_identity(reply_message.author if reply_message else None)
    target_message_id = getattr(reply_message, "id", None)
    if requested_action == "reply" and reply_message is None:
        await send_temp(ctx, GUIY_OWNER_REPLY_REQUIRED_MESSAGE, delete_after=None)
        return

    access = authorize_guiy_owner_action(
        actor_provider="discord",
        actor_user_id=getattr(ctx.author, "id", None),
        requested_action=requested_action,
        target_message_id=target_message_id,
    )
    if not access.allowed:
        await send_temp(ctx, GUIY_OWNER_DENIED_MESSAGE, delete_after=None)
        return

    bot_user = getattr(ctx.bot, "user", None)
    bot_user_id = getattr(bot_user, "id", None)
    reply_author_user_id = getattr(getattr(reply_message, "author", None), "id", None)
    target_resolution = resolve_guiy_target_account(
        provider="discord",
        bot_user_id=bot_user_id,
        reply_author_user_id=reply_author_user_id,
        explicit_owner_command=True,
    )
    if not target_resolution.ok:
        await send_temp(ctx, target_resolution.message or GUIY_OWNER_DENIED_MESSAGE, delete_after=None)
        return

    try:
        if requested_action == "say":
            await send_temp(ctx, requested_payload, delete_after=None)
            return

        if requested_action == "reply":
            await reply_message.reply(requested_payload, mention_author=False)
            return

        field_name, field_value = parse_guiy_owner_profile_payload(requested_payload)
        if not field_name:
            await send_temp(ctx, GUIY_OWNER_USAGE_TEXT, delete_after=None)
            return

        success, response = AccountsService.update_profile_field(
            "discord",
            str(bot_user_id),
            field_name,
            field_value or "",
        )
        prefix = "✅" if success else "❌"
        await send_temp(
            ctx,
            f"{prefix} {response}\nПодсказка: используйте /profile, чтобы проверить, как выглядит обновлённый профиль Гуя.",
            delete_after=None,
        )
    except Exception:
        logger.exception(
            "discord guiy owner command failed guild_id=%s channel_id=%s actor_user_id=%s action=%s target_message_id=%s",
            getattr(ctx.guild, "id", None),
            getattr(ctx.channel, "id", None),
            getattr(ctx.author, "id", None),
            requested_action,
            target_message_id,
        )
        await send_temp(ctx, "❌ Не удалось выполнить действие. Попробуйте позже.", delete_after=None)
