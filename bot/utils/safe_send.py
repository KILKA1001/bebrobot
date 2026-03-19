import logging
from discord.errors import HTTPException
from discord.ext import commands
from .rate_limiter import rate_limiter
from bot.services.accounts_service import AccountsService
from .discord_http import is_cloudflare_rate_limited_http_exception, log_discord_http_exception


async def safe_send(destination, *args, delay: float | None = None, **kwargs):
    """Send a message with global rate limiting.

    Parameters
    ----------
    destination: discord.abc.Messageable
        Channel, user or interaction to send message to.
    delay: float | None
        Optional delay override in seconds. If None, env defaults are used.
    """
    await rate_limiter.wait(delay)
    try:
        if isinstance(destination, commands.Context) and destination.interaction:
            delete_after = kwargs.pop("delete_after", None)

            async def _send_followup():
                message = await destination.interaction.followup.send(*args, **kwargs)
                if delete_after is not None:
                    await message.delete(delay=delete_after)
                return message

            if destination.interaction.response.is_done():
                return await _send_followup()

            try:
                return await destination.interaction.response.send_message(
                    *args,
                    delete_after=delete_after,
                    **kwargs,
                )
            except HTTPException as e:
                # Discord может подтвердить interaction в фоне между проверкой
                # response.is_done() и фактической отправкой первого ответа.
                # В этом случае нужно отправлять followup, иначе команда падает
                # с ошибкой 40060 (Interaction has already been acknowledged).
                if e.code == 40060:
                    discord_author_id = getattr(getattr(destination, "author", None), "id", None)
                    author_account_id = (
                        AccountsService.resolve_account_id("discord", str(discord_author_id))
                        if discord_author_id is not None
                        else None
                    )
                    logging.error(
                        "safe_send interaction already acknowledged; fallback to followup "
                        "command=%s author_account_id=%s",
                        getattr(destination.command, "qualified_name", "unknown"),
                        author_account_id or "unknown",
                    )
                    return await _send_followup()
                raise

        return await destination.send(*args, **kwargs)
    except HTTPException as e:
        if e.status == 429 or is_cloudflare_rate_limited_http_exception(e):
            log_discord_http_exception(
                "safe_send hit Discord rate limit",
                e,
                stage="safe_send.send",
                operation_id=str(getattr(destination, "id", None) or getattr(getattr(destination, "channel", None), "id", None) or "unknown"),
                destination_type=type(destination).__name__,
            )
            await rate_limiter.wait(delay)
            return None
        raise
