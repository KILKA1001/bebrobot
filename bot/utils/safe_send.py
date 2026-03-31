"""
Назначение: модуль "safe send" реализует продуктовый контур в зоне общая логика.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: общая логика.
"""

import logging

from discord.errors import HTTPException
from discord.ext import commands

from bot.services.accounts_service import AccountsService

from .discord_http import is_cloudflare_rate_limited_http_exception, log_discord_http_exception
from .rate_limiter import RateLimitWaitResult, rate_limiter

logger = logging.getLogger(__name__)

_ACK_BUCKET = "interaction_ack"
_FOLLOWUP_BUCKET = "followup"
_CHANNEL_SEND_BUCKET = "channel_send"


def _destination_operation_id(destination) -> str:
    return str(getattr(destination, "id", None) or getattr(getattr(destination, "channel", None), "id", None) or "unknown")


def _log_send_wait(operation: str, destination, wait_result: RateLimitWaitResult) -> None:
    logger.info(
        "%s rate limiter bucket=%s operation_id=%s destination_type=%s waited=%.4fs effective_delay=%.4fs requested_delay=%s",
        operation,
        wait_result.bucket,
        _destination_operation_id(destination),
        type(destination).__name__,
        wait_result.waited_for,
        wait_result.effective_delay,
        wait_result.requested_delay,
    )


def _log_interaction_ack_fast_path(destination) -> None:
    interaction = getattr(destination, "interaction", None)
    interaction_id = getattr(interaction, "id", None)
    logger.info(
        "safe_send interaction ACK fast-path interaction_id=%s bucket=%s ack_wait_before_request=0.0000s effective_delay=0.0000s throttled_before_request=%s",
        interaction_id or "unknown",
        _ACK_BUCKET,
        False,
    )


async def safe_send(destination, *args, delay: float | None = None, **kwargs):
    """Send a message with bucketed rate limiting.

    Parameters
    ----------
    destination: discord.abc.Messageable
        Channel, user or interaction to send message to.
    delay: float | None
        Optional delay override in seconds. If None, env defaults are used.
    """
    try:
        if isinstance(destination, commands.Context) and destination.interaction:
            delete_after = kwargs.pop("delete_after", None)

            async def _send_followup():
                wait_result = await rate_limiter.wait(_FOLLOWUP_BUCKET, delay)
                _log_send_wait("safe_send followup", destination, wait_result)
                message = await destination.interaction.followup.send(*args, **kwargs)
                if delete_after is not None:
                    await message.delete(delay=delete_after)
                return message

            if destination.interaction.response.is_done():
                return await _send_followup()

            _log_interaction_ack_fast_path(destination)
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

        wait_result = await rate_limiter.wait(_CHANNEL_SEND_BUCKET, delay)
        _log_send_wait("safe_send channel/user", destination, wait_result)
        return await destination.send(*args, **kwargs)
    except HTTPException as e:
        if e.status == 429 or is_cloudflare_rate_limited_http_exception(e):
            log_discord_http_exception(
                "safe_send hit Discord rate limit",
                e,
                stage="safe_send.send",
                operation_id=_destination_operation_id(destination),
                destination_type=type(destination).__name__,
            )
            retry_bucket = _FOLLOWUP_BUCKET if isinstance(destination, commands.Context) and getattr(destination, "interaction", None) else _CHANNEL_SEND_BUCKET
            retry_wait_result = await rate_limiter.wait(retry_bucket, delay)
            _log_send_wait("safe_send retry throttled", destination, retry_wait_result)
            return None
        raise
