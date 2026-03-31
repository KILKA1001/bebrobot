"""
Назначение: модуль "safe interaction" реализует продуктовый контур в зоне общая логика.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: общая логика.
"""

import logging
import time

import discord
from discord.errors import HTTPException

from .discord_http import is_cloudflare_rate_limited_http_exception, log_discord_http_exception
from .rate_limiter import RateLimitWaitResult, rate_limiter

logger = logging.getLogger(__name__)


_ACK_BUCKET = "interaction_ack"
_FOLLOWUP_BUCKET = "followup"


def _interaction_id(interaction: discord.Interaction) -> str:
    return str(getattr(interaction, "id", None) or "unknown")


def _log_ack_attempt(operation: str, interaction: discord.Interaction, ack_started_at: float) -> None:
    ack_wait = max(0.0, time.monotonic() - ack_started_at)
    logger.info(
        "%s interaction ACK attempt interaction_id=%s ack_wait_before_request=%.4fs bucket=%s effective_delay=0.0000s throttled_before_request=%s",
        operation,
        _interaction_id(interaction),
        ack_wait,
        _ACK_BUCKET,
        False,
    )


def _log_bucket_wait(operation: str, interaction: discord.Interaction, wait_result: RateLimitWaitResult) -> None:
    logger.info(
        "%s rate limiter bucket=%s interaction_id=%s waited=%.4fs effective_delay=%.4fs requested_delay=%s",
        operation,
        wait_result.bucket,
        _interaction_id(interaction),
        wait_result.waited_for,
        wait_result.effective_delay,
        wait_result.requested_delay,
    )


async def safe_defer(interaction: discord.Interaction, *, ephemeral: bool = False, delay: float | None = None):
    ack_started_at = time.monotonic()
    _log_ack_attempt("safe_defer", interaction, ack_started_at)
    try:
        return await interaction.response.defer(ephemeral=ephemeral)
    except HTTPException as e:
        if e.status == 429 or is_cloudflare_rate_limited_http_exception(e):
            log_discord_http_exception(
                "safe_defer hit Discord rate limit",
                e,
                stage="safe_interaction.defer",
                operation_id=_interaction_id(interaction),
                interaction_id=getattr(interaction, "id", None),
            )
            wait_result = await rate_limiter.wait(_ACK_BUCKET, delay)
            _log_bucket_wait("safe_defer retry throttled", interaction, wait_result)
            return None
        raise


async def safe_response_send(interaction: discord.Interaction, *args, delay: float | None = None, **kwargs):
    ack_started_at = time.monotonic()
    _log_ack_attempt("safe_response_send", interaction, ack_started_at)
    try:
        return await interaction.response.send_message(*args, **kwargs)
    except HTTPException as e:
        if e.status == 429 or is_cloudflare_rate_limited_http_exception(e):
            log_discord_http_exception(
                "safe_response_send hit Discord rate limit",
                e,
                stage="safe_interaction.response_send",
                operation_id=_interaction_id(interaction),
                interaction_id=getattr(interaction, "id", None),
            )
            wait_result = await rate_limiter.wait(_ACK_BUCKET, delay)
            _log_bucket_wait("safe_response_send retry throttled", interaction, wait_result)
            return None
        raise


async def safe_followup_send(interaction: discord.Interaction, *args, delay: float | None = None, **kwargs):
    wait_result = await rate_limiter.wait(_FOLLOWUP_BUCKET, delay)
    _log_bucket_wait("safe_followup_send", interaction, wait_result)
    try:
        return await interaction.followup.send(*args, **kwargs)
    except HTTPException as e:
        if e.status == 429 or is_cloudflare_rate_limited_http_exception(e):
            log_discord_http_exception(
                "safe_followup_send hit Discord rate limit",
                e,
                stage="safe_interaction.followup_send",
                operation_id=_interaction_id(interaction),
                interaction_id=getattr(interaction, "id", None),
            )
            retry_wait_result = await rate_limiter.wait(_FOLLOWUP_BUCKET, delay)
            _log_bucket_wait("safe_followup_send retry throttled", interaction, retry_wait_result)
            return None
        raise


async def safe_edit_original_response(interaction: discord.Interaction, *args, delay: float | None = None, **kwargs):
    wait_result = await rate_limiter.wait(_FOLLOWUP_BUCKET, delay)
    _log_bucket_wait("safe_edit_original_response", interaction, wait_result)
    try:
        return await interaction.edit_original_response(*args, **kwargs)
    except HTTPException as e:
        if e.status == 429 or is_cloudflare_rate_limited_http_exception(e):
            log_discord_http_exception(
                "safe_edit_original_response hit Discord rate limit",
                e,
                stage="safe_interaction.edit_original_response",
                operation_id=_interaction_id(interaction),
                interaction_id=getattr(interaction, "id", None),
            )
            retry_wait_result = await rate_limiter.wait(_FOLLOWUP_BUCKET, delay)
            _log_bucket_wait("safe_edit_original_response retry throttled", interaction, retry_wait_result)
            return None
        raise
