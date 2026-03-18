import discord
from discord.errors import HTTPException

from .rate_limiter import rate_limiter
from .discord_http import is_cloudflare_rate_limited_http_exception, log_discord_http_exception


async def safe_defer(interaction: discord.Interaction, *, ephemeral: bool = False, delay: float | None = None):
    await rate_limiter.wait(delay)
    try:
        return await interaction.response.defer(ephemeral=ephemeral)
    except HTTPException as e:
        if e.status == 429 or is_cloudflare_rate_limited_http_exception(e):
            log_discord_http_exception("safe_defer hit Discord rate limit", e, interaction_id=getattr(interaction, "id", None))
            await rate_limiter.wait(delay)
            return None
        raise


async def safe_response_send(interaction: discord.Interaction, *args, delay: float | None = None, **kwargs):
    await rate_limiter.wait(delay)
    try:
        return await interaction.response.send_message(*args, **kwargs)
    except HTTPException as e:
        if e.status == 429 or is_cloudflare_rate_limited_http_exception(e):
            log_discord_http_exception("safe_response_send hit Discord rate limit", e, interaction_id=getattr(interaction, "id", None))
            await rate_limiter.wait(delay)
            return None
        raise


async def safe_followup_send(interaction: discord.Interaction, *args, delay: float | None = None, **kwargs):
    await rate_limiter.wait(delay)
    try:
        return await interaction.followup.send(*args, **kwargs)
    except HTTPException as e:
        if e.status == 429 or is_cloudflare_rate_limited_http_exception(e):
            log_discord_http_exception("safe_followup_send hit Discord rate limit", e, interaction_id=getattr(interaction, "id", None))
            await rate_limiter.wait(delay)
            return None
        raise


async def safe_edit_original_response(interaction: discord.Interaction, *args, delay: float | None = None, **kwargs):
    await rate_limiter.wait(delay)
    try:
        return await interaction.edit_original_response(*args, **kwargs)
    except HTTPException as e:
        if e.status == 429 or is_cloudflare_rate_limited_http_exception(e):
            log_discord_http_exception("safe_edit_original_response hit Discord rate limit", e, interaction_id=getattr(interaction, "id", None))
            await rate_limiter.wait(delay)
            return None
        raise
