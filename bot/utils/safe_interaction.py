import logging
import discord
from discord.errors import HTTPException

from .rate_limiter import rate_limiter


async def safe_defer(interaction: discord.Interaction, *, ephemeral: bool = False, delay: float | None = None):
    await rate_limiter.wait(delay)
    try:
        return await interaction.response.defer(ephemeral=ephemeral)
    except HTTPException as e:
        if e.status == 429:
            logging.warning("safe_defer hit rate limit: %s", e.text)
            await rate_limiter.wait(delay)
            return None
        raise


async def safe_response_send(interaction: discord.Interaction, *args, delay: float | None = None, **kwargs):
    await rate_limiter.wait(delay)
    try:
        return await interaction.response.send_message(*args, **kwargs)
    except HTTPException as e:
        if e.status == 429:
            logging.warning("safe_response_send hit rate limit: %s", e.text)
            await rate_limiter.wait(delay)
            return None
        raise


async def safe_followup_send(interaction: discord.Interaction, *args, delay: float | None = None, **kwargs):
    await rate_limiter.wait(delay)
    try:
        return await interaction.followup.send(*args, **kwargs)
    except HTTPException as e:
        if e.status == 429:
            logging.warning("safe_followup_send hit rate limit: %s", e.text)
            await rate_limiter.wait(delay)
            return None
        raise


async def safe_edit_original_response(interaction: discord.Interaction, *args, delay: float | None = None, **kwargs):
    await rate_limiter.wait(delay)
    try:
        return await interaction.edit_original_response(*args, **kwargs)
    except HTTPException as e:
        if e.status == 429:
            logging.warning("safe_edit_original_response hit rate limit: %s", e.text)
            await rate_limiter.wait(delay)
            return None
        raise
