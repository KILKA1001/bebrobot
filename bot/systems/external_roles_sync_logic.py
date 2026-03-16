import asyncio
import logging
import os

import discord

from bot.services import ExternalRolesSyncService

logger = logging.getLogger(__name__)


async def sync_external_roles_once(bot: discord.Client) -> None:
    stats = ExternalRolesSyncService.sync_all_linked_accounts(bot)
    logger.info(
        "external roles sync once finished processed=%s synced=%s errors=%s",
        stats.get("processed", 0),
        stats.get("synced", 0),
        stats.get("errors", 0),
    )


async def external_roles_sync_loop(bot: discord.Client) -> None:
    await bot.wait_until_ready()
    interval_sec = int(os.getenv("EXTERNAL_ROLES_SYNC_INTERVAL_SEC", "900"))

    while not bot.is_closed():
        try:
            await sync_external_roles_once(bot)
        except Exception:
            logger.exception("external roles sync loop iteration failed")
        await asyncio.sleep(max(60, interval_sec))
