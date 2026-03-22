import asyncio
import logging
import os

import discord

from bot.services import ExternalRolesSyncService

logger = logging.getLogger(__name__)
DEFAULT_EXTERNAL_ROLES_SYNC_INTERVAL_SEC = 3600


async def sync_external_roles_once(bot: discord.Client) -> None:
    stats = ExternalRolesSyncService.sync_all_linked_accounts(bot)
    logger.info(
        "external roles sync once finished processed=%s synced=%s errors=%s",
        stats.get("processed", 0),
        stats.get("synced", 0),
        stats.get("errors", 0),
    )


async def sync_external_roles_for_account(bot: discord.Client, account_id: str, *, reason: str) -> bool:
    changed = await asyncio.to_thread(ExternalRolesSyncService.sync_account_by_account_id, bot, account_id)
    logger.info(
        "external roles targeted sync finished account_id=%s reason=%s changed=%s",
        account_id,
        reason,
        changed,
    )
    return changed


def schedule_external_roles_sync(bot: discord.Client, account_id: str, *, reason: str) -> bool:
    normalized_account_id = str(account_id or "").strip()
    if not normalized_account_id:
        logger.warning("external roles targeted sync skipped empty account_id reason=%s", reason)
        return False

    async def _runner() -> None:
        try:
            await sync_external_roles_for_account(bot, normalized_account_id, reason=reason)
        except Exception:
            logger.exception(
                "external roles targeted sync failed account_id=%s reason=%s",
                normalized_account_id,
                reason,
            )

    asyncio.create_task(_runner())
    return True


async def external_roles_sync_loop(bot: discord.Client) -> None:
    await bot.wait_until_ready()
    interval_sec = int(os.getenv("EXTERNAL_ROLES_SYNC_INTERVAL_SEC", str(DEFAULT_EXTERNAL_ROLES_SYNC_INTERVAL_SEC)))

    while not bot.is_closed():
        try:
            await sync_external_roles_once(bot)
        except Exception:
            logger.exception("external roles sync loop iteration failed")
        await asyncio.sleep(max(60, interval_sec))
