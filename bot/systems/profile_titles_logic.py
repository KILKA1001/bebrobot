import asyncio
import logging
import os

import discord

from bot.services import AccountsService

logger = logging.getLogger(__name__)


async def sync_discord_titles_once(bot: discord.Client) -> None:
    role_mappings = AccountsService.get_configured_title_roles()
    role_ids = set(role_mappings.keys())
    role_names = AccountsService.get_configured_title_role_names()

    if not role_ids and not role_names:
        logger.info(
            "profile title sync skipped: no DB mappings in profile_title_roles and env PROFILE_DISCORD_TITLE_ROLE_IDS/PROFILE_DISCORD_TITLE_ROLE_NAMES are empty"
        )
        return

    synced = 0
    skipped_without_identity = 0

    for guild in bot.guilds:
        for member in guild.members:
            if getattr(member, "bot", False):
                continue

            try:
                selected_roles = []
                for role in member.roles:
                    if role.id in role_mappings:
                        selected_roles.append(role_mappings[role.id])
                    elif role_ids and role.id in role_ids:
                        selected_roles.append(role.name)
                    elif role_names and role.name.lower() in role_names:
                        selected_roles.append(role.name)

                if not selected_roles:
                    continue

                account_id = AccountsService.resolve_account_id("discord", str(member.id))
                if not account_id:
                    skipped_without_identity += 1
                    continue

                if AccountsService.save_account_titles(account_id, selected_roles, source="discord"):
                    synced += 1
            except Exception:
                logger.exception(
                    "profile title sync member failed guild_id=%s member_id=%s",
                    guild.id,
                    getattr(member, "id", "unknown"),
                )

    logger.info(
        "profile title sync finished synced=%s skipped_without_identity=%s guilds=%s",
        synced,
        skipped_without_identity,
        len(bot.guilds),
    )


async def profile_titles_sync_loop(bot: discord.Client) -> None:
    await bot.wait_until_ready()
    interval_sec = int(os.getenv("PROFILE_TITLES_SYNC_INTERVAL_SEC", "900"))

    while not bot.is_closed():
        try:
            await sync_discord_titles_once(bot)
        except Exception:
            logger.exception("profile title sync loop iteration failed")
        await asyncio.sleep(max(60, interval_sec))
