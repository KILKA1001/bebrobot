import asyncio
import logging
import os
from collections.abc import Iterable

import discord

from bot.services import AccountsService

logger = logging.getLogger(__name__)

_member_title_role_state_cache: dict[tuple[int, int], tuple[str, ...]] = {}


def _build_title_role_state(
    member: discord.Member,
    *,
    role_mappings: dict[int, str],
    configured_role_names: set[str],
) -> tuple[str, ...]:
    selected_roles: list[str] = []
    for role in getattr(member, "roles", []) or []:
        if role.id in role_mappings:
            selected_roles.append(role_mappings[role.id])
        elif configured_role_names and str(role.name).strip().lower() in configured_role_names:
            selected_roles.append(str(role.name).strip())

    normalized = [str(item).strip() for item in selected_roles if str(item).strip()]
    return tuple(dict.fromkeys(normalized))


def _member_cache_key(member: discord.Member) -> tuple[int, int]:
    return (int(member.guild.id), int(member.id))


def _should_skip_member_sync(member: discord.Member, title_role_state: tuple[str, ...]) -> bool:
    cached_state = _member_title_role_state_cache.get(_member_cache_key(member))
    return cached_state == title_role_state


async def sync_discord_member_titles(
    member: discord.Member,
    *,
    role_mappings: dict[int, str] | None = None,
    configured_role_names: set[str] | None = None,
    force: bool = False,
) -> bool:
    if getattr(member, "bot", False):
        return False

    role_mappings = role_mappings if role_mappings is not None else AccountsService.get_configured_title_roles()
    configured_role_names = (
        configured_role_names if configured_role_names is not None else AccountsService.get_configured_title_role_names()
    )
    if not role_mappings and not configured_role_names:
        logger.info(
            "profile title sync member skipped: no configured title roles guild_id=%s member_id=%s",
            getattr(member.guild, "id", "unknown"),
            getattr(member, "id", "unknown"),
        )
        return False

    title_role_state = _build_title_role_state(
        member,
        role_mappings=role_mappings,
        configured_role_names=configured_role_names,
    )
    if not force and _should_skip_member_sync(member, title_role_state):
        logger.debug(
            "profile title sync skipped unchanged discord title roles guild_id=%s member_id=%s titles=%s",
            member.guild.id,
            member.id,
            list(title_role_state),
        )
        return False

    account_id = AccountsService.resolve_account_id("discord", str(member.id))
    if not account_id:
        _member_title_role_state_cache[_member_cache_key(member)] = title_role_state
        logger.warning(
            "profile title sync skipped member without linked account guild_id=%s member_id=%s titles=%s",
            member.guild.id,
            member.id,
            list(title_role_state),
        )
        return False

    saved_titles = tuple(AccountsService.get_account_titles(account_id))
    if saved_titles == title_role_state:
        _member_title_role_state_cache[_member_cache_key(member)] = title_role_state
        logger.debug(
            "profile title sync skipped unchanged persisted titles guild_id=%s member_id=%s account_id=%s titles=%s",
            member.guild.id,
            member.id,
            account_id,
            list(title_role_state),
        )
        return False

    saved = AccountsService.save_account_titles(account_id, list(title_role_state), source="discord")
    if saved:
        _member_title_role_state_cache[_member_cache_key(member)] = title_role_state
        logger.info(
            "profile title sync updated account titles guild_id=%s member_id=%s account_id=%s titles=%s",
            member.guild.id,
            member.id,
            account_id,
            list(title_role_state),
        )
        return True

    logger.error(
        "profile title sync failed to persist account titles guild_id=%s member_id=%s account_id=%s titles=%s",
        member.guild.id,
        member.id,
        account_id,
        list(title_role_state),
    )
    return False


async def sync_discord_titles_once(bot: discord.Client) -> None:
    role_mappings = AccountsService.get_configured_title_roles()
    configured_role_names = AccountsService.get_configured_title_role_names()

    if not role_mappings and not configured_role_names:
        logger.info(
            "profile title sync skipped: no DB mappings in profile_title_roles and env PROFILE_DISCORD_TITLE_ROLE_IDS/PROFILE_DISCORD_TITLE_ROLE_NAMES are empty"
        )
        return

    synced = 0
    skipped_unchanged = 0
    processed_members = 0

    for guild in bot.guilds:
        for member in guild.members:
            if getattr(member, "bot", False):
                continue

            processed_members += 1
            try:
                current_title_role_state = _build_title_role_state(
                    member,
                    role_mappings=role_mappings,
                    configured_role_names=configured_role_names,
                )
                if _should_skip_member_sync(member, current_title_role_state):
                    skipped_unchanged += 1
                    continue

                if await sync_discord_member_titles(
                    member,
                    role_mappings=role_mappings,
                    configured_role_names=configured_role_names,
                ):
                    synced += 1
            except Exception:
                logger.exception(
                    "profile title sync member failed guild_id=%s member_id=%s",
                    guild.id,
                    getattr(member, "id", "unknown"),
                )

    logger.info(
        "profile title reconciliation finished synced=%s skipped_unchanged=%s processed_members=%s guilds=%s",
        synced,
        skipped_unchanged,
        processed_members,
        len(bot.guilds),
    )


def _roles_changed(before_roles: Iterable[discord.Role], after_roles: Iterable[discord.Role]) -> bool:
    before_ids = {int(role.id) for role in before_roles}
    after_ids = {int(role.id) for role in after_roles}
    return before_ids != after_ids


async def handle_member_update_for_profile_titles(before: discord.Member, after: discord.Member) -> None:
    try:
        if not _roles_changed(getattr(before, "roles", []) or [], getattr(after, "roles", []) or []):
            return

        await sync_discord_member_titles(after)
    except Exception:
        logger.exception(
            "profile title event sync failed guild_id=%s member_id=%s",
            getattr(getattr(after, "guild", None), "id", "unknown"),
            getattr(after, "id", "unknown"),
        )


async def handle_member_join_for_profile_titles(member: discord.Member) -> None:
    try:
        await sync_discord_member_titles(member, force=True)
    except Exception:
        logger.exception(
            "profile title join sync failed guild_id=%s member_id=%s",
            getattr(getattr(member, "guild", None), "id", "unknown"),
            getattr(member, "id", "unknown"),
        )


async def profile_titles_sync_loop(bot: discord.Client) -> None:
    await bot.wait_until_ready()
    interval_sec = int(os.getenv("PROFILE_TITLES_SYNC_INTERVAL_SEC", "21600"))
    logger.info(
        "profile title reconciliation loop started interval_sec=%s mode=event_driven_plus_reconciliation",
        interval_sec,
    )

    while not bot.is_closed():
        try:
            await sync_discord_titles_once(bot)
        except Exception:
            logger.exception("profile title sync loop iteration failed")
        await asyncio.sleep(max(300, interval_sec))
