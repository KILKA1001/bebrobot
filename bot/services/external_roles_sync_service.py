import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Iterable

import discord

from bot.data import db

logger = logging.getLogger(__name__)


class ExternalRolesSyncService:
    """Sync snapshot of external roles for linked accounts."""

    @staticmethod
    def sync_all_linked_accounts(bot: discord.Client) -> dict[str, int]:
        if not db.supabase:
            logger.warning("external roles sync skipped: supabase is not configured")
            return {"processed": 0, "synced": 0, "errors": 0}

        linked_rows = ExternalRolesSyncService._load_linked_accounts()
        if not linked_rows:
            logger.info("external roles sync skipped: no linked accounts with both providers")
            return {"processed": 0, "synced": 0, "errors": 0}

        discord_roles_by_user = ExternalRolesSyncService._collect_discord_roles(bot)
        processed = 0
        synced = 0
        errors = 0

        for row in linked_rows:
            processed += 1
            account_id = str(row.get("account_id") or "").strip()
            discord_user_id = str(row.get("discord_user_id") or "").strip()
            telegram_user_id = str(row.get("telegram_user_id") or "").strip()
            try:
                discord_roles = discord_roles_by_user.get(discord_user_id, [])
                telegram_roles = ExternalRolesSyncService._build_telegram_snapshot(telegram_user_id)
                changed = ExternalRolesSyncService.sync_account_role_bindings(account_id, discord_roles, telegram_roles)
                if changed:
                    synced += 1
            except Exception:
                errors += 1
                logger.exception(
                    "external roles sync account failed account_id=%s discord_user_id=%s telegram_user_id=%s",
                    account_id,
                    discord_user_id,
                    telegram_user_id,
                )

        logger.info("external roles sync finished processed=%s synced=%s errors=%s", processed, synced, errors)
        return {"processed": processed, "synced": synced, "errors": errors}

    @staticmethod
    def sync_account_by_account_id(bot: discord.Client, account_id: str) -> bool:
        if not db.supabase or not account_id:
            logger.warning("external roles force sync skipped invalid account_id=%s", account_id)
            return False
        try:
            response = (
                db.supabase.table("account_links_registry")
                .select("account_id,discord_user_id,telegram_user_id")
                .eq("account_id", str(account_id))
                .limit(1)
                .execute()
            )
            rows = response.data or []
            if not rows:
                logger.warning("external roles force sync skipped no account_links_registry row account_id=%s", account_id)
                return False
            row = rows[0]
            discord_user_id = str(row.get("discord_user_id") or "").strip()
            telegram_user_id = str(row.get("telegram_user_id") or "").strip()
            if not discord_user_id or not telegram_user_id:
                logger.warning(
                    "external roles force sync skipped incomplete links account_id=%s discord_user_id=%s telegram_user_id=%s",
                    account_id,
                    discord_user_id,
                    telegram_user_id,
                )
                return False

            discord_roles = ExternalRolesSyncService._collect_discord_roles(bot).get(discord_user_id, [])
            telegram_roles = ExternalRolesSyncService._build_telegram_snapshot(telegram_user_id)
            changed = ExternalRolesSyncService.sync_account_role_bindings(account_id, discord_roles, telegram_roles)
            logger.info(
                "external roles force sync completed account_id=%s discord_roles=%s telegram_roles=%s changed=%s",
                account_id,
                len(discord_roles),
                len(telegram_roles),
                changed,
            )
            return changed
        except Exception:
            logger.exception("external roles force sync failed account_id=%s", account_id)
            return False

    @staticmethod
    def sync_account_role_bindings(
        account_id: str,
        discord_roles: Iterable[dict[str, str]],
        telegram_roles: Iterable[dict[str, str]],
    ) -> bool:
        if not db.supabase or not account_id:
            return False

        now = datetime.now(timezone.utc).isoformat()
        changed = ExternalRolesSyncService._sync_source(account_id, "discord", discord_roles, now)
        changed = ExternalRolesSyncService._sync_source(account_id, "telegram", telegram_roles, now) or changed
        return changed

    @staticmethod
    def get_last_sync_at(account_id: str) -> str | None:
        if not db.supabase or not account_id:
            return None
        try:
            response = (
                db.supabase.table("external_role_bindings")
                .select("last_synced_at")
                .eq("account_id", str(account_id))
                .is_("deleted_at", "null")
                .order("last_synced_at", desc=True)
                .limit(1)
                .execute()
            )
            rows = response.data or []
            return rows[0].get("last_synced_at") if rows else None
        except Exception:
            logger.exception("external roles last sync lookup failed account_id=%s", account_id)
            return None

    @staticmethod
    def _load_linked_accounts() -> list[dict]:
        try:
            response = (
                db.supabase.table("account_links_registry")
                .select("account_id,discord_user_id,telegram_user_id")
                .not_.is_("discord_user_id", "null")
                .not_.is_("telegram_user_id", "null")
                .execute()
            )
            return response.data or []
        except Exception:
            logger.exception("external roles sync failed to load linked accounts")
            return []

    @staticmethod
    def _collect_discord_roles(bot: discord.Client) -> dict[str, list[dict[str, str]]]:
        roles_by_user: dict[str, list[dict[str, str]]] = defaultdict(list)
        guilds = getattr(bot, "guilds", None)
        if guilds is None:
            logger.error(
                "external roles sync skipped discord snapshot: bot has no guilds attribute bot_type=%s",
                type(bot).__name__,
            )
            return roles_by_user

        for guild in bot.guilds:
            for member in guild.members:
                if getattr(member, "bot", False):
                    continue
                user_key = str(getattr(member, "id", "") or "").strip()
                if not user_key:
                    continue
                for role in member.roles:
                    if role.is_default():
                        continue
                    roles_by_user[user_key].append(
                        {
                            "external_role_id": str(role.id),
                            "external_role_name": str(role.name),
                        }
                    )
        return roles_by_user

    @staticmethod
    def _build_telegram_snapshot(telegram_user_id: str) -> list[dict[str, str]]:
        if not telegram_user_id:
            return []
        return [{"external_role_id": "linked", "external_role_name": "Telegram linked"}]

    @staticmethod
    def _sync_source(account_id: str, source: str, roles: Iterable[dict[str, str]], synced_at: str) -> bool:
        role_payloads: list[dict[str, str | None]] = []
        for role in roles:
            role_id = str(role.get("external_role_id") or "").strip()
            role_name = str(role.get("external_role_name") or "").strip()
            if role_id and role_name:
                role_payloads.append(
                    {
                        "account_id": str(account_id),
                        "source": source,
                        "external_role_id": role_id,
                        "external_role_name": role_name,
                        "last_synced_at": synced_at,
                        "deleted_at": None,
                    }
                )

        try:
            existing_response = (
                db.supabase.table("external_role_bindings")
                .select("external_role_id")
                .eq("account_id", str(account_id))
                .eq("source", source)
                .is_("deleted_at", "null")
                .execute()
            )
            existing_ids = {
                str(row.get("external_role_id") or "").strip()
                for row in (existing_response.data or [])
                if str(row.get("external_role_id") or "").strip()
            }
        except Exception:
            logger.exception(
                "external roles sync failed to load existing bindings account_id=%s source=%s",
                account_id,
                source,
            )
            return False

        incoming_ids = {str(item["external_role_id"]) for item in role_payloads}
        to_soft_delete = sorted(existing_ids - incoming_ids)
        changed = False

        for payload in role_payloads:
            try:
                db.supabase.table("external_role_bindings").upsert(
                    payload,
                    on_conflict="account_id,source,external_role_id",
                ).execute()
                changed = True
            except TypeError:
                db.supabase.table("external_role_bindings").upsert(payload).execute()
                changed = True
            except Exception:
                logger.exception(
                    "external roles sync upsert failed account_id=%s source=%s role_id=%s",
                    account_id,
                    source,
                    payload.get("external_role_id"),
                )

        if to_soft_delete:
            try:
                (
                    db.supabase.table("external_role_bindings")
                    .update({"deleted_at": synced_at, "last_synced_at": synced_at})
                    .eq("account_id", str(account_id))
                    .eq("source", source)
                    .in_("external_role_id", to_soft_delete)
                    .is_("deleted_at", "null")
                    .execute()
                )
                changed = True
            except Exception:
                logger.exception(
                    "external roles sync soft-delete failed account_id=%s source=%s missing_ids=%s",
                    account_id,
                    source,
                    to_soft_delete,
                )
        return changed
