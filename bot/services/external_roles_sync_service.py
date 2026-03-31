"""
Назначение: модуль "external roles sync service" реализует продуктовый контур в зоне общая логика.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: общая логика.
Доменные операции: операции синхронизации внешних ролей и статусов.
"""

import logging
import threading
from collections import defaultdict
from datetime import datetime, timezone
from typing import Iterable

import discord

from bot.data import db

logger = logging.getLogger(__name__)


class ExternalRolesSyncService:
    """Sync snapshot of external roles for linked accounts."""

    UPSERT_BATCH_SIZE = 500

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
        account_ids = [str(row.get("account_id") or "").strip() for row in linked_rows if str(row.get("account_id") or "").strip()]
        try:
            existing_bindings = ExternalRolesSyncService._load_existing_bindings(account_ids)
        except Exception:
            logger.exception("external roles sync aborted: failed to preload existing bindings")
            return {"processed": 0, "synced": 0, "errors": 1}
        processed = 0
        synced = 0
        errors = 0
        upsert_payloads: list[dict[str, str | None]] = []
        soft_delete_targets: list[dict[str, object]] = []

        for row in linked_rows:
            processed += 1
            account_id = str(row.get("account_id") or "").strip()
            discord_user_id = str(row.get("discord_user_id") or "").strip()
            telegram_user_id = str(row.get("telegram_user_id") or "").strip()
            try:
                discord_roles = discord_roles_by_user.get(discord_user_id, [])
                telegram_roles = ExternalRolesSyncService._build_telegram_snapshot(telegram_user_id)
                plan = ExternalRolesSyncService._build_account_sync_plan(
                    account_id,
                    discord_roles,
                    telegram_roles,
                    existing_bindings.get(account_id),
                )
                if plan["changed"]:
                    synced += 1
                upsert_payloads.extend(plan["upserts"])
                soft_delete_targets.extend(plan["soft_deletes"])
            except Exception:
                errors += 1
                logger.exception(
                    "external roles sync account planning failed account_id=%s discord_user_id=%s telegram_user_id=%s",
                    account_id,
                    discord_user_id,
                    telegram_user_id,
                )

        batch_errors = ExternalRolesSyncService._apply_sync_changes(upsert_payloads, soft_delete_targets)
        errors += batch_errors
        logger.info(
            "external roles sync finished processed=%s synced=%s errors=%s upserts=%s soft_deletes=%s",
            processed,
            synced,
            errors,
            len(upsert_payloads),
            len(soft_delete_targets),
        )
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
            existing_bindings = ExternalRolesSyncService._load_existing_bindings([str(account_id)]).get(str(account_id))
            changed = ExternalRolesSyncService.sync_account_role_bindings(
                account_id,
                discord_roles,
                telegram_roles,
                existing_bindings=existing_bindings,
            )
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
    def trigger_account_sync(account_id: str, *, reason: str, bot: discord.Client | None = None) -> bool:
        normalized_account_id = str(account_id or "").strip()
        if not normalized_account_id:
            logger.warning("external roles async trigger skipped empty account_id reason=%s", reason)
            return False

        runtime_bot = bot
        if runtime_bot is None:
            try:
                from bot.commands.base import bot as runtime_bot
            except Exception:
                logger.exception(
                    "external roles async trigger failed to resolve discord bot account_id=%s reason=%s",
                    normalized_account_id,
                    reason,
                )
                return False

        worker = threading.Thread(
            target=ExternalRolesSyncService._run_triggered_account_sync,
            args=(runtime_bot, normalized_account_id, reason),
            daemon=True,
            name=f"external-roles-sync-{normalized_account_id}",
        )
        worker.start()
        return True

    @staticmethod
    def _run_triggered_account_sync(bot: discord.Client, account_id: str, reason: str) -> None:
        try:
            changed = ExternalRolesSyncService.sync_account_by_account_id(bot, account_id)
            logger.info(
                "external roles async trigger completed account_id=%s reason=%s changed=%s",
                account_id,
                reason,
                changed,
            )
        except Exception:
            logger.exception(
                "external roles async trigger failed account_id=%s reason=%s",
                account_id,
                reason,
            )

    @staticmethod
    def sync_account_role_bindings(
        account_id: str,
        discord_roles: Iterable[dict[str, str]],
        telegram_roles: Iterable[dict[str, str]],
        *,
        existing_bindings: dict[str, list[dict[str, str]]] | None = None,
    ) -> bool:
        if not db.supabase or not account_id:
            return False

        plan = ExternalRolesSyncService._build_account_sync_plan(
            account_id,
            discord_roles,
            telegram_roles,
            existing_bindings,
        )
        if not plan["changed"]:
            return False
        errors = ExternalRolesSyncService._apply_sync_changes(plan["upserts"], plan["soft_deletes"])
        return errors == 0

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
    def _load_existing_bindings(account_ids: Iterable[str]) -> dict[str, dict[str, list[dict[str, str]]]]:
        normalized_ids = sorted({str(account_id or "").strip() for account_id in account_ids if str(account_id or "").strip()})
        bindings: dict[str, dict[str, list[dict[str, str]]]] = defaultdict(lambda: defaultdict(list))
        if not normalized_ids:
            return {}
        try:
            response = (
                db.supabase.table("external_role_bindings")
                .select("account_id,source,external_role_id,external_role_name")
                .in_("account_id", normalized_ids)
                .is_("deleted_at", "null")
                .execute()
            )
            for row in response.data or []:
                account_id = str(row.get("account_id") or "").strip()
                source = str(row.get("source") or "").strip()
                role_id = str(row.get("external_role_id") or "").strip()
                role_name = str(row.get("external_role_name") or "").strip()
                if not account_id or not source or not role_id or not role_name:
                    continue
                bindings[account_id][source].append(
                    {
                        "external_role_id": role_id,
                        "external_role_name": role_name,
                    }
                )
        except Exception:
            logger.exception(
                "external roles sync failed to load existing bindings account_ids=%s",
                normalized_ids,
            )
            raise
        return {account_id: dict(source_map) for account_id, source_map in bindings.items()}

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
    def _build_account_sync_plan(
        account_id: str,
        discord_roles: Iterable[dict[str, str]],
        telegram_roles: Iterable[dict[str, str]],
        existing_bindings: dict[str, list[dict[str, str]]] | None = None,
    ) -> dict[str, object]:
        normalized_account_id = str(account_id or "").strip()
        if not normalized_account_id:
            return {"changed": False, "upserts": [], "soft_deletes": []}

        now = datetime.now(timezone.utc).isoformat()
        existing_bindings = existing_bindings or {}
        discord_plan = ExternalRolesSyncService._build_source_sync_plan(
            normalized_account_id,
            "discord",
            discord_roles,
            now,
            existing_bindings.get("discord"),
        )
        telegram_plan = ExternalRolesSyncService._build_source_sync_plan(
            normalized_account_id,
            "telegram",
            telegram_roles,
            now,
            existing_bindings.get("telegram"),
        )
        return {
            "changed": bool(discord_plan["changed"] or telegram_plan["changed"]),
            "upserts": [*discord_plan["upserts"], *telegram_plan["upserts"]],
            "soft_deletes": [*discord_plan["soft_deletes"], *telegram_plan["soft_deletes"]],
        }

    @staticmethod
    def _build_source_sync_plan(
        account_id: str,
        source: str,
        roles: Iterable[dict[str, str]],
        synced_at: str,
        existing_rows: Iterable[dict[str, str]] | None,
    ) -> dict[str, object]:
        incoming_map = ExternalRolesSyncService._normalize_role_map(roles)
        existing_map = ExternalRolesSyncService._normalize_role_map(existing_rows or [])
        if incoming_map == existing_map:
            return {"changed": False, "upserts": [], "soft_deletes": []}

        upserts = [
            {
                "account_id": str(account_id),
                "source": source,
                "external_role_id": role_id,
                "external_role_name": role_name,
                "last_synced_at": synced_at,
                "deleted_at": None,
            }
            for role_id, role_name in sorted(incoming_map.items())
        ]
        to_soft_delete = sorted(set(existing_map) - set(incoming_map))
        soft_deletes = []
        if to_soft_delete:
            soft_deletes.append(
                {
                    "account_id": str(account_id),
                    "source": source,
                    "role_ids": to_soft_delete,
                    "synced_at": synced_at,
                }
            )
        return {"changed": True, "upserts": upserts, "soft_deletes": soft_deletes}

    @staticmethod
    def _normalize_role_map(roles: Iterable[dict[str, str]]) -> dict[str, str]:
        normalized: dict[str, str] = {}
        for role in roles:
            role_id = str(role.get("external_role_id") or "").strip()
            role_name = str(role.get("external_role_name") or "").strip()
            if role_id and role_name:
                normalized[role_id] = role_name
        return normalized

    @staticmethod
    def _apply_sync_changes(
        upsert_payloads: list[dict[str, str | None]],
        soft_delete_targets: list[dict[str, object]],
    ) -> int:
        errors = 0
        for chunk in ExternalRolesSyncService._chunked(upsert_payloads, ExternalRolesSyncService.UPSERT_BATCH_SIZE):
            try:
                ExternalRolesSyncService._upsert_batch(chunk)
            except Exception:
                errors += 1
                role_ids = [str(item.get("external_role_id") or "") for item in chunk]
                logger.exception(
                    "external roles sync bulk upsert failed payload_count=%s role_ids=%s",
                    len(chunk),
                    role_ids,
                )

        for delete_target in soft_delete_targets:
            account_id = str(delete_target.get("account_id") or "").strip()
            source = str(delete_target.get("source") or "").strip()
            synced_at = str(delete_target.get("synced_at") or "").strip()
            role_ids = [str(role_id or "").strip() for role_id in list(delete_target.get("role_ids") or []) if str(role_id or "").strip()]
            if not account_id or not source or not synced_at or not role_ids:
                continue
            try:
                (
                    db.supabase.table("external_role_bindings")
                    .update({"deleted_at": synced_at, "last_synced_at": synced_at})
                    .eq("account_id", account_id)
                    .eq("source", source)
                    .in_("external_role_id", role_ids)
                    .is_("deleted_at", "null")
                    .execute()
                )
            except Exception:
                errors += 1
                logger.exception(
                    "external roles sync soft-delete failed account_id=%s source=%s missing_ids=%s",
                    account_id,
                    source,
                    role_ids,
                )
        return errors

    @staticmethod
    def _upsert_batch(payloads: list[dict[str, str | None]]) -> None:
        if not payloads:
            return
        try:
            db.supabase.table("external_role_bindings").upsert(
                payloads,
                on_conflict="account_id,source,external_role_id",
            ).execute()
        except TypeError:
            db.supabase.table("external_role_bindings").upsert(payloads).execute()

    @staticmethod
    def _chunked(items: list[dict[str, str | None]], chunk_size: int) -> Iterable[list[dict[str, str | None]]]:
        if chunk_size <= 0:
            yield items
            return
        for index in range(0, len(items), chunk_size):
            yield items[index:index + chunk_size]
