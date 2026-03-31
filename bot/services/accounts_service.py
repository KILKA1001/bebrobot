"""
Назначение: модуль "accounts service" реализует продуктовый контур в зоне общая логика.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: общая логика.
Доменные операции: операции аккаунтов, связки профилей и миграции.
"""

import logging
import os
import secrets
import string
import uuid
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

from bot.data import db
from bot.legacy_identity_logging import log_legacy_schema_fallback
from bot.services.auth import RoleResolver

logger = logging.getLogger(__name__)
_ACCOUNT_ID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
)
_ACCOUNT_ID_CACHE_MISS = object()


class AccountsService:
    """Общий сервис аккаунтов/identity/linking без привязки к API мессенджеров."""

    LINK_CODE_LEN = 8
    LINK_TTL_MINUTES = 10
    MAX_ATTEMPTS = 5
    LINK_CODE_GENERATION_ATTEMPTS = 3
    LINK_CODES_TABLES = ("account_link_codes", "link_tokens")
    PROFILE_FIELDS_CONFIG = {
        "custom_nick": {"default": "Игрок", "max_length": 32, "label": "Никнейм"},
        "description": {"default": "—", "max_length": 100, "label": "Описание"},
        "nulls_brawl_id": {"default": "—", "max_length": 32, "label": "Null's Brawl ID"},
    }
    _account_titles_cache: dict[str, list[str]] = {}
    _account_id_cache: dict[tuple[str, str], tuple[float, str | None]] = {}
    _title_roles_cache: dict[int, str] | None = None
    MAX_VISIBLE_PROFILE_ROLES = 3
    ACCOUNT_ID_CACHE_TTL_SEC = int(os.getenv("ACCOUNT_ID_CACHE_TTL_SEC", "300"))
    FALLBACK_CHAT_MEMBER_TITLE = "участник чата"

    @staticmethod
    def _account_id_cache_key(provider: str, provider_user_id: str) -> tuple[str, str]:
        return (str(provider or "").strip().lower(), str(provider_user_id or "").strip())

    @staticmethod
    def _get_cached_account_id(provider: str, provider_user_id: str) -> str | None | object:
        cache_key = AccountsService._account_id_cache_key(provider, provider_user_id)
        cached_entry = AccountsService._account_id_cache.get(cache_key)
        if cached_entry is None:
            return _ACCOUNT_ID_CACHE_MISS

        expires_at, cached_account_id = cached_entry
        now = time.monotonic()
        if expires_at <= now:
            AccountsService._account_id_cache.pop(cache_key, None)
            logger.debug(
                "resolve_account_id cache expired provider=%s provider_user_id=%s",
                cache_key[0],
                cache_key[1],
            )
            return _ACCOUNT_ID_CACHE_MISS

        logger.debug(
            "resolve_account_id cache hit provider=%s provider_user_id=%s account_id=%s",
            cache_key[0],
            cache_key[1],
            cached_account_id,
        )
        return cached_account_id

    @staticmethod
    def _cache_account_id(provider: str, provider_user_id: str, account_id: str | None) -> None:
        cache_key = AccountsService._account_id_cache_key(provider, provider_user_id)
        ttl_sec = max(1, int(AccountsService.ACCOUNT_ID_CACHE_TTL_SEC))
        AccountsService._account_id_cache[cache_key] = (time.monotonic() + ttl_sec, str(account_id).strip() or None)

    @staticmethod
    def invalidate_account_id_cache(provider: str, provider_user_id: str) -> None:
        cache_key = AccountsService._account_id_cache_key(provider, provider_user_id)
        AccountsService._account_id_cache.pop(cache_key, None)

    @staticmethod
    def _load_account_identity_rows(account_id: str) -> list[dict]:
        account_key = str(account_id or "").strip()
        if not db.supabase or not account_key:
            return []

        select_variants = (
            "account_id,provider,provider_user_id,username,provider_username,display_name,provider_display_name,global_username",
            "account_id,provider,provider_user_id,username,display_name,global_username",
            "account_id,provider,provider_user_id,username,display_name",
            "account_id,provider,provider_user_id",
        )
        last_error: Exception | None = None
        for select_clause in select_variants:
            try:
                response = (
                    db.supabase.table("account_identities")
                    .select(select_clause)
                    .eq("account_id", account_key)
                    .execute()
                )
                return response.data or []
            except Exception as error:
                last_error = error
                logger.warning(
                    "account identity rows select failed account_id=%s select=%s error=%s",
                    account_key,
                    select_clause,
                    AccountsService._format_db_error(error),
                )

        if last_error:
            logger.warning(
                "account identity rows exhausted select variants account_id=%s error=%s",
                account_key,
                AccountsService._format_db_error(last_error),
            )
        return []

    @staticmethod
    def _load_identity_row(provider: str, provider_user_id: str) -> dict[str, object] | None:
        normalized_provider = str(provider or "").strip().lower()
        normalized_user_id = str(provider_user_id or "").strip()
        if not db.supabase or normalized_provider not in {"telegram", "discord"} or not normalized_user_id:
            return None

        select_variants = (
            "account_id,provider,provider_user_id,username,provider_username,display_name,provider_display_name,global_username",
            "account_id,provider,provider_user_id,username,display_name,global_username",
            "account_id,provider,provider_user_id,username,display_name",
            "account_id,provider,provider_user_id",
        )
        last_error: Exception | None = None
        for select_clause in select_variants:
            try:
                response = (
                    db.supabase.table("account_identities")
                    .select(select_clause)
                    .eq("provider", normalized_provider)
                    .eq("provider_user_id", normalized_user_id)
                    .limit(1)
                    .execute()
                )
                rows = response.data or []
                if rows:
                    return dict(rows[0])
                return None
            except Exception as error:
                last_error = error
                logger.warning(
                    "identity row lookup failed provider=%s user_id=%s select=%s error=%s",
                    normalized_provider,
                    normalized_user_id,
                    select_clause,
                    AccountsService._format_db_error(error),
                )

        if last_error:
            logger.warning(
                "identity row lookup exhausted select variants provider=%s user_id=%s error=%s",
                normalized_provider,
                normalized_user_id,
                AccountsService._format_db_error(last_error),
            )
        return None

    @staticmethod
    def _load_account_custom_nick(account_id: str) -> str | None:
        account_key = str(account_id or "").strip()
        if not db.supabase or not account_key:
            return None

        try:
            response = (
                db.supabase.table("accounts")
                .select("custom_nick")
                .eq("id", account_key)
                .limit(1)
                .execute()
            )
            rows = response.data or []
            if not rows:
                return None
            raw_custom_nick = str(rows[0].get("custom_nick") or "").strip()
            default_nick = str(AccountsService.PROFILE_FIELDS_CONFIG["custom_nick"]["default"]).strip()
            if raw_custom_nick and raw_custom_nick != default_nick:
                return raw_custom_nick
        except Exception as error:
            logger.exception(
                "account custom nick lookup failed account_id=%s error=%s",
                account_key,
                AccountsService._format_db_error(error),
            )
        return None

    @staticmethod
    def get_public_identity_context(
        provider: str | None,
        provider_user_id: str | int | None,
        *,
        account_id: str | None = None,
    ) -> dict[str, str | bool | None]:
        normalized_provider = str(provider or "").strip().lower() or None
        normalized_user_id = str(provider_user_id).strip() if provider_user_id is not None else ""
        resolved_account_id = str(account_id or "").strip() or None

        identity_row: dict[str, object] | None = None
        identity_rows: list[dict] = []
        if resolved_account_id:
            identity_rows = AccountsService._load_account_identity_rows(resolved_account_id)
            if normalized_provider and normalized_user_id:
                identity_row = next(
                    (
                        row
                        for row in identity_rows
                        if str(row.get("provider") or "").strip() == normalized_provider
                        and str(row.get("provider_user_id") or "").strip() == normalized_user_id
                    ),
                    None,
                )
            if not identity_row:
                identity_row = AccountsService._preferred_identity_for_account(
                    resolved_account_id,
                    identity_rows,
                    provider_hint=normalized_provider,
                    default_provider=normalized_provider,
                )
        elif normalized_provider and normalized_user_id:
            identity_row = AccountsService._load_identity_row(normalized_provider, normalized_user_id)
            resolved_account_id = str(identity_row.get("account_id") or "").strip() or None if identity_row else None
            if resolved_account_id:
                identity_rows = AccountsService._load_account_identity_rows(resolved_account_id)
                if not identity_row:
                    identity_row = AccountsService._preferred_identity_for_account(
                        resolved_account_id,
                        identity_rows,
                        provider_hint=normalized_provider,
                        default_provider=normalized_provider,
                    )

        custom_nick = AccountsService._load_account_custom_nick(resolved_account_id or "") if resolved_account_id else None
        username = None
        display_name = None
        global_username = None
        if identity_row:
            username = AccountsService._candidate_username(identity_row)
            display_name = str(identity_row.get("display_name") or identity_row.get("provider_display_name") or "").strip() or None
            global_username = str(identity_row.get("global_username") or "").strip() or None

        best_public_name = None
        name_source = None
        if custom_nick:
            best_public_name = custom_nick
            name_source = "custom_nick"
        elif display_name:
            best_public_name = display_name
            name_source = "display_name"
        elif username:
            best_public_name = username
            name_source = "username"
        elif global_username:
            best_public_name = global_username
            name_source = "global_username"

        logger.info(
            "public identity context resolved provider=%s user_id=%s account_id=%s nickname_source_found=%s name_source=%s",
            normalized_provider,
            normalized_user_id or None,
            resolved_account_id,
            bool(name_source),
            name_source,
        )
        return {
            "provider": normalized_provider,
            "user_id": normalized_user_id or None,
            "account_id": resolved_account_id,
            "username": username,
            "display_name": display_name,
            "global_username": global_username,
            "custom_nick": custom_nick,
            "best_public_name": best_public_name,
            "name_source": name_source,
            "nickname_source_found": bool(name_source),
        }

    @staticmethod
    def get_best_public_name(
        provider: str | None,
        provider_user_id: str | int | None,
        *,
        account_id: str | None = None,
    ) -> str | None:
        context = AccountsService.get_public_identity_context(
            provider,
            provider_user_id,
            account_id=account_id,
        )
        return str(context.get("best_public_name") or "").strip() or None

    @staticmethod
    def _load_identity_rows_for_lookup(provider: str) -> list[dict]:
        if not db.supabase:
            return []

        select_variants = (
            "account_id,provider_user_id,username,provider_username,display_name,provider_display_name,global_username",
            "account_id,provider_user_id,username,display_name",
            "account_id,provider_user_id",
        )
        last_error: Exception | None = None
        for select_clause in select_variants:
            try:
                response = (
                    db.supabase.table("account_identities")
                    .select(select_clause)
                    .eq("provider", str(provider))
                    .execute()
                )
                return response.data or []
            except Exception as error:
                last_error = error
                logger.warning(
                    "identity lookup select failed provider=%s select=%s error=%s",
                    provider,
                    select_clause,
                    AccountsService._format_db_error(error),
                )

        if last_error:
            logger.warning(
                "identity lookup exhausted select variants provider=%s error=%s",
                provider,
                AccountsService._format_db_error(last_error),
            )
        return []

    @staticmethod
    def find_accounts_by_identity_username(provider: str, username: str) -> list[dict[str, str | None]]:
        normalized = str(username or "").strip()
        if provider == "telegram":
            normalized = normalized.lstrip("@")
        if not normalized:
            return []

        rows = AccountsService._load_identity_rows_for_lookup(provider)
        matches: list[dict[str, str | None]] = []
        seen: set[tuple[str | None, str | None]] = set()
        normalized_lower = normalized.casefold()
        for row in rows:
            fields = [
                row.get("username"),
                row.get("provider_username"),
                row.get("display_name"),
                row.get("provider_display_name"),
                row.get("global_username"),
            ]
            matched_by: str | None = None
            for field_name, raw_value in zip(
                ("username", "provider_username", "display_name", "provider_display_name", "global_username"),
                fields,
            ):
                value = str(raw_value or "").strip()
                if not value:
                    continue
                candidate = value.lstrip("@") if provider == "telegram" else value
                if candidate.casefold() == normalized_lower:
                    matched_by = field_name
                    break

            if not matched_by and normalized.isdigit() and str(row.get("provider_user_id") or "").strip() == normalized:
                matched_by = "provider_user_id"

            if not matched_by:
                continue

            key = (str(row.get("account_id") or "").strip() or None, str(row.get("provider_user_id") or "").strip() or None)
            if key in seen:
                continue
            seen.add(key)
            matches.append(
                {
                    "account_id": key[0],
                    "provider_user_id": key[1],
                    "matched_by": matched_by,
                    "username": str(row.get("username") or row.get("provider_username") or "").strip() or None,
                    "display_name": str(
                        row.get("display_name") or row.get("provider_display_name") or row.get("global_username") or ""
                    ).strip()
                    or None,
                }
            )
        return matches

    @staticmethod
    def _preferred_identity_for_account(
        account_id: str,
        rows: list[dict],
        *,
        provider_hint: str | None = None,
        default_provider: str | None = None,
    ) -> dict[str, str | None] | None:
        account_key = str(account_id or "").strip()
        if not account_key:
            return None

        identities = [row for row in rows if str(row.get("account_id") or "").strip() == account_key]
        if not identities:
            return None

        preferred_order = [provider_hint, default_provider, "telegram", "discord"]
        for preferred_provider in preferred_order:
            if not preferred_provider:
                continue
            matched = next((row for row in identities if str(row.get("provider") or "").strip() == preferred_provider), None)
            if matched:
                return matched
        return identities[0]

    @staticmethod
    def _candidate_display_name(row: dict[str, object]) -> str | None:
        for key in ("display_name", "provider_display_name", "global_username", "username", "provider_username"):
            value = str(row.get(key) or "").strip()
            if value:
                return value
        return None

    @staticmethod
    def _candidate_username(row: dict[str, object]) -> str | None:
        for key in ("username", "provider_username", "global_username"):
            value = str(row.get(key) or "").strip()
            if value:
                return value.lstrip("@")
        return None

    @staticmethod
    def resolve_user_lookup(
        lookup_value: str | None,
        *,
        default_provider: str | None = None,
    ) -> dict[str, object]:
        token = str(lookup_value or "").strip()
        if not token:
            return {"status": "invalid", "reason": "empty_lookup", "lookup_value": token, "candidates": []}

        provider_hint: str | None = None
        token_without_prefix = token
        lowered = token.casefold()
        if lowered.startswith("tg:") or lowered.startswith("telegram:"):
            provider_hint = "telegram"
            token_without_prefix = token.split(":", 1)[1].strip()
        elif lowered.startswith("ds:") or lowered.startswith("discord:"):
            provider_hint = "discord"
            token_without_prefix = token.split(":", 1)[1].strip()

        effective_provider = provider_hint or (str(default_provider or "").strip().lower() or None)
        providers = [effective_provider] if effective_provider in {"telegram", "discord"} else ["telegram", "discord"]

        rows: list[dict] = []
        for provider in providers:
            provider_rows = AccountsService._load_identity_rows_for_lookup(provider)
            for row in provider_rows:
                rows.append({**row, "provider": provider})

        normalized = token_without_prefix.strip()
        normalized_username = normalized.lstrip("@")
        normalized_casefold = normalized.casefold()
        normalized_username_casefold = normalized_username.casefold()

        candidates: list[dict[str, str | None]] = []
        seen: set[tuple[str | None, str | None, str | None]] = set()

        def _append_candidate(row: dict, matched_by: str) -> None:
            candidate = {
                "account_id": str(row.get("account_id") or "").strip() or None,
                "provider": str(row.get("provider") or "").strip() or None,
                "provider_user_id": str(row.get("provider_user_id") or "").strip() or None,
                "display_name": AccountsService._candidate_display_name(row),
                "username": AccountsService._candidate_username(row),
                "matched_by": matched_by,
            }
            key = (candidate["account_id"], candidate["provider"], candidate["provider_user_id"])
            if key in seen:
                return
            seen.add(key)
            candidates.append(candidate)

        for row in rows:
            provider = str(row.get("provider") or "").strip()
            fields = [
                ("telegram_username" if provider == "telegram" else "discord_username", row.get("username")),
                ("provider_username", row.get("provider_username")),
                ("display_name", row.get("display_name")),
                ("provider_display_name", row.get("provider_display_name")),
                ("global_username", row.get("global_username")),
            ]
            for matched_by, raw_value in fields:
                value = str(raw_value or "").strip()
                if not value:
                    continue
                value_casefold = value.casefold()
                value_no_at_casefold = value.lstrip("@").casefold()
                if value_casefold == normalized_casefold or value_no_at_casefold == normalized_username_casefold:
                    _append_candidate(row, matched_by)
                    break

        if candidates:
            if len(candidates) == 1:
                return {"status": "ok", "lookup_value": token, "candidates": candidates, "result": candidates[0]}
            return {"status": "multiple", "lookup_value": token, "candidates": candidates}

        if normalized.isdigit():
            for row in rows:
                if str(row.get("provider_user_id") or "").strip() == normalized:
                    _append_candidate(row, "exact_id")
            if candidates:
                if len(candidates) == 1:
                    return {"status": "ok", "lookup_value": token, "candidates": candidates, "result": candidates[0]}
                return {"status": "multiple", "lookup_value": token, "candidates": candidates}

        if _ACCOUNT_ID_RE.match(normalized):
            preferred_identity = AccountsService._preferred_identity_for_account(
                normalized,
                rows,
                provider_hint=provider_hint,
                default_provider=default_provider,
            )
            if preferred_identity:
                _append_candidate(preferred_identity, "account_id")
                return {"status": "ok", "lookup_value": token, "candidates": candidates, "result": candidates[0]}

        return {"status": "not_found", "lookup_value": token, "candidates": [], "reason": "not_found"}

    @staticmethod
    def persist_identity_lookup_fields(
        provider: str,
        provider_user_id: str,
        *,
        username: str | None = None,
        display_name: str | None = None,
        global_username: str | None = None,
    ) -> None:
        if not db.supabase:
            return

        normalized_provider = str(provider or "").strip()
        normalized_provider_user_id = str(provider_user_id or "").strip()
        if not normalized_provider or not normalized_provider_user_id:
            logger.warning(
                "persist_identity_lookup_fields skipped invalid identity provider=%s provider_user_id=%s",
                provider,
                provider_user_id,
            )
            return

        normalized_username = str(username or "").lstrip("@").strip() or None
        normalized_display_name = str(display_name or "").strip() or None
        normalized_global_username = str(global_username or "").strip() or None

        payload = {
            "provider": normalized_provider,
            "provider_user_id": normalized_provider_user_id,
        }
        optional_fields = {
            "username": normalized_username,
            "display_name": normalized_display_name,
            "global_username": normalized_global_username,
        }
        for key, value in optional_fields.items():
            if value:
                payload[key] = value

        if len(payload) <= 2:
            return

        payload_variants: list[dict[str, str]] = []
        variant_keys = [
            ("username", "display_name", "global_username"),
            ("username", "display_name"),
            ("username", "global_username"),
            ("display_name", "global_username"),
            ("username",),
            ("display_name",),
            ("global_username",),
        ]
        for keys in variant_keys:
            variant = {
                "provider": normalized_provider,
                "provider_user_id": normalized_provider_user_id,
            }
            for key in keys:
                value = optional_fields.get(key)
                if value:
                    variant[key] = value
            if len(variant) <= 2:
                continue
            if variant not in payload_variants:
                payload_variants.append(variant)

        def _update_existing_identity(variant: dict[str, str]) -> bool:
            update_payload = {
                key: value
                for key, value in variant.items()
                if key not in {"provider", "provider_user_id"}
            }
            if not update_payload:
                return False
            try:
                response = (
                    db.supabase.table("account_identities")
                    .update(update_payload)
                    .eq("provider", normalized_provider)
                    .eq("provider_user_id", normalized_provider_user_id)
                    .execute()
                )
                updated_rows = list(response.data or [])
                if updated_rows:
                    if variant != payload:
                        logger.info(
                            "persist_identity_lookup_fields updated existing row with fallback columns provider=%s provider_user_id=%s payload_keys=%s requested_keys=%s",
                            normalized_provider,
                            normalized_provider_user_id,
                            sorted(update_payload.keys()),
                            sorted(k for k in payload.keys() if k not in {"provider", "provider_user_id"}),
                        )
                    return True
            except Exception as error:
                logger.warning(
                    "persist_identity_lookup_fields update failed provider=%s provider_user_id=%s payload_keys=%s error=%s",
                    normalized_provider,
                    normalized_provider_user_id,
                    sorted(update_payload.keys()),
                    AccountsService._format_db_error(error),
                )
            return False

        def _is_missing_account_id_violation(error: Exception) -> bool:
            lowered = AccountsService._format_db_error(error).lower()
            return "null value in column \"account_id\"" in lowered and "23502" in lowered

        for variant in payload_variants:
            if _update_existing_identity(variant):
                return

        last_error: Exception | None = None
        for variant in payload_variants:
            try:
                db.supabase.table("account_identities").upsert(variant, on_conflict="provider,provider_user_id").execute()
                if variant != payload:
                    logger.info(
                        "persist_identity_lookup_fields saved with fallback columns provider=%s provider_user_id=%s payload_keys=%s requested_keys=%s",
                        normalized_provider,
                        normalized_provider_user_id,
                        sorted(k for k in variant.keys() if k not in {"provider", "provider_user_id"}),
                        sorted(k for k in payload.keys() if k not in {"provider", "provider_user_id"}),
                    )
                return
            except Exception as error:
                last_error = error
                if _is_missing_account_id_violation(error):
                    logger.warning(
                        "persist_identity_lookup_fields skipped insert because account_id is required provider=%s provider_user_id=%s username=%s display_name=%s global_username=%s",
                        normalized_provider,
                        normalized_provider_user_id,
                        normalized_username,
                        normalized_display_name,
                        normalized_global_username,
                    )
                    return
                logger.warning(
                    "persist_identity_lookup_fields upsert failed provider=%s provider_user_id=%s payload_keys=%s error=%s",
                    normalized_provider,
                    normalized_provider_user_id,
                    sorted(k for k in variant.keys() if k not in {"provider", "provider_user_id"}),
                    AccountsService._format_db_error(error),
                )

        if last_error:
            logger.warning(
                "persist_identity_lookup_fields exhausted payload variants provider=%s provider_user_id=%s username=%s display_name=%s global_username=%s error=%s",
                normalized_provider,
                normalized_provider_user_id,
                normalized_username,
                normalized_display_name,
                normalized_global_username,
                AccountsService._format_db_error(last_error),
            )

    @staticmethod
    def resolve_account_id(provider: str, provider_user_id: str) -> Optional[str]:
        normalized_provider, normalized_user_id = AccountsService._account_id_cache_key(provider, provider_user_id)
        if not normalized_provider or not normalized_user_id:
            logger.warning(
                "resolve_account_id skipped invalid input provider=%s provider_user_id=%s",
                provider,
                provider_user_id,
            )
            return None

        cached_account_id = AccountsService._get_cached_account_id(normalized_provider, normalized_user_id)
        if cached_account_id is not _ACCOUNT_ID_CACHE_MISS:
            return cached_account_id

        if not db.supabase:
            return None
        try:
            response = (
                db.supabase.table("account_identities")
                .select("account_id")
                .eq("provider", normalized_provider)
                .eq("provider_user_id", normalized_user_id)
                .limit(1)
                .execute()
            )
            if response.data:
                account_id = str(response.data[0].get("account_id") or "").strip() or None
                AccountsService._cache_account_id(normalized_provider, normalized_user_id, account_id)
                return account_id
            AccountsService._cache_account_id(normalized_provider, normalized_user_id, None)
        except Exception as e:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("identity_resolve_errors")
            logger.warning(
                "resolve_account_id failed provider=%s provider_user_id=%s error=%s",
                normalized_provider,
                normalized_user_id,
                AccountsService._format_db_error(e),
            )
        return None

    @staticmethod
    def _get_account_link_registry_row(account_id: str) -> Optional[dict]:
        if not db.supabase or not account_id:
            return None
        try:
            response = (
                db.supabase.table("account_links_registry")
                .select("account_id,telegram_user_id,discord_user_id,telegram_linked_at,discord_linked_at,last_link_code_used,last_link_code_used_at,has_used_link_code")
                .eq("account_id", str(account_id))
                .limit(1)
                .execute()
            )
            if response.data:
                return response.data[0]
        except Exception as e:
            logger.warning(
                "account_links_registry lookup failed account_id=%s error=%s",
                account_id,
                AccountsService._format_db_error(e),
            )
        return None

    @staticmethod
    def _is_target_already_linked(account_id: str, target_provider: str) -> bool:
        registry_row = AccountsService._get_account_link_registry_row(account_id)
        if registry_row is not None:
            target_value = registry_row.get(f"{target_provider}_user_id")
            if target_value:
                return True

        try:
            identities_response = (
                db.supabase.table("account_identities")
                .select("provider")
                .eq("account_id", str(account_id))
                .execute()
            )
            identities = identities_response.data or []
            return any(identity.get("provider") == target_provider for identity in identities)
        except Exception as e:
            logger.warning(
                "target identity lookup failed target=%s account_id=%s error=%s",
                target_provider,
                account_id,
                AccountsService._format_db_error(e),
            )
            return False

    @staticmethod
    def _registry_blocks_new_link_code(account_id: str, target_provider: str) -> bool:
        registry_row = AccountsService._get_account_link_registry_row(account_id)
        if not registry_row:
            return False

        target_value = registry_row.get(f"{target_provider}_user_id")
        if target_value:
            logger.info(
                "registry blocks code issue: target already linked account_id=%s target_provider=%s",
                account_id,
                target_provider,
            )
            return True

        has_used_link_code = bool(registry_row.get("has_used_link_code"))
        has_telegram = bool(registry_row.get("telegram_user_id"))
        has_discord = bool(registry_row.get("discord_user_id"))
        if has_used_link_code and has_telegram and has_discord:
            logger.info(
                "registry blocks code issue: link code already consumed for fully linked account_id=%s target_provider=%s",
                account_id,
                target_provider,
            )
            return True

        return False

    @staticmethod
    def _mark_registry_link_code_usage(account_id: str, link_code: str) -> None:
        if not db.supabase or not account_id or not link_code:
            return

        payload = {
            "account_id": str(account_id),
            "last_link_code_used": str(link_code),
            "last_link_code_used_at": datetime.now(timezone.utc).isoformat(),
            "has_used_link_code": True,
        }

        try:
            db.supabase.table("account_links_registry").upsert(payload, on_conflict="account_id").execute()
            return
        except TypeError:
            try:
                db.supabase.table("account_links_registry").upsert(payload).execute()
                return
            except Exception as e:
                logger.warning(
                    "account_links_registry code usage upsert failed (legacy) account_id=%s code=%s error=%s",
                    account_id,
                    link_code,
                    AccountsService._format_db_error(e),
                )
                return
        except Exception as e:
            logger.warning(
                "account_links_registry code usage upsert failed account_id=%s code=%s error=%s",
                account_id,
                link_code,
                AccountsService._format_db_error(e),
            )

    @staticmethod
    def _generate_link_code(length: int = LINK_CODE_LEN) -> str:
        alphabet = string.ascii_uppercase + string.digits
        return "".join(secrets.choice(alphabet) for _ in range(length))

    @staticmethod
    def _format_db_error(error: Exception) -> str:
        """Return readable Supabase/PostgREST error details for logs."""
        if error is None:
            return "unknown error"
        parts = [str(error)]
        for attr in ("message", "details", "hint", "code"):
            value = getattr(error, attr, None)
            if value:
                parts.append(f"{attr}={value}")
        return " | ".join(p for p in parts if p)

    @staticmethod
    def _is_unique_violation(error: Exception) -> bool:
        return str(getattr(error, "code", "")) == "23505" or "duplicate key value" in str(error).lower()

    @staticmethod
    def _is_missing_column_error(error: Exception, *, table: str, column: str) -> bool:
        code = str(getattr(error, "code", "") or "").strip()
        if code == "42703":
            return True
        lowered = AccountsService._format_db_error(error).lower()
        expected_marker = f"column {table}.{column} does not exist"
        return expected_marker in lowered


    @staticmethod
    def _rebind_account_id(from_account_id: str, to_account_id: str) -> None:
        if not from_account_id or not to_account_id or str(from_account_id) == str(to_account_id):
            return

        AccountsService._rebind_account_identities(from_account_id, to_account_id)

        for table_name in ("scores", "actions", "ticket_actions", "bank_history", "fines", "fine_payments"):
            try:
                db.supabase.table(table_name).update({"account_id": to_account_id}).eq("account_id", from_account_id).execute()
            except Exception as e:
                logger.debug(
                    "rebind_account_id skipped table=%s from_account_id=%s to_account_id=%s error=%s",
                    table_name,
                    from_account_id,
                    to_account_id,
                    AccountsService._format_db_error(e),
                )

        try:
            db.supabase.table("accounts").delete().eq("id", from_account_id).execute()
        except Exception as e:
            logger.debug(
                "rebind_account_id accounts cleanup skipped from_account_id=%s to_account_id=%s error=%s",
                from_account_id,
                to_account_id,
                AccountsService._format_db_error(e),
            )

    @staticmethod
    def _rebind_account_identities(from_account_id: str, to_account_id: str) -> None:
        """Аккуратно переносит identities без падения на unique-конфликтах."""
        if not db.supabase:
            return

        try:
            source_resp = (
                db.supabase.table("account_identities")
                .select("provider,provider_user_id")
                .eq("account_id", str(from_account_id))
                .execute()
            )
            target_resp = (
                db.supabase.table("account_identities")
                .select("provider,provider_user_id")
                .eq("account_id", str(to_account_id))
                .execute()
            )
            source_identities = source_resp.data or []
            target_keys = {
                (str(row.get("provider")), str(row.get("provider_user_id")))
                for row in (target_resp.data or [])
                if row.get("provider") and row.get("provider_user_id")
            }

            for row in source_identities:
                provider = row.get("provider")
                provider_user_id = row.get("provider_user_id")
                if not provider or not provider_user_id:
                    logger.warning(
                        "rebind_account_identities skipped invalid identity from_account_id=%s to_account_id=%s provider=%s provider_user_id=%s",
                        from_account_id,
                        to_account_id,
                        provider,
                        provider_user_id,
                    )
                    continue

                identity_key = (str(provider), str(provider_user_id))

                if identity_key in target_keys:
                    db.supabase.table("account_identities").delete().eq("account_id", str(from_account_id)).eq(
                        "provider", str(provider)
                    ).eq("provider_user_id", str(provider_user_id)).execute()
                    logger.warning(
                        "rebind_account_identities removed duplicate source identity from_account_id=%s to_account_id=%s provider=%s provider_user_id=%s",
                        from_account_id,
                        to_account_id,
                        provider,
                        provider_user_id,
                    )
                    continue

                try:
                    db.supabase.table("account_identities").update({"account_id": str(to_account_id)}).eq(
                        "account_id", str(from_account_id)
                    ).eq("provider", str(provider)).eq("provider_user_id", str(provider_user_id)).execute()
                    target_keys.add(identity_key)
                except Exception as e:
                    if AccountsService._is_unique_violation(e):
                        db.supabase.table("account_identities").delete().eq("account_id", str(from_account_id)).eq(
                            "provider", str(provider)
                        ).eq("provider_user_id", str(provider_user_id)).execute()
                        target_keys.add(identity_key)
                        logger.warning(
                            "rebind_account_identities resolved unique conflict by deleting source identity from_account_id=%s to_account_id=%s provider=%s provider_user_id=%s error=%s",
                            from_account_id,
                            to_account_id,
                            provider,
                            provider_user_id,
                            AccountsService._format_db_error(e),
                        )
                    else:
                        logger.exception(
                            "rebind_account_identities update failed from_account_id=%s to_account_id=%s provider=%s provider_user_id=%s error=%s",
                            from_account_id,
                            to_account_id,
                            provider,
                            provider_user_id,
                            AccountsService._format_db_error(e),
                        )
                        raise
        except Exception as e:
            logger.exception(
                "rebind_account_identities failed from_account_id=%s to_account_id=%s error=%s",
                from_account_id,
                to_account_id,
                AccountsService._format_db_error(e),
            )
            raise

    @staticmethod
    def _resolve_numeric_user_id_for_account(account_id: str) -> Optional[int]:
        """Возвращает любой числовой provider_user_id для аккаунта (discord/telegram)."""
        if not db.supabase or not account_id:
            return None
        try:
            response = (
                db.supabase.table("account_identities")
                .select("provider,provider_user_id")
                .eq("account_id", str(account_id))
                .execute()
            )
            identities = response.data or []
            for provider in ("discord", "telegram"):
                for row in identities:
                    if row.get("provider") != provider:
                        continue
                    raw_user_id = row.get("provider_user_id")
                    try:
                        return int(str(raw_user_id))
                    except (TypeError, ValueError):
                        logger.warning(
                            "resolve_numeric_user_id_for_account invalid provider_user_id account_id=%s provider=%s provider_user_id=%s",
                            account_id,
                            provider,
                            raw_user_id,
                        )
            return None
        except Exception as e:
            logger.exception(
                "resolve_numeric_user_id_for_account failed account_id=%s error=%s",
                account_id,
                AccountsService._format_db_error(e),
            )
            return None

    @staticmethod
    def _merge_scores_between_accounts(from_account_id: str, to_account_id: str) -> None:
        """Складывает points/tickets из двух score-строк и удаляет исходную account-строку."""
        if not db.supabase or not from_account_id or not to_account_id or str(from_account_id) == str(to_account_id):
            return

        try:
            source_resp = (
                db.supabase.table("scores")
                .select("account_id,points,tickets_normal,tickets_gold")
                .eq("account_id", str(from_account_id))
                .limit(1)
                .execute()
            )
            target_resp = (
                db.supabase.table("scores")
                .select("account_id,points,tickets_normal,tickets_gold")
                .eq("account_id", str(to_account_id))
                .limit(1)
                .execute()
            )
            source_row = (source_resp.data or [None])[0]
            target_row = (target_resp.data or [None])[0]

            if not source_row:
                return

            merged_payload = {
                "account_id": str(to_account_id),
                "points": float((target_row or {}).get("points") or 0) + float(source_row.get("points") or 0),
                "tickets_normal": int((target_row or {}).get("tickets_normal") or 0) + int(source_row.get("tickets_normal") or 0),
                "tickets_gold": int((target_row or {}).get("tickets_gold") or 0) + int(source_row.get("tickets_gold") or 0),
            }
            db.supabase.table("scores").upsert(merged_payload, on_conflict="account_id").execute()
            db.supabase.table("scores").delete().eq("account_id", str(from_account_id)).execute()
            logger.info(
                "merge_scores_between_accounts success from_account_id=%s to_account_id=%s",
                from_account_id,
                to_account_id,
            )
        except TypeError:
            # Legacy Supabase SDK without on_conflict support.
            try:
                db.supabase.table("scores").upsert(merged_payload).execute()  # type: ignore[name-defined]
                db.supabase.table("scores").delete().eq("account_id", str(from_account_id)).execute()
            except Exception as e:
                logger.exception(
                    "merge_scores_between_accounts fallback failed from_account_id=%s to_account_id=%s error=%s",
                    from_account_id,
                    to_account_id,
                    AccountsService._format_db_error(e),
                )
        except Exception as e:
            logger.exception(
                "merge_scores_between_accounts failed from_account_id=%s to_account_id=%s error=%s",
                from_account_id,
                to_account_id,
                AccountsService._format_db_error(e),
            )

    @staticmethod
    def _merge_accounts(from_account_id: str, to_account_id: str) -> None:
        """Объединяет два общих аккаунта в один с переносом всех ссылок и накоплений."""
        if not from_account_id or not to_account_id or str(from_account_id) == str(to_account_id):
            return

        AccountsService._merge_registry_rows_for_accounts(from_account_id, to_account_id)
        AccountsService._merge_scores_between_accounts(from_account_id, to_account_id)
        AccountsService._rebind_account_id(from_account_id, to_account_id)
        logger.info(
            "merge_accounts success from_account_id=%s to_account_id=%s",
            from_account_id,
            to_account_id,
        )

    @staticmethod
    def _merge_registry_rows_for_accounts(from_account_id: str, to_account_id: str) -> None:
        """Схлопывает account_links_registry перед merge, чтобы не ловить UNIQUE-конфликты по provider id."""
        if not db.supabase:
            return

        source_row = AccountsService._get_account_link_registry_row(str(from_account_id)) or {}
        target_row = AccountsService._get_account_link_registry_row(str(to_account_id)) or {}

        if not source_row and not target_row:
            return

        source_telegram = source_row.get("telegram_user_id")
        target_telegram = target_row.get("telegram_user_id")
        if source_telegram and target_telegram and str(source_telegram) != str(target_telegram):
            logger.error(
                "merge_registry_rows_for_accounts conflicting telegram ids from_account_id=%s to_account_id=%s source=%s target=%s",
                from_account_id,
                to_account_id,
                source_telegram,
                target_telegram,
            )

        source_discord = source_row.get("discord_user_id")
        target_discord = target_row.get("discord_user_id")
        if source_discord and target_discord and str(source_discord) != str(target_discord):
            logger.error(
                "merge_registry_rows_for_accounts conflicting discord ids from_account_id=%s to_account_id=%s source=%s target=%s",
                from_account_id,
                to_account_id,
                source_discord,
                target_discord,
            )

        merged_row = {
            "account_id": str(to_account_id),
            "telegram_user_id": target_telegram or source_telegram,
            "discord_user_id": target_discord or source_discord,
            "telegram_linked_at": target_row.get("telegram_linked_at") or source_row.get("telegram_linked_at"),
            "discord_linked_at": target_row.get("discord_linked_at") or source_row.get("discord_linked_at"),
            "last_link_code_used": target_row.get("last_link_code_used") or source_row.get("last_link_code_used"),
            "last_link_code_used_at": target_row.get("last_link_code_used_at") or source_row.get("last_link_code_used_at"),
            "has_used_link_code": bool(target_row.get("has_used_link_code") or source_row.get("has_used_link_code")),
        }

        source_deleted = False
        if source_row:
            try:
                db.supabase.table("account_links_registry").delete().eq("account_id", str(from_account_id)).execute()
                source_deleted = True
            except Exception as e:
                logger.warning(
                    "merge_registry_rows_for_accounts pre-upsert cleanup failed from_account_id=%s to_account_id=%s error=%s",
                    from_account_id,
                    to_account_id,
                    AccountsService._format_db_error(e),
                )

        try:
            try:
                db.supabase.table("account_links_registry").upsert(merged_row, on_conflict="account_id").execute()
            except TypeError:
                db.supabase.table("account_links_registry").upsert(merged_row).execute()
        except Exception as e:
            logger.warning(
                "merge_registry_rows_for_accounts upsert failed from_account_id=%s to_account_id=%s payload=%s error=%s",
                from_account_id,
                to_account_id,
                merged_row,
                AccountsService._format_db_error(e),
            )
            if source_deleted:
                try:
                    db.supabase.table("account_links_registry").upsert(source_row, on_conflict="account_id").execute()
                except TypeError:
                    try:
                        db.supabase.table("account_links_registry").upsert(source_row).execute()
                    except Exception as restore_error:
                        logger.error(
                            "merge_registry_rows_for_accounts restore source failed from_account_id=%s to_account_id=%s source_row=%s error=%s",
                            from_account_id,
                            to_account_id,
                            source_row,
                            AccountsService._format_db_error(restore_error),
                        )
                except Exception as restore_error:
                    logger.error(
                        "merge_registry_rows_for_accounts restore source failed from_account_id=%s to_account_id=%s source_row=%s error=%s",
                        from_account_id,
                        to_account_id,
                        source_row,
                        AccountsService._format_db_error(restore_error),
                    )

    @staticmethod
    def _create_account() -> Optional[str]:
        if not db.supabase:
            return None

        try:
            created = db.supabase.table("accounts").insert({}, returning="representation").execute()
            if created.data:
                account_id = created.data[0].get("id")
                if account_id:
                    return str(account_id)
        except TypeError:
            try:
                created = db.supabase.table("accounts").insert({}).execute()
                if created.data:
                    account_id = created.data[0].get("id")
                    if account_id:
                        return str(account_id)
            except Exception as e:
                logger.error("create account failed (legacy): %s", e)
        except Exception as e:
            logger.error("create account failed: %s", e)

        account_id = str(uuid.uuid4())
        try:
            db.supabase.table("accounts").insert({"id": account_id}, returning="minimal").execute()
            return account_id
        except TypeError:
            db.supabase.table("accounts").insert({"id": account_id}).execute()
            return account_id
        except Exception as e:
            logger.error("create account fallback failed: %s", e)
            return None

    @staticmethod
    def _bind_identity_to_account(provider: str, provider_user_id: str, account_id: str) -> Tuple[bool, str]:
        normalized_provider = (provider or "").strip().lower()
        normalized_provider_user_id = str(provider_user_id or "").strip()
        normalized_account_id = str(account_id or "").strip()
        if not db.supabase or normalized_provider not in ("discord", "telegram") or not normalized_provider_user_id or not normalized_account_id:
            return False, "invalid"

        current_identity = AccountsService._load_identity_row(normalized_provider, normalized_provider_user_id)
        current_account_id = str(current_identity.get("account_id") or "").strip() if current_identity else ""
        if current_account_id:
            logger.warning(
                "register identity skipped repair because identity already bound provider=%s provider_user_id=%s existing_account_id=%s new_account_id=%s",
                normalized_provider,
                normalized_provider_user_id,
                current_account_id,
                normalized_account_id,
            )
            return True, "already_registered"

        try:
            query = (
                db.supabase.table("account_identities")
                .update({"account_id": normalized_account_id})
                .eq("provider", normalized_provider)
                .eq("provider_user_id", normalized_provider_user_id)
            )
            if hasattr(query, "is_"):
                query = query.is_("account_id", "null")
            response = query.execute()
            updated_rows = response.data or []
            if updated_rows:
                AccountsService._cache_account_id(normalized_provider, normalized_provider_user_id, normalized_account_id)
                logger.warning(
                    "register identity claimed lookup identity row provider=%s provider_user_id=%s account_id=%s",
                    normalized_provider,
                    normalized_provider_user_id,
                    normalized_account_id,
                )
                return True, "bound"

            refreshed_identity = AccountsService._load_identity_row(normalized_provider, normalized_provider_user_id)
            refreshed_account_id = str(refreshed_identity.get("account_id") or "").strip() if refreshed_identity else ""
            if refreshed_account_id:
                AccountsService._cache_account_id(normalized_provider, normalized_provider_user_id, refreshed_account_id)
                logger.warning(
                    "register identity observed concurrent binding provider=%s provider_user_id=%s existing_account_id=%s new_account_id=%s",
                    normalized_provider,
                    normalized_provider_user_id,
                    refreshed_account_id,
                    normalized_account_id,
                )
                return True, "already_registered"

            logger.error(
                "register identity could not claim existing identity row provider=%s provider_user_id=%s account_id=%s reason=no_rows_updated",
                normalized_provider,
                normalized_provider_user_id,
                normalized_account_id,
            )
        except Exception as error:
            logger.error(
                "register identity repair failed provider=%s provider_user_id=%s account_id=%s error=%s",
                normalized_provider,
                normalized_provider_user_id,
                normalized_account_id,
                AccountsService._format_db_error(error),
            )
        return False, "failed"

    @staticmethod
    def register_identity(provider: str, provider_user_id: str) -> Tuple[bool, str]:
        if not db.supabase:
            return False, "База данных недоступна"

        provider = (provider or "").strip().lower()
        provider_user_id = str(provider_user_id or "").strip()
        if provider not in ("discord", "telegram") or not provider_user_id:
            return False, "Некорректные параметры регистрации"

        existing_identity = AccountsService._load_identity_row(provider, provider_user_id)
        existing_account_id = str(existing_identity.get("account_id") or "").strip() if existing_identity else ""
        if existing_account_id:
            return True, "Уже зарегистрирован"

        account_id = AccountsService._create_account()
        if not account_id:
            return False, "Не удалось создать общий аккаунт"

        if existing_identity:
            bind_success, bind_status = AccountsService._bind_identity_to_account(provider, provider_user_id, account_id)
            if bind_success:
                return True, "Уже зарегистрирован" if bind_status == "already_registered" else "Регистрация завершена"
            return False, "Не удалось восстановить существующую identity"

        payload = {
            "account_id": account_id,
            "provider": provider,
            "provider_user_id": provider_user_id,
        }
        try:
            db.supabase.table("account_identities").insert(payload).execute()
            return True, "Регистрация завершена"
        except Exception as e:
            if AccountsService._is_unique_violation(e):
                conflicted_identity = AccountsService._load_identity_row(provider, provider_user_id)
                conflicted_account_id = str(conflicted_identity.get("account_id") or "").strip() if conflicted_identity else ""
                if conflicted_account_id:
                    logger.warning(
                        "register identity detected concurrent registration provider=%s provider_user_id=%s account_id=%s",
                        provider,
                        provider_user_id,
                        conflicted_account_id,
                    )
                    return True, "Уже зарегистрирован"
                if conflicted_identity:
                    bind_success, bind_status = AccountsService._bind_identity_to_account(provider, provider_user_id, account_id)
                    if bind_success:
                        return True, "Уже зарегистрирован" if bind_status == "already_registered" else "Регистрация завершена"
            logger.error("register identity failed (%s:%s): %s", provider, provider_user_id, e)
            return False, "Не удалось создать привязку identity"

    @staticmethod
    def _insert_link_code_with_fallback(payload: dict, source_provider: str, source_provider_user_id: str) -> bool:
        payload_variants = [
            payload,
            {
                "code": payload["code"],
                "account_id": payload["account_id"],
                "discord_user_id": source_provider_user_id if source_provider == "discord" else None,
                "created_at": payload["created_at"],
                "expires_at": payload["expires_at"],
                "is_used": payload["is_used"],
                "attempts": payload["attempts"],
            },
            {
                "code": payload["code"],
                "account_id": payload["account_id"],
                "created_at": payload["created_at"],
                "expires_at": payload["expires_at"],
                "is_used": payload["is_used"],
                "attempts": payload["attempts"],
            },
        ]

        table_errors: dict[str, str] = {}

        for table_name in AccountsService.LINK_CODES_TABLES:
            for variant in payload_variants:
                try:
                    db.supabase.table(table_name).insert(variant, returning="minimal").execute()
                    return True
                except TypeError:
                    try:
                        db.supabase.table(table_name).insert(variant).execute()
                        return True
                    except Exception as legacy_error:
                        table_errors[table_name] = AccountsService._format_db_error(legacy_error)
                        logger.warning(
                            "link code insert failed (legacy) table=%s payload_keys=%s error=%s",
                            table_name,
                            sorted(variant.keys()),
                            table_errors[table_name],
                        )
                        continue
                except Exception as e:
                    table_errors[table_name] = AccountsService._format_db_error(e)
                    logger.warning(
                        "link code insert failed table=%s payload_keys=%s error=%s",
                        table_name,
                        sorted(variant.keys()),
                        table_errors[table_name],
                    )
                    continue

            if table_name in table_errors:
                logger.error(
                    "link code table rejected all payload variants table=%s source=%s:%s last_error=%s",
                    table_name,
                    source_provider,
                    source_provider_user_id,
                    table_errors[table_name],
                )

        logger.error(
            "failed to persist link code in all candidate tables=%s for source=%s:%s errors=%s",
            AccountsService.LINK_CODES_TABLES,
            source_provider,
            source_provider_user_id,
            table_errors,
        )
        return False

    @staticmethod
    def _find_link_code(code: str):
        for table_name in AccountsService.LINK_CODES_TABLES:
            try:
                lookup = db.supabase.table(table_name).select("*").eq("code", code).limit(1).execute()
                if lookup.data:
                    return table_name, lookup.data[0]
            except Exception as e:
                logger.warning(
                    "link code lookup failed table=%s code=%s error=%s",
                    table_name,
                    code,
                    AccountsService._format_db_error(e),
                )
                continue
        logger.warning("link code not found in candidate tables=%s code=%s", AccountsService.LINK_CODES_TABLES, code)
        return None, None

    @staticmethod
    def issue_link_code(source_provider: str, source_provider_user_id: str, target_provider: str) -> Tuple[bool, str]:
        if not db.supabase:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("link_issue_fail")
            return False, "База данных недоступна"

        source_provider = (source_provider or "").strip().lower()
        target_provider = (target_provider or "").strip().lower()
        source_provider_user_id = str(source_provider_user_id or "").strip()

        if source_provider not in ("discord", "telegram") or target_provider not in ("discord", "telegram"):
            return False, "Некорректные параметры провайдеров"
        if source_provider == target_provider:
            return False, "Нельзя привязать аккаунт к тому же провайдеру"

        account_id = AccountsService.resolve_account_id(source_provider, source_provider_user_id)
        if not account_id:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("link_issue_fail")
            return False, "Сначала зарегистрируйтесь в боте"

        if AccountsService._registry_blocks_new_link_code(str(account_id), target_provider) or AccountsService._is_target_already_linked(str(account_id), target_provider):
            if hasattr(db, "_inc_metric"):
                db._inc_metric("link_issue_fail")
            logger.info(
                "issue_link_code skipped because target already linked source=%s:%s target=%s account_id=%s",
                source_provider,
                source_provider_user_id,
                target_provider,
                account_id,
            )
            return False, f"Аккаунт уже привязан к {target_provider}"

        reusable_code = AccountsService._find_reusable_link_code(
            str(account_id),
            source_provider,
            source_provider_user_id,
            target_provider,
        )
        if reusable_code:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("link_issue_success")
            return True, reusable_code

        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(minutes=AccountsService.LINK_TTL_MINUTES)

        for _ in range(AccountsService.LINK_CODE_GENERATION_ATTEMPTS):
            code = AccountsService._generate_link_code()
            payload = {
                "code": code,
                "account_id": account_id,
                "source_provider": source_provider,
                "source_provider_user_id": source_provider_user_id,
                "target_provider": target_provider,
                "created_at": now.isoformat(),
                "expires_at": expires_at.isoformat(),
                "is_used": False,
                "attempts": 0,
            }

            if AccountsService._insert_link_code_with_fallback(payload, source_provider, source_provider_user_id):
                if hasattr(db, "_inc_metric"):
                    db._inc_metric("link_issue_success")
                return True, code

        if hasattr(db, "_inc_metric"):
            db._inc_metric("link_issue_fail")
        return False, "Не удалось создать код привязки"

    @staticmethod
    def _safe_update_code(table_name: str, code: str, payloads: list[dict]) -> bool:
        for payload in payloads:
            try:
                update_response = db.supabase.table(table_name).update(payload).eq("code", code).execute()
                if update_response.data:
                    return True
                logger.warning(
                    "link code update affected zero rows table=%s code=%s payload_keys=%s",
                    table_name,
                    code,
                    sorted(payload.keys()),
                )
            except Exception as e:
                logger.warning(
                    "link code update failed table=%s code=%s payload_keys=%s error=%s",
                    table_name,
                    code,
                    sorted(payload.keys()),
                    AccountsService._format_db_error(e),
                )
                continue
        logger.warning("all link code updates failed for table=%s code=%s", table_name, code)
        return False

    @staticmethod
    def _find_reusable_link_code(
        account_id: str,
        source_provider: str,
        source_provider_user_id: str,
        target_provider: str,
    ) -> Optional[str]:
        now = datetime.now(timezone.utc)

        for table_name in AccountsService.LINK_CODES_TABLES:
            query_variants = [
                [
                    ("account_id", account_id),
                    ("source_provider", source_provider),
                    ("source_provider_user_id", source_provider_user_id),
                    ("target_provider", target_provider),
                    ("is_used", False),
                ],
                [("account_id", account_id), ("is_used", False)],
            ]

            for filters in query_variants:
                try:
                    query = db.supabase.table(table_name).select("*")
                    for key, value in filters:
                        query = query.eq(key, value)
                    lookup = query.limit(20).execute()
                except Exception:
                    continue

                for row in lookup.data or []:
                    code = str(row.get("code") or "").strip().upper()
                    expires_at_raw = row.get("expires_at")
                    if not code or not expires_at_raw:
                        continue
                    try:
                        expires_at = datetime.fromisoformat(str(expires_at_raw).replace("Z", "+00:00"))
                    except ValueError:
                        continue
                    if now <= expires_at and not row.get("is_used"):
                        return code

        return None

    @staticmethod
    def consume_link_code(target_provider: str, target_provider_user_id: str, code: str) -> Tuple[bool, str]:
        if not db.supabase:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("link_consume_fail")
            return False, "База данных недоступна"

        target_provider = (target_provider or "").strip().lower()
        target_provider_user_id = str(target_provider_user_id or "").strip()
        code = (code or "").strip().upper()

        if target_provider not in ("discord", "telegram") or not target_provider_user_id:
            logger.warning(
                "consume_link_code invalid params target_provider=%s target_provider_user_id=%s code=%s",
                target_provider,
                target_provider_user_id,
                code,
            )
            return False, "Некорректные параметры привязки"
        if not code:
            logger.warning(
                "consume_link_code empty code target_provider=%s target_provider_user_id=%s",
                target_provider,
                target_provider_user_id,
            )
            return False, "Пустой код"

        try:
            table_name, row = AccountsService._find_link_code(code)
            if not row:
                if hasattr(db, "_inc_metric"):
                    db._inc_metric("link_consume_fail")
                logger.warning(
                    "consume_link_code code not found target_provider=%s target_provider_user_id=%s code=%s",
                    target_provider,
                    target_provider_user_id,
                    code,
                )
                return False, "Код не найден"

            now = datetime.now(timezone.utc)
            expires_at_raw = row.get("expires_at")
            if not expires_at_raw:
                logger.warning("consume_link_code corrupted code without expires_at code=%s table=%s", code, table_name)
                return False, "Код повреждён"
            expires_at = datetime.fromisoformat(str(expires_at_raw).replace("Z", "+00:00"))
            attempts = int(row.get("attempts", 0) or 0)

            if row.get("is_used"):
                if hasattr(db, "_inc_metric"):
                    db._inc_metric("link_consume_fail")
                logger.warning("consume_link_code code already used code=%s table=%s", code, table_name)
                return False, "Код уже использован"
            if now > expires_at:
                if hasattr(db, "_inc_metric"):
                    db._inc_metric("link_consume_fail")
                logger.warning("consume_link_code code expired code=%s expires_at=%s now=%s", code, expires_at.isoformat(), now.isoformat())
                return False, "Срок действия кода истёк"
            if attempts >= AccountsService.MAX_ATTEMPTS:
                if hasattr(db, "_inc_metric"):
                    db._inc_metric("link_consume_fail")
                logger.warning("consume_link_code attempts exceeded code=%s attempts=%s", code, attempts)
                return False, "Превышено число попыток"

            expected_target = row.get("target_provider")
            if expected_target and expected_target != target_provider:
                logger.warning(
                    "consume_link_code wrong target expected=%s actual=%s code=%s",
                    expected_target,
                    target_provider,
                    code,
                )
                return False, f"Этот код предназначен для {expected_target}"

            AccountsService._safe_update_code(table_name, code, [{"attempts": attempts + 1}])

            account_id = row.get("account_id")
            if not account_id:
                logger.warning("consume_link_code missing account_id code=%s table=%s", code, table_name)
                return False, "Код не содержит account_id"

            AccountsService.invalidate_account_id_cache(target_provider, target_provider_user_id)
            existing_account_id = AccountsService.resolve_account_id(target_provider, target_provider_user_id)
            if existing_account_id and str(existing_account_id) != str(account_id):
                logger.warning(
                    "consume_link_code detected cross-account identity; merging accounts target_provider=%s target_provider_user_id=%s code=%s from_account_id=%s to_account_id=%s",
                    target_provider,
                    target_provider_user_id,
                    code,
                    existing_account_id,
                    account_id,
                )
                AccountsService._merge_accounts(str(existing_account_id), str(account_id))
                existing_account_id = str(account_id)
                AccountsService._cache_account_id(target_provider, target_provider_user_id, existing_account_id)

            if not existing_account_id:
                identity_payload = {
                    "account_id": account_id,
                    "provider": target_provider,
                    "provider_user_id": target_provider_user_id,
                }
                try:
                    db.supabase.table("account_identities").upsert(
                        identity_payload,
                        on_conflict="provider,provider_user_id",
                    ).execute()
                    AccountsService._cache_account_id(target_provider, target_provider_user_id, str(account_id))
                except TypeError:
                    db.supabase.table("account_identities").upsert(identity_payload).execute()
                    AccountsService._cache_account_id(target_provider, target_provider_user_id, str(account_id))
                except Exception as e:
                    if AccountsService._is_unique_violation(e):
                        AccountsService.invalidate_account_id_cache(target_provider, target_provider_user_id)
                        existing_account_id = AccountsService.resolve_account_id(target_provider, target_provider_user_id)
                        if existing_account_id and str(existing_account_id) != str(account_id):
                            logger.warning(
                                "consume_link_code unique violation with cross-account binding; merging accounts target_provider=%s target_provider_user_id=%s code=%s from_account_id=%s to_account_id=%s",
                                target_provider,
                                target_provider_user_id,
                                code,
                                existing_account_id,
                                account_id,
                            )
                            AccountsService._merge_accounts(str(existing_account_id), str(account_id))
                            AccountsService._cache_account_id(target_provider, target_provider_user_id, str(account_id))
                    else:
                        raise

            is_marked_used = AccountsService._safe_update_code(
                table_name,
                code,
                [
                    {
                        "is_used": True,
                        "used_at": now.isoformat(),
                        "used_by_provider": target_provider,
                        "used_by_provider_user_id": target_provider_user_id,
                    },
                    {"is_used": True, "used_at": now.isoformat()},
                ],
            )
            if not is_marked_used:
                if hasattr(db, "_inc_metric"):
                    db._inc_metric("link_consume_fail")
                logger.error("consume_link_code failed to mark code as used code=%s table=%s", code, table_name)
                return False, "Не удалось завершить привязку, попробуйте позже"

            AccountsService._mark_registry_link_code_usage(str(account_id), code)
            from bot.services.external_roles_sync_service import ExternalRolesSyncService

            ExternalRolesSyncService.trigger_account_sync(
                str(account_id),
                reason=f"link_{target_provider}",
            )

            if hasattr(db, "_inc_metric"):
                db._inc_metric("link_consume_success")
            return True, "Аккаунт успешно привязан"
        except Exception as e:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("link_consume_fail")
            logger.exception(
                "consume_link_code failed target_provider=%s target_provider_user_id=%s code=%s error=%s",
                target_provider,
                target_provider_user_id,
                code,
                AccountsService._format_db_error(e),
            )
            return False, "Ошибка привязки"

    @staticmethod
    def issue_discord_telegram_link_code(discord_user_id: int) -> Tuple[bool, str]:
        return AccountsService.issue_link_code("discord", str(discord_user_id), "telegram")

    @staticmethod
    def consume_telegram_link_code(telegram_user_id: int, code: str) -> Tuple[bool, str]:
        return AccountsService.consume_link_code("telegram", str(telegram_user_id), code)

    @staticmethod
    def consume_discord_link_code(discord_user_id: int, code: str) -> Tuple[bool, str]:
        return AccountsService.consume_link_code("discord", str(discord_user_id), code)

    @staticmethod
    def issue_telegram_discord_link_code(telegram_user_id: int) -> Tuple[bool, str]:
        return AccountsService.issue_link_code("telegram", str(telegram_user_id), "discord")

    @staticmethod
    def _format_points(points_value: object) -> str:
        try:
            points_float = float(points_value)
        except (TypeError, ValueError):
            return "0"

        if points_float.is_integer():
            return str(int(points_float))
        return f"{points_float:.2f}".rstrip("0").rstrip(".")

    @staticmethod
    def _load_points_from_actions(account_id: str, discord_identity: Optional[dict]) -> Optional[float]:
        """Возвращает баланс по сумме действий, чтобы не зависеть от дублей в scores."""
        if not db.supabase:
            return None

        action_rows: list[dict] = []
        try:
            action_response = (
                db.supabase.table("actions")
                .select("points")
                .eq("account_id", str(account_id))
                .execute()
            )
            action_rows = action_response.data or []
        except Exception as e:
            logger.exception(
                "get_profile actions read failed account_id=%s error=%s",
                account_id,
                AccountsService._format_db_error(e),
            )

        if not action_rows and discord_identity:
            discord_user_id = discord_identity.get("provider_user_id")
            if discord_user_id:
                try:
                    action_response = (
                        db.supabase.table("actions")
                        .select("points")
                        .eq("user_id", str(discord_user_id))
                        .execute()
                    )
                    action_rows = action_response.data or []
                except Exception as e:
                    if AccountsService._is_missing_column_error(e, table="actions", column="user_id"):
                        logger.warning(
                            "get_profile actions legacy fallback skipped because schema has no actions.user_id account_id=%s discord_user_id=%s error=%s",
                            account_id,
                            discord_user_id,
                            AccountsService._format_db_error(e),
                        )
                    else:
                        logger.exception(
                            "get_profile actions fallback failed account_id=%s discord_user_id=%s error=%s",
                            account_id,
                            discord_user_id,
                            AccountsService._format_db_error(e),
                        )

        if action_rows:
            return sum(float(row.get("points") or 0) for row in action_rows)
        return None

    @staticmethod
    def _normalize_profile_field_value(field_name: str, value: object) -> str:
        config = AccountsService.PROFILE_FIELDS_CONFIG.get(field_name, {})
        default_value = str(config.get("default", ""))
        max_length = int(config.get("max_length", 255))
        normalized = str(value or "").strip()
        if not normalized:
            return default_value
        return normalized[:max_length]

    @staticmethod
    def update_profile_field(provider: str, provider_user_id: str, field_name: str, value: str) -> Tuple[bool, str]:
        if field_name == "visible_roles":
            return AccountsService.update_profile_visible_roles(provider, provider_user_id, value)

        config = AccountsService.PROFILE_FIELDS_CONFIG.get(field_name)
        if not config:
            logger.warning(
                "update_profile_field unknown field provider=%s provider_user_id=%s field=%s",
                provider,
                provider_user_id,
                field_name,
            )
            return False, "Неизвестное поле профиля"

        account_id = AccountsService.resolve_account_id(provider, provider_user_id)
        if not account_id or not db.supabase:
            logger.warning(
                "update_profile_field failed to resolve account provider=%s provider_user_id=%s field=%s",
                provider,
                provider_user_id,
                field_name,
            )
            return False, "Профиль не найден. Сначала зарегистрируйтесь"

        normalized = AccountsService._normalize_profile_field_value(field_name, value)
        try:
            db.supabase.table("accounts").update({field_name: normalized}).eq("id", str(account_id)).execute()
            return True, f"{config['label']} обновлён"
        except Exception as e:
            logger.exception(
                "update_profile_field failed account_id=%s provider=%s provider_user_id=%s field=%s error=%s",
                account_id,
                provider,
                provider_user_id,
                field_name,
                AccountsService._format_db_error(e),
            )
            return False, "Не удалось обновить профиль"

    @staticmethod
    def update_profile_visible_roles(provider: str, provider_user_id: str, value: str) -> Tuple[bool, str]:
        account_id = AccountsService.resolve_account_id(provider, provider_user_id)
        if not account_id or not db.supabase:
            logger.warning(
                "update_profile_visible_roles failed to resolve account provider=%s provider_user_id=%s",
                provider,
                provider_user_id,
            )
            return False, "Профиль не найден. Сначала зарегистрируйтесь"

        role_names = [item.strip() for item in str(value or "").split(",") if item.strip()]
        if len(role_names) > AccountsService.MAX_VISIBLE_PROFILE_ROLES:
            return False, f"Можно выбрать не более {AccountsService.MAX_VISIBLE_PROFILE_ROLES} ролей"

        try:
            access = RoleResolver.resolve_for_account(str(account_id))
            available_roles = {str(item.get('name') or '').strip().lower(): str(item.get('name') or '').strip() for item in access.roles}
            normalized_roles: list[str] = []
            for role_name in role_names:
                resolved_name = available_roles.get(role_name.lower())
                if not resolved_name:
                    return False, f"Роль не найдена в вашем профиле: {role_name}"
                if resolved_name not in normalized_roles:
                    normalized_roles.append(resolved_name)

            db.supabase.table("accounts").update({"profile_visible_roles": normalized_roles}).eq("id", str(account_id)).execute()
            return True, "Роли профиля обновлены"
        except Exception as e:
            logger.exception(
                "update_profile_visible_roles failed account_id=%s provider=%s provider_user_id=%s error=%s",
                account_id,
                provider,
                provider_user_id,
                AccountsService._format_db_error(e),
            )
            return False, "Не удалось обновить отображаемые роли"

    @staticmethod
    def get_profile(provider: str, provider_user_id: str, display_name: Optional[str] = None) -> Optional[dict]:
        account_id = AccountsService.resolve_account_id(provider, provider_user_id)
        if not account_id or not db.supabase:
            return None
        return AccountsService.get_profile_by_account(str(account_id), display_name=display_name)

    @staticmethod
    def get_profile_by_account(account_id: str, display_name: Optional[str] = None) -> Optional[dict]:
        if not account_id or not db.supabase:
            return None

        identities = []
        try:
            response = (
                db.supabase.table("account_identities")
                .select("provider,provider_user_id")
                .eq("account_id", account_id)
                .execute()
            )
            identities = response.data or []
        except Exception as e:
            logger.warning("get_profile identities failed for %s: %s", account_id, e)

        has_discord = any(identity.get("provider") == "discord" for identity in identities)
        has_telegram = any(identity.get("provider") == "telegram" for identity in identities)
        discord_identity = next((identity for identity in identities if identity.get("provider") == "discord"), None)

        custom_nick = display_name or str(AccountsService.PROFILE_FIELDS_CONFIG["custom_nick"]["default"])
        description = str(AccountsService.PROFILE_FIELDS_CONFIG["description"]["default"])
        nulls_id = str(AccountsService.PROFILE_FIELDS_CONFIG["nulls_brawl_id"]["default"])
        profile_visible_roles: list[str] = []
        try:
            account_response = (
                db.supabase.table("accounts")
                .select("custom_nick,description,nulls_brawl_id,profile_visible_roles")
                .eq("id", str(account_id))
                .limit(1)
                .execute()
            )
            account_rows = account_response.data or []
            if account_rows:
                account_row = account_rows[0]
                custom_nick = AccountsService._normalize_profile_field_value(
                    "custom_nick",
                    account_row.get("custom_nick") or display_name,
                )
                description = AccountsService._normalize_profile_field_value("description", account_row.get("description"))
                nulls_id = AccountsService._normalize_profile_field_value("nulls_brawl_id", account_row.get("nulls_brawl_id"))
                visible_roles_value = account_row.get("profile_visible_roles")
                if isinstance(visible_roles_value, list):
                    profile_visible_roles = [str(item).strip() for item in visible_roles_value if str(item).strip()]
        except Exception as e:
            logger.exception(
                "get_profile_by_account account fields read failed account_id=%s error=%s",
                account_id,
                AccountsService._format_db_error(e),
            )
        nulls_status = "Не подтвержден (заглушка)"
        points = "Привяжите Discord для получения информации (временно)."
        titles: list[str] = AccountsService.get_account_titles(account_id)
        titles_text = (
            ", ".join(titles)
            if titles
            else "Привяжите Discord и/или подтвердите скрином свое звание администрации клуба для получения звания (временно)"
        )
        if not titles:
            logger.info("get_profile_by_account no titles yet account_id=%s", account_id)

        points_from_actions = AccountsService._load_points_from_actions(str(account_id), discord_identity)
        points_from_scores: Optional[float] = None
        try:
            points_response = (
                db.supabase.table("scores")
                .select("points")
                .eq("account_id", str(account_id))
                .limit(1)
                .execute()
            )
            points_rows = points_response.data or []
            if not points_rows and discord_identity:
                discord_user_id = discord_identity.get("provider_user_id")
                log_legacy_schema_fallback(
                    logger,
                    module=__name__,
                    table="scores",
                    field="user_id",
                    action="migrate_scores_lookup_to_account_id",
                    continue_execution=True,
                    account_id=account_id,
                    discord_user_id=discord_user_id,
                    recommended_field="account_id",
                    developer_hint="temporary compatibility path; migrate scores rows to scores.account_id",
                )
                points_response = (
                    db.supabase.table("scores")
                    .select("points")
                    .eq("user_id", str(discord_user_id))
                    .limit(1)
                    .execute()
                )
                points_rows = points_response.data or []
            if points_rows:
                points_from_scores = float(points_rows[0].get("points") or 0)
        except Exception as e:
            if AccountsService._is_missing_column_error(e, table="scores", column="user_id"):
                logger.warning(
                    "get_profile_by_account legacy score fallback skipped because schema has no scores.user_id account_id=%s error=%s",
                    account_id,
                    AccountsService._format_db_error(e),
                )
            else:
                logger.exception(
                    "get_profile_by_account points failed account_id=%s error=%s",
                    account_id,
                    AccountsService._format_db_error(e),
                )

        if points_from_scores is not None:
            points = AccountsService._format_points(points_from_scores)
            if points_from_actions is not None and abs(points_from_actions - points_from_scores) >= 0.01:
                logger.warning(
                    "get_profile points mismatch account_id=%s actions_total=%s scores_snapshot=%s",
                    account_id,
                    points_from_actions,
                    points_from_scores,
                )
        elif points_from_actions is not None:
            points = AccountsService._format_points(points_from_actions)
            logger.warning(
                "get_profile scores points unavailable account_id=%s; using actions fallback=%s",
                account_id,
                points_from_actions,
            )
        else:
            points = "0"
            logger.warning(
                "get_profile points unavailable account_id=%s; defaulting to zero",
                account_id,
            )

        resolved_roles: list[dict[str, str | None]] = []
        resolved_permissions: dict[str, list[str]] = {"allow": [], "deny": []}
        external_roles_last_synced_at: str | None = None
        try:
            access = RoleResolver.resolve_for_account(str(account_id))
            resolved_roles = access.roles
            resolved_permissions = access.permissions
        except Exception as e:
            logger.exception(
                "get_profile_by_account role resolution failed account_id=%s error=%s",
                account_id,
                AccountsService._format_db_error(e),
            )

        role_names = [str(item.get("name") or "").strip() for item in resolved_roles if str(item.get("name") or "").strip()]
        role_names_lower_to_original = {name.lower(): name for name in role_names}
        visible_roles: list[str] = []
        for selected in profile_visible_roles:
            resolved = role_names_lower_to_original.get(str(selected).lower())
            if resolved and resolved not in visible_roles:
                visible_roles.append(resolved)
        if not visible_roles:
            visible_roles = role_names[: AccountsService.MAX_VISIBLE_PROFILE_ROLES]
        else:
            visible_roles = visible_roles[: AccountsService.MAX_VISIBLE_PROFILE_ROLES]

        roles_by_category = RoleResolver.group_roles_by_category(resolved_roles, account_id=str(account_id))

        try:
            from bot.services.external_roles_sync_service import ExternalRolesSyncService

            external_roles_last_synced_at = ExternalRolesSyncService.get_last_sync_at(str(account_id))
        except Exception as e:
            logger.exception(
                "get_profile_by_account external roles sync timestamp failed account_id=%s error=%s",
                account_id,
                AccountsService._format_db_error(e),
            )

        return {
            "account_id": account_id,
            "custom_nick": custom_nick,
            "description": description,
            "has_discord": has_discord,
            "has_telegram": has_telegram,
            "link_status": "Привязан" if has_discord and has_telegram else "Не привязан",
            "nulls_brawl_id": nulls_id,
            "nulls_status": nulls_status,
            "points": points,
            "titles": titles,
            "titles_text": titles_text,
            "roles": resolved_roles,
            "visible_roles": visible_roles,
            "roles_by_category": roles_by_category,
            "permissions": resolved_permissions,
            "external_roles_last_synced_at": external_roles_last_synced_at,
        }

    @staticmethod
    def get_account_titles(account_id: str) -> list[str]:
        if not account_id:
            return []

        cached = AccountsService._account_titles_cache.get(str(account_id))
        if cached is not None:
            return AccountsService._ensure_default_chat_member_title(list(cached), account_id=str(account_id))

        if not db.supabase:
            return []

        try:
            response = (
                db.supabase.table("accounts")
                .select("titles")
                .eq("id", str(account_id))
                .limit(1)
                .execute()
            )
            rows = response.data or []
            if not rows:
                return []

            value = rows[0].get("titles")
            if isinstance(value, list):
                titles = [str(item).strip() for item in value if str(item).strip()]
            elif isinstance(value, str):
                titles = [item.strip() for item in value.split(",") if item.strip()]
            else:
                titles = []
            normalized_titles = AccountsService._ensure_default_chat_member_title(titles, account_id=str(account_id))
            AccountsService._account_titles_cache[str(account_id)] = list(normalized_titles)
            return normalized_titles
        except Exception as e:
            logger.warning("get_account_titles failed for %s: %s", account_id, e)
            return []

    @staticmethod
    def _ensure_default_chat_member_title(titles: list[str], *, account_id: str) -> list[str]:
        normalized = [str(item).strip() for item in (titles or []) if str(item).strip()]
        if normalized:
            return normalized
        logger.info(
            "get_account_titles fallback applied account_id=%s fallback_title=%s",
            account_id,
            AccountsService.FALLBACK_CHAT_MEMBER_TITLE,
        )
        return [AccountsService.FALLBACK_CHAT_MEMBER_TITLE]

    @staticmethod
    def save_account_titles(account_id: str, titles: list[str], source: str = "discord") -> bool:
        if not account_id:
            return False

        normalized = [str(item).strip() for item in (titles or []) if str(item).strip()]
        normalized = list(dict.fromkeys(normalized))

        if not db.supabase:
            logger.warning("save_account_titles skipped for account_id=%s source=%s: supabase is unavailable", account_id, source)
            return False

        payload = {
            "titles": normalized,
            "titles_updated_at": datetime.now(timezone.utc).isoformat(),
            "titles_source": source,
        }
        try:
            db.supabase.table("accounts").update(payload).eq("id", str(account_id)).execute()
            AccountsService._account_titles_cache[str(account_id)] = list(normalized)
            return True
        except Exception as e:
            logger.warning("save_account_titles failed for account_id=%s source=%s error=%s", account_id, source, e)
            return False

    @staticmethod
    def get_configured_title_role_ids() -> set[int]:
        configured = AccountsService.get_configured_title_roles()
        if configured:
            return set(configured.keys())

        raw = os.getenv("PROFILE_DISCORD_TITLE_ROLE_IDS", "")
        result: set[int] = set()
        for item in raw.split(","):
            item = item.strip()
            if not item:
                continue
            try:
                result.add(int(item))
            except ValueError:
                logger.warning("PROFILE_DISCORD_TITLE_ROLE_IDS contains invalid role id: %s", item)
        return result

    @staticmethod
    def get_configured_title_role_names() -> set[str]:
        configured = AccountsService.get_configured_title_roles()
        if configured:
            return {title.strip().lower() for title in configured.values() if str(title).strip()}

        raw = os.getenv("PROFILE_DISCORD_TITLE_ROLE_NAMES", "")
        return {item.strip().lower() for item in raw.split(",") if item.strip()}

    @staticmethod
    def get_configured_title_roles() -> dict[int, str]:
        """Return Discord role_id -> profile title mapping from DB with env fallback support."""
        if AccountsService._title_roles_cache is not None:
            return dict(AccountsService._title_roles_cache)

        if not db.supabase:
            AccountsService._title_roles_cache = {}
            return {}

        try:
            response = (
                db.supabase.table("profile_title_roles")
                .select("discord_role_id,title_name,is_active")
                .eq("is_active", True)
                .execute()
            )
            rows = response.data or []
            mapping: dict[int, str] = {}
            for row in rows:
                role_id_value = row.get("discord_role_id")
                title_name = str(row.get("title_name") or "").strip()
                if not role_id_value or not title_name:
                    continue
                try:
                    mapping[int(role_id_value)] = title_name
                except (TypeError, ValueError):
                    logger.warning("profile_title_roles contains invalid discord_role_id=%s", role_id_value)

            AccountsService._title_roles_cache = mapping
            if mapping:
                logger.info("loaded profile title role mappings from db count=%s", len(mapping))
            return dict(mapping)
        except Exception as e:
            logger.warning("get_configured_title_roles failed, fallback to env mapping: %s", e)
            AccountsService._title_roles_cache = {}
            return {}

    @staticmethod
    def unlink_identity(provider: str, provider_user_id: str) -> Tuple[bool, str]:
        if not db.supabase:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("unlink_fail")
            return False, "База данных недоступна"

        provider = (provider or "").strip().lower()
        provider_user_id = str(provider_user_id or "").strip()
        if not provider or not provider_user_id:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("unlink_fail")
            return False, "Некорректные параметры unlink"

        try:
            result = (
                db.supabase.table("account_identities")
                .delete()
                .eq("provider", provider)
                .eq("provider_user_id", provider_user_id)
                .execute()
            )
            if not result.data:
                if hasattr(db, "_inc_metric"):
                    db._inc_metric("unlink_fail")
                return False, "Связь не найдена"

            AccountsService.invalidate_account_id_cache(provider, provider_user_id)
            account_id = None
            if result.data:
                account_id = str(result.data[0].get("account_id") or "").strip() or None
            if account_id:
                from bot.services.external_roles_sync_service import ExternalRolesSyncService

                ExternalRolesSyncService.trigger_account_sync(
                    account_id,
                    reason=f"unlink_{provider}",
                )
            if hasattr(db, "_inc_metric"):
                db._inc_metric("unlink_success")
            logger.info("identity_unlinked provider=%s provider_user_id=%s account_id=%s", provider, provider_user_id, account_id)
            return True, "Связь удалена"
        except Exception as e:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("unlink_fail")
            logger.error("unlink_identity failed (%s:%s): %s", provider, provider_user_id, e)
            return False, "Ошибка unlink"
