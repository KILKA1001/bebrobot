import logging
from datetime import datetime, timezone
from typing import Any, Callable

from bot.data import db
from bot.services.accounts_service import AccountsService
from bot.services.authority_service import AuthorityService
from bot.services.profile_titles import is_protected_profile_title
from bot.services.auth import RoleResolver
from bot.utils.roles_and_activities import ROLE_THRESHOLDS

_AUTO_DISCORD_CATEGORY = "Discord сервер (auto)"
_LEGACY_POINTS_CATEGORY = "Роли за баллы"
_LEGACY_POINTS_ROLE_NAMES = {
    1212624623548768287: "Бог среди волонтеров",
    1105906637824331788: "Легендарный среди волонтеров",
    1137775519589466203: "Мастер волонтер",
    1105906455233703989: "Хороший Помощник Бебр",
    1105906310131744868: "Новый волонтер",
}

logger = logging.getLogger(__name__)

DELETE_ROLE_REASON_DISCORD_MANAGED = "discord_managed"
DELETE_ROLE_REASON_NOT_FOUND = "not_found"
DELETE_ROLE_REASON_ERROR = "error"
ROLE_ASSIGNMENT_REASON_PRIVILEGED_DISCORD_ROLE = "privileged_discord_role"
PRIVILEGED_DISCORD_ROLE_MESSAGE = "Эту Discord-роль может выдавать только глава/главный вице."
USER_ACQUIRE_HINT_PLACEHOLDER = "Способ получения пока не указан администратором"
PROTECTED_PROFILE_TITLE_ROLE_MESSAGE = "Это звание управляется через profile_title_roles → accounts.titles и не должно выдаваться как обычная роль."
ACQUIRE_METHOD_POINTS = "за баллы"
ACQUIRE_METHOD_ADMIN = "выдаёт администратор"
ACQUIRE_METHOD_DISCORD_SYNC = "автоматически синхронизируется с Discord"


class RoleManagementService:
    @staticmethod
    def _jsonable(value: Any) -> Any:
        if isinstance(value, dict):
            return {str(key): RoleManagementService._jsonable(val) for key, val in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [RoleManagementService._jsonable(item) for item in value]
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        return str(value)

    @staticmethod
    def _load_account_identity_snapshot(account_id: str | None) -> dict[str, str | None]:
        snapshot = {
            "account_id": str(account_id or "").strip() or None,
            "discord_user_id": None,
            "telegram_user_id": None,
        }
        account_key = str(account_id or "").strip()
        if not account_key or not db.supabase:
            return snapshot
        try:
            response = (
                db.supabase.table("account_identities")
                .select("provider,provider_user_id")
                .eq("account_id", account_key)
                .execute()
            )
            for row in response.data or []:
                provider = str(row.get("provider") or "").strip().lower()
                provider_user_id = str(row.get("provider_user_id") or "").strip() or None
                if provider == "discord" and provider_user_id:
                    snapshot["discord_user_id"] = provider_user_id
                elif provider == "telegram" and provider_user_id:
                    snapshot["telegram_user_id"] = provider_user_id
        except Exception:
            logger.exception("role audit failed to load account identities account_id=%s", account_key)
        return snapshot

    @staticmethod
    def _resolve_audit_identity(
        *,
        provider: str | None = None,
        user_id: str | None = None,
        account_id: str | None = None,
    ) -> dict[str, str | None]:
        normalized_provider = str(provider or "").strip().lower() or None
        normalized_user_id = str(user_id or "").strip() or None
        resolved_account_id = str(account_id or "").strip() or None
        if not resolved_account_id and normalized_provider and normalized_user_id:
            try:
                resolved_account_id = AccountsService.resolve_account_id(normalized_provider, normalized_user_id)
            except Exception:
                logger.exception(
                    "role audit failed to resolve account provider=%s provider_user_id=%s",
                    normalized_provider,
                    normalized_user_id,
                )
        snapshot = RoleManagementService._load_account_identity_snapshot(resolved_account_id)
        return {
            "account_id": resolved_account_id,
            "provider": normalized_provider,
            "provider_user_id": normalized_user_id,
            "discord_user_id": snapshot.get("discord_user_id"),
            "telegram_user_id": snapshot.get("telegram_user_id"),
        }

    @staticmethod
    def record_role_change_audit(
        *,
        action: str,
        role_name: str | None,
        source: str | None,
        actor_provider: str | None = None,
        actor_user_id: str | None = None,
        actor_account_id: str | None = None,
        target_provider: str | None = None,
        target_user_id: str | None = None,
        target_account_id: str | None = None,
        before: Any = None,
        after: Any = None,
        status: str = "success",
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None:
        actor = RoleManagementService._resolve_audit_identity(
            provider=actor_provider,
            user_id=actor_user_id,
            account_id=actor_account_id,
        )
        target = RoleManagementService._resolve_audit_identity(
            provider=target_provider,
            user_id=target_user_id,
            account_id=target_account_id,
        )
        normalized_action = str(action or "").strip() or "unknown"
        normalized_role_name = str(role_name or "").strip() or "*"
        normalized_source = str(source or "").strip() or "unknown"
        normalized_status = str(status or "").strip() or "success"
        normalized_error_code = str(error_code or "").strip() or None
        normalized_error_message = str(error_message or "").strip() or None
        before_value = RoleManagementService._jsonable(before)
        after_value = RoleManagementService._jsonable(after)

        log_method = logger.info if normalized_status == "success" else logger.warning
        log_method(
            "role_audit actor_account_id=%s actor_provider=%s actor_user_id=%s target_account_id=%s target_provider=%s target_user_id=%s action=%s role_name=%s source=%s status=%s before=%s after=%s error_code=%s error_message=%s",
            actor.get("account_id"),
            actor.get("provider"),
            actor.get("provider_user_id"),
            target.get("account_id"),
            target.get("provider"),
            target.get("provider_user_id"),
            normalized_action,
            normalized_role_name,
            normalized_source,
            normalized_status,
            before_value,
            after_value,
            normalized_error_code,
            normalized_error_message,
        )

        if not db.supabase:
            logger.warning("role audit skipped: supabase is not configured action=%s role_name=%s", normalized_action, normalized_role_name)
            return

        payload = {
            "actor_user_id": actor.get("provider_user_id") or "",
            "target_user_id": target.get("provider_user_id") or "",
            "action": normalized_action,
            "role_id": normalized_role_name,
            "role_name": normalized_role_name,
            "source": normalized_source,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "reason": normalized_error_message or normalized_error_code,
            "actor_account_id": actor.get("account_id"),
            "actor_provider": actor.get("provider"),
            "actor_provider_user_id": actor.get("provider_user_id"),
            "target_account_id": target.get("account_id"),
            "target_provider": target.get("provider"),
            "target_provider_user_id": target.get("provider_user_id"),
            "before_value": before_value,
            "after_value": after_value,
            "status": normalized_status,
            "error_code": normalized_error_code,
            "error_message": normalized_error_message,
        }
        try:
            db.supabase.table("role_change_audit").insert(payload).execute()
        except Exception:
            legacy_payload = {
                "actor_user_id": actor.get("provider_user_id") or "",
                "target_user_id": target.get("provider_user_id") or "",
                "action": normalized_action,
                "role_id": normalized_role_name,
                "source": normalized_source,
                "created_at": payload["created_at"],
                "reason": normalized_error_message or normalized_error_code,
            }
            try:
                db.supabase.table("role_change_audit").insert(legacy_payload).execute()
            except Exception:
                logger.exception("role audit insert failed payload=%s", payload)

    @staticmethod
    def _normalize_role_names(role_names: list[str] | tuple[str, ...] | set[str] | None) -> list[str]:
        seen: set[str] = set()
        normalized: list[str] = []
        for item in list(role_names or []):
            role_key = str(item or "").strip()
            if not role_key or role_key in seen:
                continue
            seen.add(role_key)
            normalized.append(role_key)
        return normalized

    @staticmethod
    def _log_user_role_batch_item(
        *,
        actor_id: str | None,
        target_account_id: str,
        role_name: str,
        action: str,
        success: bool,
        error: str | None = None,
    ) -> None:
        log_method = logger.info if success else logger.warning
        log_method(
            "user_role_batch actor_id=%s target_account_id=%s role_name=%s action=%s success=%s failure=%s",
            actor_id,
            target_account_id,
            role_name,
            action,
            success,
            error or "",
        )

    @staticmethod
    def _role_action_result(
        ok: bool,
        *,
        reason: str | None = None,
        message: str | None = None,
        role_name: str | None = None,
        discord_role_id: str | None = None,
    ) -> dict[str, Any]:
        return {
            "ok": ok,
            "reason": reason,
            "message": message,
            "role_name": role_name,
            "discord_role_id": discord_role_id,
        }

    @staticmethod
    def _check_privileged_discord_role_access(
        *,
        actor_provider: str | None,
        actor_user_id: str | None,
        role_name: str,
        role_info: dict[str, Any] | None = None,
        action: str,
    ) -> dict[str, Any]:
        role = dict(role_info or RoleManagementService.get_role(role_name) or {})
        discord_role_id = str(role.get("discord_role_id") or "").strip() or None
        is_discord_role = bool(discord_role_id)
        is_privileged = bool(role.get("is_privileged_discord_role"))
        if not is_discord_role or not is_privileged:
            return RoleManagementService._role_action_result(
                True,
                role_name=role_name,
                discord_role_id=discord_role_id,
            )

        provider = str(actor_provider or "").strip()
        user_id = str(actor_user_id or "").strip()
        if provider and user_id and AuthorityService.is_super_admin(provider, user_id):
            return RoleManagementService._role_action_result(
                True,
                role_name=role_name,
                discord_role_id=discord_role_id,
            )

        actor = AuthorityService.resolve_authority(provider, user_id) if provider and user_id else None
        actor_titles = sorted(AuthorityService._normalized_titles(actor.titles if actor else tuple()))
        logger.warning(
            "privileged_discord_role_access_denied actor_id=%s actor_provider=%s target_role=%s discord_role_id=%s actor_titles=%s action=%s",
            user_id or None,
            provider or None,
            role_name,
            discord_role_id,
            actor_titles,
            action,
        )
        return RoleManagementService._role_action_result(
            False,
            reason=ROLE_ASSIGNMENT_REASON_PRIVILEGED_DISCORD_ROLE,
            message=PRIVILEGED_DISCORD_ROLE_MESSAGE,
            role_name=role_name,
            discord_role_id=discord_role_id,
        )

    @staticmethod
    def _normalized_description(value: str | None) -> str | None:
        normalized = str(value or "").strip()
        return normalized or None

    @staticmethod
    def _description_text(value: object) -> str:
        return str(value or "").strip()

    @staticmethod
    def _normalized_acquire_hint(value: str | None) -> str | None:
        normalized = str(value or "").strip()
        return normalized or None

    @staticmethod
    def _acquire_hint_text(value: object) -> str:
        return str(value or "").strip()

    @staticmethod
    def _log_role_position_error(
        message: str,
        *,
        actor_id: str | None = None,
        operation: str | None = None,
        role_name: str | None = None,
        category: str | None = None,
        requested_position: int | None = None,
        computed_last_position: int | None = None,
    ) -> None:
        logger.warning(
            "%s actor_id=%s operation=%s role_name=%s category=%s requested_position=%s computed_last_position=%s",
            message,
            actor_id,
            operation,
            role_name,
            category,
            requested_position,
            computed_last_position,
        )

    @staticmethod
    def _delete_role_result(
        ok: bool,
        *,
        reason: str | None = None,
        role_name: str | None = None,
        discord_role_id: str | None = None,
        is_discord_managed: bool = False,
    ) -> dict[str, Any]:
        return {
            "ok": ok,
            "reason": reason,
            "role_name": role_name,
            "discord_role_id": discord_role_id,
            "is_discord_managed": is_discord_managed,
        }

    @staticmethod
    def _delete_role_dependencies(role_name: str, *, log_context: str) -> None:
        normalized_role_name = str(role_name or "").strip()
        if not normalized_role_name or not db.supabase:
            return
        try:
            db.supabase.table("account_role_assignments").delete().eq("role_name", normalized_role_name).execute()
            db.supabase.table("role_permissions").delete().eq("role_name", normalized_role_name).execute()
            logger.info("%s deleted dependent role rows role_name=%s", log_context, normalized_role_name)
        except Exception:
            logger.exception("%s failed to delete dependent role rows role_name=%s", log_context, normalized_role_name)
            raise

    @staticmethod
    def _load_roles_rows(*, log_context: str | None = None) -> list[dict[str, Any]]:
        """Read role rows with backward-compatible column fallback."""
        if not db.supabase:
            return []

        select_variants = (
            "name,category_name,description,acquire_hint,position,is_discord_managed,discord_role_id,is_privileged_discord_role",
            "name,category_name,acquire_hint,position,is_discord_managed,discord_role_id,is_privileged_discord_role",
            "name,category_name,position,is_discord_managed,discord_role_id,is_privileged_discord_role",
            "name,category_name,description,acquire_hint,position,is_discord_managed,discord_role_id",
            "name,category_name,acquire_hint,position,is_discord_managed,discord_role_id",
            "name,category_name,position,is_discord_managed,discord_role_id",
            "name,category_name,description,acquire_hint,position",
            "name,category_name,acquire_hint,position",
            "name,category_name,description,position",
            "name,category_name,position",
        )
        for select_clause in select_variants:
            try:
                response = db.supabase.table("roles").select(select_clause).execute()
                rows = response.data or []
                filtered_rows: list[dict[str, Any]] = []
                for row in rows:
                    role_name = str(row.get("name") or "").strip()
                    if role_name and is_protected_profile_title(role_name):
                        logger.warning(
                            "roles query filtered protected profile title from catalog command=%s role_name=%s",
                            log_context or "n/a",
                            role_name,
                        )
                        continue
                    filtered_rows.append(row)
                return filtered_rows
            except Exception:
                logger.exception("roles query failed command=%s select=%s", log_context or "n/a", select_clause)
        return []

    @staticmethod
    def _normalized_category(name: str | None) -> str:
        value = str(name or "").strip()
        return value or "Без категории"

    @staticmethod
    def _ensure_category_exists(
        name: str | None,
        *,
        default_position: int = 0,
        log_context: str = "ensure_category_exists",
    ) -> bool:
        category = RoleManagementService._normalized_category(name)
        if not db.supabase:
            logger.warning("%s skipped: supabase is not configured category=%s", log_context, category)
            return False

        try:
            response = (
                db.supabase.table("role_categories")
                .select("name,position")
                .eq("name", category)
                .limit(1)
                .execute()
            )
            existing = list(response.data or [])
            if existing:
                logger.info(
                    "%s preserved existing category position category=%s position=%s default_position=%s",
                    log_context,
                    category,
                    int((existing[0] or {}).get("position") or 0),
                    int(default_position),
                )
                return True

            db.supabase.table("role_categories").insert({"name": category, "position": int(default_position)}).execute()
            logger.info("%s created missing category category=%s position=%s", log_context, category, int(default_position))
            return True
        except Exception:
            logger.exception("%s failed category=%s default_position=%s", log_context, category, default_position)
            return False

    @staticmethod
    def _build_grouped_roles(
        categories_rows: list[dict[str, Any]] | None,
        roles_rows: list[dict[str, Any]] | None,
    ) -> list[dict[str, Any]]:
        category_positions: dict[str, int] = {}
        for row in categories_rows or []:
            name = RoleManagementService._normalized_category(row.get("name"))
            category_positions[name] = int(row.get("position") or 0)

        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in roles_rows or []:
            role_name = str(row.get("name") or "").strip()
            if not role_name:
                continue
            category = RoleManagementService._normalized_category(row.get("category_name"))
            grouped.setdefault(category, []).append(
                {
                    "name": role_name,
                    "description": RoleManagementService._description_text(row.get("description")),
                    "acquire_hint": RoleManagementService._acquire_hint_text(row.get("acquire_hint")),
                    "position": int(row.get("position") or 0),
                    "is_discord_managed": bool(row.get("is_discord_managed")),
                    "discord_role_id": str(row.get("discord_role_id") or "").strip() or None,
                    "is_privileged_discord_role": bool(row.get("is_privileged_discord_role")),
                }
            )

        categories = sorted(set(category_positions) | set(grouped), key=lambda item: (category_positions.get(item, 0), item.lower()))
        result: list[dict[str, Any]] = []
        for category in categories:
            roles = sorted(grouped.get(category, []), key=lambda item: (item["position"], item["name"].lower()))
            result.append({"category": category, "position": category_positions.get(category, 0), "roles": roles})
        return result

    @staticmethod
    def _load_external_discord_bindings(*, log_context: str | None = None) -> list[dict[str, Any]]:
        if not db.supabase:
            return []
        try:
            response = (
                db.supabase.table("external_role_bindings")
                .select("account_id,external_role_id,external_role_name")
                .eq("source", "discord")
                .is_("deleted_at", "null")
                .execute()
            )
            return response.data or []
        except Exception:
            logger.exception("external_role_bindings discord query failed command=%s", log_context or "n/a")
            return []

    @staticmethod
    def _upsert_discord_catalog_role(
        *,
        role_id: str,
        role_name: str,
        existing: dict[str, Any] | None,
        default_category: str,
        default_position: int,
        source: str,
        account_id: str | None = None,
        guild_id: str | None = None,
    ) -> bool:
        if not db.supabase or not role_id or not role_name:
            return False

        if not existing:
            logger.warning(
                "discord role missing in catalog; creating canonical entry role_name=%s external_role_id=%s account_id=%s guild_id=%s source=%s",
                role_name,
                role_id,
                account_id,
                guild_id,
                source,
            )

        preserved_category = RoleManagementService._normalized_category((existing or {}).get("category_name"))
        preserved_position = int((existing or {}).get("position") or default_position)
        payload = {
            "name": role_name,
            "category_name": preserved_category if existing else default_category,
            "position": preserved_position,
            "is_discord_managed": True,
            "discord_role_id": role_id,
            "discord_role_name": role_name,
            "is_privileged_discord_role": bool((existing or {}).get("is_privileged_discord_role")),
        }

        try:
            RoleManagementService._ensure_category_exists(
                payload["category_name"],
                default_position=9999,
                log_context="_upsert_discord_catalog_role",
            )
            if existing:
                db.supabase.table("roles").update(payload).eq("discord_role_id", role_id).execute()
            else:
                db.supabase.table("roles").upsert(payload, on_conflict="name").execute()
            return True
        except Exception:
            logger.exception(
                "discord role catalog upsert failed role_name=%s external_role_id=%s account_id=%s guild_id=%s source=%s",
                role_name,
                role_id,
                account_id,
                guild_id,
                source,
            )
            return False

    @staticmethod
    def _sync_discord_roles_from_external_bindings(
        existing_by_role_id: dict[str, dict[str, Any]],
        *,
        log_context: str | None = None,
    ) -> tuple[int, set[str]]:
        if not db.supabase:
            return 0, set()

        upserted = 0
        synced_ids: set[str] = set()
        for row in RoleManagementService._load_external_discord_bindings(log_context=log_context):
            role_id = str(row.get("external_role_id") or "").strip()
            role_name = str(row.get("external_role_name") or "").strip()
            account_id = str(row.get("account_id") or "").strip() or None
            if not role_id or not role_name:
                continue
            synced_ids.add(role_id)
            existing = existing_by_role_id.get(role_id)
            if RoleManagementService._upsert_discord_catalog_role(
                role_id=role_id,
                role_name=role_name,
                existing=existing,
                default_category=_AUTO_DISCORD_CATEGORY,
                default_position=0,
                source="external_role_bindings",
                account_id=account_id,
            ):
                upserted += 1
                existing_by_role_id[role_id] = {
                    "name": role_name,
                    "discord_role_id": role_id,
                    "category_name": (existing or {}).get("category_name") or _AUTO_DISCORD_CATEGORY,
                    "position": int((existing or {}).get("position") or 0),
                }
        return upserted, synced_ids

    @staticmethod
    def ensure_external_discord_roles_in_catalog(*, log_context: str | None = None) -> dict[str, int]:
        if not db.supabase:
            return {"upserted": 0}

        try:
            existing_managed_resp = (
                db.supabase.table("roles")
                .select("name,discord_role_id,category_name,position")
                .eq("is_discord_managed", True)
                .execute()
            )
            existing_by_role_id: dict[str, dict[str, Any]] = {}
            for row in existing_managed_resp.data or []:
                existing_role_id = str(row.get("discord_role_id") or "").strip()
                if existing_role_id and existing_role_id not in existing_by_role_id:
                    existing_by_role_id[existing_role_id] = row

            upserted, _ = RoleManagementService._sync_discord_roles_from_external_bindings(
                existing_by_role_id,
                log_context=log_context,
            )
            if upserted:
                logger.info("ensure_external_discord_roles_in_catalog completed upserted=%s", upserted)
            return {"upserted": upserted}
        except Exception:
            logger.exception("ensure_external_discord_roles_in_catalog failed command=%s", log_context or "n/a")
            return {"upserted": 0}

    @staticmethod
    def list_roles_grouped(*, log_context: str | None = None) -> list[dict[str, Any]]:
        if not db.supabase:
            logger.warning("list_roles_grouped skipped: supabase is not configured")
            return []

        try:
            RoleManagementService.ensure_external_discord_roles_in_catalog(log_context=log_context)
            categories_resp = db.supabase.table("role_categories").select("name,position").execute()
            roles_rows = RoleManagementService._load_roles_rows(log_context=log_context)
        except Exception:
            logger.exception("list_roles_grouped failed command=%s", log_context or "n/a")
            return []

        external_rows = RoleManagementService._load_external_discord_bindings(log_context=log_context)
        catalog_role_ids = {
            str(row.get("discord_role_id") or "").strip()
            for row in roles_rows
            if str(row.get("discord_role_id") or "").strip()
        }
        for row in external_rows:
            role_id = str(row.get("external_role_id") or "").strip()
            role_name = str(row.get("external_role_name") or "").strip()
            account_id = str(row.get("account_id") or "").strip() or None
            if role_id and role_name and role_id not in catalog_role_ids:
                logger.warning(
                    "discord role present in external snapshot but absent from canonical catalog role_name=%s external_role_id=%s account_id=%s guild_id=%s",
                    role_name,
                    role_id,
                    account_id,
                    None,
                )

        return RoleManagementService._build_grouped_roles(categories_resp.data or [], roles_rows)

    @staticmethod
    def _resolve_legacy_points_role_name(
        role_id: int,
        *,
        role_name_resolver: Callable[[int], str | None] | None = None,
        log_context: str | None = None,
    ) -> str:
        try:
            resolved_name = role_name_resolver(role_id) if role_name_resolver else None
        except Exception:
            logger.exception(
                "legacy points role name resolver failed command=%s role_id=%s",
                log_context or "n/a",
                role_id,
            )
            resolved_name = None
        return str(resolved_name or _LEGACY_POINTS_ROLE_NAMES.get(role_id) or f"Legacy role {role_id}").strip()

    @staticmethod
    def list_public_roles_catalog(
        *,
        role_name_resolver: Callable[[int], str | None] | None = None,
        log_context: str | None = None,
    ) -> list[dict[str, Any]]:
        grouped = RoleManagementService.list_roles_grouped(log_context=log_context)
        public_grouped: list[dict[str, Any]] = []
        roles_by_discord_id: dict[str, dict[str, Any]] = {}
        roles_by_name: dict[str, dict[str, Any]] = {}
        category_positions: dict[str, int] = {}

        for item in grouped:
            category_name = str(item.get("category") or "Без категории")
            category_positions[category_name] = int(item.get("position") or 0)
            public_item = {
                "category": category_name,
                "position": int(item.get("position") or 0),
                "roles": [],
            }
            for role in item.get("roles", []):
                public_role = dict(role)
                public_role["points_required"] = None
                public_role["acquire_method"] = (
                    ACQUIRE_METHOD_DISCORD_SYNC if public_role.get("is_discord_managed") else ACQUIRE_METHOD_ADMIN
                )
                public_role["acquire_method_label"] = str(public_role["acquire_method"])
                public_item["roles"].append(public_role)

                discord_role_id = str(public_role.get("discord_role_id") or "").strip()
                if discord_role_id:
                    roles_by_discord_id[discord_role_id] = public_role
                roles_by_name[str(public_role.get("name") or "").strip().lower()] = public_role
            public_grouped.append(public_item)

        legacy_roles: list[dict[str, Any]] = []
        for index, (role_id, points_needed) in enumerate(sorted(ROLE_THRESHOLDS.items(), key=lambda item: item[1])):
            resolved_name = RoleManagementService._resolve_legacy_points_role_name(
                role_id,
                role_name_resolver=role_name_resolver,
                log_context=log_context,
            )
            public_role = roles_by_discord_id.get(str(role_id)) or roles_by_name.get(resolved_name.lower())
            if public_role:
                public_role["points_required"] = points_needed
                public_role["acquire_method"] = ACQUIRE_METHOD_POINTS
                public_role["acquire_method_label"] = ACQUIRE_METHOD_POINTS
                if not str(public_role.get("acquire_hint") or "").strip():
                    public_role["acquire_hint"] = f"Накопить {points_needed} баллов."
                continue

            legacy_roles.append(
                {
                    "name": resolved_name,
                    "description": "",
                    "acquire_hint": f"Накопить {points_needed} баллов.",
                    "position": index,
                    "is_discord_managed": False,
                    "discord_role_id": str(role_id),
                    "points_required": points_needed,
                    "acquire_method": ACQUIRE_METHOD_POINTS,
                    "acquire_method_label": ACQUIRE_METHOD_POINTS,
                }
            )

        if legacy_roles:
            public_grouped.append(
                {
                    "category": _LEGACY_POINTS_CATEGORY,
                    "position": category_positions.get(_LEGACY_POINTS_CATEGORY, 9998),
                    "roles": legacy_roles,
                }
            )

        public_grouped.sort(key=lambda item: (int(item.get("position") or 0), str(item.get("category") or "").lower()))
        return public_grouped

    @staticmethod
    def list_roles_available_for_admin_reorder() -> list[dict[str, str]]:
        flattened: list[dict[str, str]] = []
        grouped = RoleManagementService.list_roles_grouped()
        for item in grouped:
            category = str(item.get("category") or "Без категории")
            for role in item.get("roles", []):
                role_name = str(role.get("name") or "").strip()
                if role_name:
                    flattened.append({"role": role_name, "category": category})
        return flattened

    @staticmethod
    def create_category(name: str, position: int = 0) -> bool:
        category = RoleManagementService._normalized_category(name)
        if not db.supabase:
            return False
        try:
            db.supabase.table("role_categories").upsert({"name": category, "position": int(position)}).execute()
            logger.info("create_category completed category=%s position=%s", category, int(position))
            return True
        except Exception:
            logger.exception("create_category failed category=%s position=%s", category, position)
            return False

    @staticmethod
    def delete_category(name: str, fallback_category: str = "Без категории") -> bool:
        category = RoleManagementService._normalized_category(name)
        fallback = RoleManagementService._normalized_category(fallback_category)
        if category == fallback:
            logger.warning("delete_category denied: fallback equals target category=%s", category)
            return False
        if not db.supabase:
            return False
        try:
            RoleManagementService._ensure_category_exists(
                fallback,
                default_position=999,
                log_context="delete_category_fallback",
            )
            db.supabase.table("roles").update({"category_name": fallback}).eq("category_name", category).execute()
            db.supabase.table("role_categories").delete().eq("name", category).execute()
            logger.info("delete_category completed category=%s fallback_category=%s", category, fallback)
            return True
        except Exception:
            logger.exception("delete_category failed category=%s", category)
            return False

    @staticmethod
    def create_role(
        name: str,
        category: str,
        description: str | None = None,
        acquire_hint: str | None = None,
        position: int | None = None,
        discord_role_id: str | None = None,
        discord_role_name: str | None = None,
        *,
        actor_id: str | None = None,
        actor_provider: str | None = None,
        actor_user_id: str | None = None,
        actor_account_id: str | None = None,
        operation: str = "role_create",
        source: str = "unknown",
    ) -> bool:
        role_name = str(name or "").strip()
        if not role_name:
            return False
        if is_protected_profile_title(role_name):
            logger.warning(
                "create_role denied protected profile title role_name=%s actor_id=%s actor_provider=%s actor_user_id=%s actor_account_id=%s operation=%s source=%s",
                role_name,
                actor_id,
                actor_provider,
                actor_user_id,
                actor_account_id,
                operation,
                source,
            )
            return False
        if not db.supabase:
            return False

        normalized_category = RoleManagementService._normalized_category(category)
        preview = RoleManagementService.get_category_role_positioning(
            normalized_category,
            requested_position=position,
        )
        computed_position = int(preview.get("computed_position", 0))
        computed_last_position = int(preview.get("computed_last_position", 0))
        normalized_description = RoleManagementService._normalized_description(description)
        normalized_acquire_hint = RoleManagementService._normalized_acquire_hint(acquire_hint)
        before_role = RoleManagementService.get_role(role_name) or {}
        payload = {
            "name": role_name,
            "category_name": normalized_category,
            "description": normalized_description,
            "acquire_hint": normalized_acquire_hint,
            "position": computed_position,
            "is_discord_managed": bool(discord_role_id),
            "discord_role_id": str(discord_role_id).strip() if discord_role_id else None,
            "discord_role_name": str(discord_role_name).strip() if discord_role_name else None,
            "is_privileged_discord_role": False,
        }

        try:
            RoleManagementService._ensure_category_exists(
                normalized_category,
                default_position=0,
                log_context="create_role_category",
            )
            db.supabase.table("roles").upsert(payload, on_conflict="name").execute()
            after_role = RoleManagementService.get_role(role_name) or dict(payload)
            audit_action = "role_edit" if before_role else "role_create"
            logger.info(
                "create_role completed role_name=%s category=%s description_length=%s actor_id=%s actor_provider=%s actor_user_id=%s actor_account_id=%s operation=%s source=%s computed_position=%s before=%s after=%s",
                role_name,
                normalized_category,
                len(normalized_description or ""),
                actor_id,
                actor_provider,
                actor_user_id,
                actor_account_id,
                operation,
                source,
                computed_position,
                RoleManagementService._jsonable(before_role),
                RoleManagementService._jsonable(after_role),
            )
            logger.info(
                "create_role metadata role_name=%s actor_id=%s actor_provider=%s actor_user_id=%s actor_account_id=%s field=%s value_length=%s operation=%s source=%s",
                role_name,
                actor_id,
                actor_provider,
                actor_user_id,
                actor_account_id,
                "acquire_hint",
                len(normalized_acquire_hint or ""),
                operation,
                source,
            )
            RoleManagementService.record_role_change_audit(
                action=audit_action,
                role_name=role_name,
                source=source,
                actor_provider=actor_provider,
                actor_user_id=actor_user_id or actor_id,
                actor_account_id=actor_account_id,
                before=before_role,
                after=after_role,
            )
            return True
        except Exception:
            logger.exception(
                "create_role failed actor_id=%s actor_provider=%s actor_user_id=%s actor_account_id=%s operation=%s source=%s role_name=%s category=%s description_length=%s acquire_hint_length=%s requested_position=%s computed_last_position=%s computed_position=%s",
                actor_id,
                actor_provider,
                actor_user_id,
                actor_account_id,
                operation,
                source,
                role_name,
                normalized_category,
                len(normalized_description or ""),
                len(normalized_acquire_hint or ""),
                position,
                computed_last_position,
                computed_position,
            )
            RoleManagementService.record_role_change_audit(
                action="role_create_failed",
                role_name=role_name,
                source=source,
                actor_provider=actor_provider,
                actor_user_id=actor_user_id or actor_id,
                actor_account_id=actor_account_id,
                before=before_role,
                after=payload,
                status="error",
                error_code="db_write_failed",
                error_message="create_role failed",
            )
            return False

    @staticmethod
    def delete_role(
        name: str,
        *,
        actor_id: str | None = None,
        actor_provider: str | None = None,
        actor_user_id: str | None = None,
        actor_account_id: str | None = None,
        guild_id: str | None = None,
        telegram_user_id: str | None = None,
        source: str = "unknown",
    ) -> dict[str, Any]:
        role_name = str(name or "").strip()
        if not role_name or not db.supabase:
            return RoleManagementService._delete_role_result(False, reason=DELETE_ROLE_REASON_ERROR, role_name=role_name or None)
        try:
            role_row = (
                db.supabase.table("roles")
                .select("name,is_discord_managed,discord_role_id,is_privileged_discord_role")
                .eq("name", role_name)
                .limit(1)
                .execute()
            )
            role = (role_row.data or [None])[0]
            if not role:
                logger.warning(
                    "delete_role skipped role missing role_name=%s actor_id=%s actor_provider=%s actor_user_id=%s actor_account_id=%s guild_id=%s telegram_user_id=%s source=%s",
                    role_name,
                    actor_id,
                    actor_provider,
                    actor_user_id,
                    actor_account_id,
                    guild_id,
                    telegram_user_id,
                    source,
                )
                RoleManagementService.record_role_change_audit(
                    action="role_delete_denied",
                    role_name=role_name,
                    source=source,
                    actor_provider=actor_provider,
                    actor_user_id=actor_user_id or actor_id or telegram_user_id,
                    actor_account_id=actor_account_id,
                    before={"exists": False},
                    after={"exists": False},
                    status="denied",
                    error_code=DELETE_ROLE_REASON_NOT_FOUND,
                    error_message="role not found",
                )
                return RoleManagementService._delete_role_result(
                    False,
                    reason=DELETE_ROLE_REASON_NOT_FOUND,
                    role_name=role_name,
                )

            discord_role_id = str(role.get("discord_role_id") or "").strip() or None
            is_discord_managed = bool(role.get("is_discord_managed"))
            if is_discord_managed:
                logger.warning(
                    "delete_role denied discord-managed role_name=%s discord_role_id=%s actor_id=%s actor_provider=%s actor_user_id=%s actor_account_id=%s guild_id=%s telegram_user_id=%s source=%s",
                    role_name,
                    discord_role_id,
                    actor_id,
                    actor_provider,
                    actor_user_id,
                    actor_account_id,
                    guild_id,
                    telegram_user_id,
                    source,
                )
                RoleManagementService.record_role_change_audit(
                    action="role_delete_denied",
                    role_name=role_name,
                    source=source,
                    actor_provider=actor_provider,
                    actor_user_id=actor_user_id or actor_id or telegram_user_id,
                    actor_account_id=actor_account_id,
                    before=role,
                    after=role,
                    status="denied",
                    error_code=DELETE_ROLE_REASON_DISCORD_MANAGED,
                    error_message="discord managed role cannot be deleted",
                )
                return RoleManagementService._delete_role_result(
                    False,
                    reason=DELETE_ROLE_REASON_DISCORD_MANAGED,
                    role_name=role_name,
                    discord_role_id=discord_role_id,
                    is_discord_managed=True,
                )

            RoleManagementService._delete_role_dependencies(role_name, log_context="delete_role")
            db.supabase.table("roles").delete().eq("name", role_name).execute()
            RoleManagementService.record_role_change_audit(
                action="role_delete",
                role_name=role_name,
                source=source,
                actor_provider=actor_provider,
                actor_user_id=actor_user_id or actor_id or telegram_user_id,
                actor_account_id=actor_account_id,
                before=role,
                after={"deleted": True},
            )
            return RoleManagementService._delete_role_result(
                True,
                role_name=role_name,
                discord_role_id=discord_role_id,
                is_discord_managed=is_discord_managed,
            )
        except Exception:
            logger.exception(
                "delete_role failed role_name=%s actor_id=%s actor_provider=%s actor_user_id=%s actor_account_id=%s guild_id=%s telegram_user_id=%s source=%s",
                role_name,
                actor_id,
                actor_provider,
                actor_user_id,
                actor_account_id,
                guild_id,
                telegram_user_id,
                source,
            )
            RoleManagementService.record_role_change_audit(
                action="role_delete_failed",
                role_name=role_name,
                source=source,
                actor_provider=actor_provider,
                actor_user_id=actor_user_id or actor_id or telegram_user_id,
                actor_account_id=actor_account_id,
                before={"role_name": role_name},
                after=None,
                status="error",
                error_code=DELETE_ROLE_REASON_ERROR,
                error_message="delete_role failed",
            )
            return RoleManagementService._delete_role_result(False, reason=DELETE_ROLE_REASON_ERROR, role_name=role_name)

    @staticmethod
    def move_role(
        role_name: str,
        category: str,
        position: int | None = None,
        *,
        actor_id: str | None = None,
        operation: str = "role_move",
    ) -> bool:
        name = str(role_name or "").strip()
        normalized_category = RoleManagementService._normalized_category(category)
        if not name or not db.supabase:
            return False
        try:
            existing_role = RoleManagementService.get_role(name)
            preview = RoleManagementService.get_category_role_positioning(
                normalized_category,
                requested_position=position,
                exclude_role_name=name,
            )
            computed_position = int(preview.get("computed_position", 0))
            computed_last_position = int(preview.get("computed_last_position", 0))
            if not existing_role:
                RoleManagementService._log_role_position_error(
                    "move_role denied role missing from canonical catalog",
                    actor_id=actor_id,
                    operation=operation,
                    role_name=name,
                    category=normalized_category,
                    requested_position=position,
                    computed_last_position=computed_last_position,
                )
                return False
            RoleManagementService._ensure_category_exists(
                normalized_category,
                default_position=0,
                log_context="move_role_category",
            )
            response = (
                db.supabase.table("roles")
                .update({"category_name": normalized_category, "position": computed_position})
                .eq("name", name)
                .execute()
            )
            if existing_role and response is not None and hasattr(response, "data") and response.data == []:
                RoleManagementService._log_role_position_error(
                    "move_role update returned no rows",
                    actor_id=actor_id,
                    operation=operation,
                    role_name=name,
                    category=normalized_category,
                    requested_position=position,
                    computed_last_position=computed_last_position,
                )
                return False
            logger.info(
                "move_role completed actor_id=%s operation=%s role_name=%s category=%s requested_position=%s computed_position=%s",
                actor_id,
                operation,
                name,
                normalized_category,
                position,
                computed_position,
            )
            return True
        except Exception:
            preview = RoleManagementService.get_category_role_positioning(
                normalized_category,
                requested_position=position,
                exclude_role_name=name,
            )
            logger.exception(
                "move_role failed actor_id=%s operation=%s role_name=%s category=%s requested_position=%s computed_last_position=%s computed_position=%s",
                actor_id,
                operation,
                name,
                normalized_category,
                position,
                int(preview.get('computed_last_position', 0)),
                int(preview.get('computed_position', 0)),
            )
            return False

    @staticmethod
    def get_category_role_positioning(
        category: str,
        *,
        requested_position: int | None = None,
        exclude_role_name: str | None = None,
    ) -> dict[str, Any]:
        normalized_category = RoleManagementService._normalized_category(category)
        grouped = RoleManagementService.list_roles_grouped()
        category_item = next(
            (item for item in grouped if str(item.get("category") or "") == normalized_category),
            None,
        )

        current_roles: list[dict[str, Any]] = []
        for item in list((category_item or {}).get("roles", [])):
            role_name = str(item.get("name") or "").strip()
            if not role_name:
                continue
            if exclude_role_name and role_name == str(exclude_role_name).strip():
                continue
            current_roles.append(
                {
                    "name": role_name,
                    "position": int(item.get("position") or 0),
                    "is_discord_managed": bool(item.get("is_discord_managed")),
                    "discord_role_id": str(item.get("discord_role_id") or "").strip() or None,
                }
            )

        computed_last_position = len(current_roles)
        if requested_position is None:
            computed_position = computed_last_position
        else:
            computed_position = max(0, min(int(requested_position), computed_last_position))

        insertion_positions: list[dict[str, Any]] = []
        for index in range(computed_last_position + 1):
            human_index = index + 1
            if computed_last_position == 0:
                description = "категория пуста, роль станет первой (#1)"
            elif index == 0:
                description = "будет добавлено в начало (#1)"
            elif index == computed_last_position:
                description = f"будет добавлено в конец (#{human_index})"
            else:
                before_role = current_roles[index]["name"]
                description = f"будет добавлено на позицию #{human_index} перед «{before_role}»"
            insertion_positions.append(
                {
                    "position": index,
                    "human_index": human_index,
                    "description": description,
                }
            )

        if computed_last_position == 0:
            position_description = "категория пуста, роль будет первой (#1)"
        elif computed_position == 0:
            position_description = "будет добавлено в начало (#1)"
        elif computed_position >= computed_last_position:
            position_description = f"будет добавлено в конец (#{computed_last_position + 1})"
        else:
            position_description = (
                f"будет добавлено на позицию #{computed_position + 1} "
                f"перед «{current_roles[computed_position]['name']}»"
            )

        return {
            "category": normalized_category,
            "current_roles": current_roles,
            "computed_last_position": computed_last_position,
            "requested_position": requested_position,
            "computed_position": computed_position,
            "position_description": position_description,
            "insertion_positions": insertion_positions,
        }

    @staticmethod
    def get_user_roles(provider: str, provider_user_id: str) -> list[dict[str, str | None]]:
        try:
            account_id = AccountsService.resolve_account_id(provider, str(provider_user_id))
            if not account_id:
                return []
            return RoleManagementService.get_user_roles_by_account(str(account_id))
        except Exception:
            logger.exception("get_user_roles failed provider=%s user_id=%s", provider, provider_user_id)
            return []

    @staticmethod
    def get_user_roles_by_account(account_id: str) -> list[dict[str, str | None]]:
        account_key = str(account_id or "").strip()
        if not account_key:
            return []
        try:
            resolved = RoleResolver.resolve_for_account(account_key)
            return resolved.roles
        except Exception:
            logger.exception("get_user_roles_by_account failed account_id=%s", account_key)
            return []

    @staticmethod
    def assign_user_role(provider: str, provider_user_id: str, role_name: str, category: str | None = None) -> bool:
        try:
            account_id = AccountsService.resolve_account_id(provider, str(provider_user_id))
            if not account_id:
                logger.warning("assign_user_role skipped: account not found provider=%s user_id=%s", provider, provider_user_id)
                return False
            result = RoleManagementService.assign_user_role_by_account(
                str(account_id),
                role_name,
                category=category,
                actor_provider=provider,
                actor_user_id=str(provider_user_id),
            )
            return bool(result.get("ok"))
        except Exception:
            logger.exception(
                "assign_user_role failed provider=%s user_id=%s role=%s",
                provider,
                provider_user_id,
                role_name,
            )
            return False

    @staticmethod
    def apply_user_role_changes_by_account(
        account_id: str,
        *,
        actor_id: str | None = None,
        actor_account_id: str | None = None,
        actor_provider: str | None = None,
        actor_user_id: str | None = None,
        target_provider: str | None = None,
        target_user_id: str | None = None,
        grant_roles: list[str] | tuple[str, ...] | set[str] | None = None,
        revoke_roles: list[str] | tuple[str, ...] | set[str] | None = None,
        source: str = "unknown",
    ) -> dict[str, Any]:
        account_key = str(account_id or "").strip()
        if not account_key:
            return {
                "ok": False,
                "grant_success": [],
                "grant_failed": [],
                "revoke_success": [],
                "revoke_failed": [],
                "grant_denied": [],
                "revoke_denied": [],
            }

        normalized_grants = RoleManagementService._normalize_role_names(grant_roles)
        normalized_revokes = RoleManagementService._normalize_role_names(revoke_roles)
        conflicting = set(normalized_grants) & set(normalized_revokes)
        if conflicting:
            logger.warning(
                "apply_user_role_changes_by_account skipped conflicting roles actor_id=%s actor_provider=%s actor_user_id=%s actor_account_id=%s target_account_id=%s target_provider=%s target_user_id=%s source=%s roles=%s",
                actor_id,
                actor_provider,
                actor_user_id,
                actor_account_id,
                account_key,
                target_provider,
                target_user_id,
                source,
                sorted(conflicting),
            )
            RoleManagementService.record_role_change_audit(
                action="role_batch_conflict",
                role_name="*batch*",
                source=source,
                actor_provider=actor_provider,
                actor_user_id=actor_user_id or actor_id,
                actor_account_id=actor_account_id,
                target_provider=target_provider,
                target_user_id=target_user_id,
                target_account_id=account_key,
                before={"grant_roles": normalized_grants, "revoke_roles": normalized_revokes},
                after={"conflicting_roles": sorted(conflicting)},
                status="conflict",
                error_code="conflicting_roles",
                error_message="same role requested for grant and revoke in one batch",
            )
            normalized_grants = [item for item in normalized_grants if item not in conflicting]
            normalized_revokes = [item for item in normalized_revokes if item not in conflicting]

        result: dict[str, Any] = {
            "ok": True,
            "grant_success": [],
            "grant_failed": [],
            "revoke_success": [],
            "revoke_failed": [],
            "grant_denied": [],
            "revoke_denied": [],
            "conflicting_roles": sorted(conflicting),
        }

        for role_name in normalized_grants:
            try:
                role_info = RoleManagementService.get_role(role_name) or {}
                grant_result = RoleManagementService.assign_user_role_by_account(
                    account_key,
                    role_name,
                    category=str(role_info.get("category_name") or "").strip() or None,
                    actor_account_id=actor_account_id,
                    actor_provider=actor_provider,
                    actor_user_id=actor_user_id or actor_id,
                    target_provider=target_provider,
                    target_user_id=target_user_id,
                    source=source,
                )
            except Exception:
                grant_result = RoleManagementService._role_action_result(False, role_name=role_name)
                logger.exception(
                    "apply_user_role_changes_by_account grant crashed actor_id=%s actor_provider=%s actor_user_id=%s actor_account_id=%s target_account_id=%s target_provider=%s target_user_id=%s role_name=%s source=%s",
                    actor_id,
                    actor_provider,
                    actor_user_id,
                    actor_account_id,
                    account_key,
                    target_provider,
                    target_user_id,
                    role_name,
                    source,
                )
                RoleManagementService.record_role_change_audit(
                    action="role_grant_failed",
                    role_name=role_name,
                    source=source,
                    actor_provider=actor_provider,
                    actor_user_id=actor_user_id or actor_id,
                    actor_account_id=actor_account_id,
                    target_provider=target_provider,
                    target_user_id=target_user_id,
                    target_account_id=account_key,
                    before={"assigned": False},
                    after={"assigned": False},
                    status="error",
                    error_code="grant_crashed",
                    error_message="grant crashed before service returned",
                )
            ok = bool(grant_result.get("ok"))
            if ok:
                result["grant_success"].append(role_name)
            else:
                result["grant_failed"].append(role_name)
                if grant_result.get("reason"):
                    result["grant_denied"].append(
                        {
                            "role_name": role_name,
                            "reason": grant_result.get("reason"),
                            "message": grant_result.get("message"),
                            "discord_role_id": grant_result.get("discord_role_id"),
                        }
                    )
                result["ok"] = False
            RoleManagementService._log_user_role_batch_item(
                actor_id=actor_id,
                target_account_id=account_key,
                role_name=role_name,
                action="grant",
                success=ok,
                error=None if ok else str(grant_result.get("reason") or "service_returned_false"),
            )

        for role_name in normalized_revokes:
            try:
                revoke_result = RoleManagementService.revoke_user_role_by_account(
                    account_key,
                    role_name,
                    actor_account_id=actor_account_id,
                    actor_provider=actor_provider,
                    actor_user_id=actor_user_id or actor_id,
                    target_provider=target_provider,
                    target_user_id=target_user_id,
                    source=source,
                )
            except Exception:
                revoke_result = RoleManagementService._role_action_result(False, role_name=role_name)
                logger.exception(
                    "apply_user_role_changes_by_account revoke crashed actor_id=%s actor_provider=%s actor_user_id=%s actor_account_id=%s target_account_id=%s target_provider=%s target_user_id=%s role_name=%s source=%s",
                    actor_id,
                    actor_provider,
                    actor_user_id,
                    actor_account_id,
                    account_key,
                    target_provider,
                    target_user_id,
                    role_name,
                    source,
                )
                RoleManagementService.record_role_change_audit(
                    action="role_revoke_failed",
                    role_name=role_name,
                    source=source,
                    actor_provider=actor_provider,
                    actor_user_id=actor_user_id or actor_id,
                    actor_account_id=actor_account_id,
                    target_provider=target_provider,
                    target_user_id=target_user_id,
                    target_account_id=account_key,
                    before={"assigned": True},
                    after={"assigned": True},
                    status="error",
                    error_code="revoke_crashed",
                    error_message="revoke crashed before service returned",
                )
            ok = bool(revoke_result.get("ok"))
            if ok:
                result["revoke_success"].append(role_name)
            else:
                result["revoke_failed"].append(role_name)
                if revoke_result.get("reason"):
                    result["revoke_denied"].append(
                        {
                            "role_name": role_name,
                            "reason": revoke_result.get("reason"),
                            "message": revoke_result.get("message"),
                            "discord_role_id": revoke_result.get("discord_role_id"),
                        }
                    )
                result["ok"] = False
            RoleManagementService._log_user_role_batch_item(
                actor_id=actor_id,
                target_account_id=account_key,
                role_name=role_name,
                action="revoke",
                success=ok,
                error=None if ok else str(revoke_result.get("reason") or "service_returned_false"),
            )

        if normalized_grants or normalized_revokes:
            RoleManagementService.record_role_change_audit(
                action="role_batch_change",
                role_name="*batch*",
                source=source,
                actor_provider=actor_provider,
                actor_user_id=actor_user_id or actor_id,
                actor_account_id=actor_account_id,
                target_provider=target_provider,
                target_user_id=target_user_id,
                target_account_id=account_key,
                before={"grant_roles": normalized_grants, "revoke_roles": normalized_revokes},
                after=result,
                status="success" if result.get("ok") else "partial",
                error_code=None if result.get("ok") else "partial_failure",
                error_message=None if result.get("ok") else "batch completed with errors",
            )

        return result

    @staticmethod
    def assign_user_role_by_account(
        account_id: str,
        role_name: str,
        category: str | None = None,
        *,
        actor_account_id: str | None = None,
        actor_provider: str | None = None,
        actor_user_id: str | None = None,
        target_provider: str | None = None,
        target_user_id: str | None = None,
        source: str = "unknown",
    ) -> dict[str, Any]:
        if not db.supabase:
            return RoleManagementService._role_action_result(False, role_name=role_name)
        account_key = str(account_id or "").strip()
        role_key = str(role_name or "").strip()
        if not account_key or not role_key:
            return RoleManagementService._role_action_result(False, role_name=role_key or role_name)

        try:
            if is_protected_profile_title(role_key):
                logger.warning(
                    "assign_user_role_by_account denied protected profile title account_id=%s role_name=%s actor_provider=%s actor_user_id=%s target_provider=%s target_user_id=%s source=%s",
                    account_key,
                    role_key,
                    actor_provider,
                    actor_user_id,
                    target_provider,
                    target_user_id,
                    source,
                )
                RoleManagementService.record_role_change_audit(
                    action="role_grant_denied",
                    role_name=role_key,
                    source=source,
                    actor_provider=actor_provider,
                    actor_user_id=actor_user_id,
                    actor_account_id=actor_account_id,
                    target_provider=target_provider,
                    target_user_id=target_user_id,
                    target_account_id=account_key,
                    before={"assigned": False},
                    after={"assigned": False},
                    status="denied",
                    error_code="protected_profile_title",
                    error_message=PROTECTED_PROFILE_TITLE_ROLE_MESSAGE,
                )
                return RoleManagementService._role_action_result(
                    False,
                    reason="protected_profile_title",
                    message=PROTECTED_PROFILE_TITLE_ROLE_MESSAGE,
                    role_name=role_key,
                )
            guard_result = RoleManagementService._check_privileged_discord_role_access(
                actor_provider=actor_provider,
                actor_user_id=actor_user_id,
                role_name=role_key,
                action="grant",
            )
            if not guard_result["ok"]:
                RoleManagementService.record_role_change_audit(
                    action="role_grant_denied",
                    role_name=role_key,
                    source=source,
                    actor_provider=actor_provider,
                    actor_user_id=actor_user_id,
                    actor_account_id=actor_account_id,
                    target_provider=target_provider,
                    target_user_id=target_user_id,
                    target_account_id=account_key,
                    before={"assigned": False},
                    after={"assigned": False},
                    status="denied",
                    error_code=str(guard_result.get("reason") or "denied"),
                    error_message=str(guard_result.get("message") or "role grant denied"),
                )
                return guard_result
            metadata = {"category": RoleManagementService._normalized_category(category)} if category else {}
            db.supabase.table("account_role_assignments").upsert(
                {
                    "account_id": account_key,
                    "role_name": role_key,
                    "source": "custom",
                    "metadata": metadata,
                    "origin_label": "admin role manager",
                },
                on_conflict="account_id,role_name,source",
            ).execute()
            RoleManagementService.record_role_change_audit(
                action="role_grant",
                role_name=role_key,
                source=source,
                actor_provider=actor_provider,
                actor_user_id=actor_user_id,
                actor_account_id=actor_account_id,
                target_provider=target_provider,
                target_user_id=target_user_id,
                target_account_id=account_key,
                before={"assigned": False, "category": metadata.get("category")},
                after={"assigned": True, "category": metadata.get("category")},
            )
            return RoleManagementService._role_action_result(True, role_name=role_key)
        except Exception:
            logger.exception(
                "assign_user_role_by_account failed account_id=%s target_provider=%s target_user_id=%s role=%s source=%s",
                account_key,
                target_provider,
                target_user_id,
                role_key,
                source,
            )
            RoleManagementService.record_role_change_audit(
                action="role_grant_failed",
                role_name=role_key,
                source=source,
                actor_provider=actor_provider,
                actor_user_id=actor_user_id,
                actor_account_id=actor_account_id,
                target_provider=target_provider,
                target_user_id=target_user_id,
                target_account_id=account_key,
                before={"assigned": False},
                after={"assigned": False},
                status="error",
                error_code="db_write_failed",
                error_message="assign_user_role_by_account failed",
            )
            return RoleManagementService._role_action_result(False, role_name=role_key)

    @staticmethod
    def revoke_user_role(provider: str, provider_user_id: str, role_name: str) -> bool:
        try:
            account_id = AccountsService.resolve_account_id(provider, str(provider_user_id))
            if not account_id:
                return False
            result = RoleManagementService.revoke_user_role_by_account(
                str(account_id),
                role_name,
                actor_provider=provider,
                actor_user_id=str(provider_user_id),
            )
            return bool(result.get("ok"))
        except Exception:
            logger.exception(
                "revoke_user_role failed provider=%s user_id=%s role=%s",
                provider,
                provider_user_id,
                role_name,
            )
            return False

    @staticmethod
    def revoke_user_role_by_account(
        account_id: str,
        role_name: str,
        *,
        actor_account_id: str | None = None,
        actor_provider: str | None = None,
        actor_user_id: str | None = None,
        target_provider: str | None = None,
        target_user_id: str | None = None,
        source: str = "unknown",
    ) -> dict[str, Any]:
        if not db.supabase:
            return RoleManagementService._role_action_result(False, role_name=role_name)
        account_key = str(account_id or "").strip()
        role_key = str(role_name or "").strip()
        if not account_key or not role_key:
            return RoleManagementService._role_action_result(False, role_name=role_key or role_name)

        try:
            if is_protected_profile_title(role_key):
                logger.warning(
                    "revoke_user_role_by_account denied protected profile title account_id=%s role_name=%s actor_provider=%s actor_user_id=%s target_provider=%s target_user_id=%s source=%s",
                    account_key,
                    role_key,
                    actor_provider,
                    actor_user_id,
                    target_provider,
                    target_user_id,
                    source,
                )
                RoleManagementService.record_role_change_audit(
                    action="role_revoke_denied",
                    role_name=role_key,
                    source=source,
                    actor_provider=actor_provider,
                    actor_user_id=actor_user_id,
                    actor_account_id=actor_account_id,
                    target_provider=target_provider,
                    target_user_id=target_user_id,
                    target_account_id=account_key,
                    before={"assigned": True},
                    after={"assigned": True},
                    status="denied",
                    error_code="protected_profile_title",
                    error_message=PROTECTED_PROFILE_TITLE_ROLE_MESSAGE,
                )
                return RoleManagementService._role_action_result(
                    False,
                    reason="protected_profile_title",
                    message=PROTECTED_PROFILE_TITLE_ROLE_MESSAGE,
                    role_name=role_key,
                )
            guard_result = RoleManagementService._check_privileged_discord_role_access(
                actor_provider=actor_provider,
                actor_user_id=actor_user_id,
                role_name=role_key,
                action="revoke",
            )
            if not guard_result["ok"]:
                RoleManagementService.record_role_change_audit(
                    action="role_revoke_denied",
                    role_name=role_key,
                    source=source,
                    actor_provider=actor_provider,
                    actor_user_id=actor_user_id,
                    actor_account_id=actor_account_id,
                    target_provider=target_provider,
                    target_user_id=target_user_id,
                    target_account_id=account_key,
                    before={"assigned": True},
                    after={"assigned": True},
                    status="denied",
                    error_code=str(guard_result.get("reason") or "denied"),
                    error_message=str(guard_result.get("message") or "role revoke denied"),
                )
                return guard_result
            db.supabase.table("account_role_assignments").delete().eq("account_id", account_key).eq("role_name", role_key).execute()
            RoleManagementService.record_role_change_audit(
                action="role_revoke",
                role_name=role_key,
                source=source,
                actor_provider=actor_provider,
                actor_user_id=actor_user_id,
                actor_account_id=actor_account_id,
                target_provider=target_provider,
                target_user_id=target_user_id,
                target_account_id=account_key,
                before={"assigned": True},
                after={"assigned": False},
            )
            return RoleManagementService._role_action_result(True, role_name=role_key)
        except Exception:
            logger.exception(
                "revoke_user_role_by_account failed account_id=%s target_provider=%s target_user_id=%s role=%s source=%s",
                account_key,
                target_provider,
                target_user_id,
                role_key,
                source,
            )
            RoleManagementService.record_role_change_audit(
                action="role_revoke_failed",
                role_name=role_key,
                source=source,
                actor_provider=actor_provider,
                actor_user_id=actor_user_id,
                actor_account_id=actor_account_id,
                target_provider=target_provider,
                target_user_id=target_user_id,
                target_account_id=account_key,
                before={"assigned": True},
                after={"assigned": True},
                status="error",
                error_code="db_write_failed",
                error_message="revoke_user_role_by_account failed",
            )
            return RoleManagementService._role_action_result(False, role_name=role_key)

    @staticmethod
    def get_role(role_name: str) -> dict[str, Any] | None:
        if not db.supabase:
            return None
        role_key = str(role_name or "").strip()
        if not role_key:
            return None

        select_variants = (
            "name,category_name,description,acquire_hint,is_discord_managed,discord_role_id,discord_role_name,is_privileged_discord_role",
            "name,category_name,acquire_hint,is_discord_managed,discord_role_id,discord_role_name,is_privileged_discord_role",
            "name,category_name,is_discord_managed,discord_role_id,discord_role_name,is_privileged_discord_role",
            "name,category_name,description,acquire_hint,is_discord_managed,discord_role_id,discord_role_name",
            "name,category_name,acquire_hint,is_discord_managed,discord_role_id,discord_role_name",
            "name,category_name,is_discord_managed,discord_role_id,discord_role_name",
        )
        for select_clause in select_variants:
            try:
                resp = (
                    db.supabase.table("roles")
                    .select(select_clause)
                    .eq("name", role_key)
                    .limit(1)
                    .execute()
                )
                if resp.data:
                    row = resp.data[0]
                    row["description"] = RoleManagementService._description_text(row.get("description"))
                    row["acquire_hint"] = RoleManagementService._acquire_hint_text(row.get("acquire_hint"))
                    return row
            except Exception:
                logger.exception("get_role failed role_name=%s select=%s", role_key, select_clause)
        return None

    @staticmethod
    def update_role_description(
        role_name: str,
        description: str | None,
        *,
        actor_id: str | None = None,
        actor_provider: str | None = None,
        actor_user_id: str | None = None,
        actor_account_id: str | None = None,
        operation: str = "role_edit_description",
        source: str = "unknown",
    ) -> bool:
        if not db.supabase:
            return False
        role_key = str(role_name or "").strip()
        if not role_key:
            return False

        normalized_description = RoleManagementService._normalized_description(description)
        before_role = RoleManagementService.get_role(role_key) or {}
        try:
            response = (
                db.supabase.table("roles")
                .update({"description": normalized_description})
                .eq("name", role_key)
                .execute()
            )
            if response is not None and hasattr(response, "data") and response.data == []:
                logger.warning(
                    "update_role_description skipped role_name=%s category=%s description_length=%s actor_id=%s actor_provider=%s actor_user_id=%s actor_account_id=%s operation=%s source=%s reason=%s",
                    role_key,
                    None,
                    len(normalized_description or ""),
                    actor_id,
                    actor_provider,
                    actor_user_id,
                    actor_account_id,
                    operation,
                    source,
                    "not_found",
                )
                RoleManagementService.record_role_change_audit(
                    action="role_edit_description_denied",
                    role_name=role_key,
                    source=source,
                    actor_provider=actor_provider,
                    actor_user_id=actor_user_id or actor_id,
                    actor_account_id=actor_account_id,
                    before=before_role or {"exists": False},
                    after={"description": normalized_description},
                    status="denied",
                    error_code="not_found",
                    error_message="role not found for description update",
                )
                return False
            role = RoleManagementService.get_role(role_key) or {}
            logger.info(
                "update_role_description completed role_name=%s category=%s description_length=%s actor_id=%s actor_provider=%s actor_user_id=%s actor_account_id=%s operation=%s source=%s before=%s after=%s",
                role_key,
                role.get("category_name"),
                len(normalized_description or ""),
                actor_id,
                actor_provider,
                actor_user_id,
                actor_account_id,
                operation,
                source,
                RoleManagementService._jsonable(before_role),
                RoleManagementService._jsonable(role),
            )
            RoleManagementService.record_role_change_audit(
                action="role_edit_description",
                role_name=role_key,
                source=source,
                actor_provider=actor_provider,
                actor_user_id=actor_user_id or actor_id,
                actor_account_id=actor_account_id,
                before=before_role,
                after=role,
            )
            return True
        except Exception:
            logger.exception(
                "update_role_description failed role_name=%s category=%s description_length=%s actor_id=%s actor_provider=%s actor_user_id=%s actor_account_id=%s operation=%s source=%s",
                role_key,
                None,
                len(normalized_description or ""),
                actor_id,
                actor_provider,
                actor_user_id,
                actor_account_id,
                operation,
                source,
            )
            RoleManagementService.record_role_change_audit(
                action="role_edit_description_failed",
                role_name=role_key,
                source=source,
                actor_provider=actor_provider,
                actor_user_id=actor_user_id or actor_id,
                actor_account_id=actor_account_id,
                before=before_role,
                after={"description": normalized_description},
                status="error",
                error_code="db_write_failed",
                error_message="update_role_description failed",
            )
            return False

    @staticmethod
    def update_role_acquire_hint(
        role_name: str,
        acquire_hint: str | None,
        *,
        actor_id: str | None = None,
        actor_provider: str | None = None,
        actor_user_id: str | None = None,
        actor_account_id: str | None = None,
        operation: str = "role_edit_acquire_hint",
        source: str = "unknown",
    ) -> bool:
        if not db.supabase:
            return False
        role_key = str(role_name or "").strip()
        if not role_key:
            return False

        normalized_acquire_hint = RoleManagementService._normalized_acquire_hint(acquire_hint)
        before_role = RoleManagementService.get_role(role_key) or {}
        try:
            response = (
                db.supabase.table("roles")
                .update({"acquire_hint": normalized_acquire_hint})
                .eq("name", role_key)
                .execute()
            )
            if response is not None and hasattr(response, "data") and response.data == []:
                logger.warning(
                    "update_role_metadata skipped role_name=%s actor_id=%s actor_provider=%s actor_user_id=%s actor_account_id=%s operation=%s source=%s field=%s value_length=%s reason=%s",
                    role_key,
                    actor_id,
                    actor_provider,
                    actor_user_id,
                    actor_account_id,
                    operation,
                    source,
                    "acquire_hint",
                    len(normalized_acquire_hint or ""),
                    "not_found",
                )
                RoleManagementService.record_role_change_audit(
                    action="role_edit_acquire_hint_denied",
                    role_name=role_key,
                    source=source,
                    actor_provider=actor_provider,
                    actor_user_id=actor_user_id or actor_id,
                    actor_account_id=actor_account_id,
                    before=before_role or {"exists": False},
                    after={"acquire_hint": normalized_acquire_hint},
                    status="denied",
                    error_code="not_found",
                    error_message="role not found for acquire_hint update",
                )
                return False
            role = RoleManagementService.get_role(role_key) or {}
            logger.info(
                "update_role_metadata completed actor_id=%s actor_provider=%s actor_user_id=%s actor_account_id=%s role_name=%s field=%s value_length=%s operation=%s source=%s before=%s after=%s",
                actor_id,
                actor_provider,
                actor_user_id,
                actor_account_id,
                role_key,
                "acquire_hint",
                len(normalized_acquire_hint or ""),
                operation,
                source,
                RoleManagementService._jsonable(before_role),
                RoleManagementService._jsonable(role),
            )
            RoleManagementService.record_role_change_audit(
                action="role_edit_acquire_hint",
                role_name=role_key,
                source=source,
                actor_provider=actor_provider,
                actor_user_id=actor_user_id or actor_id,
                actor_account_id=actor_account_id,
                before=before_role,
                after=role,
            )
            return True
        except Exception:
            logger.exception(
                "update_role_metadata failed actor_id=%s actor_provider=%s actor_user_id=%s actor_account_id=%s role_name=%s field=%s value_length=%s operation=%s source=%s",
                actor_id,
                actor_provider,
                actor_user_id,
                actor_account_id,
                role_key,
                "acquire_hint",
                len(normalized_acquire_hint or ""),
                operation,
                source,
            )
            RoleManagementService.record_role_change_audit(
                action="role_edit_acquire_hint_failed",
                role_name=role_key,
                source=source,
                actor_provider=actor_provider,
                actor_user_id=actor_user_id or actor_id,
                actor_account_id=actor_account_id,
                before=before_role,
                after={"acquire_hint": normalized_acquire_hint},
                status="error",
                error_code="db_write_failed",
                error_message="update_role_acquire_hint failed",
            )
            return False

    @staticmethod
    def sync_discord_guild_roles(guild_roles: list[dict[str, Any]]) -> dict[str, int]:
        if not db.supabase:
            return {"upserted": 0, "removed": 0}

        upserted = 0
        removed = 0
        active_ids: set[str] = set()
        try:
            db.supabase.table("role_categories").upsert({"name": _AUTO_DISCORD_CATEGORY, "position": 9999}).execute()
            existing_managed_resp = (
                db.supabase.table("roles")
                .select("name,discord_role_id,category_name,position")
                .eq("is_discord_managed", True)
                .execute()
            )
            existing_by_role_id: dict[str, dict[str, Any]] = {}
            for row in existing_managed_resp.data or []:
                existing_role_id = str(row.get("discord_role_id") or "").strip()
                if existing_role_id and existing_role_id not in existing_by_role_id:
                    existing_by_role_id[existing_role_id] = row

            for role in guild_roles:
                role_id = str(role.get("id") or "").strip()
                role_name = str(role.get("name") or "").strip()
                if not role_id or not role_name:
                    continue
                active_ids.add(role_id)

                existing = existing_by_role_id.get(role_id)
                if RoleManagementService._upsert_discord_catalog_role(
                    role_id=role_id,
                    role_name=role_name,
                    existing=existing,
                    default_category=_AUTO_DISCORD_CATEGORY,
                    default_position=int(role.get("position") or 0),
                    source="guild_roles",
                    guild_id=str(role.get("guild_id") or "").strip() or None,
                ):
                    upserted += 1
                    existing_by_role_id[role_id] = {
                        "name": role_name,
                        "discord_role_id": role_id,
                        "category_name": (existing or {}).get("category_name") or _AUTO_DISCORD_CATEGORY,
                        "position": int((existing or {}).get("position") or int(role.get("position") or 0)),
                    }

            external_upserted, external_active_ids = RoleManagementService._sync_discord_roles_from_external_bindings(existing_by_role_id)
            upserted += external_upserted
            active_ids.update(external_active_ids)

            existing_resp = (
                db.supabase.table("roles")
                .select("name,discord_role_id,category_name")
                .eq("is_discord_managed", True)
                .execute()
            )
            for row in existing_resp.data or []:
                role_id = str(row.get("discord_role_id") or "").strip()
                role_name = str(row.get("name") or "").strip()
                if role_id and role_id not in active_ids and role_name:
                    RoleManagementService._delete_role_dependencies(role_name, log_context="sync_discord_guild_roles")
                    db.supabase.table("roles").delete().eq("name", role_name).execute()
                    removed += 1

            logger.info(
                "sync_discord_guild_roles completed upserted=%s removed=%s active_ids=%s",
                upserted,
                removed,
                len(active_ids),
            )
            return {"upserted": upserted, "removed": removed}
        except Exception:
            logger.exception("sync_discord_guild_roles failed")
            return {"upserted": upserted, "removed": removed}
