import logging
from typing import Any

from bot.data import db
from bot.services.accounts_service import AccountsService
from bot.services.auth import RoleResolver

_AUTO_DISCORD_CATEGORY = "Discord сервер (auto)"

logger = logging.getLogger(__name__)

DELETE_ROLE_REASON_DISCORD_MANAGED = "discord_managed"
DELETE_ROLE_REASON_NOT_FOUND = "not_found"
DELETE_ROLE_REASON_ERROR = "error"


class RoleManagementService:
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
    def _load_roles_rows() -> list[dict[str, Any]]:
        """Read role rows with backward-compatible column fallback."""
        if not db.supabase:
            return []

        try:
            response = (
                db.supabase.table("roles")
                .select("name,category_name,position,is_discord_managed,discord_role_id")
                .execute()
            )
            return response.data or []
        except Exception:
            logger.exception("roles query with discord columns failed, fallback to base columns")

        try:
            response = db.supabase.table("roles").select("name,category_name,position").execute()
            return response.data or []
        except Exception:
            logger.exception("roles query fallback failed")
            return []

    @staticmethod
    def _normalized_category(name: str | None) -> str:
        value = str(name or "").strip()
        return value or "Без категории"

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
                    "position": int(row.get("position") or 0),
                    "is_discord_managed": bool(row.get("is_discord_managed")),
                    "discord_role_id": str(row.get("discord_role_id") or "").strip() or None,
                }
            )

        categories = sorted(set(category_positions) | set(grouped), key=lambda item: (category_positions.get(item, 0), item.lower()))
        result: list[dict[str, Any]] = []
        for category in categories:
            roles = sorted(grouped.get(category, []), key=lambda item: (item["position"], item["name"].lower()))
            result.append({"category": category, "position": category_positions.get(category, 0), "roles": roles})
        return result

    @staticmethod
    def _load_external_discord_bindings() -> list[dict[str, Any]]:
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
            logger.exception("external_role_bindings discord query failed")
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
        }

        try:
            db.supabase.table("role_categories").upsert({"name": payload["category_name"], "position": 9999}).execute()
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
    ) -> tuple[int, set[str]]:
        if not db.supabase:
            return 0, set()

        upserted = 0
        synced_ids: set[str] = set()
        for row in RoleManagementService._load_external_discord_bindings():
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
    def ensure_external_discord_roles_in_catalog() -> dict[str, int]:
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

            upserted, _ = RoleManagementService._sync_discord_roles_from_external_bindings(existing_by_role_id)
            if upserted:
                logger.info("ensure_external_discord_roles_in_catalog completed upserted=%s", upserted)
            return {"upserted": upserted}
        except Exception:
            logger.exception("ensure_external_discord_roles_in_catalog failed")
            return {"upserted": 0}

    @staticmethod
    def list_roles_grouped() -> list[dict[str, Any]]:
        if not db.supabase:
            logger.warning("list_roles_grouped skipped: supabase is not configured")
            return []

        try:
            RoleManagementService.ensure_external_discord_roles_in_catalog()
            categories_resp = db.supabase.table("role_categories").select("name,position").execute()
            roles_rows = RoleManagementService._load_roles_rows()
        except Exception:
            logger.exception("list_roles_grouped failed")
            return []

        external_rows = RoleManagementService._load_external_discord_bindings()
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
            db.supabase.table("role_categories").upsert({"name": fallback, "position": 999}).execute()
            db.supabase.table("roles").update({"category_name": fallback}).eq("category_name", category).execute()
            db.supabase.table("role_categories").delete().eq("name", category).execute()
            return True
        except Exception:
            logger.exception("delete_category failed category=%s", category)
            return False

    @staticmethod
    def create_role(
        name: str,
        category: str,
        position: int = 0,
        discord_role_id: str | None = None,
        discord_role_name: str | None = None,
    ) -> bool:
        role_name = str(name or "").strip()
        if not role_name:
            return False
        if not db.supabase:
            return False

        normalized_category = RoleManagementService._normalized_category(category)
        payload = {
            "name": role_name,
            "category_name": normalized_category,
            "position": int(position),
            "is_discord_managed": bool(discord_role_id),
            "discord_role_id": str(discord_role_id).strip() if discord_role_id else None,
            "discord_role_name": str(discord_role_name).strip() if discord_role_name else None,
        }

        try:
            db.supabase.table("role_categories").upsert({"name": normalized_category, "position": 0}).execute()
            db.supabase.table("roles").upsert(payload, on_conflict="name").execute()
            return True
        except Exception:
            logger.exception("create_role failed role_name=%s category=%s", role_name, normalized_category)
            return False

    @staticmethod
    def delete_role(
        name: str,
        *,
        actor_id: str | None = None,
        guild_id: str | None = None,
        telegram_user_id: str | None = None,
    ) -> dict[str, Any]:
        role_name = str(name or "").strip()
        if not role_name or not db.supabase:
            return RoleManagementService._delete_role_result(False, reason=DELETE_ROLE_REASON_ERROR, role_name=role_name or None)
        try:
            role_row = (
                db.supabase.table("roles")
                .select("name,is_discord_managed,discord_role_id")
                .eq("name", role_name)
                .limit(1)
                .execute()
            )
            role = (role_row.data or [None])[0]
            if not role:
                logger.warning(
                    "delete_role skipped role missing role_name=%s actor_id=%s guild_id=%s telegram_user_id=%s",
                    role_name,
                    actor_id,
                    guild_id,
                    telegram_user_id,
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
                    "delete_role denied discord-managed role_name=%s discord_role_id=%s actor_id=%s guild_id=%s telegram_user_id=%s",
                    role_name,
                    discord_role_id,
                    actor_id,
                    guild_id,
                    telegram_user_id,
                )
                return RoleManagementService._delete_role_result(
                    False,
                    reason=DELETE_ROLE_REASON_DISCORD_MANAGED,
                    role_name=role_name,
                    discord_role_id=discord_role_id,
                    is_discord_managed=True,
                )

            db.supabase.table("roles").delete().eq("name", role_name).execute()
            db.supabase.table("account_role_assignments").delete().eq("role_name", role_name).execute()
            return RoleManagementService._delete_role_result(
                True,
                role_name=role_name,
                discord_role_id=discord_role_id,
                is_discord_managed=is_discord_managed,
            )
        except Exception:
            logger.exception(
                "delete_role failed role_name=%s actor_id=%s guild_id=%s telegram_user_id=%s",
                role_name,
                actor_id,
                guild_id,
                telegram_user_id,
            )
            return RoleManagementService._delete_role_result(False, reason=DELETE_ROLE_REASON_ERROR, role_name=role_name)

    @staticmethod
    def move_role(role_name: str, category: str, position: int = 0) -> bool:
        name = str(role_name or "").strip()
        normalized_category = RoleManagementService._normalized_category(category)
        if not name or not db.supabase:
            return False
        try:
            existing_role = RoleManagementService.get_role(name)
            if not existing_role:
                logger.warning(
                    "move_role denied role missing from canonical catalog role_name=%s category=%s position=%s",
                    name,
                    normalized_category,
                    position,
                )
                return False
            db.supabase.table("role_categories").upsert({"name": normalized_category, "position": 0}).execute()
            response = (
                db.supabase.table("roles")
                .update({"category_name": normalized_category, "position": int(position)})
                .eq("name", name)
                .execute()
            )
            if existing_role and response is not None and hasattr(response, "data") and response.data == []:
                logger.warning(
                    "move_role update returned no rows role_name=%s category=%s position=%s",
                    name,
                    normalized_category,
                    position,
                )
                return False
            return True
        except Exception:
            logger.exception("move_role failed role_name=%s category=%s position=%s", name, normalized_category, position)
            return False

    @staticmethod
    def get_user_roles(provider: str, provider_user_id: str) -> list[dict[str, str | None]]:
        try:
            account_id = AccountsService.resolve_account_id(provider, str(provider_user_id))
            if not account_id:
                return []
            resolved = RoleResolver.resolve_for_account(account_id)
            return resolved.roles
        except Exception:
            logger.exception("get_user_roles failed provider=%s user_id=%s", provider, provider_user_id)
            return []

    @staticmethod
    def assign_user_role(provider: str, provider_user_id: str, role_name: str, category: str | None = None) -> bool:
        if not db.supabase:
            return False
        role_key = str(role_name or "").strip()
        if not role_key:
            return False

        try:
            account_id = AccountsService.resolve_account_id(provider, str(provider_user_id))
            if not account_id:
                logger.warning("assign_user_role skipped: account not found provider=%s user_id=%s", provider, provider_user_id)
                return False
            metadata = {"category": RoleManagementService._normalized_category(category)} if category else {}
            db.supabase.table("account_role_assignments").upsert(
                {
                    "account_id": str(account_id),
                    "role_name": role_key,
                    "source": "custom",
                    "metadata": metadata,
                    "origin_label": "admin role manager",
                },
                on_conflict="account_id,role_name,source",
            ).execute()
            return True
        except Exception:
            logger.exception(
                "assign_user_role failed provider=%s user_id=%s role=%s",
                provider,
                provider_user_id,
                role_key,
            )
            return False

    @staticmethod
    def revoke_user_role(provider: str, provider_user_id: str, role_name: str) -> bool:
        if not db.supabase:
            return False
        role_key = str(role_name or "").strip()
        if not role_key:
            return False

        try:
            account_id = AccountsService.resolve_account_id(provider, str(provider_user_id))
            if not account_id:
                return False
            db.supabase.table("account_role_assignments").delete().eq("account_id", str(account_id)).eq("role_name", role_key).execute()
            return True
        except Exception:
            logger.exception(
                "revoke_user_role failed provider=%s user_id=%s role=%s",
                provider,
                provider_user_id,
                role_key,
            )
            return False

    @staticmethod
    def get_role(role_name: str) -> dict[str, Any] | None:
        if not db.supabase:
            return None
        role_key = str(role_name or "").strip()
        if not role_key:
            return None
        try:
            resp = (
                db.supabase.table("roles")
                .select("name,category_name,is_discord_managed,discord_role_id,discord_role_name")
                .eq("name", role_key)
                .limit(1)
                .execute()
            )
            if resp.data:
                return resp.data[0]
        except Exception:
            logger.exception("get_role failed role_name=%s", role_key)
        return None

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
                    db.supabase.table("roles").delete().eq("name", role_name).execute()
                    db.supabase.table("account_role_assignments").delete().eq("role_name", role_name).execute()
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
