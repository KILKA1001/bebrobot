import logging
from typing import Any

from bot.data import db
from bot.services.accounts_service import AccountsService
from bot.services.auth import RoleResolver

logger = logging.getLogger(__name__)


class RoleManagementService:
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
    def list_roles_grouped() -> list[dict[str, Any]]:
        if not db.supabase:
            logger.warning("list_roles_grouped skipped: supabase is not configured")
            return []

        try:
            categories_resp = db.supabase.table("role_categories").select("name,position").execute()
            roles_rows = RoleManagementService._load_roles_rows()
        except Exception:
            logger.exception("list_roles_grouped failed")
            return []

        category_positions: dict[str, int] = {}
        for row in categories_resp.data or []:
            name = RoleManagementService._normalized_category(row.get("name"))
            category_positions[name] = int(row.get("position") or 0)

        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in roles_rows:
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
    def delete_role(name: str) -> bool:
        role_name = str(name or "").strip()
        if not role_name or not db.supabase:
            return False
        try:
            db.supabase.table("roles").delete().eq("name", role_name).execute()
            db.supabase.table("account_role_assignments").delete().eq("role_name", role_name).execute()
            return True
        except Exception:
            logger.exception("delete_role failed role_name=%s", role_name)
            return False

    @staticmethod
    def move_role(role_name: str, category: str, position: int = 0) -> bool:
        name = str(role_name or "").strip()
        normalized_category = RoleManagementService._normalized_category(category)
        if not name or not db.supabase:
            return False
        try:
            db.supabase.table("role_categories").upsert({"name": normalized_category, "position": 0}).execute()
            db.supabase.table("roles").update({"category_name": normalized_category, "position": int(position)}).eq("name", name).execute()
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

        auto_category = "Discord сервер (auto)"
        upserted = 0
        removed = 0
        active_ids: set[str] = set()
        try:
            db.supabase.table("role_categories").upsert({"name": auto_category, "position": 9999}).execute()
            for role in guild_roles:
                role_id = str(role.get("id") or "").strip()
                role_name = str(role.get("name") or "").strip()
                if not role_id or not role_name:
                    continue
                active_ids.add(role_id)
                payload = {
                    "name": role_name,
                    "category_name": auto_category,
                    "position": int(role.get("position") or 0),
                    "is_discord_managed": True,
                    "discord_role_id": role_id,
                    "discord_role_name": role_name,
                }
                db.supabase.table("roles").upsert(payload, on_conflict="name").execute()
                upserted += 1

            existing_resp = (
                db.supabase.table("roles")
                .select("name,discord_role_id,category_name")
                .eq("category_name", auto_category)
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

            logger.info("sync_discord_guild_roles completed upserted=%s removed=%s", upserted, removed)
            return {"upserted": upserted, "removed": removed}
        except Exception:
            logger.exception("sync_discord_guild_roles failed")
            return {"upserted": upserted, "removed": removed}
