from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from bot.data import db
from bot.domain.auth import AssignmentSource, Permission, Role, UserRoleAssignment

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ResolvedAccess:
    roles: list[dict[str, str | None]]
    permissions: dict[str, list[str]]


class RoleResolver:
    """Единая политика резолва ролей/прав для Discord и Telegram."""

    SOURCE_PRIORITY: dict[AssignmentSource, int] = {
        "custom": 0,
        "system": 1,
        "discord": 2,
        "telegram": 3,
    }

    @staticmethod
    def resolve_for_account(account_id: str) -> ResolvedAccess:
        if not account_id:
            logger.warning("resolve_for_account called with empty account_id")
            return ResolvedAccess(roles=[], permissions={"allow": [], "deny": []})

        assignments = RoleResolver._collect_assignments(account_id)
        roles = RoleResolver._load_roles(assignments)
        permissions = RoleResolver._resolve_permissions(assignments, roles)

        role_payload = [
            {
                "name": assignment.role_name,
                "source": assignment.source,
                "origin_label": assignment.origin_label,
                "synced_at": assignment.synced_at.isoformat() if assignment.synced_at else None,
            }
            for assignment in assignments
        ]

        return ResolvedAccess(roles=role_payload, permissions=permissions)

    @staticmethod
    def _collect_assignments(account_id: str) -> list[UserRoleAssignment]:
        assignments: list[UserRoleAssignment] = []
        assignments.extend(RoleResolver._load_db_assignments(account_id))
        assignments.extend(RoleResolver._load_title_assignments(account_id))

        now = datetime.now(timezone.utc)
        active = [
            item
            for item in assignments
            if not item.expires_at or item.expires_at.astimezone(timezone.utc) > now
        ]
        active.sort(key=lambda item: (RoleResolver.SOURCE_PRIORITY.get(item.source, 99), item.role_name.lower()))

        deduplicated: dict[str, UserRoleAssignment] = {}
        for item in active:
            key = item.role_name.lower()
            current = deduplicated.get(key)
            if current is None or RoleResolver.SOURCE_PRIORITY.get(item.source, 99) < RoleResolver.SOURCE_PRIORITY.get(
                current.source,
                99,
            ):
                deduplicated[key] = item
        return list(deduplicated.values())

    @staticmethod
    def _load_db_assignments(account_id: str) -> list[UserRoleAssignment]:
        if not db.supabase:
            return []
        try:
            response = (
                db.supabase.table("account_role_assignments")
                .select("role_name,source,external_id,expires_at,metadata,origin_label,synced_at")
                .eq("account_id", str(account_id))
                .execute()
            )
        except Exception as error:
            logger.warning(
                "role resolver: account_role_assignments read failed account_id=%s error=%s",
                account_id,
                error,
            )
            return []

        result: list[UserRoleAssignment] = []
        for row in response.data or []:
            source_raw = str(row.get("source") or "custom").lower()
            source: AssignmentSource = source_raw if source_raw in {"custom", "discord", "telegram", "system"} else "custom"
            result.append(
                UserRoleAssignment(
                    role_name=str(row.get("role_name") or "").strip(),
                    source=source,
                    external_id=str(row.get("external_id") or "").strip() or None,
                    expires_at=RoleResolver._parse_datetime(row.get("expires_at")),
                    metadata=row.get("metadata") if isinstance(row.get("metadata"), dict) else {},
                    origin_label=str(row.get("origin_label") or "").strip() or None,
                    synced_at=RoleResolver._parse_datetime(row.get("synced_at")),
                )
            )
        return [item for item in result if item.role_name]

    @staticmethod
    def _load_title_assignments(account_id: str) -> list[UserRoleAssignment]:
        if not db.supabase:
            return []
        try:
            response = (
                db.supabase.table("accounts")
                .select("titles,titles_source,titles_updated_at")
                .eq("id", str(account_id))
                .limit(1)
                .execute()
            )
        except Exception as error:
            logger.warning("role resolver: account titles read failed account_id=%s error=%s", account_id, error)
            return []

        rows = response.data or []
        if not rows:
            return []

        row = rows[0]
        titles_raw = row.get("titles")
        if isinstance(titles_raw, list):
            titles = [str(value).strip() for value in titles_raw if str(value).strip()]
        elif isinstance(titles_raw, str):
            titles = [value.strip() for value in titles_raw.split(",") if value.strip()]
        else:
            titles = []

        source_raw = str(row.get("titles_source") or "discord").lower()
        source: AssignmentSource = source_raw if source_raw in {"custom", "discord", "telegram", "system"} else "discord"
        synced_at = RoleResolver._parse_datetime(row.get("titles_updated_at"))
        return [
            UserRoleAssignment(
                role_name=title,
                source=source,
                external_id=None,
                metadata={"source": "accounts.titles"},
                origin_label="Звание из профиля",
                synced_at=synced_at,
            )
            for title in titles
        ]

    @staticmethod
    def _load_roles(assignments: list[UserRoleAssignment]) -> dict[str, Role]:
        if not db.supabase or not assignments:
            return {}
        role_names = [assignment.role_name for assignment in assignments]

        try:
            roles_resp = db.supabase.table("roles").select("name").execute()
            role_permission_resp = db.supabase.table("role_permissions").select("role_name,permission_name,effect").execute()
        except Exception as error:
            logger.warning("role resolver: roles read failed error=%s", error)
            return {}

        existing = {str(row.get("name")).strip().lower() for row in (roles_resp.data or []) if row.get("name")}
        wanted = {name.strip().lower() for name in role_names}

        role_map: dict[str, Role] = {
            key: Role(name=key, permissions=[])
            for key in wanted
            if not existing or key in existing
        }

        for row in role_permission_resp.data or []:
            role_name = str(row.get("role_name") or "").strip().lower()
            permission_name = str(row.get("permission_name") or "").strip()
            effect_raw = str(row.get("effect") or "allow").strip().lower()
            effect = "deny" if effect_raw == "deny" else "allow"
            if role_name and permission_name and role_name in role_map:
                role_map[role_name].permissions.append(Permission(name=permission_name, effect=effect))
        return role_map

    @staticmethod
    def _resolve_permissions(assignments: list[UserRoleAssignment], roles: dict[str, Role]) -> dict[str, list[str]]:
        # Политика конфликтов зафиксирована здесь:
        # 1) deny всегда важнее allow;
        # 2) custom/system роли имеют приоритет над внешними (discord/telegram).
        state: dict[str, tuple[str, int]] = {}

        for assignment in assignments:
            role_key = assignment.role_name.lower()
            role = roles.get(role_key)
            if role is None:
                continue
            source_priority = RoleResolver.SOURCE_PRIORITY.get(assignment.source, 99)
            for permission in role.permissions:
                current = state.get(permission.name)
                if current is None:
                    state[permission.name] = (permission.effect, source_priority)
                    continue

                current_effect, current_priority = current
                if current_effect == "deny" and permission.effect == "allow":
                    continue
                if permission.effect == "deny":
                    if source_priority <= current_priority or current_effect != "deny":
                        state[permission.name] = ("deny", source_priority)
                    continue
                if source_priority < current_priority:
                    state[permission.name] = (permission.effect, source_priority)

        allow = sorted([name for name, (effect, _) in state.items() if effect == "allow"])
        deny = sorted([name for name, (effect, _) in state.items() if effect == "deny"])
        return {"allow": allow, "deny": deny}

    @staticmethod
    def _parse_datetime(value: Any) -> datetime | None:
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value
        if isinstance(value, str) and value.strip():
            try:
                normalized = value.replace("Z", "+00:00")
                parsed = datetime.fromisoformat(normalized)
                if parsed.tzinfo is None:
                    return parsed.replace(tzinfo=timezone.utc)
                return parsed
            except ValueError:
                logger.warning("role resolver: invalid datetime value=%s", value)
        return None
