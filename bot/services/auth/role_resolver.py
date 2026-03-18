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

    EXTERNAL_CATEGORY_FALLBACK = "Внешние роли"
    DEFAULT_CATEGORY = "Без категории"

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
                "category": RoleResolver.normalize_category_value(assignment.metadata.get("category")),
            }
            for assignment in assignments
        ]

        return ResolvedAccess(roles=role_payload, permissions=permissions)

    @staticmethod
    def _collect_assignments(account_id: str) -> list[UserRoleAssignment]:
        assignments: list[UserRoleAssignment] = []
        db_assignments = RoleResolver._load_db_assignments(account_id)
        assignments.extend(db_assignments)
        if not db_assignments:
            fallback_assignments = RoleResolver._load_external_bindings_assignments(account_id)
            assignments.extend(fallback_assignments)
            if fallback_assignments:
                logger.info(
                    "role resolver: fallback to external_role_bindings account_id=%s assignments=%s",
                    account_id,
                    len(fallback_assignments),
                )
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
    def _load_external_bindings_assignments(account_id: str) -> list[UserRoleAssignment]:
        if not db.supabase:
            return []

        catalog_roles = RoleResolver._load_catalog_roles_for_external_bindings()
        try:
            response = (
                db.supabase.table("external_role_bindings")
                .select("source,external_role_id,external_role_name,last_synced_at")
                .eq("account_id", str(account_id))
                .is_("deleted_at", "null")
                .execute()
            )
        except Exception as error:
            logger.warning(
                "role resolver: external_role_bindings fallback read failed account_id=%s error=%s",
                account_id,
                error,
            )
            return []

        result: list[UserRoleAssignment] = []
        for row in response.data or []:
            source_raw = str(row.get("source") or "discord").lower()
            source: AssignmentSource = source_raw if source_raw in {"custom", "discord", "telegram", "system"} else "discord"
            role_name = str(row.get("external_role_name") or "").strip()
            external_id = str(row.get("external_role_id") or "").strip() or None
            if not role_name:
                continue
            catalog_match = RoleResolver._match_catalog_role_for_external_binding(
                account_id=account_id,
                source=source,
                external_id=external_id,
                external_role_name=role_name,
                catalog_roles=catalog_roles,
            )
            result.append(
                UserRoleAssignment(
                    role_name=str((catalog_match or {}).get("name") or role_name).strip() or role_name,
                    source=source,
                    external_id=external_id,
                    expires_at=None,
                    metadata={
                        "source": "external_role_bindings",
                        "category": RoleResolver.normalize_category_value(
                            (catalog_match or {}).get("category_name"),
                            fallback=RoleResolver.EXTERNAL_CATEGORY_FALLBACK,
                        ),
                    },
                    origin_label="legacy external_role_bindings",
                    synced_at=RoleResolver._parse_datetime(row.get("last_synced_at")),
                )
            )
        return result

    @staticmethod
    def normalize_category_value(value: object, fallback: str | None = None) -> str:
        normalized = str(value or "").strip()
        if normalized:
            return normalized
        return str(fallback or RoleResolver.DEFAULT_CATEGORY)

    @staticmethod
    def _load_catalog_roles_for_external_bindings() -> list[dict[str, Any]]:
        if not db.supabase:
            return []

        select_variants = (
            "name,category_name,discord_role_id,external_role_id",
            "name,category_name,discord_role_id",
            "name,category_name",
        )
        last_error: Exception | None = None
        for select_clause in select_variants:
            try:
                response = db.supabase.table("roles").select(select_clause).execute()
                rows = response.data or []
                logger.info(
                    "role resolver: loaded catalog roles for external bindings count=%s select=%s",
                    len(rows),
                    select_clause,
                )
                return rows
            except Exception as error:
                last_error = error
                logger.warning(
                    "role resolver: roles catalog select failed for external binding resolution select=%s error=%s",
                    select_clause,
                    error,
                )

        if last_error:
            logger.warning(
                "role resolver: unable to load roles catalog for external binding resolution error=%s",
                last_error,
            )
        return []

    @staticmethod
    def _match_catalog_role_for_external_binding(
        *,
        account_id: str,
        source: AssignmentSource,
        external_id: str | None,
        external_role_name: str,
        catalog_roles: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        role_name_key = external_role_name.strip().lower()
        external_id_key = str(external_id or "").strip()
        id_matches: list[dict[str, Any]] = []
        if external_id_key:
            for row in catalog_roles:
                discord_role_id = str(row.get("discord_role_id") or "").strip()
                catalog_external_role_id = str(row.get("external_role_id") or "").strip()
                if external_id_key and external_id_key in {discord_role_id, catalog_external_role_id}:
                    id_matches.append(row)

        if len(id_matches) > 1:
            logger.warning(
                "role resolver: multiple catalog matches by external id account_id=%s source=%s external_role_id=%s external_role_name=%s matches=%s",
                account_id,
                source,
                external_id_key,
                external_role_name,
                [
                    {
                        "name": str(match.get("name") or "").strip(),
                        "discord_role_id": str(match.get("discord_role_id") or "").strip() or None,
                        "external_role_id": str(match.get("external_role_id") or "").strip() or None,
                        "category_name": RoleResolver.normalize_category_value(match.get("category_name")),
                    }
                    for match in id_matches
                ],
            )
            return sorted(id_matches, key=lambda item: str(item.get("name") or "").lower())[0]

        if len(id_matches) == 1:
            id_match = id_matches[0]
            if role_name_key:
                catalog_name_key = str(id_match.get("name") or "").strip().lower()
                if catalog_name_key and catalog_name_key != role_name_key:
                    logger.info(
                        "role resolver: external binding matched by id with different name account_id=%s source=%s external_role_id=%s external_role_name=%s catalog_role_name=%s",
                        account_id,
                        source,
                        external_id_key,
                        external_role_name,
                        str(id_match.get("name") or "").strip(),
                    )
            return id_match

        name_matches = [
            row
            for row in catalog_roles
            if role_name_key and str(row.get("name") or "").strip().lower() == role_name_key
        ]
        if len(name_matches) > 1:
            logger.warning(
                "role resolver: multiple catalog matches by name account_id=%s source=%s external_role_id=%s external_role_name=%s matches=%s",
                account_id,
                source,
                external_id_key or None,
                external_role_name,
                [
                    {
                        "name": str(match.get("name") or "").strip(),
                        "discord_role_id": str(match.get("discord_role_id") or "").strip() or None,
                        "external_role_id": str(match.get("external_role_id") or "").strip() or None,
                        "category_name": RoleResolver.normalize_category_value(match.get("category_name")),
                    }
                    for match in name_matches
                ],
            )
            return sorted(name_matches, key=lambda item: str(item.get("discord_role_id") or "").lower())[0]

        if len(name_matches) == 1:
            name_match = name_matches[0]
            matched_discord_role_id = str(name_match.get("discord_role_id") or "").strip()
            matched_external_role_id = str(name_match.get("external_role_id") or "").strip()
            matched_catalog_id = matched_discord_role_id or matched_external_role_id
            if external_id_key and matched_catalog_id and matched_catalog_id != external_id_key:
                logger.warning(
                    "role resolver: catalog name matched but external id mismatched account_id=%s source=%s external_role_name=%s external_role_id=%s catalog_role_name=%s catalog_discord_role_id=%s catalog_external_role_id=%s",
                    account_id,
                    source,
                    external_role_name,
                    external_id_key,
                    str(name_match.get("name") or "").strip(),
                    matched_discord_role_id or None,
                    matched_external_role_id or None,
                )
            else:
                logger.info(
                    "role resolver: catalog role matched by name fallback account_id=%s source=%s external_role_name=%s external_role_id=%s catalog_role_name=%s",
                    account_id,
                    source,
                    external_role_name,
                    external_id_key or None,
                    str(name_match.get("name") or "").strip(),
                )
            return name_match

        logger.warning(
            "role resolver: external role binding not found in catalog account_id=%s source=%s external_role_id=%s external_role_name=%s",
            account_id,
            source,
            external_id_key or None,
            external_role_name,
        )
        return None

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
