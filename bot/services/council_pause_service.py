from __future__ import annotations

import logging
from datetime import datetime, timezone

from bot.data import db
from bot.services.role_management_service import RoleManagementService
from bot.utils.structured_logging import log_critical_event

logger = logging.getLogger(__name__)

_OPERATION_CODE = "council.lifecycle.pause_mode"
_ENTITY_TYPE = "council_pause"
_TERM_ROLE_CODE_TO_PROJECT_ROLE: dict[str, str] = {
    "vice_council": "Вице Советчанин",
    "vice_council_member": "Вице Советчанин",
    "council_member": "Советчанин",
    "observer": "Наблюдатель",
}


class CouncilPauseService:
    @staticmethod
    def _load_latest_term() -> dict[str, object] | None:
        if not db.supabase:
            return None
        try:
            response = (
                db.supabase.table("council_terms")
                .select("id,status,starts_at,ends_at")
                .order("ends_at", desc=True)
                .order("id", desc=True)
                .limit(1)
                .execute()
            )
            rows = response.data or []
            return rows[0] if rows else None
        except Exception:
            logger.exception("council pause failed to load latest term")
            return None

    @staticmethod
    def _load_pending_term_confirmation_count() -> int:
        if not db.supabase:
            return 0
        try:
            pending_response = (
                db.supabase.table("council_terms")
                .select("id,status")
                .eq("status", "pending_launch_confirmation")
                .order("id", desc=True)
                .limit(1)
                .execute()
            )
            pending_rows = pending_response.data or []
            if not pending_rows:
                return 0
            pending_id = pending_rows[0].get("id")
            if not isinstance(pending_id, int):
                return 0
            confirmations = (
                db.supabase.table("council_term_launch_confirmations")
                .select("id", count="exact")
                .eq("term_id", pending_id)
                .eq("status", "confirmed")
                .execute()
            )
            return int(getattr(confirmations, "count", 0) or 0)
        except Exception:
            logger.exception("council pause failed to load pending launch confirmations")
            return 0

    @staticmethod
    def _parse_dt(value: object) -> datetime | None:
        if not isinstance(value, str) or not value.strip():
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            logger.exception("council pause failed to parse datetime value=%s", value)
            return None

    @staticmethod
    def _is_pause_required(*, now: datetime | None = None) -> tuple[bool, str | None, int | None]:
        if not db.supabase:
            return False, None, None

        current = now or datetime.now(timezone.utc)
        term = CouncilPauseService._load_latest_term()
        if not term:
            return False, None, None

        term_id = term.get("id") if isinstance(term.get("id"), int) else None
        status = str(term.get("status") or "").strip().lower()
        ends_at = CouncilPauseService._parse_dt(term.get("ends_at"))

        if status == "active":
            if ends_at and ends_at <= current:
                confirmations_count = CouncilPauseService._load_pending_term_confirmation_count()
                if confirmations_count <= 0:
                    return True, "term_ended_without_launch_confirmation", term_id
            return False, None, term_id

        if ends_at and ends_at <= current:
            confirmations_count = CouncilPauseService._load_pending_term_confirmation_count()
            if confirmations_count <= 0:
                return True, "term_ended_without_launch_confirmation", term_id

        return False, None, term_id

    @staticmethod
    def _read_latest_state() -> dict[str, object]:
        if not db.supabase:
            return {"paused": False, "reason": None, "paused_at": None}
        try:
            response = (
                db.supabase.table("council_audit_log")
                .select("action,details,created_at")
                .eq("entity_type", _ENTITY_TYPE)
                .order("created_at", desc=True)
                .limit(1)
                .execute()
            )
            rows = response.data or []
            if not rows:
                return {"paused": False, "reason": None, "paused_at": None}
            row = rows[0]
            action = str(row.get("action") or "").strip().lower()
            details = row.get("details") if isinstance(row.get("details"), dict) else {}
            return {
                "paused": action == "pause_enabled",
                "reason": details.get("reason") if isinstance(details, dict) else None,
                "paused_at": str(row.get("created_at") or "") or None,
            }
        except Exception:
            logger.exception("council pause failed to read latest pause state")
            return {"paused": False, "reason": None, "paused_at": None}

    @staticmethod
    def _write_pause_event(
        *,
        paused: bool,
        reason: str,
        platform: str,
        user_id: str | None,
        entity_id: int | None,
        role_cleanup: dict[str, object] | None = None,
    ) -> None:
        action = "pause_enabled" if paused else "pause_disabled"
        correlation_id, request_id = log_critical_event(
            logger,
            level=logging.WARNING if paused else logging.INFO,
            operation_code=_OPERATION_CODE,
            reason=reason,
            platform=platform,
            user_id=user_id,
            entity_type=_ENTITY_TYPE,
            entity_id=entity_id,
            paused=paused,
        )
        if not db.supabase:
            return
        try:
            db.supabase.table("council_audit_log").insert(
                {
                    "term_id": entity_id,
                    "entity_type": _ENTITY_TYPE,
                    "entity_id": entity_id,
                    "action": action,
                    "status": "success",
                    "actor_profile_id": None,
                    "source_platform": platform if platform in {"telegram", "discord", "system"} else "unknown",
                    "details": {
                        "operation_code": _OPERATION_CODE,
                        "reason": reason,
                        "platform": platform,
                        "user_id": user_id,
                        "entity_id": entity_id,
                        "role_cleanup": role_cleanup or {},
                        "correlation_id": correlation_id,
                        "request_id": request_id,
                    },
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
            ).execute()
        except Exception:
            logger.exception(
                "council pause failed to write audit row action=%s reason=%s platform=%s user_id=%s entity_id=%s",
                action,
                reason,
                platform,
                user_id,
                entity_id,
            )

    @staticmethod
    def _load_term_members_for_role_cleanup(term_id: int | None) -> list[dict[str, object]]:
        if not db.supabase or not isinstance(term_id, int):
            return []
        try:
            response = (
                db.supabase.table("council_term_members")
                .select("profile_id,role_code,is_active")
                .eq("term_id", term_id)
                .execute()
            )
            return response.data or []
        except Exception:
            logger.exception("council pause failed to load term members for role cleanup term_id=%s", term_id)
            return []

    @staticmethod
    def _revoke_term_member_project_roles(
        *,
        term_id: int | None,
        platform: str,
        user_id: str | None,
        reason: str,
    ) -> dict[str, object]:
        members = CouncilPauseService._load_term_members_for_role_cleanup(term_id)
        if not members:
            logger.info("council pause role cleanup skipped no members term_id=%s reason=%s", term_id, reason)
            return {"attempted": 0, "removed": 0, "not_removed": 0, "errors": []}

        removed = 0
        not_removed = 0
        errors: list[dict[str, str]] = []
        actor_provider = platform if platform in {"telegram", "discord", "system"} else "system"
        actor_user_id = str(user_id or "council_lifecycle").strip() or "council_lifecycle"

        for row in members:
            account_id = str((row or {}).get("profile_id") or "").strip()
            role_code = str((row or {}).get("role_code") or "").strip().lower()
            project_role_name = _TERM_ROLE_CODE_TO_PROJECT_ROLE.get(role_code)
            if not account_id or not project_role_name:
                logger.warning(
                    "council pause role cleanup skipped member term_id=%s account_id=%s role_code=%s",
                    term_id,
                    account_id or None,
                    role_code or None,
                )
                continue
            try:
                result = RoleManagementService.revoke_user_role_by_account(
                    account_id,
                    project_role_name,
                    actor_provider=actor_provider,
                    actor_user_id=actor_user_id,
                    source="council_term_paused_or_ended",
                )
            except Exception:
                logger.exception(
                    "council pause role cleanup crashed term_id=%s account_id=%s role_code=%s project_role=%s",
                    term_id,
                    account_id,
                    role_code,
                    project_role_name,
                )
                not_removed += 1
                errors.append(
                    {
                        "account_id": account_id,
                        "role_code": role_code,
                        "project_role_name": project_role_name,
                        "reason": "exception",
                        "message": "revoke_user_role_by_account crashed",
                    }
                )
                continue

            if bool(result.get("ok")):
                removed += 1
                continue

            not_removed += 1
            failure_reason = str(result.get("reason") or "unknown").strip() or "unknown"
            failure_message = str(result.get("message") or "").strip() or "role revoke returned not ok"
            logger.error(
                "council pause role cleanup failed term_id=%s account_id=%s role_code=%s project_role=%s reason=%s message=%s",
                term_id,
                account_id,
                role_code,
                project_role_name,
                failure_reason,
                failure_message,
            )
            errors.append(
                {
                    "account_id": account_id,
                    "role_code": role_code,
                    "project_role_name": project_role_name,
                    "reason": failure_reason,
                    "message": failure_message,
                }
            )

        summary = {
            "attempted": removed + not_removed,
            "removed": removed,
            "not_removed": not_removed,
            "errors": errors,
        }
        logger.info(
            "council pause role cleanup summary term_id=%s reason=%s attempted=%s removed=%s not_removed=%s",
            term_id,
            reason,
            summary["attempted"],
            summary["removed"],
            summary["not_removed"],
        )
        return summary

    @staticmethod
    def sync_pause_state(*, platform: str = "system", user_id: str | None = None) -> dict[str, object]:
        required, reason, entity_id = CouncilPauseService._is_pause_required()
        current = CouncilPauseService._read_latest_state()

        if required and not current.get("paused"):
            role_cleanup = CouncilPauseService._revoke_term_member_project_roles(
                term_id=entity_id,
                platform=platform,
                user_id=user_id,
                reason=reason or "term_ended_without_launch_confirmation",
            )
            CouncilPauseService._write_pause_event(
                paused=True,
                reason=reason or "term_ended_without_launch_confirmation",
                platform=platform,
                user_id=user_id,
                entity_id=entity_id,
                role_cleanup=role_cleanup,
            )
            return {
                "paused": True,
                "reason": reason or "term_ended_without_launch_confirmation",
                "paused_at": datetime.now(timezone.utc).isoformat(),
                "entity_id": entity_id,
                "role_cleanup": role_cleanup,
            }

        if (not required) and current.get("paused"):
            CouncilPauseService._write_pause_event(
                paused=False,
                reason="launch_confirmation_received",
                platform=platform,
                user_id=user_id,
                entity_id=entity_id,
            )
            return {
                "paused": False,
                "reason": "launch_confirmation_received",
                "paused_at": None,
                "entity_id": entity_id,
            }

        return {
            "paused": bool(current.get("paused")) if current else required,
            "reason": current.get("reason") if current else reason,
            "paused_at": current.get("paused_at") if current else None,
            "entity_id": entity_id,
        }

    @staticmethod
    def get_pause_status_for_admin() -> dict[str, object]:
        state = CouncilPauseService.sync_pause_state(platform="admin_api")
        if not state.get("paused"):
            return {
                "paused": False,
                "reason": None,
                "paused_at": None,
                "message": "Пауза не включена. Запуск новых выборов и голосований доступен.",
            }
        return {
            "paused": True,
            "reason": state.get("reason"),
            "paused_at": state.get("paused_at"),
            "message": "Пауза включена: запуск новых выборов и голосований Совета временно остановлен.",
        }
