from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Callable

from bot.data import db
from bot.services.authority_service import AuthorityService

logger = logging.getLogger(__name__)

_TABLE_NAME = "council_system_event_channels"
_SUPPORTED_PROVIDERS = {"telegram", "discord"}
_EVENT_CODES = {
    "election_started",
    "election_progress",
    "election_results",
    "discussion_started",
    "voting_started",
    "decision_published",
}


class CouncilSystemEventsService:
    @staticmethod
    def _record_admin_action(
        *,
        provider: str,
        actor_user_id: str,
        action: str,
        destination_id: str | None,
        status: str,
        reason: str | None = None,
    ) -> None:
        logger.info(
            "council_admin_action provider=%s actor_user_id=%s action=%s destination_id=%s status=%s reason=%s",
            provider,
            actor_user_id,
            action,
            destination_id or None,
            status,
            reason or None,
        )
        if not db.supabase:
            return
        try:
            db.supabase.table("council_audit_log").insert(
                {
                    "entity_type": "system_event_channel",
                    "entity_id": None,
                    "action": action,
                    "status": status,
                    "actor_profile_id": None,
                    "source_platform": provider,
                    "details": {
                        "actor_user_id": actor_user_id,
                        "destination_id": destination_id,
                        "reason": reason,
                    },
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
            ).execute()
        except Exception:
            logger.exception(
                "council admin action write failed provider=%s actor_user_id=%s action=%s status=%s",
                provider,
                actor_user_id,
                action,
                status,
            )

    @staticmethod
    def set_channel(*, provider: str, actor_user_id: str, destination_id: str | None) -> dict[str, object]:
        normalized_provider = str(provider or "").strip().lower()
        normalized_actor_id = str(actor_user_id or "").strip()
        normalized_destination = str(destination_id or "").strip()

        if normalized_provider not in _SUPPORTED_PROVIDERS:
            logger.error(
                "council system events set channel rejected: unsupported provider provider=%s actor_user_id=%s",
                normalized_provider or None,
                normalized_actor_id or None,
            )
            CouncilSystemEventsService._record_admin_action(
                provider=normalized_provider or "unknown",
                actor_user_id=normalized_actor_id,
                action="set_channel",
                destination_id=normalized_destination or None,
                status="failed",
                reason="unsupported_provider",
            )
            return {"ok": False, "reason": "unsupported_provider", "message": "❌ Неизвестная платформа."}

        if not normalized_actor_id:
            logger.error(
                "council system events set channel rejected: empty actor id provider=%s",
                normalized_provider,
            )
            CouncilSystemEventsService._record_admin_action(
                provider=normalized_provider,
                actor_user_id=normalized_actor_id,
                action="set_channel",
                destination_id=normalized_destination or None,
                status="failed",
                reason="empty_actor_id",
            )
            return {"ok": False, "reason": "empty_actor_id", "message": "❌ Не удалось определить администратора."}

        if not AuthorityService.is_super_admin(normalized_provider, normalized_actor_id):
            logger.warning(
                "council system events set channel denied non-superadmin provider=%s actor_user_id=%s destination_id=%s",
                normalized_provider,
                normalized_actor_id,
                normalized_destination or None,
            )
            CouncilSystemEventsService._record_admin_action(
                provider=normalized_provider,
                actor_user_id=normalized_actor_id,
                action="set_channel",
                destination_id=normalized_destination or None,
                status="denied",
                reason="forbidden",
            )
            return {"ok": False, "reason": "forbidden", "message": "❌ Действие доступно только суперадмину."}

        if not db.supabase:
            logger.warning(
                "council system events set channel skipped: supabase is not configured provider=%s actor_user_id=%s",
                normalized_provider,
                normalized_actor_id,
            )
            CouncilSystemEventsService._record_admin_action(
                provider=normalized_provider,
                actor_user_id=normalized_actor_id,
                action="set_channel",
                destination_id=normalized_destination or None,
                status="failed",
                reason="db_unavailable",
            )
            return {"ok": False, "reason": "db_unavailable", "message": "❌ База данных недоступна. Попробуйте позже."}

        try:
            if normalized_destination:
                db.supabase.table(_TABLE_NAME).upsert(
                    {
                        "provider": normalized_provider,
                        "destination_id": normalized_destination,
                        "updated_by_user_id": normalized_actor_id,
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    },
                    on_conflict="provider",
                ).execute()
                logger.info(
                    "council system events channel configured provider=%s destination_id=%s actor_user_id=%s",
                    normalized_provider,
                    normalized_destination,
                    normalized_actor_id,
                )
                CouncilSystemEventsService._record_admin_action(
                    provider=normalized_provider,
                    actor_user_id=normalized_actor_id,
                    action="set_channel",
                    destination_id=normalized_destination,
                    status="success",
                )
                return {
                    "ok": True,
                    "configured": True,
                    "provider": normalized_provider,
                    "destination_id": normalized_destination,
                }

            db.supabase.table(_TABLE_NAME).delete().eq("provider", normalized_provider).execute()
            logger.info(
                "council system events channel cleared provider=%s actor_user_id=%s",
                normalized_provider,
                normalized_actor_id,
            )
            CouncilSystemEventsService._record_admin_action(
                provider=normalized_provider,
                actor_user_id=normalized_actor_id,
                action="clear_channel",
                destination_id=None,
                status="success",
            )
            return {"ok": True, "configured": False, "provider": normalized_provider, "destination_id": None}
        except Exception:
            logger.exception(
                "council system events set channel failed provider=%s actor_user_id=%s destination_id=%s",
                normalized_provider,
                normalized_actor_id,
                normalized_destination or None,
            )
            CouncilSystemEventsService._record_admin_action(
                provider=normalized_provider,
                actor_user_id=normalized_actor_id,
                action="set_channel",
                destination_id=normalized_destination or None,
                status="failed",
                reason="db_error",
            )
            return {"ok": False, "reason": "db_error", "message": "❌ Не удалось сохранить настройку. Подробности в логах."}

    @staticmethod
    def get_channel(provider: str) -> str | None:
        normalized_provider = str(provider or "").strip().lower()
        if normalized_provider not in _SUPPORTED_PROVIDERS or not db.supabase:
            return None
        try:
            response = db.supabase.table(_TABLE_NAME).select("destination_id").eq("provider", normalized_provider).limit(1).execute()
            rows = response.data or []
            if not rows:
                return None
            value = str(rows[0].get("destination_id") or "").strip()
            return value or None
        except Exception:
            logger.exception("council system events get channel failed provider=%s", normalized_provider)
            return None

    @staticmethod
    def build_event_text(*, event_code: str, title: str | None = None, details: str | None = None) -> str:
        normalized_event = str(event_code or "").strip().lower()
        safe_title = str(title or "").strip()
        safe_details = str(details or "").strip()

        title_line = f"\nТема: {safe_title}" if safe_title else ""
        details_line = f"\nПодробности: {safe_details}" if safe_details else ""
        mapping = {
            "election_started": "🗳 Старт выборов Совета." + title_line + details_line,
            "election_progress": "📈 Обновление по выборам Совета." + title_line + details_line,
            "election_results": "🏁 Итоги выборов Совета." + title_line + details_line,
            "discussion_started": "💬 Старт обсуждения вопроса Совета." + title_line + details_line,
            "voting_started": "🗳 Старт голосования по вопросу Совета." + title_line + details_line,
            "decision_published": "✅ Опубликовано принятое решение Совета." + title_line + details_line,
        }
        return mapping.get(normalized_event, "ℹ️ Системное событие Совета.")

    @staticmethod
    def publish_event(
        *,
        provider: str,
        event_code: str,
        publisher: Callable[[str, str], bool],
        title: str | None = None,
        details: str | None = None,
        confirmed: bool = False,
    ) -> dict[str, object]:
        normalized_provider = str(provider or "").strip().lower()
        normalized_event = str(event_code or "").strip().lower()

        if normalized_provider not in _SUPPORTED_PROVIDERS:
            logger.error("council system event publish rejected unsupported provider=%s event_code=%s", normalized_provider, normalized_event)
            return {"ok": False, "reason": "unsupported_provider"}
        if normalized_event not in _EVENT_CODES:
            logger.error("council system event publish rejected unsupported event provider=%s event_code=%s", normalized_provider, normalized_event)
            return {"ok": False, "reason": "unsupported_event"}
        if normalized_event == "decision_published" and not confirmed:
            logger.info(
                "council system event publish requires confirmation provider=%s event_code=%s title=%s",
                normalized_provider,
                normalized_event,
                str(title or "").strip() or None,
            )
            return {"ok": False, "reason": "confirmation_required", "message": "Требуется отдельное подтверждение публикации решения."}

        destination_id = CouncilSystemEventsService.get_channel(normalized_provider)
        if not destination_id:
            logger.warning(
                "council system event publish skipped: channel not configured provider=%s event_code=%s",
                normalized_provider,
                normalized_event,
            )
            return {"ok": False, "reason": "channel_not_configured"}

        text = CouncilSystemEventsService.build_event_text(event_code=normalized_event, title=title, details=details)
        try:
            delivered = bool(publisher(destination_id, text))
            if not delivered:
                logger.error(
                    "council system event publish failed provider=%s destination_id=%s event_code=%s",
                    normalized_provider,
                    destination_id,
                    normalized_event,
                )
                return {"ok": False, "reason": "publish_failed"}
            return {"ok": True, "destination_id": destination_id, "event_code": normalized_event}
        except Exception:
            logger.exception(
                "council system event publish crashed provider=%s destination_id=%s event_code=%s",
                normalized_provider,
                destination_id,
                normalized_event,
            )
            return {"ok": False, "reason": "publish_exception"}
