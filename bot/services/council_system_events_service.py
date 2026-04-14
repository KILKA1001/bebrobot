from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Callable

from bot.data import db
from bot.services.authority_service import AuthorityService

logger = logging.getLogger(__name__)

_TABLE_NAME = "council_system_event_channels"
_EVENT_MESSAGES_TABLE_NAME = "council_system_event_messages"
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
    def _normalize_event_key(event_key: str | None) -> str | None:
        normalized = str(event_key or "").strip().lower()
        return normalized or None

    @staticmethod
    def _extract_message_id(payload: object) -> str | None:
        if isinstance(payload, str):
            normalized = payload.strip()
            return normalized or None
        if isinstance(payload, int):
            return str(payload)
        if isinstance(payload, dict):
            candidate = payload.get("message_id")
            if candidate is None:
                return None
            normalized = str(candidate).strip()
            return normalized or None
        return None

    @staticmethod
    def _record_admin_action(
        *,
        provider: str,
        actor_user_id: str,
        action: str,
        destination_id: str | None,
        status: str,
        reason: str | None = None,
        target_object: str | None = None,
    ) -> None:
        CouncilSystemEventsService.record_admin_action(
            provider=provider,
            actor_user_id=actor_user_id,
            action=action,
            destination_id=destination_id,
            status=status,
            reason=reason,
            target_object=target_object,
        )

    @staticmethod
    def record_admin_action(
        *,
        provider: str,
        actor_user_id: str,
        action: str,
        destination_id: str | None,
        status: str,
        reason: str | None = None,
        target_object: str | None = None,
    ) -> None:
        log_message = (
            "council_admin_action provider=%s actor_user_id=%s action=%s destination_id=%s "
            "target_object=%s status=%s reason=%s"
        )
        log_args = (
            provider,
            actor_user_id,
            action,
            destination_id or None,
            target_object or None,
            status,
            reason or None,
        )
        exception_reasons = {"db_error", "external_error", "unexpected_error"}
        if status == "success":
            logger.info(log_message, *log_args)
        elif status == "denied":
            logger.warning(log_message, *log_args)
        elif status == "failed" and str(reason or "").strip().lower() in exception_reasons:
            logger.exception(log_message, *log_args)
        elif status == "failed":
            logger.warning(log_message, *log_args)
        else:
            logger.info(log_message, *log_args)
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
                        "action": action,
                        "destination_id": destination_id,
                        "target_object": target_object,
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
    def get_event_message_binding(*, provider: str, event_key: str) -> dict[str, str] | None:
        normalized_provider = str(provider or "").strip().lower()
        normalized_event_key = CouncilSystemEventsService._normalize_event_key(event_key)
        if normalized_provider not in _SUPPORTED_PROVIDERS or not normalized_event_key or not db.supabase:
            return None
        try:
            response = (
                db.supabase.table(_EVENT_MESSAGES_TABLE_NAME)
                .select("destination_id,message_id")
                .eq("provider", normalized_provider)
                .eq("event_key", normalized_event_key)
                .limit(1)
                .execute()
            )
            rows = response.data or []
            if not rows:
                return None
            destination_id = str(rows[0].get("destination_id") or "").strip()
            message_id = str(rows[0].get("message_id") or "").strip()
            if not destination_id or not message_id:
                return None
            return {"destination_id": destination_id, "message_id": message_id}
        except Exception:
            logger.exception(
                "council system event binding read failed provider=%s event_key=%s",
                normalized_provider,
                normalized_event_key,
            )
            return None

    @staticmethod
    def save_event_message_binding(*, provider: str, event_key: str, destination_id: str, message_id: str) -> bool:
        normalized_provider = str(provider or "").strip().lower()
        normalized_event_key = CouncilSystemEventsService._normalize_event_key(event_key)
        normalized_destination = str(destination_id or "").strip()
        normalized_message_id = str(message_id or "").strip()
        if (
            normalized_provider not in _SUPPORTED_PROVIDERS
            or not normalized_event_key
            or not normalized_destination
            or not normalized_message_id
            or not db.supabase
        ):
            return False
        try:
            db.supabase.table(_EVENT_MESSAGES_TABLE_NAME).upsert(
                {
                    "provider": normalized_provider,
                    "event_key": normalized_event_key,
                    "destination_id": normalized_destination,
                    "message_id": normalized_message_id,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
                on_conflict="provider,event_key",
            ).execute()
            return True
        except Exception:
            logger.exception(
                "council system event binding write failed provider=%s event_key=%s destination_id=%s message_id=%s",
                normalized_provider,
                normalized_event_key,
                normalized_destination,
                normalized_message_id,
            )
            return False

    @staticmethod
    def publish_event(
        *,
        provider: str,
        event_code: str,
        publisher: Callable[[str, str], object],
        editor: Callable[[str, str, str], bool] | None = None,
        event_key: str | None = None,
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
        normalized_event_key = CouncilSystemEventsService._normalize_event_key(event_key)

        if normalized_event_key and editor:
            binding = CouncilSystemEventsService.get_event_message_binding(
                provider=normalized_provider,
                event_key=normalized_event_key,
            )
            if binding:
                bound_destination = binding["destination_id"]
                bound_message_id = binding["message_id"]
                if bound_destination != destination_id:
                    logger.warning(
                        "council system event edit fallback: destination changed provider=%s event_key=%s old_destination=%s new_destination=%s",
                        normalized_provider,
                        normalized_event_key,
                        bound_destination,
                        destination_id,
                    )
                else:
                    try:
                        edited = bool(editor(bound_destination, bound_message_id, text))
                        if edited:
                            return {
                                "ok": True,
                                "destination_id": bound_destination,
                                "event_code": normalized_event,
                                "event_key": normalized_event_key,
                                "message_id": bound_message_id,
                                "updated": True,
                            }
                        logger.warning(
                            "council system event edit fallback: edit failed provider=%s event_key=%s destination_id=%s message_id=%s",
                            normalized_provider,
                            normalized_event_key,
                            bound_destination,
                            bound_message_id,
                        )
                    except Exception:
                        logger.exception(
                            "council system event edit fallback: edit exception provider=%s event_key=%s destination_id=%s message_id=%s",
                            normalized_provider,
                            normalized_event_key,
                            bound_destination,
                            bound_message_id,
                        )
        try:
            publish_result = publisher(destination_id, text)
            delivered = bool(publish_result)
            if not delivered:
                logger.error(
                    "council system event publish failed provider=%s destination_id=%s event_code=%s",
                    normalized_provider,
                    destination_id,
                    normalized_event,
                )
                return {"ok": False, "reason": "publish_failed"}

            message_id = CouncilSystemEventsService._extract_message_id(publish_result)
            if normalized_event_key and message_id:
                CouncilSystemEventsService.save_event_message_binding(
                    provider=normalized_provider,
                    event_key=normalized_event_key,
                    destination_id=destination_id,
                    message_id=message_id,
                )
            elif normalized_event_key:
                logger.warning(
                    "council system event publish missing message id for binding provider=%s event_key=%s destination_id=%s",
                    normalized_provider,
                    normalized_event_key,
                    destination_id,
                )

            return {
                "ok": True,
                "destination_id": destination_id,
                "event_code": normalized_event,
                "event_key": normalized_event_key,
                "message_id": message_id,
                "updated": False,
            }
        except Exception:
            logger.exception(
                "council system event publish crashed provider=%s destination_id=%s event_code=%s",
                normalized_provider,
                destination_id,
                normalized_event,
            )
            return {"ok": False, "reason": "publish_exception"}
