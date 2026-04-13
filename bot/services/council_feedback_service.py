"""
Назначение: модуль "council feedback service" реализует продуктовый контур в зоне общая логика.
Ответственность: единая точка для сценариев подачи предложений Совету без дублирования между платформами.
Где используется: Discord, Telegram.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from bot.data import db
from bot.services.accounts_service import AccountsService
from bot.services.authority_service import AuthorityService
from bot.services.council_pause_service import CouncilPauseService

logger = logging.getLogger(__name__)

_DECISION_ENTITY_TYPE = "council_decision"
_FINAL_DECISION_FORBIDDEN_MESSAGE = (
    "❌ Недостаточно прав. Это действие доступно только суперадмину. "
    "Если нужно, попросите суперадмина выполнить его."
)


class CouncilFeedbackService:
    STATUS_LABELS: dict[str, str] = {
        "awaiting_term_launch": "⏳ Ожидает запуска созыва",
        "draft": "🕓 На первичной проверке",
        "discussion": "💬 Обсуждение",
        "voting": "🗳 Голосование",
        "decided": "✅ Решение принято",
        "archived": "📚 Перенесено в архив",
    }

    @staticmethod
    def _resolve_account_id(provider: str, provider_user_id: str) -> str | None:
        try:
            return AccountsService.resolve_account_id(provider, provider_user_id)
        except Exception:
            logger.exception(
                "council feedback failed to resolve account provider=%s provider_user_id=%s",
                provider,
                provider_user_id,
            )
            return None

    @staticmethod
    def _record_final_decision_audit(
        *,
        provider: str,
        actor_user_id: str,
        action: str,
        decision_id: int | None,
        result: str,
        reason: str,
    ) -> None:
        logger.info(
            "council final decision audit provider=%s actor_user_id=%s action=%s decision_id=%s result=%s reason=%s",
            provider,
            actor_user_id,
            action,
            decision_id,
            result,
            reason,
        )
        if not db.supabase:
            return
        try:
            db.supabase.table("council_audit_log").insert(
                {
                    "entity_type": _DECISION_ENTITY_TYPE,
                    "entity_id": decision_id,
                    "action": action,
                    "status": result,
                    "actor_profile_id": None,
                    "source_platform": provider,
                    "details": {
                        "actor_user_id": actor_user_id,
                        "result": result,
                        "reason": reason,
                    },
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
            ).execute()
        except Exception:
            logger.exception(
                "council final decision audit write failed provider=%s actor_user_id=%s action=%s decision_id=%s",
                provider,
                actor_user_id,
                action,
                decision_id,
            )

    @staticmethod
    def edit_final_decision(*, provider: str, actor_user_id: str, decision_id: int, decision_text: str) -> dict[str, object]:
        normalized_provider = str(provider or "").strip().lower()
        normalized_actor_user_id = str(actor_user_id or "").strip()
        normalized_text = str(decision_text or "").strip()

        if not AuthorityService.is_super_admin(normalized_provider, normalized_actor_user_id):
            CouncilFeedbackService._record_final_decision_audit(
                provider=normalized_provider,
                actor_user_id=normalized_actor_user_id,
                action="edit_final",
                decision_id=decision_id,
                result="denied",
                reason="not_superadmin",
            )
            return {"ok": False, "reason": "forbidden", "message": _FINAL_DECISION_FORBIDDEN_MESSAGE}

        CouncilFeedbackService._record_final_decision_audit(
            provider=normalized_provider,
            actor_user_id=normalized_actor_user_id,
            action="edit_final",
            decision_id=decision_id,
            result="allowed",
            reason="superadmin",
        )

        if not db.supabase:
            logger.error("council edit final decision failed: db unavailable decision_id=%s", decision_id)
            return {"ok": False, "reason": "db_unavailable", "message": "❌ База данных недоступна. Попробуйте позже."}
        if not normalized_text:
            return {"ok": False, "reason": "empty_text", "message": "❌ Введите текст итога перед сохранением."}

        try:
            db.supabase.table("council_decisions").update(
                {
                    "decision_text": normalized_text,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
            ).eq("id", int(decision_id)).execute()
            return {
                "ok": True,
                "message": (
                    "✅ Данные обновлены в текущем сообщении. Итог решения сохранён. "
                    "Если нужно, вы можете сразу внести ещё одно изменение."
                ),
            }
        except Exception:
            logger.exception(
                "council edit final decision failed provider=%s actor_user_id=%s decision_id=%s",
                normalized_provider,
                normalized_actor_user_id,
                decision_id,
            )
            return {"ok": False, "reason": "db_error", "message": "❌ Не удалось обновить итог. Попробуйте позже."}

    @staticmethod
    def delete_final_decision(*, provider: str, actor_user_id: str, decision_id: int) -> dict[str, object]:
        normalized_provider = str(provider or "").strip().lower()
        normalized_actor_user_id = str(actor_user_id or "").strip()

        if not AuthorityService.is_super_admin(normalized_provider, normalized_actor_user_id):
            CouncilFeedbackService._record_final_decision_audit(
                provider=normalized_provider,
                actor_user_id=normalized_actor_user_id,
                action="delete_final",
                decision_id=decision_id,
                result="denied",
                reason="not_superadmin",
            )
            return {"ok": False, "reason": "forbidden", "message": _FINAL_DECISION_FORBIDDEN_MESSAGE}

        CouncilFeedbackService._record_final_decision_audit(
            provider=normalized_provider,
            actor_user_id=normalized_actor_user_id,
            action="delete_final",
            decision_id=decision_id,
            result="allowed",
            reason="superadmin",
        )

        if not db.supabase:
            logger.error("council delete final decision failed: db unavailable decision_id=%s", decision_id)
            return {"ok": False, "reason": "db_unavailable", "message": "❌ База данных недоступна. Попробуйте позже."}

        try:
            db.supabase.table("council_decisions").delete().eq("id", int(decision_id)).execute()
            return {"ok": True, "message": "✅ Итог удалён."}
        except Exception:
            logger.exception(
                "council delete final decision failed provider=%s actor_user_id=%s decision_id=%s",
                normalized_provider,
                normalized_actor_user_id,
                decision_id,
            )
            return {"ok": False, "reason": "db_error", "message": "❌ Не удалось удалить итог. Попробуйте позже."}

    @staticmethod
    def _get_active_term_id() -> int | None:
        if not db.supabase:
            return None
        try:
            response = (
                db.supabase.table("council_terms")
                .select("id,status,starts_at")
                .eq("status", "active")
                .order("starts_at", desc=True)
                .limit(1)
                .execute()
            )
            rows = response.data or []
            if rows:
                return int(rows[0]["id"])
        except Exception:
            logger.exception("council feedback failed to load active term")
        return None


    @staticmethod
    def _get_latest_term_id() -> int | None:
        if not db.supabase:
            return None
        try:
            response = (
                db.supabase.table("council_terms")
                .select("id")
                .order("ends_at", desc=True)
                .order("id", desc=True)
                .limit(1)
                .execute()
            )
            rows = response.data or []
            if rows:
                return int(rows[0]["id"])
        except Exception:
            logger.exception("council feedback failed to load latest term")
        return None

    @staticmethod
    def submit_proposal(*, provider: str, provider_user_id: str, title: str, proposal_text: str) -> dict[str, object]:
        normalized_title = str(title or "").strip()
        normalized_text = str(proposal_text or "").strip()

        if len(normalized_title) < 5:
            return {"ok": False, "error": "title_too_short", "message": "Заголовок должен быть не короче 5 символов."}
        if len(normalized_title) > 140:
            return {"ok": False, "error": "title_too_long", "message": "Заголовок должен быть не длиннее 140 символов."}
        if len(normalized_text) < 20:
            return {"ok": False, "error": "proposal_too_short", "message": "Текст предложения должен быть не короче 20 символов."}
        if len(normalized_text) > 1000:
            return {"ok": False, "error": "proposal_too_long", "message": "Текст предложения должен быть не длиннее 1000 символов."}

        account_id = CouncilFeedbackService._resolve_account_id(provider, provider_user_id)
        if not account_id:
            return {
                "ok": False,
                "error": "account_not_linked",
                "message": "Сначала привяжите общий аккаунт через /link, затем повторите отправку.",
            }
        if not db.supabase:
            return {"ok": False, "error": "db_unavailable", "message": "База данных недоступна. Повторите попытку позже."}

        pause_state = CouncilPauseService.sync_pause_state(platform=provider, user_id=str(provider_user_id))
        term_id = CouncilFeedbackService._get_active_term_id()
        queued_by_pause = False
        if term_id is None:
            if pause_state.get("paused"):
                term_id = CouncilFeedbackService._get_latest_term_id()
                queued_by_pause = term_id is not None
            if term_id is None:
                return {
                    "ok": False,
                    "error": "term_not_active",
                    "message": "Сейчас нет активного созыва Совета. Отправка предложения временно недоступна.",
                }

        try:
            now_iso = datetime.now(timezone.utc).isoformat()
            response = (
                db.supabase.table("council_questions")
                .insert(
                    {
                        "term_id": term_id,
                        "author_profile_id": account_id,
                        "title": normalized_title,
                        "question_text": normalized_text,
                        "proposal_text": normalized_text,
                        "status": "draft",
                        "created_at": now_iso,
                        "updated_at": now_iso,
                    }
                )
                .execute()
            )
            row = (response.data or [{}])[0]
            logger.info(
                "council feedback proposal submitted provider=%s provider_user_id=%s account_id=%s term_id=%s question_id=%s",
                provider,
                provider_user_id,
                account_id,
                term_id,
                row.get("id"),
            )
            status_code = "awaiting_term_launch" if queued_by_pause else str(row.get("status") or "draft")
            return {
                "ok": True,
                "proposal_id": row.get("id"),
                "status": status_code,
                "status_label": CouncilFeedbackService.render_status_label(status_code),
            }
        except Exception:
            logger.exception(
                "council feedback proposal submit failed provider=%s provider_user_id=%s account_id=%s",
                provider,
                provider_user_id,
                account_id,
            )
            return {"ok": False, "error": "insert_failed", "message": "Не удалось сохранить предложение. Попробуйте ещё раз."}

    @staticmethod
    def render_status_label(status: str) -> str:
        return CouncilFeedbackService.STATUS_LABELS.get(str(status or "").strip().lower(), "ℹ️ Статус обновляется")

    @staticmethod
    def get_latest_status(*, provider: str, provider_user_id: str) -> dict[str, object]:
        account_id = CouncilFeedbackService._resolve_account_id(provider, provider_user_id)
        if not account_id:
            return {
                "ok": False,
                "error": "account_not_linked",
                "message": "Сначала привяжите общий аккаунт через /link, затем повторите запрос статуса.",
            }
        if not db.supabase:
            return {"ok": False, "error": "db_unavailable", "message": "База данных недоступна. Повторите попытку позже."}

        try:
            response = (
                db.supabase.table("council_questions")
                .select("id,title,status,created_at,updated_at")
                .eq("author_profile_id", account_id)
                .order("created_at", desc=True)
                .limit(1)
                .execute()
            )
            rows = response.data or []
            if not rows:
                return {
                    "ok": True,
                    "has_data": False,
                    "message": "У вас пока нет предложений. Нажмите «Подать предложение», чтобы отправить первое.",
                }
            row = rows[0]
            status_code = str(row.get("status") or "draft")
            if status_code == "draft":
                pause_state = CouncilPauseService.sync_pause_state(platform=provider, user_id=str(provider_user_id))
                if pause_state.get("paused"):
                    status_code = "awaiting_term_launch"
            return {
                "ok": True,
                "has_data": True,
                "proposal_id": row.get("id"),
                "title": str(row.get("title") or "Без заголовка"),
                "status": status_code,
                "status_label": CouncilFeedbackService.render_status_label(status_code),
                "created_at": str(row.get("created_at") or ""),
                "updated_at": str(row.get("updated_at") or ""),
            }
        except Exception:
            logger.exception(
                "council feedback status load failed provider=%s provider_user_id=%s account_id=%s",
                provider,
                provider_user_id,
                account_id,
            )
            return {"ok": False, "error": "status_failed", "message": "Не удалось получить статус. Попробуйте ещё раз."}

    @staticmethod
    def get_decisions_archive(
        *,
        limit: int = 5,
        period_code: str = "90d",
        status_code: str = "all",
        question_type_code: str = "all",
    ) -> list[dict[str, object]]:
        if not db.supabase:
            return []
        try:
            normalized_period = CouncilFeedbackService._normalize_archive_period(period_code)
            normalized_status = CouncilFeedbackService._normalize_archive_status(status_code)
            normalized_type = CouncilFeedbackService._normalize_archive_question_type(question_type_code)
            response = (
                db.supabase.table("council_decisions")
                .select("id,decision_code,decision_text,decided_at")
                .order("decided_at", desc=True)
                .limit(max(1, min(int(limit), 50)))
                .execute()
            )
            rows: list[dict[str, object]] = []
            cutoff_dt = None
            period_days = CouncilFeedbackService.ARCHIVE_PERIOD_DAYS.get(normalized_period)
            if period_days is not None:
                cutoff_dt = datetime.now(timezone.utc) - timedelta(days=period_days)

            for row in response.data or []:
                if not isinstance(row, dict):
                    continue
                decided_raw = str(row.get("decided_at") or "")
                decided_dt: datetime | None = None
                if decided_raw:
                    try:
                        decided_dt = datetime.fromisoformat(decided_raw.replace("Z", "+00:00"))
                    except Exception:
                        logger.exception("council feedback archive parse failed decided_at=%s row_id=%s", decided_raw, row.get("id"))
                if cutoff_dt and decided_dt and decided_dt < cutoff_dt:
                    continue

                resolved_status = CouncilFeedbackService._resolve_status_code_from_decision(row.get("decision_code"))
                resolved_type = CouncilFeedbackService._resolve_question_type_from_decision(row.get("decision_code"))
                if normalized_status != "all" and resolved_status != normalized_status:
                    continue
                if normalized_type != "all" and resolved_type != normalized_type:
                    continue

                row["archive_status_code"] = resolved_status
                row["archive_question_type_code"] = resolved_type
                row["final_comment"] = str(row.get("decision_text") or "").strip()
                rows.append(row)
                if len(rows) >= max(1, min(int(limit), 20)):
                    break
            return rows
        except Exception:
            logger.exception("council feedback archive load failed")
            return []
    ARCHIVE_PERIOD_DAYS: dict[str, int | None] = {
        "30d": 30,
        "90d": 90,
        "365d": 365,
        "all": None,
    }
    ARCHIVE_STATUS_CODES: tuple[str, ...] = ("all", "accepted", "rejected", "pending")
    ARCHIVE_QUESTION_TYPES: tuple[str, ...] = ("all", "general", "election", "other")

    @staticmethod
    def _normalize_archive_period(period_code: str | None) -> str:
        code = str(period_code or "90d").strip().lower()
        return code if code in CouncilFeedbackService.ARCHIVE_PERIOD_DAYS else "90d"

    @staticmethod
    def _normalize_archive_status(status_code: str | None) -> str:
        code = str(status_code or "all").strip().lower()
        return code if code in CouncilFeedbackService.ARCHIVE_STATUS_CODES else "all"

    @staticmethod
    def _normalize_archive_question_type(type_code: str | None) -> str:
        code = str(type_code or "all").strip().lower()
        return code if code in CouncilFeedbackService.ARCHIVE_QUESTION_TYPES else "all"

    @staticmethod
    def _resolve_status_code_from_decision(decision_code: object) -> str:
        normalized = str(decision_code or "").strip().lower()
        if any(token in normalized for token in ("accept", "approved", "yes", "pass")):
            return "accepted"
        if any(token in normalized for token in ("reject", "decline", "no", "deny")):
            return "rejected"
        return "pending"

    @staticmethod
    def _resolve_question_type_from_decision(decision_code: object) -> str:
        normalized = str(decision_code or "").strip().lower()
        if "election" in normalized or "candidate" in normalized:
            return "election"
        if not normalized:
            return "other"
        if any(token in normalized for token in ("question", "proposal", "council")):
            return "general"
        return "other"
