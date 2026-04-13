"""
Назначение: модуль "council feedback service" реализует продуктовый контур в зоне общая логика.
Ответственность: единая точка для сценариев подачи предложений Совету без дублирования между платформами.
Где используется: Discord, Telegram.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from bot.data import db
from bot.services.accounts_service import AccountsService

logger = logging.getLogger(__name__)


class CouncilFeedbackService:
    STATUS_LABELS: dict[str, str] = {
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

        term_id = CouncilFeedbackService._get_active_term_id()
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
            return {
                "ok": True,
                "proposal_id": row.get("id"),
                "status": str(row.get("status") or "draft"),
                "status_label": CouncilFeedbackService.render_status_label(str(row.get("status") or "draft")),
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
            return {
                "ok": True,
                "has_data": True,
                "proposal_id": row.get("id"),
                "title": str(row.get("title") or "Без заголовка"),
                "status": str(row.get("status") or "draft"),
                "status_label": CouncilFeedbackService.render_status_label(str(row.get("status") or "draft")),
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
    def get_decisions_archive(limit: int = 5) -> list[dict[str, object]]:
        if not db.supabase:
            return []
        try:
            response = (
                db.supabase.table("council_decisions")
                .select("id,decision_code,decision_text,decided_at")
                .order("decided_at", desc=True)
                .limit(max(1, min(int(limit), 20)))
                .execute()
            )
            rows = [row for row in (response.data or []) if isinstance(row, dict)]
            return rows
        except Exception:
            logger.exception("council feedback archive load failed")
            return []
