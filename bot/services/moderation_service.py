import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from bot.data import db

from .accounts_service import AccountsService
from .authority_service import AuthorityService, ModerationAuthorityDecision


logger = logging.getLogger(__name__)


class ModerationService:
    """Account-first moderation service with shared preview/apply payloads for all transports."""

    BAN_WARN_THRESHOLD = 5
    ACTION_WARN = "warn"
    ACTION_MUTE = "mute"
    ACTION_BAN = "ban"
    ACTION_FINE_POINTS = "fine_points"
    _ACTION_PRIORITY = {
        ACTION_MUTE: 1,
        ACTION_WARN: 2,
        ACTION_BAN: 3,
    }

    @staticmethod
    def _resolve_account_id(provider: str, provider_user_id: str | int, *, role: str) -> Optional[str]:
        normalized_provider = str(provider or "").strip().lower()
        normalized_user_id = str(provider_user_id or "").strip()
        if not normalized_provider or not normalized_user_id:
            logger.error(
                "moderation resolve account skipped role=%s provider=%s provider_user_id=%s",
                role,
                normalized_provider,
                normalized_user_id,
            )
            return None

        account_id = AccountsService.resolve_account_id(normalized_provider, normalized_user_id)
        if account_id:
            return str(account_id)

        if hasattr(db, "_inc_metric"):
            db._inc_metric("identity_resolve_errors")
        logger.error(
            "moderation resolve account failed role=%s provider=%s provider_user_id=%s",
            role,
            normalized_provider,
            normalized_user_id,
        )
        return None

    @staticmethod
    def _select_single(table: str, **filters: Any) -> Optional[dict]:
        if not db.supabase:
            logger.error("moderation select skipped: supabase is not initialized table=%s", table)
            return None

        try:
            query = db.supabase.table(table).select("*")
            for key, value in filters.items():
                query = query.eq(key, value)
            response = query.limit(1).execute()
            rows = response.data or []
            return dict(rows[0]) if rows else None
        except Exception as exc:
            logger.exception("moderation select failed table=%s filters=%s error=%s", table, filters, exc)
            return None

    @staticmethod
    def _select_many(table: str, **filters: Any) -> list[dict]:
        if not db.supabase:
            logger.error("moderation select many skipped: supabase is not initialized table=%s", table)
            return []

        try:
            query = db.supabase.table(table).select("*")
            for key, value in filters.items():
                query = query.eq(key, value)
            response = query.execute()
            return [dict(row) for row in (response.data or [])]
        except Exception as exc:
            logger.exception("moderation select many failed table=%s filters=%s error=%s", table, filters, exc)
            return []

    @staticmethod
    def _insert_row(table: str, payload: dict[str, Any]) -> Optional[dict]:
        if not db.supabase:
            logger.error("moderation insert skipped: supabase is not initialized table=%s", table)
            return None

        try:
            response = db.supabase.table(table).insert(payload).execute()
            rows = response.data or []
            if not rows:
                logger.error("moderation insert returned empty payload table=%s payload=%s", table, payload)
                return None
            return dict(rows[0])
        except Exception as exc:
            logger.exception("moderation insert failed table=%s payload=%s error=%s", table, payload, exc)
            return None

    @staticmethod
    def _update_rows(table: str, filters: dict[str, Any], payload: dict[str, Any]) -> list[dict]:
        if not db.supabase:
            logger.error("moderation update skipped: supabase is not initialized table=%s", table)
            return []

        try:
            query = db.supabase.table(table).update(payload)
            for key, value in filters.items():
                query = query.eq(key, value)
            response = query.execute()
            return [dict(row) for row in (response.data or [])]
        except Exception as exc:
            logger.exception(
                "moderation update failed table=%s filters=%s payload=%s error=%s",
                table,
                filters,
                payload,
                exc,
            )
            return []

    @staticmethod
    def _is_truthy(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        return str(value or "").strip().lower() in {"1", "true", "t", "yes", "y"}

    @staticmethod
    def list_active_violation_types() -> list[dict[str, Any]]:
        rows = ModerationService._select_many("moderation_violation_types", is_active=True)
        rows.sort(key=lambda item: (str(item.get("title") or item.get("code") or "").casefold(), str(item.get("code") or "").casefold()))
        return rows

    @staticmethod
    def _load_violation_type(violation_code: str) -> Optional[dict]:
        normalized_code = str(violation_code or "").strip().lower()
        violation_type = ModerationService._select_single(
            "moderation_violation_types",
            code=normalized_code,
            is_active=True,
        )
        if violation_type:
            return violation_type

        logger.error("moderation violation type not found code=%s", normalized_code)
        return None

    @staticmethod
    def _load_warn_state(account_id: str) -> dict[str, Any]:
        state = ModerationService._select_single("moderation_warn_state", account_id=account_id)
        return state or {}

    @staticmethod
    def _current_warn_count(state: dict[str, Any]) -> int:
        raw_value = state.get("active_warn_count", state.get("warn_count", 0))
        try:
            return max(0, int(raw_value or 0))
        except (TypeError, ValueError):
            logger.warning("moderation invalid warn count state=%s", state)
            return 0

    @staticmethod
    def _rule_escalation_step(rule: dict[str, Any], warn_count_before: int) -> int:
        for key in ("escalation_step", "step_no"):
            raw_value = rule.get(key)
            if raw_value is None:
                continue
            try:
                return max(1, int(raw_value))
            except (TypeError, ValueError):
                logger.warning("moderation invalid escalation step key=%s value=%s rule_id=%s", key, raw_value, rule.get("id"))
        warn_count_rule = rule.get("warn_count_before")
        try:
            if warn_count_rule is not None:
                return max(1, int(warn_count_rule) + 1)
        except (TypeError, ValueError):
            logger.warning("moderation invalid warn_count_before value=%s rule_id=%s", warn_count_rule, rule.get("id"))
        return max(1, warn_count_before + 1)

    @staticmethod
    def _rule_matches(rule: dict[str, Any], warn_count_before: int) -> bool:
        warn_before = rule.get("warn_count_before")
        if warn_before is not None:
            try:
                return int(warn_before) == warn_count_before
            except (TypeError, ValueError):
                logger.warning("moderation invalid rule warn_count_before=%s rule_id=%s", warn_before, rule.get("id"))
        escalation_step = rule.get("escalation_step", rule.get("step_no"))
        if escalation_step is not None:
            try:
                return int(escalation_step) == warn_count_before + 1
            except (TypeError, ValueError):
                logger.warning("moderation invalid rule escalation_step=%s rule_id=%s", escalation_step, rule.get("id"))
        return False

    @staticmethod
    def _load_penalty_rules(violation_type_id: Any) -> list[dict]:
        rules = ModerationService._select_many(
            "moderation_penalty_rules",
            violation_type_id=violation_type_id,
            is_active=True,
        )
        return rules

    @staticmethod
    def _load_penalty_rule(violation_type_id: Any, warn_count_before: int) -> Optional[dict]:
        rules = ModerationService._load_penalty_rules(violation_type_id)
        if not rules:
            logger.error(
                "moderation penalty rules not found violation_type_id=%s warn_count_before=%s",
                violation_type_id,
                warn_count_before,
            )
            return None

        exact_match = next((rule for rule in rules if ModerationService._rule_matches(rule, warn_count_before)), None)
        if exact_match:
            return exact_match

        fallback_rule = max(
            rules,
            key=lambda row: ModerationService._rule_escalation_step(row, warn_count_before),
        )
        logger.warning(
            "moderation rule fallback used violation_type_id=%s warn_count_before=%s selected_rule_id=%s",
            violation_type_id,
            warn_count_before,
            fallback_rule.get("id"),
        )
        return fallback_rule

    @staticmethod
    def _resolve_subject(provider: str, raw_subject: Any, *, role: str) -> dict[str, Any]:
        if isinstance(raw_subject, dict):
            resolved_provider = str(raw_subject.get("provider") or provider or "").strip().lower()
            provider_user_id = str(raw_subject.get("provider_user_id") or raw_subject.get("id") or "").strip()
            label = str(raw_subject.get("label") or raw_subject.get("username") or raw_subject.get("display_name") or provider_user_id or role)
        else:
            resolved_provider = str(provider or "").strip().lower()
            provider_user_id = str(raw_subject or "").strip()
            label = provider_user_id or role
        account_id = ModerationService._resolve_account_id(resolved_provider, provider_user_id, role=role)
        return {
            "provider": resolved_provider,
            "provider_user_id": provider_user_id,
            "account_id": account_id,
            "label": label,
        }

    @staticmethod
    def _human_violation_title(violation_type: dict[str, Any]) -> str:
        return str(violation_type.get("title") or violation_type.get("name") or violation_type.get("code") or "Нарушение")

    @staticmethod
    def _format_duration(minutes: int) -> str:
        total_minutes = max(0, int(minutes or 0))
        if total_minutes == 0:
            return "0 минут"
        days, rem = divmod(total_minutes, 24 * 60)
        hours, mins = divmod(rem, 60)
        parts: list[str] = []
        if days:
            parts.append(f"{days} дн.")
        if hours:
            parts.append(f"{hours} ч.")
        if mins:
            parts.append(f"{mins} мин.")
        return " ".join(parts)

    @staticmethod
    def _planned_actions(rule: dict[str, Any], warn_count_before: int) -> tuple[list[str], int, bool]:
        actions: list[str] = []
        warn_count_after = warn_count_before
        if ModerationService._is_truthy(rule.get("apply_warn")):
            actions.append(ModerationService.ACTION_WARN)
            warn_count_after += 1
        mute_minutes = int(rule.get("mute_minutes") or 0)
        if mute_minutes > 0:
            actions.append(ModerationService.ACTION_MUTE)
        fine_points = float(rule.get("fine_points") or 0)
        if fine_points > 0:
            actions.append(ModerationService.ACTION_FINE_POINTS)
        should_ban = ModerationService._is_truthy(rule.get("apply_ban")) or warn_count_after >= ModerationService.BAN_WARN_THRESHOLD
        if should_ban:
            actions.append(ModerationService.ACTION_BAN)
        return actions, warn_count_after, should_ban

    @staticmethod
    def _required_authority_action(actions: list[str]) -> str:
        moderation_actions = [item for item in actions if item in ModerationService._ACTION_PRIORITY]
        if not moderation_actions:
            return ModerationService.ACTION_MUTE
        return max(moderation_actions, key=lambda item: ModerationService._ACTION_PRIORITY[item])

    @staticmethod
    def _action_summary_lines(rule: dict[str, Any], warn_count_before: int) -> tuple[list[str], int, bool]:
        actions, warn_count_after, should_ban = ModerationService._planned_actions(rule, warn_count_before)
        lines: list[str] = []
        mute_minutes = int(rule.get("mute_minutes") or 0)
        fine_points = float(rule.get("fine_points") or 0)
        if ModerationService.ACTION_MUTE in actions and mute_minutes > 0:
            lines.append(f"мут {ModerationService._format_duration(mute_minutes)}")
        if ModerationService.ACTION_WARN in actions:
            lines.append("предупреждение")
        if ModerationService.ACTION_FINE_POINTS in actions and fine_points > 0:
            value = int(fine_points) if float(fine_points).is_integer() else fine_points
            lines.append(f"штраф {value} баллов")
        if ModerationService.ACTION_BAN in actions:
            lines.append("бан")
        return lines, warn_count_after, should_ban

    @staticmethod
    def _join_human_actions(lines: list[str]) -> str:
        if not lines:
            return "действие не назначено"
        if len(lines) == 1:
            return lines[0]
        return " + ".join(lines)

    @staticmethod
    def _next_step_explanation(next_rule: dict[str, Any] | None, warn_count_after: int) -> str:
        if not next_rule:
            return "Следующего шага эскалации пока нет в таблице правил. Если поведение повторится, обнови экран и проверь логи."
        next_lines, _, _ = ModerationService._action_summary_lines(next_rule, warn_count_after)
        return f"При следующем таком нарушении наказание усилится: {ModerationService._join_human_actions(next_lines)}."

    @staticmethod
    def _format_points_value(value: float | int) -> str:
        numeric = float(value or 0)
        return str(int(numeric)) if numeric.is_integer() else str(numeric)

    @staticmethod
    def _how_it_works_lines() -> list[str]:
        return [
            "• Наказание выбрано автоматически по типу нарушения и числу предупреждений.",
            "• Изменение вручную в этом сценарии не требуется.",
            "• Если наказание выглядит неверным — отмените и проверьте историю пользователя.",
        ]

    @staticmethod
    def _build_ui_payload(
        *,
        provider: str,
        actor_subject: dict[str, Any],
        target_subject: dict[str, Any],
        violation_type: dict[str, Any],
        rule: dict[str, Any],
        next_rule: dict[str, Any] | None,
        warn_count_before: int,
        authority: ModerationAuthorityDecision,
        context: dict[str, Any],
        case_id: Any | None = None,
    ) -> dict[str, Any]:
        action_lines, warn_count_after, should_ban = ModerationService._action_summary_lines(rule, warn_count_before)
        violation_title = ModerationService._human_violation_title(violation_type)
        next_step_text = ModerationService._next_step_explanation(next_rule, warn_count_after)
        selected_actions, _, _ = ModerationService._planned_actions(rule, warn_count_before)
        required_authority_action = ModerationService._required_authority_action(selected_actions)
        selected_action_summary = ModerationService._join_human_actions(action_lines)
        mute_minutes = int(rule.get("mute_minutes") or 0)
        fine_points = float(rule.get("fine_points") or 0)
        how_it_works_lines = ModerationService._how_it_works_lines()
        preview_lines = [
            f"👤 Нарушитель: {target_subject.get('label') or target_subject.get('provider_user_id') or 'неизвестно'}",
            f"📘 Нарушение: {violation_title}",
            f"⚠️ Предупреждений до применения: {warn_count_before}/{ModerationService.BAN_WARN_THRESHOLD}",
            f"🧮 Будет применено сейчас: {selected_action_summary}",
            f"📈 Предупреждений после применения: {warn_count_after}/{ModerationService.BAN_WARN_THRESHOLD}",
            f"⏭️ Следующий шаг: {next_step_text}",
        ]
        moderator_result_lines = [
            f"Причина: {violation_title}",
            f"Выдано сейчас: {selected_action_summary}",
            f"Предупреждений теперь: {warn_count_after}/{ModerationService.BAN_WARN_THRESHOLD}",
            next_step_text,
        ]
        history_hint = "Где посмотреть дальше: журнал moderation cases и активные наказания пользователя."
        return {
            "provider": provider,
            "target_label": target_subject.get("label"),
            "target_account_id": target_subject.get("account_id"),
            "target_provider_user_id": target_subject.get("provider_user_id"),
            "actor_account_id": actor_subject.get("account_id"),
            "actor_provider_user_id": actor_subject.get("provider_user_id"),
            "violation_code": str(violation_type.get("code") or ""),
            "violation_title": violation_title,
            "warn_count_before": warn_count_before,
            "warn_count_after": warn_count_after,
            "ban_threshold": ModerationService.BAN_WARN_THRESHOLD,
            "selected_actions": selected_actions,
            "selected_action_summary": selected_action_summary,
            "next_step_text": next_step_text,
            "preview_lines": preview_lines,
            "preview_text": "\n".join(preview_lines),
            "moderator_result_lines": moderator_result_lines,
            "moderator_result_text": "\n".join(moderator_result_lines),
            "violator_result_lines": [],
            "violator_result_text": "",
            "how_it_works_lines": how_it_works_lines,
            "how_it_works_text": "\n".join(how_it_works_lines),
            "history_hint": history_hint,
            "footer_hint": how_it_works_lines[-1].lstrip("• "),
            "required_authority_action": required_authority_action,
            "authority_allowed": authority.allowed,
            "authority_message": authority.message,
            "case_id": case_id,
            "rule_id": rule.get("id"),
            "escalation_step": ModerationService._rule_escalation_step(rule, warn_count_before),
            "context": dict(context),
            "ban_applied": should_ban,
            "mute_minutes": mute_minutes,
            "fine_points": fine_points,
        }

    @staticmethod
    def prepare_moderation_payload(
        provider: str,
        actor: Any,
        target: Any,
        violation_code: str,
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        context = dict(context or {})
        if not db.supabase:
            return {"ok": False, "error_code": "supabase_unavailable", "message": "Supabase не инициализирован."}

        actor_subject = ModerationService._resolve_subject(provider, actor, role="actor")
        target_subject = ModerationService._resolve_subject(provider, target, role="target")
        if not actor_subject.get("account_id") or not target_subject.get("account_id"):
            return {"ok": False, "error_code": "identity_unresolved", "message": "Не удалось определить аккаунт модератора или нарушителя."}

        violation_type = ModerationService._load_violation_type(violation_code)
        if not violation_type:
            return {"ok": False, "error_code": "violation_not_found", "message": "Тип нарушения не найден. Обнови экран и попробуй ещё раз."}

        warn_state_before = ModerationService._load_warn_state(str(target_subject["account_id"]))
        warn_count_before = ModerationService._current_warn_count(warn_state_before)
        rule = ModerationService._load_penalty_rule(violation_type["id"], warn_count_before)
        if not rule:
            return {"ok": False, "error_code": "rule_not_found", "message": "Не найдено правило наказания. Проверь логи и таблицу moderation_penalty_rules."}

        selected_actions, _, _ = ModerationService._planned_actions(rule, warn_count_before)
        requested_action = ModerationService._required_authority_action(selected_actions)
        if context.get("skip_authority"):
            authority = ModerationAuthorityDecision(
                allowed=True,
                deny_reason=None,
                message="Разрешено",
                actor_account_id=actor_subject.get("account_id"),
                target_account_id=target_subject.get("account_id"),
                actor_titles=tuple(),
                target_titles=tuple(),
                requested_action=requested_action,
            )
        else:
            authority = AuthorityService.can_apply_moderation_action(
                actor_subject["provider"],
                actor_subject["provider_user_id"],
                target_subject["provider"],
                target_subject["provider_user_id"],
                requested_action,
            )
        if not authority.allowed:
            return {
                "ok": False,
                "error_code": authority.deny_reason or "authority_denied",
                "message": authority.message,
                "authority": authority,
                "actor": actor_subject,
                "target": target_subject,
                "violation_code": str(violation_type.get("code") or ""),
                "selected_actions": selected_actions,
            }

        next_rule = ModerationService._load_penalty_rule(violation_type["id"], warn_count_before + 1)
        payload = ModerationService._build_ui_payload(
            provider=provider,
            actor_subject=actor_subject,
            target_subject=target_subject,
            violation_type=violation_type,
            rule=rule,
            next_rule=next_rule,
            warn_count_before=warn_count_before,
            authority=authority,
            context=context,
        )
        return {
            "ok": True,
            "actor": actor_subject,
            "target": target_subject,
            "violation_type": violation_type,
            "rule": rule,
            "next_rule": next_rule,
            "warn_count_before": warn_count_before,
            "warn_count_after": payload["warn_count_after"],
            "selected_actions": payload["selected_actions"],
            "authority": authority,
            "ui_payload": payload,
        }

    @staticmethod
    def _save_warn_state(
        *,
        account_id: str,
        violation_type_id: Any,
        warn_count_after: int,
        case_id: Any,
        updated_at: str,
    ) -> dict[str, Any]:
        payload = {
            "account_id": account_id,
            "active_warn_count": warn_count_after,
            "last_violation_type_id": violation_type_id,
            "last_case_id": case_id,
            "updated_at": updated_at,
        }
        existing_state = ModerationService._select_single("moderation_warn_state", account_id=account_id)
        if existing_state:
            updated_rows = ModerationService._update_rows(
                "moderation_warn_state",
                {"account_id": account_id},
                payload,
            )
            return updated_rows[0] if updated_rows else payload

        inserted = ModerationService._insert_row("moderation_warn_state", payload)
        return inserted or payload

    @staticmethod
    def _create_action(
        *,
        case_id: Any,
        action_type: str,
        value_numeric: float | int | None = None,
        value_text: str | None = None,
        starts_at: str | None = None,
        ends_at: str | None = None,
        created_at: str,
    ) -> Optional[dict]:
        payload = {
            "case_id": case_id,
            "action_type": action_type,
            "value_numeric": value_numeric,
            "value_text": value_text,
            "starts_at": starts_at,
            "ends_at": ends_at,
            "created_at": created_at,
        }
        return ModerationService._insert_row("moderation_actions", payload)

    @staticmethod
    def moderate(
        provider: str,
        actor: Any,
        target: Any,
        violation_code: str,
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        context = dict(context or {})
        preview = ModerationService.prepare_moderation_payload(provider, actor, target, violation_code, context)
        if not preview.get("ok"):
            return preview

        actor_subject = dict(preview["actor"])
        target_subject = dict(preview["target"])
        violation_type = dict(preview["violation_type"])
        rule = dict(preview["rule"])
        ui_payload = dict(preview["ui_payload"])
        warn_count_before = int(preview["warn_count_before"])
        warn_count_after = int(preview["warn_count_after"])
        authority: ModerationAuthorityDecision = preview["authority"]
        now = datetime.now(timezone.utc)
        created_at = now.isoformat()
        case_payload = {
            "account_id": target_subject["account_id"],
            "actor_account_id": actor_subject["account_id"],
            "violation_type_id": violation_type["id"],
            "penalty_rule_id": rule.get("id"),
            "escalation_step": ui_payload["escalation_step"],
            "source_platform": str(context.get("source_platform") or provider),
            "source_chat_id": str(context.get("chat_id") or context.get("source_chat_id") or "") or None,
            "reason_text": str(context.get("reason_text") or context.get("reason") or ""),
            "created_at": created_at,
        }
        moderation_case = ModerationService._insert_row("moderation_cases", case_payload)
        if not moderation_case:
            logger.error(
                "moderation apply violation aborted: failed to create case target_account_id=%s actor_account_id=%s violation_code=%s",
                target_subject["account_id"],
                actor_subject["account_id"],
                violation_code,
            )
            return {"ok": False, "error_code": "case_create_failed", "message": "Не удалось создать moderation-case. Проверь логи."}

        applied_actions: list[dict[str, Any]] = []

        if ModerationService.ACTION_WARN in ui_payload["selected_actions"]:
            warn_action = ModerationService._create_action(
                case_id=moderation_case["id"],
                action_type=ModerationService.ACTION_WARN,
                value_numeric=1,
                value_text=rule.get("description_for_admin") or str(context.get("reason_text") or context.get("reason") or violation_code),
                starts_at=created_at,
                created_at=created_at,
            )
            if warn_action:
                applied_actions.append(warn_action)
            ModerationService._save_warn_state(
                account_id=target_subject["account_id"],
                violation_type_id=violation_type["id"],
                warn_count_after=warn_count_after,
                case_id=moderation_case["id"],
                updated_at=created_at,
            )

        mute_minutes = int(rule.get("mute_minutes") or 0)
        if ModerationService.ACTION_MUTE in ui_payload["selected_actions"] and mute_minutes > 0:
            mute_ends_at = (now + timedelta(minutes=mute_minutes)).isoformat()
            mute_reason = str(context.get("reason_text") or context.get("reason") or rule.get("description_for_user") or violation_code)
            mute_row = ModerationService._insert_row(
                "moderation_mutes",
                {
                    "account_id": target_subject["account_id"],
                    "case_id": moderation_case["id"],
                    "reason_text": mute_reason,
                    "starts_at": created_at,
                    "ends_at": mute_ends_at,
                    "is_active": True,
                    "created_at": created_at,
                },
            )
            if mute_row:
                mute_action = ModerationService._create_action(
                    case_id=moderation_case["id"],
                    action_type=ModerationService.ACTION_MUTE,
                    value_numeric=mute_minutes,
                    value_text=mute_reason,
                    starts_at=created_at,
                    ends_at=mute_ends_at,
                    created_at=created_at,
                )
                if mute_action:
                    applied_actions.append(mute_action)

        fine_points = float(rule.get("fine_points") or 0)
        if ModerationService.ACTION_FINE_POINTS in ui_payload["selected_actions"] and fine_points > 0:
            fine_reason = f"Модерация кейс #{moderation_case['id']}: {context.get('reason_text') or context.get('reason') or violation_code}"
            if not db.add_action_by_account(target_subject["account_id"], -fine_points, fine_reason, actor_subject["account_id"]):
                logger.error(
                    "moderation fine_points apply failed case_id=%s account_id=%s fine_points=%s",
                    moderation_case["id"],
                    target_subject["account_id"],
                    fine_points,
                )
            fine_action = ModerationService._create_action(
                case_id=moderation_case["id"],
                action_type=ModerationService.ACTION_FINE_POINTS,
                value_numeric=fine_points,
                value_text=fine_reason,
                starts_at=created_at,
                created_at=created_at,
            )
            if fine_action:
                applied_actions.append(fine_action)

        if ModerationService.ACTION_BAN in ui_payload["selected_actions"]:
            ban_reason = str(context.get("reason_text") or context.get("reason") or rule.get("description_for_user") or violation_code)
            ban_row = ModerationService._insert_row(
                "moderation_bans",
                {
                    "account_id": target_subject["account_id"],
                    "case_id": moderation_case["id"],
                    "reason_text": ban_reason,
                    "starts_at": created_at,
                    "ends_at": None,
                    "is_active": True,
                    "created_at": created_at,
                },
            )
            if ban_row:
                ban_action = ModerationService._create_action(
                    case_id=moderation_case["id"],
                    action_type=ModerationService.ACTION_BAN,
                    value_numeric=None,
                    value_text=ban_reason,
                    starts_at=created_at,
                    created_at=created_at,
                )
                if ban_action:
                    applied_actions.append(ban_action)

        ui_payload["case_id"] = moderation_case.get("id")
        result_lines = [f"Кейс #{moderation_case.get('id')} создан"]
        if ModerationService.ACTION_MUTE in ui_payload["selected_actions"] and mute_minutes > 0:
            result_lines.append(f"Выдан мут на {ModerationService._format_duration(mute_minutes)}")
        if ModerationService.ACTION_WARN in ui_payload["selected_actions"]:
            result_lines.append(f"Добавлено предупреждение: {warn_count_after}/{ModerationService.BAN_WARN_THRESHOLD}")
        if ModerationService.ACTION_FINE_POINTS in ui_payload["selected_actions"] and fine_points > 0:
            result_lines.append(f"Списан штраф {ModerationService._format_points_value(fine_points)} баллов в банк")
        if ModerationService.ACTION_BAN in ui_payload["selected_actions"]:
            result_lines.append("Применён бан")
        result_lines.append(next_step_text if (next_step_text := str(ui_payload.get("next_step_text") or "").strip()) else "")
        ui_payload["moderator_result_lines"] = [line for line in result_lines if line]
        ui_payload["moderator_result_text"] = "\n".join(ui_payload["moderator_result_lines"])

        violator_lines = [
            f"Нарушение: {ui_payload.get('violation_title')}",
            f"Применено наказание: {ui_payload.get('selected_action_summary')}",
            f"Предупреждений теперь: {warn_count_after}/{ModerationService.BAN_WARN_THRESHOLD}",
        ]
        if ModerationService.ACTION_MUTE in ui_payload["selected_actions"] and mute_minutes > 0:
            violator_lines.append(f"Мут закончится: {(now + timedelta(minutes=mute_minutes)).strftime('%d.%m.%Y %H:%M UTC')}")
        violator_lines.append("Наказание выбирается автоматически по типу нарушения и числу предупреждений.")
        if next_step_text:
            violator_lines.append(next_step_text)
        violator_lines.append("Чтобы избежать следующего усиления, не повторяйте это нарушение и проверьте историю кейсов у модератора.")
        ui_payload["violator_result_lines"] = violator_lines
        ui_payload["violator_result_text"] = "\n".join(violator_lines)

        result = {
            "ok": True,
            "case": moderation_case,
            "rule": rule,
            "violation_type": violation_type,
            "warn_count_before": warn_count_before,
            "warn_count_after": warn_count_after,
            "actions": applied_actions,
            "selected_actions": ui_payload["selected_actions"],
            "ban_applied": ui_payload["ban_applied"],
            "authority": authority,
            "ui_payload": ui_payload,
            "actor": actor_subject,
            "target": target_subject,
        }
        logger.info(
            "moderation case created case_id=%s target_account_id=%s actor_account_id=%s violation_code=%s actions=%s warn_before=%s warn_after=%s ban_applied=%s",
            moderation_case.get("id"),
            target_subject.get("account_id"),
            actor_subject.get("account_id"),
            violation_code,
            list(ui_payload.get("selected_actions") or []),
            warn_count_before,
            warn_count_after,
            ui_payload.get("ban_applied"),
        )
        return result

    @staticmethod
    def apply_violation(
        provider: str,
        actor: str | int,
        target: str | int,
        violation_code: str,
        *,
        reason_text: str = "",
        source_platform: str | None = None,
        source_chat_id: str | int | None = None,
    ) -> Optional[dict[str, Any]]:
        result = ModerationService.moderate(
            provider,
            actor,
            target,
            violation_code,
            {
                "reason_text": reason_text,
                "source_platform": source_platform or provider,
                "source_chat_id": source_chat_id,
                "skip_authority": True,
            },
        )
        return result if result.get("ok") else None
