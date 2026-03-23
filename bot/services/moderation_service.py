import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from uuid import uuid4

from bot.data import db

from .accounts_service import AccountsService
from .authority_service import AuthorityService, ModerationAuthorityDecision


logger = logging.getLogger(__name__)


class ModerationService:
    """Account-first moderation service with shared preview/apply payloads for all transports."""

    ACTION_WARN = "warn"
    ACTION_MUTE = "mute"
    ACTION_BAN = "ban"
    ACTION_KICK = "kick"
    ACTION_DEMOTION = "demotion"
    ACTION_FINE_POINTS = "fine_points"
    ACTION_BANK_INCOME = "bank_income"
    DEFAULT_WARN_TTL_MINUTES = 10 * 24 * 60
    _ACTION_PRIORITY = {
        ACTION_MUTE: 1,
        ACTION_KICK: 2,
        ACTION_WARN: 3,
        ACTION_BAN: 4,
        ACTION_DEMOTION: 5,
        ACTION_BANK_INCOME: 6,
    }
    STATUS_PENDING = "pending"
    STATUS_APPLIED = "applied"
    STATUS_FAILED = "failed"
    STATUS_ROLLED_BACK = "rolled_back"
    STATUS_DUPLICATE = "duplicate"
    FRIENDLY_ERROR_MESSAGE = (
        "Не удалось завершить кейс модерации. Действие не подтверждено.\n"
        "Попробуйте ещё раз позже.\n"
        "Подробности ошибки записаны в консоль."
    )

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
        if hasattr(db, "tables") and table not in getattr(db, "tables", {}):
            logger.warning("moderation select skipped missing fake table=%s filters=%s", table, filters)
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
        if hasattr(db, "tables") and table not in getattr(db, "tables", {}):
            logger.warning("moderation select many skipped missing fake table=%s filters=%s", table, filters)
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
    def _safe_int(value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _parse_dt(raw_value: Any) -> datetime | None:
        if not raw_value:
            return None
        try:
            parsed = datetime.fromisoformat(str(raw_value))
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed
        except ValueError:
            logger.warning("moderation invalid datetime raw_value=%s", raw_value)
            return None

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
        projected_state = ModerationService._select_single("moderation_warn_state", account_id=account_id) or {}
        now = datetime.now(timezone.utc)
        cases = ModerationService._select_many("moderation_cases", account_id=account_id)
        active_warn_count = 0
        has_prior_warns = ModerationService._is_truthy(projected_state.get("has_prior_warns"))
        for case_row in cases:
            if str(case_row.get("status") or "").strip().lower() != ModerationService.STATUS_APPLIED:
                continue
            for action_row in ModerationService._select_many("moderation_actions", case_id=case_row.get("id")):
                if str(action_row.get("action_type") or "").strip().lower() != ModerationService.ACTION_WARN:
                    continue
                has_prior_warns = True
                ends_at = ModerationService._parse_dt(action_row.get("ends_at"))
                if ends_at is not None and ends_at <= now:
                    continue
                active_warn_count += max(0, ModerationService._safe_int(action_row.get("value_numeric"), 1) or 1)

        if not cases and projected_state:
            active_warn_count = ModerationService._safe_int(
                projected_state.get("active_warn_count", projected_state.get("warn_count", 0)),
                active_warn_count,
            )

        mute_rows = ModerationService._select_many("moderation_mutes", account_id=account_id)
        has_prior_mutes = bool(mute_rows) or ModerationService._is_truthy(projected_state.get("has_prior_mutes"))
        state = dict(projected_state)
        state["active_warn_count"] = active_warn_count
        state["has_prior_warns"] = has_prior_warns
        state["has_prior_mutes"] = has_prior_mutes
        return state

    @staticmethod
    def _current_warn_count(state: dict[str, Any]) -> int:
        raw_value = state.get("active_warn_count", state.get("warn_count", 0))
        try:
            return max(0, int(raw_value or 0))
        except (TypeError, ValueError):
            logger.warning("moderation invalid warn count state=%s", state)
            return 0

    @staticmethod
    def _has_clean_record(state: dict[str, Any]) -> bool:
        return (
            ModerationService._current_warn_count(state) == 0
            and not ModerationService._is_truthy(state.get("has_prior_warns"))
            and not ModerationService._is_truthy(state.get("has_prior_mutes"))
        )

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
    def _rule_warn_count_before(rule: dict[str, Any]) -> int | None:
        raw_value = rule.get("warn_count_before")
        if raw_value is None:
            return None
        try:
            return max(0, int(raw_value))
        except (TypeError, ValueError):
            logger.warning("moderation invalid rule warn_count_before=%s rule_id=%s", raw_value, rule.get("id"))
            return None

    @staticmethod
    def _rule_warn_increment(rule: dict[str, Any]) -> int:
        raw_value = rule.get("warn_increment")
        if raw_value is not None:
            value = ModerationService._safe_int(raw_value, 0)
            if value < 0:
                logger.warning("moderation invalid negative warn_increment=%s rule_id=%s", raw_value, rule.get("id"))
                return 0
            return value
        return 1 if ModerationService._is_truthy(rule.get("apply_warn")) else 0

    @staticmethod
    def _rule_warn_ttl_minutes(rule: dict[str, Any]) -> int:
        raw_value = rule.get("warn_ttl_minutes")
        if raw_value is None:
            return ModerationService.DEFAULT_WARN_TTL_MINUTES
        value = ModerationService._safe_int(raw_value, ModerationService.DEFAULT_WARN_TTL_MINUTES)
        if value < 0:
            logger.warning("moderation invalid negative warn_ttl_minutes=%s rule_id=%s", raw_value, rule.get("id"))
            return ModerationService.DEFAULT_WARN_TTL_MINUTES
        return value

    @staticmethod
    def _rule_ban_minutes(rule: dict[str, Any]) -> int:
        raw_value = rule.get("ban_minutes")
        if raw_value is None:
            return 0
        value = ModerationService._safe_int(raw_value, 0)
        if value < 0:
            logger.warning("moderation invalid negative ban_minutes=%s rule_id=%s", raw_value, rule.get("id"))
            return 0
        return value

    @staticmethod
    def _rule_has_temporary_ban(rule: dict[str, Any]) -> bool:
        return ModerationService._rule_ban_minutes(rule) > 0

    @staticmethod
    def _rule_has_permanent_ban(rule: dict[str, Any]) -> bool:
        return ModerationService._is_truthy(rule.get("apply_permanent_ban")) or ModerationService._is_truthy(rule.get("apply_ban"))

    @staticmethod
    def _rule_only_if_clean_record(rule: dict[str, Any]) -> bool:
        return ModerationService._is_truthy(rule.get("only_if_clean_record"))

    @staticmethod
    def _load_penalty_rules(violation_type_id: Any) -> list[dict]:
        rules = ModerationService._select_many(
            "moderation_penalty_rules",
            violation_type_id=violation_type_id,
            is_active=True,
        )
        rules.sort(
            key=lambda row: (
                ModerationService._rule_escalation_step(row, 0),
                ModerationService._rule_warn_count_before(row) if ModerationService._rule_warn_count_before(row) is not None else 10**9,
                str(row.get("id") or ""),
            )
        )
        return rules

    @staticmethod
    def _load_penalty_rule(violation_type_id: Any, warn_count_before: int, *, is_clean_record: bool = False) -> Optional[dict]:
        rules = ModerationService._load_penalty_rules(violation_type_id)
        if not rules:
            logger.error(
                "moderation penalty rules not found violation_type_id=%s warn_count_before=%s",
                violation_type_id,
                warn_count_before,
            )
            return None

        exact_rules = [rule for rule in rules if ModerationService._rule_matches(rule, warn_count_before)]
        exact_match = None
        if is_clean_record:
            exact_match = next((rule for rule in exact_rules if ModerationService._rule_only_if_clean_record(rule)), None)
        if not exact_match:
            exact_match = next((rule for rule in exact_rules if not ModerationService._rule_only_if_clean_record(rule)), None)
        if exact_match:
            return exact_match

        candidate_rules = [
            rule
            for rule in rules
            if not ModerationService._rule_only_if_clean_record(rule) or is_clean_record
        ] or rules

        fallback_rule = max(
            candidate_rules,
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
    def _build_op_key(context: dict[str, Any], actor_subject: dict[str, Any], target_subject: dict[str, Any], violation_code: str) -> str:
        existing = str(context.get("moderation_op_key") or context.get("op_key") or "").strip()
        if existing:
            return existing
        return (
            f"rep:{actor_subject.get('account_id') or actor_subject.get('provider_user_id')}:"
            f"{target_subject.get('account_id') or target_subject.get('provider_user_id')}:"
            f"{str(violation_code or '').strip().lower()}:{uuid4()}"
        )

    @staticmethod
    def _serialize_exception(exc: Exception | None) -> str | None:
        return None if exc is None else str(exc)

    @staticmethod
    def _log_case_event(
        level: str,
        *,
        message: str,
        provider: str,
        chat_id: Any,
        actor_account_id: str | None,
        target_account_id: str | None,
        violation_code: str | None,
        requested_action_set: list[str] | None,
        selected_rule_id: Any,
        case_id: Any,
        op_key: str | None,
        status: str | None,
        error_code: str | None,
        rollback_status: str | None,
        step: str | None = None,
    ) -> None:
        log_method = getattr(logger, level)
        log_method(
            (
                "%s provider=%s chat_id=%s actor_account_id=%s target_account_id=%s violation_code=%s "
                "requested_action_set=%s selected_rule_id=%s case_id=%s op_key=%s status=%s error_code=%s "
                "rollback_status=%s step=%s"
            ),
            message,
            provider,
            chat_id,
            actor_account_id,
            target_account_id,
            violation_code,
            list(requested_action_set or []),
            selected_rule_id,
            case_id,
            op_key,
            status,
            error_code,
            rollback_status,
            step,
        )

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
        warn_increment = ModerationService._rule_warn_increment(rule)
        if warn_increment > 0:
            actions.append(ModerationService.ACTION_WARN)
            warn_count_after += warn_increment
        mute_minutes = int(rule.get("mute_minutes") or 0)
        if mute_minutes > 0:
            actions.append(ModerationService.ACTION_MUTE)
        if ModerationService._is_truthy(rule.get("apply_kick")):
            actions.append(ModerationService.ACTION_KICK)
        fine_points = float(rule.get("fine_points") or 0)
        if fine_points > 0:
            actions.append(ModerationService.ACTION_FINE_POINTS)
        should_ban = ModerationService._rule_has_temporary_ban(rule) or ModerationService._rule_has_permanent_ban(rule)
        if should_ban:
            actions.append(ModerationService.ACTION_BAN)
        if ModerationService._is_truthy(rule.get("apply_demotion")):
            actions.append(ModerationService.ACTION_DEMOTION)
        return actions, warn_count_after, should_ban

    @staticmethod
    def _required_authority_action(actions: list[str]) -> str:
        if ModerationService.ACTION_DEMOTION in actions or ModerationService.ACTION_BAN in actions:
            return ModerationService.ACTION_BAN
        if ModerationService.ACTION_KICK in actions:
            return ModerationService.ACTION_MUTE
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
        warn_increment = ModerationService._rule_warn_increment(rule)
        warn_ttl_minutes = ModerationService._rule_warn_ttl_minutes(rule)
        ban_minutes = ModerationService._rule_ban_minutes(rule)
        if ModerationService.ACTION_MUTE in actions and mute_minutes > 0:
            lines.append(f"мут {ModerationService._format_duration(mute_minutes)}")
        if ModerationService.ACTION_WARN in actions and warn_increment > 0:
            warn_title = "предупреждение" if warn_increment == 1 else f"предупреждения ×{warn_increment}"
            if warn_ttl_minutes > 0:
                warn_title = f"{warn_title} на {ModerationService._format_duration(warn_ttl_minutes)}"
            lines.append(warn_title)
        if ModerationService.ACTION_KICK in actions:
            lines.append("кик")
        if ModerationService.ACTION_FINE_POINTS in actions and fine_points > 0:
            value = int(fine_points) if float(fine_points).is_integer() else fine_points
            lines.append(f"штраф {value} баллов")
        if ModerationService.ACTION_BAN in actions:
            if ModerationService._rule_has_permanent_ban(rule):
                lines.append("перманентный бан")
            elif ban_minutes > 0:
                lines.append(f"бан {ModerationService._format_duration(ban_minutes)}")
            else:
                lines.append("бан")
        if ModerationService.ACTION_DEMOTION in actions:
            lines.append("понижение")
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
    def _warn_limit_from_rules(rules: list[dict[str, Any]]) -> int | None:
        limits: list[int] = []
        for rule in rules:
            if not (ModerationService._rule_has_temporary_ban(rule) or ModerationService._rule_has_permanent_ban(rule)):
                continue
            warn_before = ModerationService._rule_warn_count_before(rule)
            if warn_before is None:
                warn_before = max(0, ModerationService._rule_escalation_step(rule, 0) - 1)
            warn_after = warn_before + ModerationService._rule_warn_increment(rule)
            limits.append(max(0, warn_after))
        return min(limits) if limits else None

    @staticmethod
    def _warn_progress_text(*, warn_count: int, warn_limit: int | None, suffix: str) -> str:
        if warn_limit is None:
            return f"{warn_count} ({suffix}: бан не настроен в таблице эскалации)"
        return f"{warn_count}/{warn_limit}"

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
        all_rules: list[dict[str, Any]],
        warn_count_before: int,
        authority: ModerationAuthorityDecision,
        context: dict[str, Any],
        case_id: Any | None = None,
        moderation_op_key: str | None = None,
    ) -> dict[str, Any]:
        action_lines, warn_count_after, should_ban = ModerationService._action_summary_lines(rule, warn_count_before)
        violation_title = ModerationService._human_violation_title(violation_type)
        next_step_text = ModerationService._next_step_explanation(next_rule, warn_count_after)
        warn_limit = ModerationService._warn_limit_from_rules(all_rules)
        warn_before_text = ModerationService._warn_progress_text(
            warn_count=warn_count_before,
            warn_limit=warn_limit,
            suffix="до применения",
        )
        warn_after_text = ModerationService._warn_progress_text(
            warn_count=warn_count_after,
            warn_limit=warn_limit,
            suffix="после применения",
        )
        selected_actions, _, _ = ModerationService._planned_actions(rule, warn_count_before)
        required_authority_action = ModerationService._required_authority_action(selected_actions)
        selected_action_summary = ModerationService._join_human_actions(action_lines)
        warn_increment = ModerationService._rule_warn_increment(rule)
        warn_ttl_minutes = ModerationService._rule_warn_ttl_minutes(rule)
        mute_minutes = int(rule.get("mute_minutes") or 0)
        ban_minutes = ModerationService._rule_ban_minutes(rule)
        permanent_ban = ModerationService._rule_has_permanent_ban(rule)
        fine_points = float(rule.get("fine_points") or 0)
        how_it_works_lines = ModerationService._how_it_works_lines()
        if ModerationService._rule_only_if_clean_record(rule):
            how_it_works_lines.append("• Для этого кейса сработало мягкое правило первого чистого проступка: сначала только предупреждение.")
        preview_lines = [
            f"👤 Нарушитель: {target_subject.get('label') or target_subject.get('provider_user_id') or 'неизвестно'}",
            f"📘 Нарушение: {violation_title}",
            f"⚠️ Предупреждений до применения: {warn_before_text}",
            f"🧮 Будет применено сейчас: {selected_action_summary}",
            f"📈 Предупреждений после применения: {warn_after_text}",
            f"⏭️ Следующий шаг: {next_step_text}",
        ]
        moderator_result_lines = [
            f"Причина: {violation_title}",
            f"Выдано сейчас: {selected_action_summary}",
            f"Предупреждений теперь: {warn_after_text}",
            next_step_text,
        ]
        history_hint = "Историю кейсов, активные наказания, историю нарушений и списания в банк по кейсу смотри в журнале moderation cases."
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
            "warn_count_before_text": warn_before_text,
            "warn_count_after_text": warn_after_text,
            "ban_threshold": warn_limit,
            "warn_increment": warn_increment,
            "warn_ttl_minutes": warn_ttl_minutes,
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
            "moderation_op_key": moderation_op_key,
            "rule_id": rule.get("id"),
            "escalation_step": ModerationService._rule_escalation_step(rule, warn_count_before),
            "context": dict(context),
            "ban_applied": should_ban,
            "permanent_ban": permanent_ban,
            "ban_minutes": ban_minutes,
            "kick_applied": ModerationService.ACTION_KICK in selected_actions,
            "demotion_applied": ModerationService.ACTION_DEMOTION in selected_actions,
            "soft_warning_only": ModerationService._rule_only_if_clean_record(rule),
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
        if str(violation_type.get("subject_scope") or "all").strip().lower() == "admins_only":
            target_authority = AuthorityService.resolve_authority(target_subject["provider"], target_subject["provider_user_id"])
            if target_authority.level < 30:
                logger.warning(
                    "moderation violation scope mismatch code=%s target_account_id=%s scope=admins_only",
                    violation_type.get("code"),
                    target_subject.get("account_id"),
                )
                return {
                    "ok": False,
                    "error_code": "scope_mismatch",
                    "message": "Это нарушение применяется только к администраторам.",
                }

        warn_state_before = ModerationService._load_warn_state(str(target_subject["account_id"]))
        warn_count_before = ModerationService._current_warn_count(warn_state_before)
        is_clean_record = ModerationService._has_clean_record(warn_state_before)
        all_rules = ModerationService._load_penalty_rules(violation_type["id"])
        rule = ModerationService._load_penalty_rule(violation_type["id"], warn_count_before, is_clean_record=is_clean_record)
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

        next_rule = ModerationService._load_penalty_rule(violation_type["id"], warn_count_before + ModerationService._rule_warn_increment(rule))
        moderation_op_key = ModerationService._build_op_key(context, actor_subject, target_subject, str(violation_type.get("code") or violation_code))
        payload = ModerationService._build_ui_payload(
            provider=provider,
            actor_subject=actor_subject,
            target_subject=target_subject,
            violation_type=violation_type,
            rule=rule,
            next_rule=next_rule,
            all_rules=all_rules,
            warn_count_before=warn_count_before,
            authority=authority,
            context=context,
            moderation_op_key=moderation_op_key,
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
            "moderation_op_key": moderation_op_key,
        }

    @staticmethod
    def _save_warn_state(
        *,
        account_id: str,
        violation_type_id: Any,
        warn_count_after: int,
        case_id: Any,
        updated_at: str,
        has_prior_warns: bool = True,
        has_prior_mutes: bool = False,
    ) -> dict[str, Any]:
        payload = {
            "account_id": account_id,
            "active_warn_count": warn_count_after,
            "last_violation_type_id": violation_type_id,
            "last_case_id": case_id,
            "updated_at": updated_at,
            "last_warn_refresh_at": updated_at,
            "has_prior_warns": has_prior_warns,
            "has_prior_mutes": has_prior_mutes,
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
    def _remember_mute_history(account_id: str, updated_at: str) -> dict[str, Any]:
        existing_state = ModerationService._select_single("moderation_warn_state", account_id=account_id) or {}
        payload = {
            "account_id": account_id,
            "active_warn_count": ModerationService._current_warn_count(existing_state),
            "last_violation_type_id": existing_state.get("last_violation_type_id"),
            "last_case_id": existing_state.get("last_case_id"),
            "updated_at": updated_at,
            "last_warn_refresh_at": updated_at,
            "has_prior_warns": ModerationService._is_truthy(existing_state.get("has_prior_warns")),
            "has_prior_mutes": True,
        }
        if existing_state:
            updated_rows = ModerationService._update_rows("moderation_warn_state", {"account_id": account_id}, payload)
            return updated_rows[0] if updated_rows else payload
        inserted = ModerationService._insert_row("moderation_warn_state", payload)
        return inserted or payload

    @staticmethod
    def _create_action(
        *,
        case_id: Any,
        action_type: str,
        op_key: str | None = None,
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
            "op_key": op_key,
        }
        return ModerationService._insert_row("moderation_actions", payload)

    @staticmethod
    def _build_result(
        *,
        ok: bool,
        provider: str,
        actor_subject: dict[str, Any] | None,
        target_subject: dict[str, Any] | None,
        violation_code: str,
        selected_actions: list[str],
        op_key: str | None,
        status: str,
        error_code: str | None,
        user_message: str,
        moderator_message: str,
        case_row: dict[str, Any] | None = None,
        ui_payload: dict[str, Any] | None = None,
        rule: dict[str, Any] | None = None,
        violation_type: dict[str, Any] | None = None,
        warnings_before: int | None = None,
        warnings_after: int | None = None,
        applied_actions: list[dict[str, Any]] | None = None,
        mute_until: str | None = None,
        ban_applied: bool = False,
        fine_points_applied: float | int = 0,
        authority: ModerationAuthorityDecision | None = None,
        rollback_status: str | None = None,
        case_status: str | None = None,
    ) -> dict[str, Any]:
        payload = dict(ui_payload or {})
        if op_key and not payload.get("moderation_op_key"):
            payload["moderation_op_key"] = op_key
        if case_row and case_row.get("id") and not payload.get("case_id"):
            payload["case_id"] = case_row.get("id")
        return {
            "ok": ok,
            "provider": provider,
            "case_id": case_row.get("id") if case_row else None,
            "case": case_row,
            "applied_actions": list(applied_actions or []),
            "actions": list(applied_actions or []),
            "warnings_before": warnings_before,
            "warn_count_before": warnings_before,
            "warnings_after": warnings_after,
            "warn_count_after": warnings_after,
            "mute_until": mute_until,
            "ban_applied": ban_applied,
            "fine_points_applied": fine_points_applied,
            "op_key": op_key,
            "moderation_op_key": op_key,
            "status": status,
            "case_status": case_status or status,
            "error_code": error_code,
            "message": user_message,
            "user_message": user_message,
            "moderator_message": moderator_message,
            "selected_actions": list(selected_actions or []),
            "rule": rule,
            "violation_type": violation_type,
            "authority": authority,
            "ui_payload": payload,
            "actor": actor_subject or {},
            "target": target_subject or {},
            "rollback_status": rollback_status,
        }

    @staticmethod
    def _rollback_case(
        *,
        provider: str,
        chat_id: Any,
        actor_subject: dict[str, Any],
        target_subject: dict[str, Any],
        violation_code: str,
        selected_actions: list[str],
        selected_rule_id: Any,
        case_row: dict[str, Any] | None,
        op_key: str,
        warn_state_before: dict[str, Any],
        warn_changed: bool,
        mute_row: dict[str, Any] | None,
        ban_row: dict[str, Any] | None,
        fine_points: float,
        fine_applied: bool,
        bank_income_applied: bool,
        completed_steps: list[str],
    ) -> tuple[str, list[str], list[str]]:
        rolled_back: list[str] = []
        dirty_state: list[str] = []
        rollback_status = ModerationService.STATUS_ROLLED_BACK

        if bank_income_applied and fine_points > 0:
            rollback_bank_reason = f"Rollback bank income for moderation case #{case_row.get('id') if case_row else 'unknown'} op_key={op_key}"
            if db.add_to_bank(-fine_points):
                rolled_back.append("bank_income_apply")
                logger.info(
                    "✅ moderation rollback bank income reverted case_id=%s op_key=%s amount=%s",
                    case_row.get("id") if case_row else None,
                    op_key,
                    fine_points,
                )
            else:
                dirty_state.append("bank_income_apply")
                logger.error(
                    "❌ moderation rollback bank income revert failed case_id=%s op_key=%s amount=%s reason=%s",
                    case_row.get("id") if case_row else None,
                    op_key,
                    fine_points,
                    rollback_bank_reason,
                )

        if fine_applied and fine_points > 0:
            rollback_reason = f"Rollback moderation case #{case_row.get('id') if case_row else 'unknown'}"
            if db.add_action_by_account(target_subject["account_id"], fine_points, rollback_reason, actor_subject["account_id"], is_undo=True, op_key=f"{op_key}:rollback:fine"):
                rolled_back.append("fine_apply")
            else:
                dirty_state.append("fine_apply")

        if ban_row:
            updated = ModerationService._update_rows(
                "moderation_bans",
                {"id": ban_row.get("id")},
                {"is_active": False, "revoked_at": datetime.now(timezone.utc).isoformat(), "rollback_op_key": op_key},
            )
            if updated:
                rolled_back.append("ban_apply")
            else:
                dirty_state.append("ban_apply")

        if mute_row:
            updated = ModerationService._update_rows(
                "moderation_mutes",
                {"id": mute_row.get("id")},
                {"is_active": False, "revoked_at": datetime.now(timezone.utc).isoformat(), "rollback_op_key": op_key},
            )
            if updated:
                rolled_back.append("mute_apply")
            else:
                dirty_state.append("mute_apply")

        if warn_changed:
            warn_payload = {
                "active_warn_count": ModerationService._current_warn_count(warn_state_before),
                "last_violation_type_id": warn_state_before.get("last_violation_type_id"),
                "last_case_id": warn_state_before.get("last_case_id"),
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "rollback_op_key": op_key,
                "has_prior_warns": ModerationService._is_truthy(warn_state_before.get("has_prior_warns")),
                "has_prior_mutes": ModerationService._is_truthy(warn_state_before.get("has_prior_mutes")),
            }
            updated = ModerationService._update_rows("moderation_warn_state", {"account_id": target_subject["account_id"]}, warn_payload)
            if updated:
                rolled_back.append("warn_update")
            else:
                dirty_state.append("warn_update")

        if case_row:
            case_status = ModerationService.STATUS_ROLLED_BACK if not dirty_state else ModerationService.STATUS_FAILED
            updated_case = ModerationService._update_rows(
                "moderation_cases",
                {"id": case_row.get("id")},
                {
                    "status": case_status,
                    "rollback_status": "ok" if not dirty_state else "manual_review_required",
                    "rollback_steps": ", ".join(rolled_back),
                    "dirty_steps": ", ".join(dirty_state),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            if not updated_case:
                dirty_state.append("finalize_case")

        if dirty_state:
            rollback_status = "manual_review_required"

        ModerationService._log_case_event(
            "error" if dirty_state else "warning",
            message="moderation rollback completed" if not dirty_state else "moderation rollback requires manual review",
            provider=provider,
            chat_id=chat_id,
            actor_account_id=actor_subject.get("account_id"),
            target_account_id=target_subject.get("account_id"),
            violation_code=violation_code,
            requested_action_set=selected_actions,
            selected_rule_id=selected_rule_id,
            case_id=case_row.get("id") if case_row else None,
            op_key=op_key,
            status=ModerationService.STATUS_ROLLED_BACK if not dirty_state else ModerationService.STATUS_FAILED,
            error_code=None if not dirty_state else "rollback_incomplete",
            rollback_status=rollback_status,
            step="rollback",
        )
        logger.error(
            "moderation rollback details op_key=%s completed_steps=%s rolled_back=%s dirty_state=%s manual_review_required=%s",
            op_key,
            completed_steps,
            rolled_back,
            dirty_state,
            bool(dirty_state),
        )
        return rollback_status, rolled_back, dirty_state

    @staticmethod
    def commit_case(
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
        moderation_op_key = str(preview.get("moderation_op_key") or ui_payload.get("moderation_op_key") or ModerationService._build_op_key(context, actor_subject, target_subject, violation_code))
        ui_payload["moderation_op_key"] = moderation_op_key
        selected_actions = list(ui_payload.get("selected_actions") or [])
        chat_id = context.get("chat_id") or context.get("source_chat_id")
        existing_case = ModerationService._select_single("moderation_cases", op_key=moderation_op_key)
        if existing_case:
            existing_status = str(existing_case.get("status") or "").strip().lower() or ModerationService.STATUS_APPLIED
            existing_action_rows = ModerationService._select_many("moderation_actions", case_id=existing_case.get("id"))
            result_ui_payload = dict(ui_payload)
            result_ui_payload["case_id"] = existing_case.get("id")
            result_ui_payload["moderation_op_key"] = moderation_op_key
            duplicate_message = "Кейс уже был подтверждён ранее. Повторное применение пропущено."
            ModerationService._log_case_event(
                "warning",
                message="moderation duplicate submit ignored",
                provider=provider,
                chat_id=chat_id,
                actor_account_id=actor_subject.get("account_id"),
                target_account_id=target_subject.get("account_id"),
                violation_code=str(violation_type.get("code") or violation_code),
                requested_action_set=selected_actions,
                selected_rule_id=rule.get("id"),
                case_id=existing_case.get("id"),
                op_key=moderation_op_key,
                status=ModerationService.STATUS_DUPLICATE,
                error_code="duplicate_submit",
                rollback_status=existing_case.get("rollback_status"),
                step="case_insert",
            )
            return ModerationService._build_result(
                ok=True,
                provider=provider,
                actor_subject=actor_subject,
                target_subject=target_subject,
                violation_code=str(violation_type.get("code") or violation_code),
                selected_actions=selected_actions,
                op_key=moderation_op_key,
                status=ModerationService.STATUS_DUPLICATE,
                error_code=None,
                user_message=duplicate_message,
                moderator_message=duplicate_message,
                case_row=existing_case,
                ui_payload=result_ui_payload,
                rule=rule,
                violation_type=violation_type,
                warnings_before=warn_count_before,
                warnings_after=ModerationService._current_warn_count(ModerationService._load_warn_state(target_subject["account_id"])),
                applied_actions=existing_action_rows,
                mute_until=None,
                ban_applied=ModerationService.ACTION_BAN in selected_actions,
                fine_points_applied=float(rule.get("fine_points") or 0) if ModerationService.ACTION_FINE_POINTS in selected_actions else 0,
                authority=authority,
                rollback_status=existing_case.get("rollback_status"),
                case_status=existing_status,
            )

        now = datetime.now(timezone.utc)
        created_at = now.isoformat()
        warn_state_before = ModerationService._load_warn_state(str(target_subject["account_id"]))
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
            "op_key": moderation_op_key,
            "status": ModerationService.STATUS_PENDING,
        }
        moderation_case = None
        applied_actions: list[dict[str, Any]] = []
        mute_row = None
        ban_row = None
        mute_until = None
        ban_until = None
        fine_points = float(rule.get("fine_points") or 0)
        warn_increment = ModerationService._rule_warn_increment(rule)
        warn_ttl_minutes = ModerationService._rule_warn_ttl_minutes(rule)
        ban_minutes = ModerationService._rule_ban_minutes(rule)
        permanent_ban = ModerationService._rule_has_permanent_ban(rule)
        fine_applied = False
        bank_income_applied = False
        warn_changed = False
        completed_steps: list[str] = []
        current_step = "authority_check"
        rollback_status = "not_required"
        try:
            ModerationService._log_case_event(
                "info",
                message="moderation case apply started",
                provider=provider,
                chat_id=chat_id,
                actor_account_id=actor_subject.get("account_id"),
                target_account_id=target_subject.get("account_id"),
                violation_code=str(violation_type.get("code") or violation_code),
                requested_action_set=selected_actions,
                selected_rule_id=rule.get("id"),
                case_id=None,
                op_key=moderation_op_key,
                status=ModerationService.STATUS_PENDING,
                error_code=None,
                rollback_status=rollback_status,
                step=current_step,
            )
            completed_steps.append(current_step)
            current_step = "rule_selection"
            completed_steps.append(current_step)

            current_step = "case_insert"
            moderation_case = ModerationService._insert_row("moderation_cases", case_payload)
            if not moderation_case:
                raise RuntimeError("Не удалось создать moderation-case")
            ui_payload["case_id"] = moderation_case.get("id")
            completed_steps.append(current_step)

            if ModerationService.ACTION_WARN in selected_actions:
                current_step = "create_moderation_actions"
                warn_expires_at = (now + timedelta(minutes=warn_ttl_minutes)).isoformat() if warn_ttl_minutes > 0 else None
                warn_action = ModerationService._create_action(
                    case_id=moderation_case["id"],
                    action_type=ModerationService.ACTION_WARN,
                    op_key=moderation_op_key,
                    value_numeric=warn_increment,
                    value_text=rule.get("description_for_admin") or str(context.get("reason_text") or context.get("reason") or violation_code),
                    starts_at=created_at,
                    ends_at=warn_expires_at,
                    created_at=created_at,
                )
                if not warn_action:
                    raise RuntimeError("Не удалось создать warn action")
                applied_actions.append(warn_action)
                completed_steps.append(current_step)

                current_step = "warn_update"
                warn_state = ModerationService._save_warn_state(
                    account_id=target_subject["account_id"],
                    violation_type_id=violation_type["id"],
                    warn_count_after=warn_count_after,
                    case_id=moderation_case["id"],
                    updated_at=created_at,
                    has_prior_warns=True,
                    has_prior_mutes=ModerationService._is_truthy(warn_state_before.get("has_prior_mutes")),
                )
                if not warn_state:
                    raise RuntimeError("Не удалось обновить состояние предупреждений")
                warn_changed = True
                completed_steps.append(current_step)

            mute_minutes = int(rule.get("mute_minutes") or 0)
            if ModerationService.ACTION_MUTE in selected_actions and mute_minutes > 0:
                current_step = "mute_apply"
                mute_until = (now + timedelta(minutes=mute_minutes)).isoformat()
                mute_reason = str(context.get("reason_text") or context.get("reason") or rule.get("description_for_user") or violation_code)
                mute_row = ModerationService._insert_row(
                    "moderation_mutes",
                    {
                        "account_id": target_subject["account_id"],
                        "case_id": moderation_case["id"],
                        "reason_text": mute_reason,
                        "starts_at": created_at,
                        "ends_at": mute_until,
                        "is_active": True,
                        "created_at": created_at,
                        "op_key": moderation_op_key,
                    },
                )
                if not mute_row:
                    raise RuntimeError("Не удалось применить мут")
                mute_action = ModerationService._create_action(
                    case_id=moderation_case["id"],
                    action_type=ModerationService.ACTION_MUTE,
                    op_key=moderation_op_key,
                    value_numeric=mute_minutes,
                    value_text=mute_reason,
                    starts_at=created_at,
                    ends_at=mute_until,
                    created_at=created_at,
                )
                if not mute_action:
                    raise RuntimeError("Не удалось создать mute action")
                applied_actions.append(mute_action)
                ModerationService._remember_mute_history(target_subject["account_id"], created_at)
                completed_steps.append(current_step)

            if ModerationService.ACTION_BAN in selected_actions:
                current_step = "ban_apply"
                ban_reason = str(context.get("reason_text") or context.get("reason") or rule.get("description_for_user") or violation_code)
                ban_until = (now + timedelta(minutes=ban_minutes)).isoformat() if ban_minutes > 0 else None
                ban_row = ModerationService._insert_row(
                    "moderation_bans",
                    {
                        "account_id": target_subject["account_id"],
                        "case_id": moderation_case["id"],
                        "reason_text": ban_reason,
                        "starts_at": created_at,
                        "ends_at": ban_until,
                        "is_active": True,
                        "created_at": created_at,
                        "op_key": moderation_op_key,
                    },
                )
                if not ban_row:
                    raise RuntimeError("Не удалось применить бан")
                ban_action = ModerationService._create_action(
                    case_id=moderation_case["id"],
                    action_type=ModerationService.ACTION_BAN,
                    op_key=moderation_op_key,
                    value_numeric=ban_minutes if ban_minutes > 0 else None,
                    value_text=ban_reason,
                    starts_at=created_at,
                    ends_at=ban_until,
                    created_at=created_at,
                )
                if not ban_action:
                    raise RuntimeError("Не удалось создать ban action")
                applied_actions.append(ban_action)
                completed_steps.append(current_step)

            if ModerationService.ACTION_KICK in selected_actions:
                current_step = "kick_apply"
                kick_action = ModerationService._create_action(
                    case_id=moderation_case["id"],
                    action_type=ModerationService.ACTION_KICK,
                    op_key=moderation_op_key,
                    value_text=str(context.get("reason_text") or context.get("reason") or rule.get("description_for_user") or violation_code),
                    starts_at=created_at,
                    created_at=created_at,
                )
                if not kick_action:
                    raise RuntimeError("Не удалось создать kick action")
                applied_actions.append(kick_action)
                completed_steps.append(current_step)

            if ModerationService.ACTION_DEMOTION in selected_actions:
                current_step = "demotion_apply"
                demotion_action = ModerationService._create_action(
                    case_id=moderation_case["id"],
                    action_type=ModerationService.ACTION_DEMOTION,
                    op_key=moderation_op_key,
                    value_text=str(context.get("reason_text") or context.get("reason") or rule.get("description_for_user") or violation_code),
                    starts_at=created_at,
                    created_at=created_at,
                )
                if not demotion_action:
                    raise RuntimeError("Не удалось создать demotion action")
                applied_actions.append(demotion_action)
                completed_steps.append(current_step)

            if ModerationService.ACTION_FINE_POINTS in selected_actions and fine_points > 0:
                current_step = "fine_apply"
                fine_reason = f"Модерация кейс #{moderation_case['id']}: {context.get('reason_text') or context.get('reason') or violation_code}"
                bank_reason = f"Поступление штрафа moderation case #{moderation_case['id']} op_key={moderation_op_key}: {context.get('reason_text') or context.get('reason') or violation_code}"
                if not db.add_action_by_account(
                    target_subject["account_id"],
                    -fine_points,
                    fine_reason,
                    actor_subject["account_id"],
                    op_key=f"{moderation_op_key}:fine_points",
                ):
                    logger.error(
                        "❌ moderation fine apply failed case_id=%s op_key=%s target_account_id=%s amount=%s",
                        moderation_case["id"],
                        moderation_op_key,
                        target_subject["account_id"],
                        fine_points,
                    )
                    raise RuntimeError("Не удалось применить денежный штраф")
                fine_applied = True
                fine_action = ModerationService._create_action(
                    case_id=moderation_case["id"],
                    action_type=ModerationService.ACTION_FINE_POINTS,
                    op_key=moderation_op_key,
                    value_numeric=fine_points,
                    value_text=fine_reason,
                    starts_at=created_at,
                    created_at=created_at,
                )
                if not fine_action:
                    logger.error(
                        "❌ moderation fine action create failed case_id=%s op_key=%s amount=%s",
                        moderation_case["id"],
                        moderation_op_key,
                        fine_points,
                    )
                    raise RuntimeError("Не удалось создать fine_points action")
                applied_actions.append(fine_action)
                completed_steps.append(current_step)

                current_step = "bank_income_apply"
                if not db.add_to_bank(fine_points):
                    logger.error(
                        "❌ moderation bank income apply failed case_id=%s op_key=%s amount=%s",
                        moderation_case["id"],
                        moderation_op_key,
                        fine_points,
                    )
                    raise RuntimeError("Не удалось зачислить штраф в банк")
                bank_income_applied = True
                completed_steps.append(current_step)

                current_step = "bank_income_log"
                log_bank_income_by_account = getattr(db, "log_bank_income_by_account", None)
                bank_logged = False
                if callable(log_bank_income_by_account):
                    bank_logged = bool(log_bank_income_by_account(target_subject["account_id"], fine_points, bank_reason))
                else:
                    provider_user_id = target_subject.get("provider_user_id")
                    if provider_user_id is not None:
                        try:
                            bank_logged = bool(db.log_bank_income(int(provider_user_id), fine_points, bank_reason))
                        except (TypeError, ValueError):
                            logger.error(
                                "❌ moderation bank income fallback log failed invalid provider_user_id case_id=%s op_key=%s provider_user_id=%s",
                                moderation_case["id"],
                                moderation_op_key,
                                provider_user_id,
                            )
                if not bank_logged:
                    logger.error(
                        "❌ moderation bank income log failed case_id=%s op_key=%s target_account_id=%s amount=%s",
                        moderation_case["id"],
                        moderation_op_key,
                        target_subject["account_id"],
                        fine_points,
                    )
                    raise RuntimeError("Не удалось записать поступление штрафа в банк")
                completed_steps.append(current_step)

                current_step = "bank_income_action"
                bank_action = ModerationService._create_action(
                    case_id=moderation_case["id"],
                    action_type=ModerationService.ACTION_BANK_INCOME,
                    op_key=moderation_op_key,
                    value_numeric=fine_points,
                    value_text=bank_reason,
                    starts_at=created_at,
                    created_at=created_at,
                )
                if not bank_action:
                    logger.error(
                        "❌ moderation bank income action create failed case_id=%s op_key=%s amount=%s",
                        moderation_case["id"],
                        moderation_op_key,
                        fine_points,
                    )
                    raise RuntimeError("Не удалось привязать поступление штрафа к истории кейса")
                applied_actions.append(bank_action)
                completed_steps.append(current_step)

            current_step = "finalize_case"
            finalized_case_rows = ModerationService._update_rows(
                "moderation_cases",
                {"id": moderation_case["id"]},
                {
                    "status": ModerationService.STATUS_APPLIED,
                    "applied_actions": ", ".join(selected_actions),
                    "warnings_before": warn_count_before,
                    "warnings_after": warn_count_after,
                    "mute_until": mute_until,
                    "ban_applied": ModerationService.ACTION_BAN in selected_actions,
                    "ban_until": ban_until,
                    "fine_points_applied": fine_points if fine_applied else 0,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            if not finalized_case_rows:
                raise RuntimeError("Не удалось зафиксировать итоговый статус кейса")
            moderation_case = finalized_case_rows[0]
            completed_steps.append(current_step)
        except Exception as exc:
            error_code = f"{current_step}_failed"
            ModerationService._log_case_event(
                "exception",
                message="moderation case apply failed",
                provider=provider,
                chat_id=chat_id,
                actor_account_id=actor_subject.get("account_id"),
                target_account_id=target_subject.get("account_id"),
                violation_code=str(violation_type.get("code") or violation_code),
                requested_action_set=selected_actions,
                selected_rule_id=rule.get("id"),
                case_id=moderation_case.get("id") if moderation_case else None,
                op_key=moderation_op_key,
                status=ModerationService.STATUS_FAILED,
                error_code=error_code,
                rollback_status="started",
                step=current_step,
            )
            rollback_status, rolled_back_steps, dirty_state = ModerationService._rollback_case(
                provider=provider,
                chat_id=chat_id,
                actor_subject=actor_subject,
                target_subject=target_subject,
                violation_code=str(violation_type.get("code") or violation_code),
                selected_actions=selected_actions,
                selected_rule_id=rule.get("id"),
                case_row=moderation_case,
                op_key=moderation_op_key,
                warn_state_before=warn_state_before,
                warn_changed=warn_changed,
                mute_row=mute_row,
                ban_row=ban_row,
                fine_points=fine_points,
                fine_applied=fine_applied,
                bank_income_applied=bank_income_applied,
                completed_steps=completed_steps,
            )
            moderator_message = (
                f"Кейс модерации не завершён. Шаг сбоя: {current_step}. "
                f"Rollback: {rollback_status}. Успешные шаги: {', '.join(completed_steps) or 'нет'}."
            )
            logger.error(
                "moderation case apply exception step=%s op_key=%s error=%s rolled_back=%s dirty_state=%s",
                current_step,
                moderation_op_key,
                exc,
                rolled_back_steps,
                dirty_state,
            )
            return ModerationService._build_result(
                ok=False,
                provider=provider,
                actor_subject=actor_subject,
                target_subject=target_subject,
                violation_code=str(violation_type.get("code") or violation_code),
                selected_actions=selected_actions,
                op_key=moderation_op_key,
                status=ModerationService.STATUS_FAILED,
                error_code=error_code,
                user_message=ModerationService.FRIENDLY_ERROR_MESSAGE,
                moderator_message=moderator_message,
                case_row=moderation_case,
                ui_payload=ui_payload,
                rule=rule,
                violation_type=violation_type,
                warnings_before=warn_count_before,
                warnings_after=ModerationService._current_warn_count(ModerationService._load_warn_state(target_subject["account_id"])),
                applied_actions=applied_actions,
                mute_until=mute_until,
                ban_applied=bool(ban_row),
                fine_points_applied=fine_points if fine_applied else 0,
                authority=authority,
                rollback_status=rollback_status,
            )

        result_lines = [f"Кейс #{moderation_case.get('id')} создан"]
        mute_minutes = int(rule.get("mute_minutes") or 0)
        ban_minutes = ModerationService._rule_ban_minutes(rule)
        permanent_ban = ModerationService._rule_has_permanent_ban(rule)
        warn_increment = ModerationService._rule_warn_increment(rule)
        warn_ttl_minutes = ModerationService._rule_warn_ttl_minutes(rule)
        if ModerationService.ACTION_MUTE in selected_actions and mute_minutes > 0:
            result_lines.append(f"Выдан мут на {ModerationService._format_duration(mute_minutes)}")
        if ModerationService.ACTION_WARN in selected_actions:
            warn_label = "Добавлено предупреждение" if warn_increment == 1 else f"Добавлено предупреждений: {warn_increment}"
            if warn_ttl_minutes > 0:
                warn_label = f"{warn_label} (срок {ModerationService._format_duration(warn_ttl_minutes)})"
            result_lines.append(f"{warn_label}. Активных теперь: {ui_payload.get('warn_count_after_text') or warn_count_after}")
        if ModerationService.ACTION_KICK in selected_actions:
            result_lines.append("Зафиксирован кик")
        if ModerationService.ACTION_FINE_POINTS in selected_actions and fine_points > 0:
            result_lines.append(f"Списан штраф {ModerationService._format_points_value(fine_points)} баллов в банк")
        if ModerationService.ACTION_BAN in selected_actions:
            if permanent_ban:
                result_lines.append("Применён перманентный бан")
            elif ban_minutes > 0:
                result_lines.append(f"Применён бан на {ModerationService._format_duration(ban_minutes)}")
            else:
                result_lines.append("Применён бан")
        if ModerationService.ACTION_DEMOTION in selected_actions:
            result_lines.append("Зафиксировано понижение")
        result_lines.append(next_step_text if (next_step_text := str(ui_payload.get("next_step_text") or "").strip()) else "")
        ui_payload["moderator_result_lines"] = [line for line in result_lines if line]
        ui_payload["moderator_result_text"] = "\n".join(ui_payload["moderator_result_lines"])

        violator_lines = [
            f"Нарушение: {ui_payload.get('violation_title')}",
            f"Применено наказание: {ui_payload.get('selected_action_summary')}",
            f"Предупреждений теперь: {ui_payload.get('warn_count_after_text') or warn_count_after}",
        ]
        if ModerationService.ACTION_MUTE in selected_actions and mute_minutes > 0:
            violator_lines.append(f"Мут закончится: {datetime.fromisoformat(mute_until).strftime('%d.%m.%Y %H:%M UTC') if mute_until else (now + timedelta(minutes=mute_minutes)).strftime('%d.%m.%Y %H:%M UTC')}")
        if ModerationService.ACTION_BAN in selected_actions and not permanent_ban and ban_until:
            violator_lines.append(f"Бан закончится: {datetime.fromisoformat(ban_until).strftime('%d.%m.%Y %H:%M UTC')}")
        if ModerationService.ACTION_BAN in selected_actions and permanent_ban:
            violator_lines.append("Бан является бессрочным.")
        violator_lines.append("Наказание выбирается автоматически по типу нарушения и числу предупреждений.")
        if next_step_text:
            violator_lines.append(next_step_text)
        violator_lines.append("Чтобы избежать следующего усиления, не повторяйте это нарушение и при необходимости запросите у модератора историю кейсов, активные наказания и текущий счётчик предупреждений.")
        ui_payload["violator_result_lines"] = violator_lines
        ui_payload["violator_result_text"] = "\n".join(violator_lines)
        moderator_message = f"Кейс #{moderation_case.get('id')} успешно применён."
        ModerationService._log_case_event(
            "info",
            message="moderation case apply success",
            provider=provider,
            chat_id=chat_id,
            actor_account_id=actor_subject.get("account_id"),
            target_account_id=target_subject.get("account_id"),
            violation_code=str(violation_type.get("code") or violation_code),
            requested_action_set=selected_actions,
            selected_rule_id=rule.get("id"),
            case_id=moderation_case.get("id"),
            op_key=moderation_op_key,
            status=ModerationService.STATUS_APPLIED,
            error_code=None,
            rollback_status=rollback_status,
            step="finalize_case",
        )
        return ModerationService._build_result(
            ok=True,
            provider=provider,
            actor_subject=actor_subject,
            target_subject=target_subject,
            violation_code=str(violation_type.get("code") or violation_code),
            selected_actions=selected_actions,
            op_key=moderation_op_key,
            status=ModerationService.STATUS_APPLIED,
            error_code=None,
            user_message="Кейс модерации успешно подтверждён.",
            moderator_message=moderator_message,
            case_row=moderation_case,
            ui_payload=ui_payload,
            rule=rule,
            violation_type=violation_type,
            warnings_before=warn_count_before,
            warnings_after=warn_count_after,
            applied_actions=applied_actions,
            mute_until=mute_until,
            ban_applied=ui_payload["ban_applied"],
            fine_points_applied=fine_points if fine_applied else 0,
            authority=authority,
            rollback_status=rollback_status,
        )

    @staticmethod
    def moderate(
        provider: str,
        actor: Any,
        target: Any,
        violation_code: str,
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return ModerationService.commit_case(provider, actor, target, violation_code, context)

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
