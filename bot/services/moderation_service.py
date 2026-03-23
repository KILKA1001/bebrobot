import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from bot.data import db
from .accounts_service import AccountsService


logger = logging.getLogger(__name__)


class ModerationService:
    """Account-first moderation service with rule-based penalties."""

    BAN_WARN_THRESHOLD = 5
    ACTION_WARN = "warn"
    ACTION_MUTE = "mute"
    ACTION_BAN = "ban"
    ACTION_FINE_POINTS = "fine_points"

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
    def _load_penalty_rule(violation_type_id: Any, warn_count_before: int) -> Optional[dict]:
        rules = ModerationService._select_many(
            "moderation_penalty_rules",
            violation_type_id=violation_type_id,
            is_active=True,
        )
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
        if not db.supabase:
            logger.error("moderation apply violation aborted: supabase is not initialized")
            return None

        actor_account_id = ModerationService._resolve_account_id(provider, actor, role="actor")
        target_account_id = ModerationService._resolve_account_id(provider, target, role="target")
        if not actor_account_id or not target_account_id:
            return None

        violation_type = ModerationService._load_violation_type(violation_code)
        if not violation_type:
            return None

        warn_state_before = ModerationService._load_warn_state(target_account_id)
        warn_count_before = ModerationService._current_warn_count(warn_state_before)
        penalty_rule = ModerationService._load_penalty_rule(violation_type["id"], warn_count_before)
        if not penalty_rule:
            return None

        now = datetime.now(timezone.utc)
        created_at = now.isoformat()
        escalation_step = ModerationService._rule_escalation_step(penalty_rule, warn_count_before)
        case_payload = {
            "account_id": target_account_id,
            "actor_account_id": actor_account_id,
            "violation_type_id": violation_type["id"],
            "penalty_rule_id": penalty_rule.get("id"),
            "escalation_step": escalation_step,
            "source_platform": source_platform or provider,
            "source_chat_id": str(source_chat_id) if source_chat_id is not None else None,
            "reason_text": reason_text,
            "created_at": created_at,
        }
        moderation_case = ModerationService._insert_row("moderation_cases", case_payload)
        if not moderation_case:
            logger.error(
                "moderation apply violation aborted: failed to create case target_account_id=%s actor_account_id=%s violation_code=%s",
                target_account_id,
                actor_account_id,
                violation_code,
            )
            return None

        applied_actions: list[dict[str, Any]] = []
        warn_count_after = warn_count_before

        if ModerationService._is_truthy(penalty_rule.get("apply_warn")):
            warn_count_after += 1
            warn_action = ModerationService._create_action(
                case_id=moderation_case["id"],
                action_type=ModerationService.ACTION_WARN,
                value_numeric=1,
                value_text=penalty_rule.get("description_for_admin") or reason_text or violation_code,
                starts_at=created_at,
                created_at=created_at,
            )
            if warn_action:
                applied_actions.append(warn_action)
            ModerationService._save_warn_state(
                account_id=target_account_id,
                violation_type_id=violation_type["id"],
                warn_count_after=warn_count_after,
                case_id=moderation_case["id"],
                updated_at=created_at,
            )

        mute_minutes = int(penalty_rule.get("mute_minutes") or 0)
        if mute_minutes > 0:
            mute_ends_at = (now + timedelta(minutes=mute_minutes)).isoformat()
            mute_reason = reason_text or penalty_rule.get("description_for_user") or violation_code
            mute_row = ModerationService._insert_row(
                "moderation_mutes",
                {
                    "account_id": target_account_id,
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

        fine_points = float(penalty_rule.get("fine_points") or 0)
        if fine_points > 0:
            fine_reason = f"Модерация кейс #{moderation_case['id']}: {reason_text or violation_code}"
            if not db.add_action_by_account(target_account_id, -fine_points, fine_reason, actor_account_id):
                logger.error(
                    "moderation fine_points apply failed case_id=%s account_id=%s fine_points=%s",
                    moderation_case["id"],
                    target_account_id,
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

        should_ban = ModerationService._is_truthy(penalty_rule.get("apply_ban")) or warn_count_after >= ModerationService.BAN_WARN_THRESHOLD
        if should_ban:
            ban_reason = reason_text or penalty_rule.get("description_for_user") or violation_code
            ban_row = ModerationService._insert_row(
                "moderation_bans",
                {
                    "account_id": target_account_id,
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

        result = {
            "case": moderation_case,
            "rule": penalty_rule,
            "violation_type": violation_type,
            "warn_count_before": warn_count_before,
            "warn_count_after": warn_count_after,
            "actions": applied_actions,
            "ban_applied": should_ban,
        }
        logger.info(
            "moderation case created case_id=%s target_account_id=%s actor_account_id=%s violation_code=%s actions=%s warn_before=%s warn_after=%s ban_applied=%s",
            moderation_case.get("id"),
            target_account_id,
            actor_account_id,
            violation_code,
            [action.get("action_type") for action in applied_actions],
            warn_count_before,
            warn_count_after,
            should_ban,
        )
        return result
