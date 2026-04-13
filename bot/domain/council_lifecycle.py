"""
Единые коды жизненного цикла для Совета.
Используются и в Telegram, и в Discord, чтобы исключить расхождения по статусам.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Созыв (term) lifecycle.
TERM_STATUS_DRAFT = "draft"
TERM_STATUS_PENDING_LAUNCH_CONFIRMATION = "pending_launch_confirmation"
TERM_STATUS_ACTIVE = "active"
TERM_STATUS_ARCHIVED = "archived"
TERM_STATUS_CANCELLED = "cancelled"
TERM_STATUS_VALUES: tuple[str, ...] = (
    TERM_STATUS_DRAFT,
    TERM_STATUS_PENDING_LAUNCH_CONFIRMATION,
    TERM_STATUS_ACTIVE,
    TERM_STATUS_ARCHIVED,
    TERM_STATUS_CANCELLED,
)

# Выборы (election) lifecycle.
ELECTION_STATUS_DRAFT = "draft"
ELECTION_STATUS_NOMINATION = "nomination"
ELECTION_STATUS_VOTING = "voting"
ELECTION_STATUS_COMPLETED = "completed"
ELECTION_STATUS_CANCELLED = "cancelled"
ELECTION_STATUS_VALUES: tuple[str, ...] = (
    ELECTION_STATUS_DRAFT,
    ELECTION_STATUS_NOMINATION,
    ELECTION_STATUS_VOTING,
    ELECTION_STATUS_COMPLETED,
    ELECTION_STATUS_CANCELLED,
)

# Вопрос (question) lifecycle.
QUESTION_STATUS_DRAFT = "draft"
QUESTION_STATUS_DISCUSSION = "discussion"
QUESTION_STATUS_VOTING = "voting"
QUESTION_STATUS_DECIDED = "decided"
QUESTION_STATUS_ARCHIVED = "archived"
QUESTION_STATUS_VALUES: tuple[str, ...] = (
    QUESTION_STATUS_DRAFT,
    QUESTION_STATUS_DISCUSSION,
    QUESTION_STATUS_VOTING,
    QUESTION_STATUS_DECIDED,
    QUESTION_STATUS_ARCHIVED,
)

MAX_COUNCIL_TEXT_LEN = 1000
TERM_LAUNCH_ALLOWED_CONFIRM_ROLES: tuple[str, ...] = ("head_club", "main_vice")
TERM_LAUNCH_ALLOWED_CONFIRM_ROLES_SET = set(TERM_LAUNCH_ALLOWED_CONFIRM_ROLES)


@dataclass(frozen=True)
class LaunchConfirmationDecision:
    accepted: bool
    launch_activated: bool
    event_should_be_saved: bool
    rejection_reason: str | None = None
    confirmed_by_role: str | None = None


def decide_term_launch_confirmation(
    *,
    term_status: str,
    actor_profile_id: str,
    actor_role_codes: tuple[str, ...] | list[str],
    existing_confirmed_profile_ids: tuple[str, ...] | list[str],
) -> LaunchConfirmationDecision:
    cleaned_status = (term_status or "").strip().lower()
    actor_id = (actor_profile_id or "").strip()
    actor_roles = {str(role or "").strip().lower() for role in actor_role_codes}
    allowed_roles = actor_roles.intersection(TERM_LAUNCH_ALLOWED_CONFIRM_ROLES_SET)

    if cleaned_status not in (TERM_STATUS_PENDING_LAUNCH_CONFIRMATION, TERM_STATUS_ACTIVE):
        logger.error(
            "Council launch confirmation rejected: invalid term status term_status=%s actor_profile_id=%s",
            cleaned_status,
            actor_id,
        )
        return LaunchConfirmationDecision(
            accepted=False,
            launch_activated=False,
            event_should_be_saved=False,
            rejection_reason="invalid_term_status",
        )

    if not actor_id:
        logger.error("Council launch confirmation rejected: empty actor profile id")
        return LaunchConfirmationDecision(
            accepted=False,
            launch_activated=False,
            event_should_be_saved=False,
            rejection_reason="empty_actor_profile_id",
        )

    if not allowed_roles:
        logger.error(
            "Council launch confirmation rejected: actor role is not allowed actor_profile_id=%s actor_roles=%s",
            actor_id,
            sorted(actor_roles),
        )
        return LaunchConfirmationDecision(
            accepted=False,
            launch_activated=False,
            event_should_be_saved=False,
            rejection_reason="role_not_allowed",
        )

    normalized_existing = {str(profile_id or "").strip() for profile_id in existing_confirmed_profile_ids}
    if actor_id in normalized_existing:
        logger.error(
            "Council launch confirmation rejected: duplicate confirmation actor_profile_id=%s term_status=%s",
            actor_id,
            cleaned_status,
        )
        return LaunchConfirmationDecision(
            accepted=False,
            launch_activated=False,
            event_should_be_saved=False,
            rejection_reason="duplicate_confirmation",
        )

    has_any_valid_confirmation = bool(normalized_existing)
    launch_activated = (not has_any_valid_confirmation) and cleaned_status == TERM_STATUS_PENDING_LAUNCH_CONFIRMATION
    confirmed_by_role = "head_club" if "head_club" in allowed_roles else "main_vice"

    logger.info(
        "Council launch confirmation accepted actor_profile_id=%s confirmed_by_role=%s launch_activated=%s term_status=%s",
        actor_id,
        confirmed_by_role,
        launch_activated,
        cleaned_status,
    )
    return LaunchConfirmationDecision(
        accepted=True,
        launch_activated=launch_activated,
        event_should_be_saved=True,
        confirmed_by_role=confirmed_by_role,
    )


def build_term_launch_notification_targets(
    *,
    head_club_profile_id: str | None,
    main_vice_profile_id: str | None,
) -> tuple[str, ...]:
    request_targets: list[str] = []
    for profile_id in (head_club_profile_id, main_vice_profile_id):
        cleaned = (profile_id or "").strip()
        if cleaned and cleaned not in request_targets:
            request_targets.append(cleaned)
    return tuple(request_targets)


def is_valid_lifecycle_status(status: str, *, lifecycle: str) -> bool:
    value = (status or "").strip().lower()
    if lifecycle == "term":
        return value in TERM_STATUS_VALUES
    if lifecycle == "election":
        return value in ELECTION_STATUS_VALUES
    if lifecycle == "question":
        return value in QUESTION_STATUS_VALUES
    logger.error("Unknown lifecycle passed to is_valid_lifecycle_status lifecycle=%s", lifecycle)
    return False


def validate_council_text_length(text: str | None, *, field_name: str) -> tuple[bool, str | None]:
    cleaned = (text or "").strip()
    if len(cleaned) <= MAX_COUNCIL_TEXT_LEN:
        return True, None
    logger.error(
        "Council text is too long field=%s actual_len=%s max_len=%s",
        field_name,
        len(cleaned),
        MAX_COUNCIL_TEXT_LEN,
    )
    return False, f"Текст поля «{field_name}» должен быть не длиннее {MAX_COUNCIL_TEXT_LEN} символов."
