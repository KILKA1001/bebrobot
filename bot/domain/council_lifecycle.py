"""
Единые коды жизненного цикла для Совета.
Используются и в Telegram, и в Discord, чтобы исключить расхождения по статусам.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

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

COUNCIL_ROLE_VICE_COUNCIL_MEMBER = "vice_council_member"
COUNCIL_ROLE_COUNCIL_MEMBER = "council_member"
COUNCIL_ROLE_OBSERVER = "observer"

CANDIDATE_STATUS_PENDING = "pending"
CANDIDATE_STATUS_CONFIRMED = "confirmed"
CANDIDATE_STATUS_REJECTED = "rejected"
CANDIDATE_STATUS_WITHDRAWN = "withdrawn"
CANDIDATE_STATUS_VALUES: tuple[str, ...] = (
    CANDIDATE_STATUS_PENDING,
    CANDIDATE_STATUS_CONFIRMED,
    CANDIDATE_STATUS_REJECTED,
    CANDIDATE_STATUS_WITHDRAWN,
)

COUNCIL_MIN_VALID_BALLOTS = 3
COUNCIL_BALLOT_LIMITS_BY_ROLE: dict[str, int] = {
    COUNCIL_ROLE_VICE_COUNCIL_MEMBER: 1,
    COUNCIL_ROLE_COUNCIL_MEMBER: 2,
    COUNCIL_ROLE_OBSERVER: 1,
}


@dataclass(frozen=True)
class LaunchConfirmationDecision:
    accepted: bool
    launch_activated: bool
    event_should_be_saved: bool
    rejection_reason: str | None = None
    confirmed_by_role: str | None = None


@dataclass(frozen=True)
class CouncilInviteSegment:
    role_code: str
    segment_code: str
    required_titles: tuple[str, ...]
    requires_profile_application: bool = False


@dataclass(frozen=True)
class CandidateReviewDecision:
    accepted: bool
    next_status: str | None
    reason: str | None = None


@dataclass(frozen=True)
class ManualCandidateAddDecision:
    accepted: bool
    reason: str | None
    assignment_log: dict[str, object] | None = None


@dataclass(frozen=True)
class BallotSubmissionDecision:
    accepted: bool
    reason: str | None = None
    allowed_limit: int | None = None
    user_message: str | None = None
    remaining_votes: int | None = None


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


def build_election_invite_segments() -> tuple[CouncilInviteSegment, ...]:
    return (
        CouncilInviteSegment(
            role_code=COUNCIL_ROLE_VICE_COUNCIL_MEMBER,
            segment_code="vice_city_plus_main_vice",
            required_titles=("vice_city", "main_vice"),
        ),
        CouncilInviteSegment(
            role_code=COUNCIL_ROLE_COUNCIL_MEMBER,
            segment_code="veterans",
            required_titles=("veteran",),
        ),
        CouncilInviteSegment(
            role_code=COUNCIL_ROLE_OBSERVER,
            segment_code="profile_application",
            required_titles=(),
            requires_profile_application=True,
        ),
    )


def decide_candidate_review_action(
    *,
    current_status: str,
    action: str,
    candidate_profile_id: str,
    election_role_code: str,
    actor_profile_id: str,
    source_platform: str,
) -> CandidateReviewDecision:
    cleaned_current = (current_status or "").strip().lower()
    cleaned_action = (action or "").strip().lower()
    cleaned_candidate_id = (candidate_profile_id or "").strip()
    cleaned_role_code = (election_role_code or "").strip().lower()
    cleaned_actor_id = (actor_profile_id or "").strip()
    cleaned_platform = (source_platform or "").strip().lower() or "unknown"

    if cleaned_current not in CANDIDATE_STATUS_VALUES:
        logger.error(
            "Council candidate review rejected: invalid current status status=%s candidate_profile_id=%s role_code=%s actor_profile_id=%s source_platform=%s",
            cleaned_current,
            cleaned_candidate_id,
            cleaned_role_code,
            cleaned_actor_id,
            cleaned_platform,
        )
        return CandidateReviewDecision(accepted=False, next_status=None, reason="invalid_current_status")

    if cleaned_action == "confirm":
        if cleaned_current == CANDIDATE_STATUS_CONFIRMED:
            return CandidateReviewDecision(accepted=False, next_status=None, reason="already_confirmed")
        if cleaned_current in (CANDIDATE_STATUS_REJECTED, CANDIDATE_STATUS_WITHDRAWN):
            return CandidateReviewDecision(accepted=False, next_status=None, reason="immutable_terminal_status")
        return CandidateReviewDecision(accepted=True, next_status=CANDIDATE_STATUS_CONFIRMED)

    if cleaned_action == "reject":
        if cleaned_current == CANDIDATE_STATUS_REJECTED:
            return CandidateReviewDecision(accepted=False, next_status=None, reason="already_rejected")
        if cleaned_current == CANDIDATE_STATUS_WITHDRAWN:
            return CandidateReviewDecision(accepted=False, next_status=None, reason="immutable_terminal_status")
        return CandidateReviewDecision(accepted=True, next_status=CANDIDATE_STATUS_REJECTED)

    logger.error(
        "Council candidate review rejected: unsupported action action=%s candidate_profile_id=%s role_code=%s actor_profile_id=%s source_platform=%s",
        cleaned_action,
        cleaned_candidate_id,
        cleaned_role_code,
        cleaned_actor_id,
        cleaned_platform,
    )
    return CandidateReviewDecision(accepted=False, next_status=None, reason="unsupported_action")


def filter_confirmed_ballot_candidates(
    candidates: list[dict[str, object]] | tuple[dict[str, object], ...],
    *,
    election_id: int | None = None,
) -> list[dict[str, object]]:
    approved: list[dict[str, object]] = []
    for row in candidates:
        status = str((row or {}).get("status") or "").strip().lower()
        if status == CANDIDATE_STATUS_CONFIRMED:
            approved.append(dict(row))
            continue
        logger.warning(
            "Council ballot candidate excluded: non-confirmed status election_id=%s candidate_id=%s profile_id=%s status=%s role_code=%s",
            election_id,
            (row or {}).get("id"),
            (row or {}).get("profile_id"),
            status or "missing",
            (row or {}).get("role_code"),
        )
    return approved


def decide_manual_candidate_addition(
    *,
    term_id: int | None,
    election_status: str,
    candidate_profile_id: str,
    election_role_code: str,
    actor_profile_id: str,
    existing_candidates: list[dict[str, object]] | tuple[dict[str, object], ...],
    assigned_at: datetime | None = None,
) -> ManualCandidateAddDecision:
    cleaned_status = (election_status or "").strip().lower()
    cleaned_candidate_id = (candidate_profile_id or "").strip()
    cleaned_role_code = (election_role_code or "").strip().lower()
    cleaned_actor_id = (actor_profile_id or "").strip()

    if not isinstance(term_id, int) or term_id <= 0:
        logger.error(
            "Council manual candidate add rejected: invalid term_id=%s actor_profile_id=%s candidate_profile_id=%s role_code=%s",
            term_id,
            cleaned_actor_id,
            cleaned_candidate_id,
            cleaned_role_code,
        )
        return ManualCandidateAddDecision(accepted=False, reason="invalid_term_id")

    if cleaned_status not in ELECTION_STATUS_VALUES:
        logger.error(
            "Council manual candidate add rejected: invalid election status value status=%s term_id=%s actor_profile_id=%s role_code=%s",
            cleaned_status,
            term_id,
            cleaned_actor_id,
            cleaned_role_code,
        )
        return ManualCandidateAddDecision(accepted=False, reason="invalid_election_status_value")

    if cleaned_status not in (ELECTION_STATUS_NOMINATION, ELECTION_STATUS_VOTING):
        logger.error(
            "Council manual candidate add rejected: election status disallows manual add status=%s term_id=%s role_code=%s",
            cleaned_status,
            term_id,
            cleaned_role_code,
        )
        return ManualCandidateAddDecision(accepted=False, reason="election_status_not_open_for_manual_add")

    if not cleaned_candidate_id:
        logger.error(
            "Council manual candidate add rejected: empty candidate profile id term_id=%s role_code=%s actor_profile_id=%s",
            term_id,
            cleaned_role_code,
            cleaned_actor_id,
        )
        return ManualCandidateAddDecision(accepted=False, reason="empty_candidate_profile_id")

    if not cleaned_actor_id:
        logger.error(
            "Council manual candidate add rejected: empty actor profile id term_id=%s role_code=%s candidate_profile_id=%s",
            term_id,
            cleaned_role_code,
            cleaned_candidate_id,
        )
        return ManualCandidateAddDecision(accepted=False, reason="empty_actor_profile_id")

    if not cleaned_role_code:
        logger.error(
            "Council manual candidate add rejected: empty election role code term_id=%s actor_profile_id=%s candidate_profile_id=%s",
            term_id,
            cleaned_actor_id,
            cleaned_candidate_id,
        )
        return ManualCandidateAddDecision(accepted=False, reason="empty_election_role_code")

    for existing in existing_candidates:
        existing_term_id = existing.get("term_id")
        existing_role_code = str(existing.get("role_code") or "").strip().lower()
        existing_candidate = str(existing.get("profile_id") or "").strip()
        if existing_term_id == term_id and existing_role_code == cleaned_role_code and existing_candidate == cleaned_candidate_id:
            logger.error(
                "Council manual candidate add rejected: duplicate in term-role pool term_id=%s role_code=%s candidate_profile_id=%s actor_profile_id=%s",
                term_id,
                cleaned_role_code,
                cleaned_candidate_id,
                cleaned_actor_id,
            )
            return ManualCandidateAddDecision(accepted=False, reason="duplicate_candidate_for_role_term")

    assignment_dt = assigned_at or datetime.now(timezone.utc)
    assignment_log = {
        "term_id": term_id,
        "candidate_profile_id": cleaned_candidate_id,
        "election_role_code": cleaned_role_code,
        "assigned_by_profile_id": cleaned_actor_id,
        "assigned_at": assignment_dt.isoformat(),
    }
    logger.info(
        "Council manual candidate add accepted term_id=%s role_code=%s candidate_profile_id=%s assigned_by_profile_id=%s assigned_at=%s",
        term_id,
        cleaned_role_code,
        cleaned_candidate_id,
        cleaned_actor_id,
        assignment_log["assigned_at"],
    )
    return ManualCandidateAddDecision(accepted=True, reason=None, assignment_log=assignment_log)


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


def get_ballot_limit_for_role(role_code: str) -> int | None:
    cleaned_role = (role_code or "").strip().lower()
    return COUNCIL_BALLOT_LIMITS_BY_ROLE.get(cleaned_role)


def decide_ballot_submission(
    *,
    election_id: int | None,
    voter_profile_id: str,
    voter_role_code: str,
    selected_candidate_ids: list[int] | tuple[int, ...],
    already_submitted_ballots_count: int = 0,
) -> BallotSubmissionDecision:
    cleaned_profile_id = (voter_profile_id or "").strip()
    limit = get_ballot_limit_for_role(voter_role_code)
    selected_ids = [candidate_id for candidate_id in selected_candidate_ids if isinstance(candidate_id, int) and candidate_id > 0]
    unique_selected_ids = tuple(dict.fromkeys(selected_ids))
    invalid_selected_items_count = len(selected_candidate_ids) - len(selected_ids)

    if not isinstance(election_id, int) or election_id <= 0:
        logger.error("Council ballot rejected: invalid election id election_id=%s voter_profile_id=%s", election_id, cleaned_profile_id)
        return BallotSubmissionDecision(accepted=False, reason="invalid_election_id")

    if not cleaned_profile_id:
        logger.error("Council ballot rejected: empty voter profile id election_id=%s", election_id)
        return BallotSubmissionDecision(accepted=False, reason="empty_voter_profile_id")

    if limit is None:
        logger.error(
            "Council ballot rejected: unsupported voter role election_id=%s voter_profile_id=%s voter_role_code=%s",
            election_id,
            cleaned_profile_id,
            voter_role_code,
        )
        return BallotSubmissionDecision(accepted=False, reason="unsupported_voter_role")

    if not isinstance(already_submitted_ballots_count, int) or already_submitted_ballots_count < 0:
        logger.error(
            "Council ballot rejected: invalid already submitted count election_id=%s voter_profile_id=%s voter_role_code=%s already_submitted_ballots_count=%s",
            election_id,
            cleaned_profile_id,
            voter_role_code,
            already_submitted_ballots_count,
        )
        return BallotSubmissionDecision(
            accepted=False,
            reason="invalid_already_submitted_ballots_count",
            allowed_limit=limit,
        )

    if invalid_selected_items_count > 0:
        logger.error(
            "Council ballot rejected: invalid candidate ids election_id=%s voter_profile_id=%s voter_role_code=%s invalid_items_count=%s",
            election_id,
            cleaned_profile_id,
            voter_role_code,
            invalid_selected_items_count,
        )
        return BallotSubmissionDecision(
            accepted=False,
            reason="invalid_candidate_ids",
            allowed_limit=limit,
            user_message="В списке выбора есть некорректные кандидаты. Обновите бюллетень и попробуйте снова.",
        )

    if not unique_selected_ids:
        logger.error(
            "Council ballot rejected: empty candidate selection election_id=%s voter_profile_id=%s voter_role_code=%s",
            election_id,
            cleaned_profile_id,
            voter_role_code,
        )
        return BallotSubmissionDecision(accepted=False, reason="empty_candidate_selection", allowed_limit=limit)

    if len(unique_selected_ids) > limit:
        logger.error(
            "Council ballot rejected: limit exceeded election_id=%s voter_profile_id=%s voter_role_code=%s allowed_limit=%s selected_count=%s",
            election_id,
            cleaned_profile_id,
            voter_role_code,
            limit,
            len(unique_selected_ids),
        )
        return BallotSubmissionDecision(
            accepted=False,
            reason="ballot_limit_exceeded",
            allowed_limit=limit,
            user_message=f"Можно выбрать не более {limit} кандидатов. Уменьшите выбор и отправьте бюллетень снова.",
        )

    if already_submitted_ballots_count + len(unique_selected_ids) > limit:
        logger.error(
            "Council ballot rejected: cumulative limit exceeded election_id=%s voter_profile_id=%s voter_role_code=%s allowed_limit=%s already_submitted=%s selected_count=%s",
            election_id,
            cleaned_profile_id,
            voter_role_code,
            limit,
            already_submitted_ballots_count,
            len(unique_selected_ids),
        )
        return BallotSubmissionDecision(
            accepted=False,
            reason="ballot_limit_exceeded",
            allowed_limit=limit,
            user_message=f"Лимит голосов уже достигнут ({limit}). Новые голоса нельзя отправить.",
            remaining_votes=max(0, limit - already_submitted_ballots_count),
        )

    return BallotSubmissionDecision(
        accepted=True,
        allowed_limit=limit,
        remaining_votes=max(0, limit - already_submitted_ballots_count - len(unique_selected_ids)),
        user_message=f"Бюллетень принят. Осталось голосов: {max(0, limit - already_submitted_ballots_count - len(unique_selected_ids))}.",
    )


def is_election_valid_by_ballots(total_ballots_count: int, *, min_valid_ballots: int = COUNCIL_MIN_VALID_BALLOTS) -> bool:
    return int(total_ballots_count or 0) >= int(min_valid_ballots or 0)
