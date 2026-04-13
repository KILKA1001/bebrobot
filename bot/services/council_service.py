from __future__ import annotations

import logging
from dataclasses import dataclass

from bot.domain.council_lifecycle import (
    COUNCIL_MIN_VALID_BALLOTS,
    CANDIDATE_STATUS_VALUES,
    ELECTION_STATUS_VALUES,
    QUESTION_STATUS_VALUES,
    TERM_STATUS_VALUES,
    BallotSubmissionDecision,
    CandidateReviewDecision,
    CouncilInviteSegment,
    LaunchConfirmationDecision,
    ManualCandidateAddDecision,
    build_election_invite_segments,
    decide_ballot_submission,
    build_term_launch_notification_targets,
    decide_manual_candidate_addition,
    decide_candidate_review_action,
    decide_term_launch_confirmation,
    filter_confirmed_ballot_candidates,
    get_ballot_limit_for_role,
    is_election_valid_by_ballots,
    validate_council_text_length,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CouncilLifecycleSnapshot:
    term_statuses: tuple[str, ...]
    election_statuses: tuple[str, ...]
    question_statuses: tuple[str, ...]
    candidate_statuses: tuple[str, ...]


class CouncilService:
    """Единый сервисный модуль доменных правил Совета для всех платформенных адаптеров."""

    def get_lifecycle_snapshot(self) -> CouncilLifecycleSnapshot:
        return CouncilLifecycleSnapshot(
            term_statuses=TERM_STATUS_VALUES,
            election_statuses=ELECTION_STATUS_VALUES,
            question_statuses=QUESTION_STATUS_VALUES,
            candidate_statuses=CANDIDATE_STATUS_VALUES,
        )

    def is_valid_status(self, *, lifecycle: str, status: str) -> bool:
        normalized_lifecycle = (lifecycle or "").strip().lower()
        normalized_status = (status or "").strip().lower()

        if normalized_lifecycle == "term":
            return normalized_status in TERM_STATUS_VALUES
        if normalized_lifecycle == "election":
            return normalized_status in ELECTION_STATUS_VALUES
        if normalized_lifecycle == "question":
            return normalized_status in QUESTION_STATUS_VALUES

        logger.error(
            "CouncilService received unknown lifecycle lifecycle=%s status=%s",
            lifecycle,
            status,
        )
        return False

    def validate_text(self, *, field_name: str, text: str | None) -> tuple[bool, str | None]:
        return validate_council_text_length(text, field_name=field_name)

    def decide_launch_confirmation(
        self,
        *,
        term_status: str,
        actor_profile_id: str,
        actor_role_codes: tuple[str, ...] | list[str],
        existing_confirmed_profile_ids: tuple[str, ...] | list[str],
    ) -> LaunchConfirmationDecision:
        return decide_term_launch_confirmation(
            term_status=term_status,
            actor_profile_id=actor_profile_id,
            actor_role_codes=actor_role_codes,
            existing_confirmed_profile_ids=existing_confirmed_profile_ids,
        )

    def build_launch_notification_targets(
        self,
        *,
        head_club_profile_id: str | None,
        main_vice_profile_id: str | None,
    ) -> tuple[str, ...]:
        return build_term_launch_notification_targets(
            head_club_profile_id=head_club_profile_id,
            main_vice_profile_id=main_vice_profile_id,
        )

    def build_election_invite_segments(self) -> tuple[CouncilInviteSegment, ...]:
        return build_election_invite_segments()

    def decide_candidate_review_action(
        self,
        *,
        current_status: str,
        action: str,
        candidate_profile_id: str,
        election_role_code: str,
        actor_profile_id: str,
        source_platform: str,
    ) -> CandidateReviewDecision:
        return decide_candidate_review_action(
            current_status=current_status,
            action=action,
            candidate_profile_id=candidate_profile_id,
            election_role_code=election_role_code,
            actor_profile_id=actor_profile_id,
            source_platform=source_platform,
        )

    def filter_confirmed_ballot_candidates(
        self,
        candidates: list[dict[str, object]] | tuple[dict[str, object], ...],
        *,
        election_id: int | None = None,
    ) -> list[dict[str, object]]:
        return filter_confirmed_ballot_candidates(candidates, election_id=election_id)

    def decide_manual_candidate_addition(
        self,
        *,
        term_id: int | None,
        election_status: str,
        candidate_profile_id: str,
        election_role_code: str,
        actor_profile_id: str,
        existing_candidates: list[dict[str, object]] | tuple[dict[str, object], ...],
    ) -> ManualCandidateAddDecision:
        return decide_manual_candidate_addition(
            term_id=term_id,
            election_status=election_status,
            candidate_profile_id=candidate_profile_id,
            election_role_code=election_role_code,
            actor_profile_id=actor_profile_id,
            existing_candidates=existing_candidates,
        )

    def get_ballot_limit_for_role(self, *, role_code: str) -> int | None:
        return get_ballot_limit_for_role(role_code)

    def decide_ballot_submission(
        self,
        *,
        election_id: int | None,
        voter_profile_id: str,
        voter_role_code: str,
        selected_candidate_ids: list[int] | tuple[int, ...],
        already_submitted_ballots_count: int = 0,
    ) -> BallotSubmissionDecision:
        return decide_ballot_submission(
            election_id=election_id,
            voter_profile_id=voter_profile_id,
            voter_role_code=voter_role_code,
            selected_candidate_ids=selected_candidate_ids,
            already_submitted_ballots_count=already_submitted_ballots_count,
        )

    def is_election_valid_by_ballots(self, *, total_ballots_count: int, min_valid_ballots: int = COUNCIL_MIN_VALID_BALLOTS) -> bool:
        return is_election_valid_by_ballots(total_ballots_count, min_valid_ballots=min_valid_ballots)


council_service = CouncilService()
