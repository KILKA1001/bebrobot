from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime

from bot.domain.council_lifecycle import (
    COUNCIL_MIN_VALID_BALLOTS,
    CANDIDATE_STATUS_VALUES,
    ElectionRoundResolution,
    ElectionSchedulerAction,
    ElectionStatusPublication,
    ActiveVotingQuorumSnapshot,
    QuestionArchiveDecision,
    QuestionModerationDecision,
    QuestionVotingTransitionDecision,
    QuestionVoteSubmissionDecision,
    ELECTION_STATUS_VALUES,
    QUESTION_STATUS_VALUES,
    TERM_STATUS_VALUES,
    BallotSubmissionDecision,
    CandidateReviewDecision,
    InviteDeadlineDecision,
    CouncilInviteSegment,
    LaunchConfirmationDecision,
    ManualCandidateAddDecision,
    ReplacementAssignmentDecision,
    TermMemberExitDecision,
    build_active_voting_quorum_snapshot,
    build_election_invite_segments,
    decide_ballot_submission,
    build_election_status_publication,
    build_term_launch_notification_targets,
    decide_question_moderation_approval,
    decide_question_start_voting,
    decide_question_vote_submission,
    resolve_question_voting_for_archive,
    decide_manual_candidate_addition,
    decide_candidate_review_action,
    compute_candidate_invite_expires_at,
    resolve_candidate_invite_deadline,
    decide_replacement_assignment,
    decide_term_member_exit,
    decide_term_launch_confirmation,
    filter_confirmed_ballot_candidates,
    get_ballot_limit_for_role,
    is_election_valid_by_ballots,
    plan_election_deadline_jobs,
    resolve_election_round_on_deadline,
    validate_council_text_length,
)

from bot.services.council_pause_service import CouncilPauseService
from bot.services.role_management_service import RoleManagementService
from bot.data import db

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CouncilLifecycleSnapshot:
    term_statuses: tuple[str, ...]
    election_statuses: tuple[str, ...]
    question_statuses: tuple[str, ...]
    candidate_statuses: tuple[str, ...]


@dataclass(frozen=True)
class CouncilDiscordRolesConfig:
    vice_council_role_id: int | None
    council_member_role_id: int | None
    observer_role_id: int | None
    grant_scenario_enabled: bool
    missing_required_keys: tuple[str, ...]


class CouncilService:
    """Единый сервисный модуль доменных правил Совета для всех платформенных адаптеров."""

    def __init__(self) -> None:
        self._discord_roles_config = self._load_discord_roles_config()

    def _load_discord_roles_config(self) -> CouncilDiscordRolesConfig:
        if not db.supabase:
            logger.error(
                "council profile title config unavailable: supabase is not configured; scenario=council_term_formation_role_grant will be blocked"
            )
            return CouncilDiscordRolesConfig(
                vice_council_role_id=None,
                council_member_role_id=None,
                observer_role_id=None,
                grant_scenario_enabled=False,
                missing_required_keys=("profile_title_roles:Вице Советчанин", "profile_title_roles:Советчанин"),
            )

        title_to_role_id: dict[str, int] = {}
        try:
            response = (
                db.supabase.table("profile_title_roles")
                .select("discord_role_id,title_name,is_active")
                .eq("is_active", True)
                .execute()
            )
            for row in response.data or []:
                title_name = str(row.get("title_name") or "").strip().lower()
                discord_role_id_raw = row.get("discord_role_id")
                if not title_name or not discord_role_id_raw:
                    continue
                try:
                    discord_role_id = int(discord_role_id_raw)
                except (TypeError, ValueError):
                    logger.error(
                        "council profile title config invalid discord_role_id title_name=%s discord_role_id=%s",
                        title_name,
                        discord_role_id_raw,
                    )
                    continue
                if discord_role_id <= 0:
                    logger.error(
                        "council profile title config non-positive discord_role_id title_name=%s discord_role_id=%s",
                        title_name,
                        discord_role_id_raw,
                    )
                    continue
                if title_name not in title_to_role_id:
                    title_to_role_id[title_name] = discord_role_id
        except Exception:
            logger.exception(
                "council profile title config failed to load table=profile_title_roles; scenario=council_term_formation_role_grant will be blocked"
            )
            return CouncilDiscordRolesConfig(
                vice_council_role_id=None,
                council_member_role_id=None,
                observer_role_id=None,
                grant_scenario_enabled=False,
                missing_required_keys=("profile_title_roles:Вице Советчанин", "profile_title_roles:Советчанин"),
            )

        vice_council_role_id = title_to_role_id.get("вице советчанин")
        council_member_role_id = title_to_role_id.get("советчанин")
        observer_role_id = title_to_role_id.get("наблюдатель")

        missing_required_keys: list[str] = []
        if vice_council_role_id is None:
            missing_required_keys.append("profile_title_roles:Вице Советчанин")
        if council_member_role_id is None:
            missing_required_keys.append("profile_title_roles:Советчанин")

        grant_scenario_enabled = len(missing_required_keys) == 0
        if not grant_scenario_enabled:
            logger.error(
                "council profile title config missing required mappings keys=%s; scenario=council_term_formation_role_grant will be blocked",
                ",".join(missing_required_keys),
            )
        else:
            logger.info(
                "council profile title config loaded from profile_title_roles vice_council=%s council_member=%s observer=%s",
                vice_council_role_id,
                council_member_role_id,
                observer_role_id,
            )

        return CouncilDiscordRolesConfig(
            vice_council_role_id=vice_council_role_id,
            council_member_role_id=council_member_role_id,
            observer_role_id=observer_role_id,
            grant_scenario_enabled=grant_scenario_enabled,
            missing_required_keys=tuple(missing_required_keys),
        )

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

    def grant_project_roles_for_term_members(
        self,
        *,
        term_members: list[dict[str, object]] | tuple[dict[str, object], ...],
        observer_enabled: bool,
        actor_provider: str = "system",
        actor_user_id: str = "council_lifecycle",
    ) -> dict[str, object]:
        """Выдать проектные роли для сформированного состава созыва через существующий role-management механизм."""
        if not self._discord_roles_config.grant_scenario_enabled:
            logger.error(
                "council term formation role grant blocked due to missing required config keys=%s",
                ",".join(self._discord_roles_config.missing_required_keys),
            )
            return {
                "ok": False,
                "blocked": True,
                "reason": "missing_required_profile_title_role_mapping",
                "message": "В таблице profile_title_roles не заполнены обязательные соответствия ролей Совета. Сценарий назначения ролей созыва остановлен.",
                "attempts": 0,
                "assigned": 0,
                "failed": 0,
            }

        role_mapping: dict[str, tuple[str, int]] = {
            "vice_council": ("Вице Советчанин", 1),
            "vice_council_member": ("Вице Советчанин", 1),
            "council_member": ("Советчанин", 2),
            "observer": ("Наблюдатель", 1 if observer_enabled else 0),
        }
        selected_by_role: dict[str, list[str]] = {role_code: [] for role_code in role_mapping}

        for row in term_members:
            role_code = str((row or {}).get("role_code") or "").strip().lower()
            profile_id = str((row or {}).get("profile_id") or "").strip()
            is_active = bool((row or {}).get("is_active", True))
            if not role_code or not profile_id or not is_active:
                continue
            mapping = role_mapping.get(role_code)
            if not mapping:
                continue
            _, limit = mapping
            if limit <= 0:
                continue
            if profile_id in selected_by_role[role_code]:
                continue
            if len(selected_by_role[role_code]) >= limit:
                continue
            selected_by_role[role_code].append(profile_id)

        attempts = 0
        assigned = 0
        failed = 0
        for role_code, (project_role_name, _) in role_mapping.items():
            for account_id in selected_by_role[role_code]:
                attempts += 1
                result = RoleManagementService.assign_user_role_by_account(
                    account_id,
                    project_role_name,
                    actor_provider=actor_provider,
                    actor_user_id=actor_user_id,
                    source="council_term_formation",
                )
                if result.get("ok"):
                    assigned += 1
                    logger.info(
                        "council term formation role grant success account_id=%s role_code=%s project_role=%s discord_role_id=%s permissions_scope=organizational_visual_status_only",
                        account_id,
                        role_code,
                        project_role_name,
                        self._resolve_configured_discord_role_id(role_code),
                    )
                    continue
                failed += 1
                logger.error(
                    "council term formation role grant failed account_id=%s role_code=%s project_role=%s discord_role_id=%s reason=%s message=%s",
                    account_id,
                    role_code,
                    project_role_name,
                    self._resolve_configured_discord_role_id(role_code),
                    result.get("reason"),
                    result.get("message"),
                )

        return {
            "ok": failed == 0,
            "attempts": attempts,
            "assigned": assigned,
            "failed": failed,
        }

    def _resolve_configured_discord_role_id(self, role_code: str) -> int | None:
        normalized_role_code = (role_code or "").strip().lower()
        if normalized_role_code in {"vice_council", "vice_council_member"}:
            return self._discord_roles_config.vice_council_role_id
        if normalized_role_code == "council_member":
            return self._discord_roles_config.council_member_role_id
        if normalized_role_code == "observer":
            return self._discord_roles_config.observer_role_id
        return None

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

    def compute_candidate_invite_expires_at(self, *, created_at: datetime) -> datetime:
        return compute_candidate_invite_expires_at(created_at=created_at)

    def resolve_candidate_invite_deadline(
        self,
        *,
        current_status: str,
        created_at: datetime | None,
        invite_expires_at: datetime | None = None,
        confirmed_at: datetime | None = None,
        now: datetime | None = None,
    ) -> InviteDeadlineDecision:
        return resolve_candidate_invite_deadline(
            current_status=current_status,
            created_at=created_at,
            invite_expires_at=invite_expires_at,
            confirmed_at=confirmed_at,
            now=now,
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

    def decide_term_member_exit(
        self,
        *,
        term_id: int | None,
        member_profile_id: str,
        role_code: str,
        was_active: bool,
        left_at: datetime | None = None,
    ) -> TermMemberExitDecision:
        return decide_term_member_exit(
            term_id=term_id,
            member_profile_id=member_profile_id,
            role_code=role_code,
            was_active=was_active,
            left_at=left_at,
        )

    def decide_replacement_assignment(
        self,
        *,
        term_id: int | None,
        actor_profile_id: str,
        actor_role_code: str,
        replaced_role_code: str,
        replacement_profile_id: str,
        source_list_code: str,
        already_active_profile_ids: tuple[str, ...] | list[str],
    ) -> ReplacementAssignmentDecision:
        return decide_replacement_assignment(
            term_id=term_id,
            actor_profile_id=actor_profile_id,
            actor_role_code=actor_role_code,
            replaced_role_code=replaced_role_code,
            replacement_profile_id=replacement_profile_id,
            source_list_code=source_list_code,
            already_active_profile_ids=already_active_profile_ids,
        )

    def build_active_voting_quorum_snapshot(
        self,
        *,
        term_members: list[dict[str, object]] | tuple[dict[str, object], ...],
        votes: list[dict[str, object]] | tuple[dict[str, object], ...],
    ) -> ActiveVotingQuorumSnapshot:
        return build_active_voting_quorum_snapshot(term_members=term_members, votes=votes)

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
        source_platform: str | None = None,
        existing_ballot_platform: str | None = None,
    ) -> BallotSubmissionDecision:
        return decide_ballot_submission(
            election_id=election_id,
            voter_profile_id=voter_profile_id,
            voter_role_code=voter_role_code,
            selected_candidate_ids=selected_candidate_ids,
            already_submitted_ballots_count=already_submitted_ballots_count,
            source_platform=source_platform,
            existing_ballot_platform=existing_ballot_platform,
        )

    def is_election_valid_by_ballots(self, *, total_ballots_count: int, min_valid_ballots: int = COUNCIL_MIN_VALID_BALLOTS) -> bool:
        return is_election_valid_by_ballots(total_ballots_count, min_valid_ballots=min_valid_ballots)

    def resolve_election_round_on_deadline(
        self,
        *,
        election_id: int | None,
        election_role_code: str,
        current_round_number: int,
        voting_ends_at: datetime | None,
        candidate_votes: list[dict[str, object]] | tuple[dict[str, object], ...],
    ) -> ElectionRoundResolution:
        return resolve_election_round_on_deadline(
            election_id=election_id,
            election_role_code=election_role_code,
            current_round_number=current_round_number,
            voting_ends_at=voting_ends_at,
            candidate_votes=candidate_votes,
        )

    def plan_election_deadline_jobs(
        self,
        elections: list[dict[str, object]] | tuple[dict[str, object], ...],
    ) -> tuple[ElectionSchedulerAction, ...]:
        return plan_election_deadline_jobs(elections)

    def build_election_status_publication(
        self,
        *,
        action: str,
        role_name: str,
        round_number: int,
        winner_mentions: tuple[str, ...] = (),
    ) -> ElectionStatusPublication:
        return build_election_status_publication(
            action=action,
            role_name=role_name,
            round_number=round_number,
            winner_mentions=winner_mentions,
        )

    def decide_question_moderation_approval(
        self,
        *,
        question_id: int | None,
        current_status: str,
        moderator_profile_id: str,
        approved_at: datetime | None = None,
    ) -> QuestionModerationDecision:
        return decide_question_moderation_approval(
            question_id=question_id,
            current_status=current_status,
            moderator_profile_id=moderator_profile_id,
            approved_at=approved_at,
        )

    def decide_question_start_voting(
        self,
        *,
        question_id: int | None,
        current_status: str,
        actor_profile_id: str,
        started_at: datetime | None = None,
        source_platform: str = "system",
    ) -> QuestionVotingTransitionDecision:
        pause_state = CouncilPauseService.sync_pause_state(platform=source_platform, user_id=actor_profile_id)
        if pause_state.get("paused"):
            logger.warning(
                "CouncilService blocked question voting start by pause question_id=%s actor_profile_id=%s reason=%s",
                question_id,
                actor_profile_id,
                pause_state.get("reason"),
            )
            return QuestionVotingTransitionDecision(
                accepted=False,
                next_status=None,
                reason="council_paused",
            )
        return decide_question_start_voting(
            question_id=question_id,
            current_status=current_status,
            actor_profile_id=actor_profile_id,
            started_at=started_at,
        )

    def resolve_question_voting_for_archive(
        self,
        *,
        question_id: int | None,
        current_status: str,
        votes: list[dict[str, object]] | tuple[dict[str, object], ...],
        required_comment: str,
        closed_by_profile_id: str,
        closed_at: datetime | None = None,
    ) -> QuestionArchiveDecision:
        return resolve_question_voting_for_archive(
            question_id=question_id,
            current_status=current_status,
            votes=votes,
            required_comment=required_comment,
            closed_by_profile_id=closed_by_profile_id,
            closed_at=closed_at,
        )

    def decide_question_vote_submission(
        self,
        *,
        question_id: int | None,
        current_status: str,
        voter_profile_id: str,
        voter_role_code: str,
        vote_value: str,
        existing_vote_value: str | None = None,
        changed_once: bool = False,
        current_score_yes: int = 0,
        current_score_no: int = 0,
        has_unreplaced_dropout: bool = False,
        source_platform: str | None = None,
        existing_vote_platform: str | None = None,
    ) -> QuestionVoteSubmissionDecision:
        return decide_question_vote_submission(
            question_id=question_id,
            current_status=current_status,
            voter_profile_id=voter_profile_id,
            voter_role_code=voter_role_code,
            vote_value=vote_value,
            existing_vote_value=existing_vote_value,
            changed_once=changed_once,
            current_score_yes=current_score_yes,
            current_score_no=current_score_no,
            has_unreplaced_dropout=has_unreplaced_dropout,
            source_platform=source_platform,
            existing_vote_platform=existing_vote_platform,
        )


    def get_pause_status(self, *, source_platform: str = "system", actor_profile_id: str | None = None) -> dict[str, object]:
        return CouncilPauseService.sync_pause_state(platform=source_platform, user_id=actor_profile_id)


council_service = CouncilService()
