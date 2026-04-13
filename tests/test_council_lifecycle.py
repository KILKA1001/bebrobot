from datetime import datetime, timezone

from bot.domain.council_lifecycle import (
    COUNCIL_BALLOT_LIMITS_BY_ROLE,
    COUNCIL_MIN_VALID_BALLOTS,
    CANDIDATE_STATUS_VALUES,
    MAX_COUNCIL_TEXT_LEN,
    ELECTION_STATUS_VALUES,
    QUESTION_STATUS_VALUES,
    TERM_LAUNCH_ALLOWED_CONFIRM_ROLES,
    TERM_STATUS_VALUES,
    build_election_invite_segments,
    compute_candidate_invite_expires_at,
    build_term_launch_notification_targets,
    decide_manual_candidate_addition,
    decide_replacement_assignment,
    decide_term_member_exit,
    build_active_voting_quorum_snapshot,
    decide_question_moderation_approval,
    decide_question_start_voting,
    decide_question_vote_submission,
    decide_candidate_review_action,
    resolve_candidate_invite_deadline,
    decide_ballot_submission,
    decide_term_launch_confirmation,
    filter_confirmed_ballot_candidates,
    build_election_status_publication,
    is_election_valid_by_ballots,
    is_valid_lifecycle_status,
    plan_election_deadline_jobs,
    resolve_election_round_on_deadline,
    resolve_question_voting_for_archive,
    validate_council_text_length,
)


def test_term_election_question_status_values_are_stable():
    assert TERM_STATUS_VALUES == (
        "draft",
        "pending_launch_confirmation",
        "active",
        "archived",
        "cancelled",
    )
    assert ELECTION_STATUS_VALUES == (
        "draft",
        "nomination",
        "voting",
        "completed",
        "cancelled",
    )
    assert QUESTION_STATUS_VALUES == (
        "draft",
        "discussion",
        "voting",
        "decided",
        "archived",
    )
    assert CANDIDATE_STATUS_VALUES == ("pending", "confirmed", "rejected", "withdrawn", "expired")


def test_status_validator_for_all_lifecycles():
    assert is_valid_lifecycle_status("active", lifecycle="term")
    assert is_valid_lifecycle_status("voting", lifecycle="election")
    assert is_valid_lifecycle_status("decided", lifecycle="question")
    assert not is_valid_lifecycle_status("unknown", lifecycle="term")
    assert not is_valid_lifecycle_status("active", lifecycle="missing")


def test_council_text_length_validation():
    valid, err = validate_council_text_length("x" * MAX_COUNCIL_TEXT_LEN, field_name="Вопрос")
    assert valid is True
    assert err is None

    valid, err = validate_council_text_length("x" * (MAX_COUNCIL_TEXT_LEN + 1), field_name="Предложение")
    assert valid is False
    assert "1000" in (err or "")


def test_term_launch_confirmation_allows_only_head_club_and_main_vice():
    assert TERM_LAUNCH_ALLOWED_CONFIRM_ROLES == ("head_club", "main_vice")

    denied = decide_term_launch_confirmation(
        term_status="pending_launch_confirmation",
        actor_profile_id="actor-1",
        actor_role_codes=("vice_city",),
        existing_confirmed_profile_ids=(),
    )
    assert denied.accepted is False
    assert denied.rejection_reason == "role_not_allowed"


def test_term_launch_confirmation_activates_on_first_valid_confirmation_only():
    first = decide_term_launch_confirmation(
        term_status="pending_launch_confirmation",
        actor_profile_id="head-1",
        actor_role_codes=("head_club",),
        existing_confirmed_profile_ids=(),
    )
    assert first.accepted is True
    assert first.launch_activated is True
    assert first.event_should_be_saved is True
    assert first.confirmed_by_role == "head_club"

    second = decide_term_launch_confirmation(
        term_status="active",
        actor_profile_id="vice-1",
        actor_role_codes=("main_vice",),
        existing_confirmed_profile_ids=("head-1",),
    )
    assert second.accepted is True
    assert second.launch_activated is False
    assert second.event_should_be_saved is True
    assert second.confirmed_by_role == "main_vice"


def test_term_launch_confirmation_rejects_duplicate_from_same_user():
    duplicate = decide_term_launch_confirmation(
        term_status="active",
        actor_profile_id="head-1",
        actor_role_codes=("head_club",),
        existing_confirmed_profile_ids=("head-1",),
    )
    assert duplicate.accepted is False
    assert duplicate.rejection_reason == "duplicate_confirmation"


def test_term_launch_notification_targets_include_both_roles_without_duplicates():
    targets = build_term_launch_notification_targets(
        head_club_profile_id="head-1",
        main_vice_profile_id="vice-1",
    )
    assert targets == ("head-1", "vice-1")

    same_targets = build_term_launch_notification_targets(
        head_club_profile_id="same-1",
        main_vice_profile_id="same-1",
    )
    assert same_targets == ("same-1",)


def test_invite_segments_cover_required_target_groups():
    segments = build_election_invite_segments()
    by_role = {item.role_code: item for item in segments}
    assert by_role["vice_council_member"].required_titles == ("vice_city", "main_vice")
    assert by_role["council_member"].required_titles == ("veteran",)
    assert by_role["observer"].requires_profile_application is True


def test_candidate_review_action_supports_confirm_and_reject_with_terminal_guards():
    confirm = decide_candidate_review_action(
        current_status="pending",
        action="confirm",
        candidate_profile_id="cand-1",
        election_role_code="council_member",
        actor_profile_id="actor-1",
        source_platform="discord",
    )
    assert confirm.accepted is True
    assert confirm.next_status == "confirmed"

    duplicate = decide_candidate_review_action(
        current_status="confirmed",
        action="confirm",
        candidate_profile_id="cand-1",
        election_role_code="council_member",
        actor_profile_id="actor-1",
        source_platform="telegram",
    )
    assert duplicate.accepted is False
    assert duplicate.reason == "already_confirmed"

    unsupported = decide_candidate_review_action(
        current_status="pending",
        action="skip",
        candidate_profile_id="cand-1",
        election_role_code="observer",
        actor_profile_id="actor-1",
        source_platform="telegram",
    )
    assert unsupported.accepted is False
    assert unsupported.reason == "unsupported_action"


def test_candidate_invite_expiry_defaults_to_24_hours_from_created_at():
    created_at = datetime(2026, 4, 13, 10, 0, tzinfo=timezone.utc)
    expires_at = compute_candidate_invite_expires_at(created_at=created_at)
    assert expires_at == datetime(2026, 4, 14, 10, 0, tzinfo=timezone.utc)


def test_candidate_invite_deadline_moves_pending_to_expired_and_returns_notification():
    decision = resolve_candidate_invite_deadline(
        current_status="pending",
        created_at=datetime(2026, 4, 10, 9, 0, tzinfo=timezone.utc),
        now=datetime(2026, 4, 11, 9, 1, tzinfo=timezone.utc),
    )
    assert decision.accepted is True
    assert decision.next_status == "expired"
    assert decision.reason == "invite_expired_without_confirmation"
    assert decision.notify_candidate is True
    assert decision.status_transition_log is not None


def test_candidate_invite_deadline_keeps_confirmed_when_confirmation_is_in_time():
    decision = resolve_candidate_invite_deadline(
        current_status="pending",
        created_at=datetime(2026, 4, 10, 9, 0, tzinfo=timezone.utc),
        confirmed_at=datetime(2026, 4, 10, 16, 0, tzinfo=timezone.utc),
        now=datetime(2026, 4, 10, 16, 1, tzinfo=timezone.utc),
    )
    assert decision.accepted is True
    assert decision.next_status == "confirmed"
    assert decision.reason == "confirmed_in_time"


def test_candidate_invite_deadline_does_not_confirm_after_expiry():
    decision = resolve_candidate_invite_deadline(
        current_status="pending",
        created_at=datetime(2026, 4, 10, 9, 0, tzinfo=timezone.utc),
        confirmed_at=datetime(2026, 4, 11, 10, 0, tzinfo=timezone.utc),
        now=datetime(2026, 4, 11, 10, 1, tzinfo=timezone.utc),
    )
    assert decision.accepted is True
    assert decision.next_status == "expired"
    assert decision.reason == "invite_expired_without_confirmation"


def test_ballot_includes_only_confirmed_candidates():
    raw_candidates = [
        {"id": 1, "profile_id": "u1", "status": "confirmed"},
        {"id": 2, "profile_id": "u2", "status": "pending"},
        {"id": 3, "profile_id": "u3", "status": "rejected"},
    ]
    filtered = filter_confirmed_ballot_candidates(raw_candidates, election_id=88)
    assert [item["id"] for item in filtered] == [1]


def test_manual_candidate_addition_rejects_duplicate_within_role_and_term():
    decision = decide_manual_candidate_addition(
        term_id=7,
        election_status="nomination",
        candidate_profile_id="cand-1",
        election_role_code="council_member",
        actor_profile_id="admin-1",
        existing_candidates=(
            {"term_id": 7, "role_code": "council_member", "profile_id": "cand-1"},
            {"term_id": 7, "role_code": "observer", "profile_id": "cand-1"},
        ),
    )
    assert decision.accepted is False
    assert decision.reason == "duplicate_candidate_for_role_term"


def test_manual_candidate_addition_rejects_closed_election_status():
    decision = decide_manual_candidate_addition(
        term_id=8,
        election_status="completed",
        candidate_profile_id="cand-2",
        election_role_code="observer",
        actor_profile_id="admin-2",
        existing_candidates=(),
    )
    assert decision.accepted is False
    assert decision.reason == "election_status_not_open_for_manual_add"


def test_manual_candidate_addition_accepts_and_returns_assignment_audit_payload():
    decision = decide_manual_candidate_addition(
        term_id=11,
        election_status="voting",
        candidate_profile_id="cand-4",
        election_role_code="vice_council_member",
        actor_profile_id="head-7",
        existing_candidates=({"term_id": 11, "role_code": "observer", "profile_id": "cand-4"},),
    )
    assert decision.accepted is True
    assert decision.reason is None
    assert decision.assignment_log is not None
    assert decision.assignment_log["assigned_by_profile_id"] == "head-7"
    assert decision.assignment_log["election_role_code"] == "vice_council_member"


def test_ballot_limits_by_role_are_stable():
    assert COUNCIL_BALLOT_LIMITS_BY_ROLE == {
        "vice_council_member": 1,
        "council_member": 2,
        "observer": 1,
    }


def test_ballot_submission_rejects_when_limit_exceeded():
    decision = decide_ballot_submission(
        election_id=15,
        voter_profile_id="profile-101",
        voter_role_code="council_member",
        selected_candidate_ids=[11, 12, 13],
    )
    assert decision.accepted is False
    assert decision.reason == "ballot_limit_exceeded"
    assert decision.allowed_limit == 2


def test_ballot_submission_uses_profile_id_and_blocks_cumulative_overflow():
    decision = decide_ballot_submission(
        election_id=16,
        voter_profile_id="profile-shared-tg-discord",
        voter_role_code="vice_council_member",
        selected_candidate_ids=[21],
        already_submitted_ballots_count=1,
    )
    assert decision.accepted is False
    assert decision.reason == "ballot_limit_exceeded"
    assert decision.allowed_limit == 1
    assert decision.remaining_votes == 0


def test_election_validity_requires_minimum_three_ballots():
    assert COUNCIL_MIN_VALID_BALLOTS == 3
    assert is_election_valid_by_ballots(2) is False
    assert is_election_valid_by_ballots(3) is True


def test_ballot_submission_rejects_invalid_candidate_ids_and_invalid_submitted_count():
    invalid_ids = decide_ballot_submission(
        election_id=17,
        voter_profile_id="profile-501",
        voter_role_code="observer",
        selected_candidate_ids=[31, -1],
    )
    assert invalid_ids.accepted is False
    assert invalid_ids.reason == "invalid_candidate_ids"

    invalid_count = decide_ballot_submission(
        election_id=18,
        voter_profile_id="profile-502",
        voter_role_code="observer",
        selected_candidate_ids=[31],
        already_submitted_ballots_count=-2,
    )
    assert invalid_count.accepted is False
    assert invalid_count.reason == "invalid_already_submitted_ballots_count"


def test_ballot_submission_success_reports_remaining_votes_for_ui():
    accepted = decide_ballot_submission(
        election_id=19,
        voter_profile_id="profile-777",
        voter_role_code="council_member",
        selected_candidate_ids=[41],
        already_submitted_ballots_count=0,
    )
    assert accepted.accepted is True
    assert accepted.remaining_votes == 1


def test_ballot_submission_blocks_cross_platform_duplicate_with_telegram_priority():
    denied = decide_ballot_submission(
        election_id=20,
        voter_profile_id="profile-shared-20",
        voter_role_code="observer",
        selected_candidate_ids=[51],
        already_submitted_ballots_count=1,
        source_platform="discord",
        existing_ballot_platform="telegram",
    )
    assert denied.accepted is False
    assert denied.reason == "cross_platform_duplicate_vote"
    assert "уже зарегистрирован через Telegram" in (denied.user_message or "")
    assert "Приоритет проведения выборов остаётся за Telegram" in (denied.user_message or "")


def test_election_deadline_resolver_starts_second_round_on_tie_for_single_seat():
    deadline = datetime(2026, 4, 13, 10, 0, tzinfo=timezone.utc)
    decision = resolve_election_round_on_deadline(
        election_id=21,
        election_role_code="vice_council_member",
        current_round_number=1,
        voting_ends_at=deadline,
        candidate_votes=(
            {"candidate_id": 1, "votes": 7},
            {"candidate_id": 2, "votes": 7},
            {"candidate_id": 3, "votes": 1},
        ),
    )
    assert decision.accepted is True
    assert decision.decision == "runoff"
    assert decision.next_round_number == 2
    assert decision.voting_ends_at == datetime(2026, 4, 14, 10, 0, tzinfo=timezone.utc)
    assert decision.runoff_candidate_ids == (1, 2)


def test_election_deadline_resolver_uses_cutoff_tie_logic_for_council_seats():
    decision = resolve_election_round_on_deadline(
        election_id=22,
        election_role_code="council_member",
        current_round_number=1,
        voting_ends_at=None,
        now=datetime(2026, 4, 13, 9, 0, tzinfo=timezone.utc),
        candidate_votes=(
            {"candidate_id": 10, "votes": 10},
            {"candidate_id": 11, "votes": 6},
            {"candidate_id": 12, "votes": 6},
        ),
    )
    assert decision.decision == "runoff"
    assert decision.runoff_candidate_ids == (11, 12)


def test_election_deadline_scheduler_collects_expired_voting_rows():
    actions = plan_election_deadline_jobs(
        (
            {"id": 1, "status": "voting", "voting_ends_at": datetime(2026, 4, 12, 0, 0, tzinfo=timezone.utc)},
            {"id": 2, "status": "nomination", "voting_ends_at": datetime(2026, 4, 12, 0, 0, tzinfo=timezone.utc)},
            {"id": 3, "status": "voting", "voting_ends_at": datetime(2026, 4, 14, 0, 0, tzinfo=timezone.utc)},
        ),
        now=datetime(2026, 4, 13, 0, 0, tzinfo=timezone.utc),
    )
    assert len(actions) == 1
    assert actions[0].election_id == 1
    assert actions[0].action == "close_and_resolve_tie"


def test_election_publication_templates_cover_start_runoff_and_final():
    start = build_election_status_publication(action="start", role_name="Советчане", round_number=1)
    runoff = build_election_status_publication(action="runoff", role_name="Советчане", round_number=2)
    final = build_election_status_publication(
        action="final",
        role_name="Советчане",
        round_number=2,
        winner_mentions=("<@1>", "<@2>"),
    )
    assert "Раунд 1 открыт" in start.body
    assert "второй" not in runoff.body.lower()
    assert "+1 день" in runoff.body
    assert "<@1>, <@2>" in final.body


def test_question_after_moderation_moves_to_discussion_then_voting_for_30_minutes():
    approved_at = datetime(2026, 4, 13, 12, 0, tzinfo=timezone.utc)
    moderation = decide_question_moderation_approval(
        question_id=17,
        current_status="draft",
        moderator_profile_id="mod-1",
        approved_at=approved_at,
    )
    assert moderation.accepted is True
    assert moderation.next_status == "discussion"
    assert moderation.discussion_started_at == approved_at

    voting = decide_question_start_voting(
        question_id=17,
        current_status="discussion",
        actor_profile_id="mod-1",
        started_at=approved_at,
    )
    assert voting.accepted is True
    assert voting.next_status == "voting"
    assert voting.voting_ends_at == datetime(2026, 4, 13, 12, 30, tzinfo=timezone.utc)
    assert "30 минут" in (voting.user_message or "")


def test_question_archive_requires_comment_and_keeps_result_score_and_closed_at():
    closed_at = datetime(2026, 4, 13, 12, 30, tzinfo=timezone.utc)
    decision = resolve_question_voting_for_archive(
        question_id=18,
        current_status="voting",
        votes=(
            {"vote_value": "yes"},
            {"vote_value": "yes"},
            {"vote_value": "no"},
            {"vote_value": "abstain"},
        ),
        required_comment="Решение принято большинством голосов.",
        closed_by_profile_id="mod-2",
        closed_at=closed_at,
    )
    assert decision.accepted is True
    assert decision.next_status == "decided"
    payload = decision.archive_payload or {}
    assert payload["result"] == "accepted"
    assert payload["score"] == {"yes": 2, "no": 1, "abstain": 1}
    assert payload["required_comment"] == "Решение принято большинством голосов."
    assert payload["closed_at"] == closed_at.isoformat()

    rejected = resolve_question_voting_for_archive(
        question_id=18,
        current_status="voting",
        votes=(),
        required_comment="",
        closed_by_profile_id="mod-2",
        closed_at=closed_at,
    )
    assert rejected.accepted is False
    assert rejected.reason == "required_comment_missing"


def test_question_vote_submission_sets_vice_weight_to_two_on_two_to_two_or_unreplaced_dropout():
    tie_decision = decide_question_vote_submission(
        question_id=19,
        current_status="voting",
        voter_profile_id="vice-1",
        voter_role_code="vice_council_member",
        vote_value="yes",
        current_score_yes=2,
        current_score_no=2,
        has_unreplaced_dropout=False,
    )
    assert tie_decision.accepted is True
    assert tie_decision.vote_weight == 2

    dropout_decision = decide_question_vote_submission(
        question_id=19,
        current_status="voting",
        voter_profile_id="vice-1",
        voter_role_code="vice_council_member",
        vote_value="no",
        current_score_yes=1,
        current_score_no=0,
        has_unreplaced_dropout=True,
    )
    assert dropout_decision.accepted is True
    assert dropout_decision.vote_weight == 2

    default_decision = decide_question_vote_submission(
        question_id=19,
        current_status="voting",
        voter_profile_id="vice-1",
        voter_role_code="vice_council_member",
        vote_value="abstain",
        current_score_yes=1,
        current_score_no=1,
        has_unreplaced_dropout=False,
    )
    assert default_decision.accepted is True
    assert default_decision.vote_weight == 1


def test_question_vote_submission_allows_only_one_vote_change():
    first_change = decide_question_vote_submission(
        question_id=20,
        current_status="voting",
        voter_profile_id="member-1",
        voter_role_code="council_member",
        vote_value="no",
        existing_vote_value="yes",
        changed_once=False,
    )
    assert first_change.accepted is True
    assert first_change.changed_once is True

    blocked_change = decide_question_vote_submission(
        question_id=20,
        current_status="voting",
        voter_profile_id="member-1",
        voter_role_code="council_member",
        vote_value="yes",
        existing_vote_value="no",
        changed_once=True,
    )
    assert blocked_change.accepted is False
    assert blocked_change.reason == "vote_change_limit_reached"
    assert "уже меняли голос" in (blocked_change.user_message or "").lower()


def test_question_vote_submission_blocks_cross_platform_duplicate_for_shared_profile():
    blocked = decide_question_vote_submission(
        question_id=21,
        current_status="voting",
        voter_profile_id="profile-shared-7",
        voter_role_code="council_member",
        vote_value="yes",
        source_platform="discord",
        existing_vote_platform="telegram",
    )
    assert blocked.accepted is False
    assert blocked.reason == "cross_platform_duplicate_vote"
    assert "уже учтён через Telegram" in (blocked.user_message or "")


def test_term_member_exit_marks_member_as_dropout_for_active_term():
    decision = decide_term_member_exit(
        term_id=31,
        member_profile_id="member-31",
        role_code="council_member",
        was_active=True,
        left_at=datetime(2026, 4, 13, 14, 0, tzinfo=timezone.utc),
    )
    assert decision.accepted is True
    assert decision.member_patch is not None
    assert decision.member_patch["is_active"] is False
    assert decision.member_patch["role_code"] == "council_member"
    assert "выбывш" in (decision.user_message or "").lower()


def test_replacement_assignment_allows_vice_from_candidate_or_election_sources():
    decision = decide_replacement_assignment(
        term_id=32,
        actor_profile_id="vice-32",
        actor_role_code="vice_council_member",
        replaced_role_code="council_member",
        replacement_profile_id="replacement-32",
        source_list_code="candidate_pool",
        already_active_profile_ids=("member-1",),
    )
    assert decision.accepted is True
    assert decision.assignment_payload is not None
    assert decision.assignment_payload["role_code"] == "council_member"
    assert decision.assignment_payload["source_list_code"] == "candidate_pool"

    denied = decide_replacement_assignment(
        term_id=32,
        actor_profile_id="member-32",
        actor_role_code="council_member",
        replaced_role_code="council_member",
        replacement_profile_id="replacement-33",
        source_list_code="election_results",
        already_active_profile_ids=(),
    )
    assert denied.accepted is False
    assert denied.reason == "actor_not_vice"


def test_active_voting_quorum_snapshot_recomputes_quorum_and_dropout_flag():
    snapshot = build_active_voting_quorum_snapshot(
        term_members=(
            {"profile_id": "vice-1", "role_code": "vice_council_member", "is_active": True},
            {"profile_id": "member-1", "role_code": "council_member", "is_active": True},
            {"profile_id": "member-2", "role_code": "council_member", "is_active": False, "dropout_reason": "left_club"},
            {"profile_id": "obs-1", "role_code": "observer", "is_active": True},
        ),
        votes=(
            {"voter_profile_id": "vice-1", "vote_value": "yes"},
            {"voter_profile_id": "member-1", "vote_value": "yes"},
        ),
    )
    assert snapshot.accepted is True
    assert snapshot.total_active_members == 3
    assert snapshot.quorum_min_votes == 2
    assert snapshot.has_quorum is True
    assert snapshot.has_unreplaced_dropout is True

    vice_vote = decide_question_vote_submission(
        question_id=33,
        current_status="voting",
        voter_profile_id="vice-1",
        voter_role_code="vice_council_member",
        vote_value="yes",
        has_unreplaced_dropout=snapshot.has_unreplaced_dropout,
    )
    assert vice_vote.accepted is True
    assert vice_vote.vote_weight == 2
