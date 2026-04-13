from datetime import datetime, timezone

from bot.services.council_service import council_service


def test_council_service_lifecycle_snapshot_contains_expected_statuses():
    snapshot = council_service.get_lifecycle_snapshot()

    assert "pending_launch_confirmation" in snapshot.term_statuses
    assert "voting" in snapshot.election_statuses
    assert "archived" in snapshot.question_statuses
    assert "confirmed" in snapshot.candidate_statuses


def test_council_service_validates_statuses_and_rejects_unknown_lifecycle():
    assert council_service.is_valid_status(lifecycle="term", status="active")
    assert council_service.is_valid_status(lifecycle="election", status="nomination")
    assert not council_service.is_valid_status(lifecycle="unknown", status="active")


def test_council_service_launch_confirmation_and_targets():
    decision = council_service.decide_launch_confirmation(
        term_status="pending_launch_confirmation",
        actor_profile_id="101",
        actor_role_codes=("head_club",),
        existing_confirmed_profile_ids=(),
    )

    assert decision.accepted is True
    assert decision.launch_activated is True
    assert decision.confirmed_by_role == "head_club"

    targets = council_service.build_launch_notification_targets(
        head_club_profile_id="101",
        main_vice_profile_id="102",
    )
    assert targets == ("101", "102")


def test_council_service_text_validation_uses_shared_domain_rules():
    valid, err = council_service.validate_text(field_name="Вопрос", text="x" * 1000)
    assert valid is True
    assert err is None

    valid_too_long, err_too_long = council_service.validate_text(field_name="Вопрос", text="x" * 1001)
    assert valid_too_long is False
    assert "1000" in (err_too_long or "")


def test_council_service_supports_candidate_confirmation_flow_and_ballot_filter():
    decision = council_service.decide_candidate_review_action(
        current_status="pending",
        action="confirm",
        candidate_profile_id="candidate-1",
        election_role_code="council_member",
        actor_profile_id="moderator-1",
        source_platform="discord",
    )
    assert decision.accepted is True
    assert decision.next_status == "confirmed"

    candidates = [
        {"id": 10, "status": "confirmed"},
        {"id": 11, "status": "pending"},
    ]
    filtered = council_service.filter_confirmed_ballot_candidates(candidates, election_id=1)
    assert len(filtered) == 1
    assert filtered[0]["id"] == 10


def test_council_service_manual_candidate_addition_returns_assignment_audit():
    decision = council_service.decide_manual_candidate_addition(
        term_id=3,
        election_status="nomination",
        candidate_profile_id="candidate-9",
        election_role_code="council_member",
        actor_profile_id="moderator-4",
        existing_candidates=(),
    )
    assert decision.accepted is True
    assert decision.assignment_log is not None
    assert decision.assignment_log["assigned_by_profile_id"] == "moderator-4"


def test_council_service_manual_candidate_addition_blocks_duplicates():
    decision = council_service.decide_manual_candidate_addition(
        term_id=3,
        election_status="voting",
        candidate_profile_id="candidate-9",
        election_role_code="council_member",
        actor_profile_id="moderator-4",
        existing_candidates=(
            {"term_id": 3, "role_code": "council_member", "profile_id": "candidate-9"},
        ),
    )
    assert decision.accepted is False
    assert decision.reason == "duplicate_candidate_for_role_term"


def test_council_service_ballot_submission_enforces_role_limits():
    assert council_service.get_ballot_limit_for_role(role_code="vice_council_member") == 1
    assert council_service.get_ballot_limit_for_role(role_code="council_member") == 2
    assert council_service.get_ballot_limit_for_role(role_code="observer") == 1

    denied = council_service.decide_ballot_submission(
        election_id=44,
        voter_profile_id="profile-900",
        voter_role_code="observer",
        selected_candidate_ids=[1, 2],
    )
    assert denied.accepted is False
    assert denied.reason == "ballot_limit_exceeded"


def test_council_service_ballot_submission_uses_shared_profile_id_and_threshold():
    denied = council_service.decide_ballot_submission(
        election_id=45,
        voter_profile_id="profile-shared-tg-discord",
        voter_role_code="vice_council_member",
        selected_candidate_ids=[5],
        already_submitted_ballots_count=1,
    )
    assert denied.accepted is False
    assert denied.reason == "ballot_limit_exceeded"

    assert council_service.is_election_valid_by_ballots(total_ballots_count=2) is False
    assert council_service.is_election_valid_by_ballots(total_ballots_count=3) is True


def test_council_service_ballot_submission_success_exposes_ui_details():
    accepted = council_service.decide_ballot_submission(
        election_id=46,
        voter_profile_id="profile-901",
        voter_role_code="council_member",
        selected_candidate_ids=[10],
        already_submitted_ballots_count=0,
    )
    assert accepted.accepted is True
    assert accepted.remaining_votes == 1
    assert "Осталось голосов" in (accepted.user_message or "")


def test_council_service_supports_tie_resolution_scheduler_and_publication():
    resolution = council_service.resolve_election_round_on_deadline(
        election_id=99,
        election_role_code="vice_council_member",
        current_round_number=1,
        voting_ends_at=datetime(2026, 4, 13, 10, 0, tzinfo=timezone.utc),
        candidate_votes=(
            {"candidate_id": 1, "votes": 4},
            {"candidate_id": 2, "votes": 4},
        ),
    )
    assert resolution.accepted is True
    assert resolution.decision == "runoff"
    assert resolution.next_round_number == 2

    jobs = council_service.plan_election_deadline_jobs(
        (
            {"id": 99, "status": "voting", "voting_ends_at": datetime(2026, 4, 13, 9, 0, tzinfo=timezone.utc)},
        )
    )
    assert jobs[0].action == "close_and_resolve_tie"

    publication = council_service.build_election_status_publication(
        action="runoff",
        role_name="Советчане",
        round_number=2,
    )
    assert publication.action == "runoff"
    assert "+1 день" in publication.body
