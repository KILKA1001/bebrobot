from bot.domain.council_lifecycle import (
    CANDIDATE_STATUS_VALUES,
    MAX_COUNCIL_TEXT_LEN,
    ELECTION_STATUS_VALUES,
    QUESTION_STATUS_VALUES,
    TERM_LAUNCH_ALLOWED_CONFIRM_ROLES,
    TERM_STATUS_VALUES,
    build_election_invite_segments,
    build_term_launch_notification_targets,
    decide_candidate_review_action,
    decide_term_launch_confirmation,
    filter_confirmed_ballot_candidates,
    is_valid_lifecycle_status,
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
    assert CANDIDATE_STATUS_VALUES == ("pending", "confirmed", "rejected", "withdrawn")


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


def test_ballot_includes_only_confirmed_candidates():
    raw_candidates = [
        {"id": 1, "profile_id": "u1", "status": "confirmed"},
        {"id": 2, "profile_id": "u2", "status": "pending"},
        {"id": 3, "profile_id": "u3", "status": "rejected"},
    ]
    filtered = filter_confirmed_ballot_candidates(raw_candidates, election_id=88)
    assert [item["id"] for item in filtered] == [1]
