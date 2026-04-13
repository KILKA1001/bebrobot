from bot.services.council_service import council_service


def test_council_service_lifecycle_snapshot_contains_expected_statuses():
    snapshot = council_service.get_lifecycle_snapshot()

    assert "pending_launch_confirmation" in snapshot.term_statuses
    assert "voting" in snapshot.election_statuses
    assert "archived" in snapshot.question_statuses


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
