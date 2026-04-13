from datetime import datetime, timezone
import importlib

from bot.services.council_service import council_service

council_service_module = importlib.import_module("bot.services.council_service")


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


def test_council_service_ballot_submission_blocks_cross_platform_duplicate():
    denied = council_service.decide_ballot_submission(
        election_id=47,
        voter_profile_id="profile-shared-47",
        voter_role_code="observer",
        selected_candidate_ids=[10],
        source_platform="discord",
        existing_ballot_platform="telegram",
    )
    assert denied.accepted is False
    assert denied.reason == "cross_platform_duplicate_vote"
    assert "Telegram" in (denied.user_message or "")


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


def test_council_service_question_flow_from_moderation_to_archive():
    approved = council_service.decide_question_moderation_approval(
        question_id=501,
        current_status="draft",
        moderator_profile_id="mod-501",
        approved_at=datetime(2026, 4, 13, 13, 0, tzinfo=timezone.utc),
    )
    assert approved.accepted is True
    assert approved.next_status == "discussion"

    voting = council_service.decide_question_start_voting(
        question_id=501,
        current_status="discussion",
        actor_profile_id="mod-501",
        started_at=datetime(2026, 4, 13, 13, 0, tzinfo=timezone.utc),
    )
    assert voting.accepted is True
    assert voting.next_status == "voting"
    assert voting.voting_ends_at == datetime(2026, 4, 13, 13, 30, tzinfo=timezone.utc)

    archive = council_service.resolve_question_voting_for_archive(
        question_id=501,
        current_status="voting",
        votes=(
            {"vote_value": "yes"},
            {"vote_value": "no"},
            {"vote_value": "yes"},
        ),
        required_comment="Большинство поддержало предложение.",
        closed_by_profile_id="mod-501",
        closed_at=datetime(2026, 4, 13, 13, 30, tzinfo=timezone.utc),
    )
    assert archive.accepted is True


def test_council_service_grants_project_roles_for_formed_term_members(monkeypatch):
    calls: list[tuple[str, str]] = []
    monkeypatch.setattr(
        council_service,
        "_discord_roles_config",
        council_service_module.CouncilDiscordRolesConfig(
            vice_council_role_id=1,
            council_member_role_id=2,
            observer_role_id=3,
            grant_scenario_enabled=True,
            missing_required_keys=(),
        ),
    )

    def _fake_assign(account_id: str, role_name: str, **_kwargs):
        calls.append((account_id, role_name))
        return {"ok": True}

    monkeypatch.setattr(council_service_module.RoleManagementService, "assign_user_role_by_account", staticmethod(_fake_assign))

    result = council_service.grant_project_roles_for_term_members(
        term_members=(
            {"profile_id": "vice-1", "role_code": "vice_council_member", "is_active": True},
            {"profile_id": "member-1", "role_code": "council_member", "is_active": True},
            {"profile_id": "member-2", "role_code": "council_member", "is_active": True},
            {"profile_id": "member-3", "role_code": "council_member", "is_active": True},
            {"profile_id": "obs-1", "role_code": "observer", "is_active": True},
        ),
        observer_enabled=True,
    )

    assert result == {"ok": True, "attempts": 4, "assigned": 4, "failed": 0}
    assert ("vice-1", "Вице Советчанин") in calls
    assert ("member-1", "Советчанин") in calls
    assert ("member-2", "Советчанин") in calls
    assert ("obs-1", "Наблюдатель") in calls
    assert ("member-3", "Советчанин") not in calls


def test_council_service_grants_project_roles_observer_disabled_and_idempotent(monkeypatch):
    calls: list[tuple[str, str]] = []
    monkeypatch.setattr(
        council_service,
        "_discord_roles_config",
        council_service_module.CouncilDiscordRolesConfig(
            vice_council_role_id=1,
            council_member_role_id=2,
            observer_role_id=3,
            grant_scenario_enabled=True,
            missing_required_keys=(),
        ),
    )

    def _fake_assign(account_id: str, role_name: str, **_kwargs):
        calls.append((account_id, role_name))
        return {"ok": True}

    monkeypatch.setattr(council_service_module.RoleManagementService, "assign_user_role_by_account", staticmethod(_fake_assign))
    payload = (
        {"profile_id": "vice-2", "role_code": "vice_council", "is_active": True},
        {"profile_id": "member-4", "role_code": "council_member", "is_active": True},
        {"profile_id": "obs-2", "role_code": "observer", "is_active": True},
    )
    first = council_service.grant_project_roles_for_term_members(
        term_members=payload,
        observer_enabled=False,
    )
    second = council_service.grant_project_roles_for_term_members(
        term_members=payload,
        observer_enabled=False,
    )

    assert first == {"ok": True, "attempts": 2, "assigned": 2, "failed": 0}
    assert second == {"ok": True, "attempts": 2, "assigned": 2, "failed": 0}
    assert ("obs-2", "Наблюдатель") not in calls


def test_council_service_grants_project_roles_logs_failed_attempts(monkeypatch, caplog):
    monkeypatch.setattr(
        council_service,
        "_discord_roles_config",
        council_service_module.CouncilDiscordRolesConfig(
            vice_council_role_id=1,
            council_member_role_id=2,
            observer_role_id=3,
            grant_scenario_enabled=True,
            missing_required_keys=(),
        ),
    )

    def _fake_assign(account_id: str, role_name: str, **_kwargs):
        if account_id == "member-fail":
            return {"ok": False, "reason": "db_error", "message": "insert failed"}
        return {"ok": True}

    monkeypatch.setattr(council_service_module.RoleManagementService, "assign_user_role_by_account", staticmethod(_fake_assign))

    result = council_service.grant_project_roles_for_term_members(
        term_members=(
            {"profile_id": "vice-ok", "role_code": "vice_council_member", "is_active": True},
            {"profile_id": "member-fail", "role_code": "council_member", "is_active": True},
        ),
        observer_enabled=True,
    )

    assert result == {"ok": False, "attempts": 2, "assigned": 1, "failed": 1}
    assert "council term formation role grant failed account_id=member-fail" in caplog.text


def test_council_service_question_vote_submission_has_weight_and_change_limit():
    weighted = council_service.decide_question_vote_submission(
        question_id=700,
        current_status="voting",
        voter_profile_id="vice-700",
        voter_role_code="vice_council_member",
        vote_value="yes",
        current_score_yes=2,
        current_score_no=2,
    )
    assert weighted.accepted is True
    assert weighted.vote_weight == 2

    blocked = council_service.decide_question_vote_submission(
        question_id=700,
        current_status="voting",
        voter_profile_id="member-700",
        voter_role_code="council_member",
        vote_value="no",
        existing_vote_value="yes",
        changed_once=True,
    )
    assert blocked.accepted is False
    assert blocked.reason == "vote_change_limit_reached"


def test_council_service_question_vote_submission_blocks_cross_platform_duplicate():
    blocked = council_service.decide_question_vote_submission(
        question_id=701,
        current_status="voting",
        voter_profile_id="profile-shared-701",
        voter_role_code="council_member",
        vote_value="yes",
        source_platform="discord",
        existing_vote_platform="telegram",
    )
    assert blocked.accepted is False
    assert blocked.reason == "cross_platform_duplicate_vote"
    assert "с другой платформы недоступен" in (blocked.user_message or "")


def test_council_service_supports_member_dropout_replacement_and_quorum_snapshot():
    exit_decision = council_service.decide_term_member_exit(
        term_id=801,
        member_profile_id="member-801",
        role_code="council_member",
        was_active=True,
        left_at=datetime(2026, 4, 13, 14, 0, tzinfo=timezone.utc),
    )
    assert exit_decision.accepted is True
    assert (exit_decision.member_patch or {}).get("is_active") is False

    replacement = council_service.decide_replacement_assignment(
        term_id=801,
        actor_profile_id="vice-801",
        actor_role_code="vice_council_member",
        replaced_role_code="council_member",
        replacement_profile_id="member-new-801",
        source_list_code="election_results",
        already_active_profile_ids=("vice-801", "observer-801"),
    )
    assert replacement.accepted is True
    assert (replacement.assignment_payload or {}).get("source_list_code") == "election_results"

    snapshot = council_service.build_active_voting_quorum_snapshot(
        term_members=(
            {"profile_id": "vice-801", "role_code": "vice_council_member", "is_active": True},
            {"profile_id": "member-801", "role_code": "council_member", "is_active": False, "dropout_reason": "left_club"},
            {"profile_id": "observer-801", "role_code": "observer", "is_active": True},
        ),
        votes=(
            {"voter_profile_id": "vice-801", "vote_value": "yes"},
            {"voter_profile_id": "observer-801", "vote_value": "no"},
        ),
    )
    assert snapshot.accepted is True
    assert snapshot.quorum_min_votes == 2
    assert snapshot.has_quorum is True
    assert snapshot.has_unreplaced_dropout is True


def test_council_service_process_term_member_exit_runs_status_then_role_then_journal(monkeypatch):
    sequence: list[str] = []

    class _Query:
        def __init__(self, table: str):
            self.table = table

        def update(self, _payload):
            sequence.append("status_update")
            return self

        def eq(self, *_args):
            return self

        def execute(self):
            return type("Resp", (), {"data": [{"id": 91}]})()

    class _Supabase:
        def table(self, name: str):
            if name == "council_term_members":
                return _Query(name)
            if name == "council_audit_log":
                return self
            raise AssertionError(name)

        def insert(self, _payload):
            sequence.append("journal")
            return self

        def execute(self):
            return type("Resp", (), {"data": []})()

    monkeypatch.setattr(council_service_module.db, "supabase", _Supabase())

    def _fake_revoke(*_args, **_kwargs):
        sequence.append("discord_role")
        return {"ok": True}

    monkeypatch.setattr(council_service_module.RoleManagementService, "revoke_user_role_by_account", staticmethod(_fake_revoke))

    result = council_service.process_term_member_exit(
        term_id=777,
        member_profile_id="member-777",
        role_code="council_member",
        was_active=True,
        actor_profile_id="vice-777",
        source_platform="discord",
    )

    assert result["ok"] is True
    assert sequence == ["status_update", "discord_role", "journal"]


def test_council_service_process_replacement_assignment_triggers_retry_on_partial_failure(monkeypatch):
    sequence: list[str] = []
    triggered: list[tuple[str, str]] = []

    class _Query:
        def upsert(self, _payload, **_kwargs):
            sequence.append("status_upsert")
            return self

        def execute(self):
            return type("Resp", (), {"data": [{"id": 32}]})()

    class _Supabase:
        def table(self, name: str):
            if name == "council_term_members":
                return _Query()
            if name == "council_audit_log":
                return self
            raise AssertionError(name)

        def insert(self, _payload):
            sequence.append("journal")
            return self

        def execute(self):
            return type("Resp", (), {"data": []})()

    monkeypatch.setattr(council_service_module.db, "supabase", _Supabase())

    def _fake_assign(*_args, **_kwargs):
        sequence.append("discord_role")
        return {"ok": False, "reason": "discord_error", "message": "api timeout"}

    def _fake_trigger(account_id: str, *, reason: str, bot=None):
        _ = bot
        triggered.append((account_id, reason))
        return True

    monkeypatch.setattr(council_service_module.RoleManagementService, "assign_user_role_by_account", staticmethod(_fake_assign))
    monkeypatch.setattr(council_service_module.ExternalRolesSyncService, "trigger_account_sync", staticmethod(_fake_trigger))

    result = council_service.process_replacement_assignment(
        term_id=778,
        actor_profile_id="vice-778",
        actor_role_code="vice_council_member",
        replaced_role_code="council_member",
        replacement_profile_id="member-new-778",
        source_list_code="election_results",
        already_active_profile_ids=("vice-778",),
        source_platform="discord",
    )

    assert result["ok"] is False
    assert sequence == ["status_upsert", "discord_role", "journal", "journal"]
    assert triggered == [("member-new-778", "council_member_replacement_role_grant_failed")]


def test_council_service_classifies_discord_link_missing_reason():
    reason = council_service._classify_discord_sync_reason("discord_api_error", "timeout", discord_user_id=None)
    assert reason == "discord_link_missing"


def test_council_service_blocks_question_voting_start_when_pause_enabled(monkeypatch):
    import importlib

    council_service_module = importlib.import_module("bot.services.council_service")

    monkeypatch.setattr(
        council_service_module.CouncilPauseService,
        "sync_pause_state",
        staticmethod(lambda **_kwargs: {"paused": True, "reason": "term_ended_without_launch_confirmation"}),
    )

    blocked = council_service.decide_question_start_voting(
        question_id=900,
        current_status="discussion",
        actor_profile_id="mod-900",
        source_platform="telegram",
    )

    assert blocked.accepted is False
    assert blocked.reason == "council_paused"
