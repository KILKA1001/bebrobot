from types import SimpleNamespace

from bot.services.council_feedback_service import CouncilFeedbackService


def test_archive_filters_by_period_status_type_and_adds_final_comment(monkeypatch):
    rows = [
        {"id": 1, "decision_code": "accepted_question", "decision_text": "Комментарий 1", "decided_at": "2026-04-10T10:00:00+00:00"},
        {"id": 2, "decision_code": "rejected_election", "decision_text": "Комментарий 2", "decided_at": "2026-03-01T10:00:00+00:00"},
        {"id": 3, "decision_code": "pending_question", "decision_text": "Комментарий 3", "decided_at": "2025-01-01T10:00:00+00:00"},
    ]

    class _Table:
        def select(self, *_args, **_kwargs):
            return self

        def order(self, *_args, **_kwargs):
            return self

        def limit(self, *_args, **_kwargs):
            return self

        def execute(self):
            return SimpleNamespace(data=rows)

    monkeypatch.setattr("bot.services.council_feedback_service.db.supabase", SimpleNamespace(table=lambda _name: _Table()))

    filtered = CouncilFeedbackService.get_decisions_archive(
        limit=5,
        period_code="90d",
        status_code="accepted",
        question_type_code="general",
    )

    assert len(filtered) == 1
    assert filtered[0]["id"] == 1
    assert filtered[0]["final_comment"] == "Комментарий 1"
    assert filtered[0]["archive_status_code"] == "accepted"
    assert filtered[0]["archive_question_type_code"] == "general"



def test_submit_proposal_when_pause_enabled_sets_waiting_launch_status(monkeypatch):
    inserted_payloads: list[dict[str, object]] = []

    class _TermsTable:
        def __init__(self):
            self._status = None

        def select(self, *_args, **_kwargs):
            return self

        def eq(self, field: str, value: str):
            if field == "status":
                self._status = value
            return self

        def order(self, *_args, **_kwargs):
            return self

        def limit(self, *_args, **_kwargs):
            return self

        def execute(self):
            if self._status == "active":
                return SimpleNamespace(data=[])
            return SimpleNamespace(data=[{"id": 77}])

    class _QuestionsTable:
        def insert(self, payload: dict[str, object]):
            inserted_payloads.append(payload)
            return self

        def execute(self):
            return SimpleNamespace(data=[{"id": 501, "status": "draft"}])

    class _Supabase:
        def table(self, name: str):
            if name == "council_terms":
                return _TermsTable()
            if name == "council_questions":
                return _QuestionsTable()
            raise AssertionError(name)

    monkeypatch.setattr("bot.services.council_feedback_service.db.supabase", _Supabase())
    monkeypatch.setattr(
        "bot.services.council_feedback_service.CouncilPauseService.sync_pause_state",
        staticmethod(lambda **_kwargs: {"paused": True, "reason": "term_ended_without_launch_confirmation"}),
    )
    monkeypatch.setattr(
        "bot.services.council_feedback_service.CouncilFeedbackService._resolve_account_id",
        staticmethod(lambda _provider, _provider_user_id: "acc-1"),
    )

    result = CouncilFeedbackService.submit_proposal(
        provider="telegram",
        provider_user_id="100",
        title="Новый вопрос",
        proposal_text="Очень важное предложение для запуска следующего шага.",
    )

    assert result["ok"] is True
    assert result["status"] == "awaiting_term_launch"
    assert "Ожидает запуска созыва" in result["status_label"]
    assert inserted_payloads and inserted_payloads[0]["term_id"] == 77
