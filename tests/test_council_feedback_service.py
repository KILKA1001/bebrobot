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

