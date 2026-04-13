from types import SimpleNamespace

from bot.services.council_pause_service import CouncilPauseService


def test_sync_pause_state_writes_required_audit_fields(monkeypatch):
    audit_rows: list[dict[str, object]] = []

    class _TermsQuery:
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
            if self._status == "pending_launch_confirmation":
                return SimpleNamespace(data=[])
            return SimpleNamespace(data=[{"id": 10, "status": "active", "ends_at": "2026-04-01T00:00:00+00:00"}])

    class _AuditQuery:
        def __init__(self):
            self._insert_payload = None

        def select(self, *_args, **_kwargs):
            return self

        def eq(self, *_args, **_kwargs):
            return self

        def order(self, *_args, **_kwargs):
            return self

        def limit(self, *_args, **_kwargs):
            return self

        def insert(self, payload: dict[str, object]):
            self._insert_payload = payload
            return self

        def execute(self):
            if self._insert_payload is not None:
                audit_rows.append(self._insert_payload)
                return SimpleNamespace(data=[self._insert_payload])
            return SimpleNamespace(data=[])

    class _Supabase:
        def table(self, name: str):
            if name == "council_terms":
                return _TermsQuery()
            if name == "council_audit_log":
                return _AuditQuery()
            if name == "council_term_launch_confirmations":
                return _AuditQuery()
            raise AssertionError(name)

    monkeypatch.setattr("bot.services.council_pause_service.db.supabase", _Supabase())

    state = CouncilPauseService.sync_pause_state(platform="telegram", user_id="321")

    assert state["paused"] is True
    assert audit_rows
    details = audit_rows[0]["details"]
    assert details["operation_code"] == "council.lifecycle.pause_mode"
    assert details["reason"] == "term_ended_without_launch_confirmation"
    assert details["platform"] == "telegram"
    assert details["user_id"] == "321"
    assert str(details["entity_id"]) == "10"
