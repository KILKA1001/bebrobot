from types import SimpleNamespace

from bot.services.council_system_events_service import CouncilSystemEventsService


def test_publish_decision_requires_separate_confirmation(monkeypatch):
    monkeypatch.setattr(CouncilSystemEventsService, "get_channel", staticmethod(lambda _provider: "room-1"))

    result = CouncilSystemEventsService.publish_event(
        provider="telegram",
        event_code="decision_published",
        publisher=lambda _destination, _text: True,
        title="Тест",
    )

    assert result.get("ok") is False
    assert result.get("reason") == "confirmation_required"


def test_superadmin_can_save_and_clear_channel(monkeypatch):
    upserts: list[dict[str, object]] = []
    deleted_providers: list[str] = []
    audit_rows: list[dict[str, object]] = []

    class _Table:
        def __init__(self, name: str):
            self.name = name
            self._provider = ""

        def upsert(self, payload: dict[str, object], on_conflict: str | None = None):
            upserts.append({"payload": payload, "on_conflict": on_conflict})
            return self

        def delete(self):
            return self

        def insert(self, payload: dict[str, object]):
            if self.name == "council_audit_log":
                audit_rows.append(payload)
            return self

        def eq(self, field: str, value: str):
            if field == "provider":
                self._provider = value
            return self

        def execute(self):
            if self._provider:
                deleted_providers.append(self._provider)
            return SimpleNamespace(data=[])

    supabase = SimpleNamespace(table=lambda name: _Table(name))
    monkeypatch.setattr("bot.services.council_system_events_service.db.supabase", supabase)
    monkeypatch.setattr(
        "bot.services.council_system_events_service.AuthorityService.is_super_admin",
        staticmethod(lambda provider, actor_id: provider == "telegram" and actor_id == "101"),
    )

    save_result = CouncilSystemEventsService.set_channel(
        provider="telegram",
        actor_user_id="101",
        destination_id="-100123",
    )
    clear_result = CouncilSystemEventsService.set_channel(
        provider="telegram",
        actor_user_id="101",
        destination_id="",
    )

    assert save_result["ok"] is True
    assert upserts and upserts[0]["payload"]["destination_id"] == "-100123"
    assert clear_result["ok"] is True
    assert "telegram" in deleted_providers
    assert len(audit_rows) >= 2
    assert all("details" in row for row in audit_rows)
    assert any(row.get("details", {}).get("action") == row.get("action") for row in audit_rows)
    assert any(row.get("details", {}).get("actor_user_id") == "101" for row in audit_rows)
    assert any(row.get("action") == "set_channel" and row.get("status") == "success" for row in audit_rows)
    assert any(row.get("action") == "clear_channel" and row.get("status") == "success" for row in audit_rows)


def test_non_superadmin_cannot_set_channel(monkeypatch):
    monkeypatch.setattr(
        "bot.services.council_system_events_service.AuthorityService.is_super_admin",
        staticmethod(lambda _provider, _actor_id: False),
    )
    result = CouncilSystemEventsService.set_channel(
        provider="discord",
        actor_user_id="777",
        destination_id="1:2",
    )
    assert result["ok"] is False
    assert result["reason"] == "forbidden"


def test_publish_updates_existing_bound_message(monkeypatch):
    monkeypatch.setattr(CouncilSystemEventsService, "get_channel", staticmethod(lambda _provider: "room-1"))
    monkeypatch.setattr(
        CouncilSystemEventsService,
        "get_event_message_binding",
        staticmethod(lambda **_kwargs: {"destination_id": "room-1", "message_id": "555"}),
    )

    called = {"edited": False, "published": False}

    def _editor(destination_id: str, message_id: str, text: str) -> bool:
        called["edited"] = destination_id == "room-1" and message_id == "555" and bool(text)
        return True

    def _publisher(_destination_id: str, _text: str) -> object:
        called["published"] = True
        return {"message_id": "777"}

    result = CouncilSystemEventsService.publish_event(
        provider="telegram",
        event_code="election_progress",
        event_key="election:42",
        publisher=_publisher,
        editor=_editor,
    )

    assert result.get("ok") is True
    assert result.get("updated") is True
    assert result.get("message_id") == "555"
    assert called["edited"] is True
    assert called["published"] is False


def test_publish_fallbacks_to_new_message_and_rebinds_on_edit_error(monkeypatch):
    monkeypatch.setattr(CouncilSystemEventsService, "get_channel", staticmethod(lambda _provider: "room-2"))
    monkeypatch.setattr(
        CouncilSystemEventsService,
        "get_event_message_binding",
        staticmethod(lambda **_kwargs: {"destination_id": "room-2", "message_id": "old-1"}),
    )

    saved_payloads: list[dict[str, str]] = []
    monkeypatch.setattr(
        CouncilSystemEventsService,
        "save_event_message_binding",
        staticmethod(lambda **kwargs: saved_payloads.append(kwargs) or True),
    )

    result = CouncilSystemEventsService.publish_event(
        provider="discord",
        event_code="voting_started",
        event_key="question:9",
        publisher=lambda _destination_id, _text: {"message_id": "new-88"},
        editor=lambda _destination_id, _message_id, _text: False,
    )

    assert result.get("ok") is True
    assert result.get("updated") is False
    assert result.get("message_id") == "new-88"
    assert saved_payloads and saved_payloads[0]["event_key"] == "question:9"
    assert saved_payloads[0]["message_id"] == "new-88"
