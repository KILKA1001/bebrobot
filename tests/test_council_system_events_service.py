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

    class _Table:
        def __init__(self, name: str):
            self.name = name
            self._provider = ""

        def upsert(self, payload: dict[str, object], on_conflict: str | None = None):
            upserts.append({"payload": payload, "on_conflict": on_conflict})
            return self

        def delete(self):
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
