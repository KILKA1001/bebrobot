"""
Назначение: модуль "test telegram pending state" реализует продуктовый контур в зоне Discord/Telegram/общая логика (тесты).
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Discord/Telegram/общая логика (тесты).
"""

from unittest.mock import patch

from bot.telegram_bot.commands import engagement, linking
from bot.telegram_bot.commands.engagement import PendingAction


def test_has_pending_action_expires_stale_state():
    engagement._PENDING_ACTIONS.clear()
    engagement._PENDING_ACTIONS[1] = PendingAction(
        domain="points",
        operation="add",
        target_provider_user_id="2",
        actor_provider_user_id="1",
        created_at=1_000.0,
    )

    with patch("bot.telegram_bot.commands.engagement.time.time", return_value=1_700.0):
        assert engagement.has_pending_action(1) is False

    assert 1 not in engagement._PENDING_ACTIONS


def test_has_pending_profile_edit_expires_stale_state():
    linking._PENDING_EDIT_FIELD.clear()
    linking._PENDING_EDIT_FIELD_CREATED_AT.clear()
    linking._PENDING_EDIT_FIELD[10] = "description"
    linking._PENDING_EDIT_FIELD_CREATED_AT[10] = 100.0

    with patch("bot.telegram_bot.commands.linking.time.time", return_value=1_500.0):
        assert linking.has_pending_profile_edit(10) is False

    assert 10 not in linking._PENDING_EDIT_FIELD
    assert 10 not in linking._PENDING_EDIT_FIELD_CREATED_AT


def test_parse_callback_payload_supports_owner_id_and_legacy_format():
    assert engagement._parse_callback_payload("points:add:123:777") == ("add", "123", "777")
    assert engagement._parse_callback_payload("points:add:123") == ("add", "123", None)
    assert engagement._parse_callback_payload("points:add") is None


def test_points_permission_requires_vice_city_or_higher():
    assert engagement._can_manage_points(79) is False
    assert engagement._can_manage_points(80) is True


def test_own_edit_allowed_only_for_head_club_and_main_vice():
    assert engagement._can_manage_own_engagement(("Вице города",)) is False
    assert engagement._can_manage_own_engagement(("Глава клуба",)) is True
    assert engagement._can_manage_own_engagement(("Глава клубов",)) is True
    assert engagement._can_manage_own_engagement(("Главный вице",)) is True
