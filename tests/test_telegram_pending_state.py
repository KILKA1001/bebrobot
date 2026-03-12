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
