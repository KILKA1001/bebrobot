import unittest
from unittest.mock import patch

from bot.services.guiy_admin_service import (
    GUIY_OWNER_REPLY_REQUIRED_MESSAGE,
    authorize_guiy_owner_action,
    parse_guiy_owner_profile_payload,
    resolve_guiy_owner_telegram_ids,
    resolve_guiy_target_account,
)


class GuiyAdminServiceTests(unittest.TestCase):
    @patch("bot.services.guiy_admin_service.AccountsService.resolve_account_id", return_value="father-acc")
    @patch("bot.services.guiy_admin_service._is_father_user", return_value=True)
    def test_authorize_allows_owner(self, father_mock, resolve_mock):
        result = authorize_guiy_owner_action(
            actor_provider="telegram",
            actor_user_id="111",
            requested_action="say",
            target_message_id="77",
        )

        self.assertTrue(result.allowed)
        self.assertEqual(result.resolved_account_id, "father-acc")
        resolve_mock.assert_called_once_with("telegram", "111")
        father_mock.assert_called_once_with("telegram", "111")

    @patch("bot.services.guiy_admin_service.AccountsService.resolve_account_id", return_value="other-acc")
    @patch("bot.services.guiy_admin_service._is_father_user", return_value=False)
    def test_authorize_denies_non_owner(self, father_mock, resolve_mock):
        result = authorize_guiy_owner_action(
            actor_provider="telegram",
            actor_user_id="222",
            requested_action="profile",
            target_message_id=None,
        )

        self.assertFalse(result.allowed)
        self.assertEqual(result.resolved_account_id, "other-acc")
        resolve_mock.assert_called_once_with("telegram", "222")
        father_mock.assert_called_once_with("telegram", "222")


    @patch.dict("os.environ", {"GUIY_FATHER_TELEGRAM_IDS": "101, 202"}, clear=True)
    def test_resolve_guiy_owner_telegram_ids_from_env(self):
        self.assertEqual(resolve_guiy_owner_telegram_ids(), [101, 202])

    def test_target_resolution_requires_reply_when_not_explicit(self):
        result = resolve_guiy_target_account(
            provider="telegram",
            bot_user_id="999",
            reply_author_user_id=None,
            explicit_owner_command=False,
        )

        self.assertFalse(result.ok)
        self.assertEqual(result.message, GUIY_OWNER_REPLY_REQUIRED_MESSAGE)

    @patch("bot.services.guiy_admin_service.AccountsService.resolve_account_id", return_value=None)
    def test_target_resolution_denies_when_guiy_account_not_linked(self, resolve_mock):
        result = resolve_guiy_target_account(
            provider="discord",
            bot_user_id="555",
            reply_author_user_id="555",
            explicit_owner_command=True,
        )

        self.assertFalse(result.ok)
        self.assertIsNone(result.target_account_id)
        resolve_mock.assert_called_once_with("discord", "555")

    def test_parse_profile_payload_accepts_clear_value(self):
        field_name, field_value = parse_guiy_owner_profile_payload("description | -")
        self.assertEqual(field_name, "description")
        self.assertEqual(field_value, "")


if __name__ == "__main__":
    unittest.main()
