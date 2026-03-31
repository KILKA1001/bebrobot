"""
Назначение: модуль "test guiy admin service" реализует продуктовый контур в зоне Discord/Telegram/общая логика (тесты).
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Discord/Telegram/общая логика (тесты).
"""

import unittest
from unittest.mock import patch

from bot.services.guiy_admin_service import (
    GUIY_OWNER_REPLY_REQUIRED_MESSAGE,
    authorize_guiy_owner_action,
    bootstrap_guiy_profile,
    parse_guiy_owner_profile_payload,
    resolve_guiy_owner_telegram_ids,
    resolve_guiy_target_account,
)
from bot.services.guiy_owner_flow_service import execute_guiy_owner_flow


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


class GuiyProfileBootstrapTests(unittest.TestCase):
    @patch("bot.services.guiy_admin_service.AccountsService.resolve_account_id", side_effect=[None, "guiy-acc"])
    @patch("bot.services.guiy_admin_service.AccountsService.register_identity", return_value=(True, "Регистрация завершена"))
    def test_bootstrap_creates_missing_profile(self, register_mock, resolve_mock):
        result = bootstrap_guiy_profile(provider="telegram", bot_user_id="999")

        self.assertTrue(result.ok)
        self.assertTrue(result.created)
        self.assertEqual(result.status, "created")
        self.assertEqual(result.guiy_account_id, "guiy-acc")
        self.assertIn("Теперь можно открыть редактирование профиля", result.message)
        resolve_mock.assert_any_call("telegram", "999")
        register_mock.assert_called_once_with("telegram", "999")

    @patch("bot.services.guiy_admin_service.AccountsService.resolve_account_id", return_value="guiy-acc")
    @patch("bot.services.guiy_admin_service.AccountsService.register_identity")
    def test_bootstrap_returns_neutral_message_when_profile_exists(self, register_mock, resolve_mock):
        result = bootstrap_guiy_profile(provider="discord", bot_user_id="555")

        self.assertTrue(result.ok)
        self.assertFalse(result.created)
        self.assertEqual(result.status, "already_exists")
        self.assertEqual(result.guiy_account_id, "guiy-acc")
        self.assertIn("Профиль Гуя уже зарегистрирован", result.message)
        resolve_mock.assert_called_once_with("discord", "555")
        register_mock.assert_not_called()

    @patch("bot.services.guiy_owner_flow_service.authorize_guiy_owner_action", return_value=type("Access", (), {"allowed": True})())
    @patch("bot.services.guiy_owner_flow_service.bootstrap_guiy_profile")
    def test_register_flow_returns_clear_error_message(self, bootstrap_mock, _access_mock):
        bootstrap_mock.return_value = type(
            "BootstrapResult",
            (),
            {
                "ok": False,
                "message": "❌ Не удалось зарегистрировать профиль Гуя. Причина: База данных недоступна.",
                "guiy_account_id": None,
            },
        )()

        result = execute_guiy_owner_flow(
            provider="telegram",
            actor_user_id="42",
            bot_user_id="999",
            selected_action="register_profile",
        )

        self.assertFalse(result.ok)
        self.assertIn("Не удалось зарегистрировать профиль Гуя", result.message)
        self.assertIn("База данных недоступна", result.message)
        bootstrap_mock.assert_called_once_with(provider="telegram", bot_user_id="999")

if __name__ == "__main__":
    unittest.main()
