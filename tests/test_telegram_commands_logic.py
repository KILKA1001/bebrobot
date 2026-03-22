import unittest
from unittest.mock import patch

from bot.telegram_bot.systems.commands_logic import (
    get_helpy_text,
    process_link_command,
    process_link_discord_command,
    prepare_roles_catalog_pages,
    process_profile_command,
    process_roles_catalog_command,
    render_roles_catalog_page,
)


class TelegramCommandsLogicTests(unittest.TestCase):
    @patch("bot.telegram_bot.systems.commands_logic.AccountsService.get_profile_by_account")
    @patch("bot.telegram_bot.systems.commands_logic.AccountsService.resolve_account_id")
    def test_profile_uses_target_user_from_reply(self, mock_resolve_account_id, mock_get_profile):
        mock_resolve_account_id.return_value = "acc-200"
        mock_get_profile.return_value = {
            "custom_nick": "Target",
            "description": "desc",
            "nulls_brawl_id": "NB123",
            "link_status": "linked",
            "nulls_status": "linked",
            "points": 10,
        }

        result = process_profile_command(
            telegram_user_id=100,
            display_name="Caller",
            target_telegram_user_id=200,
            target_display_name="Target User",
        )

        mock_resolve_account_id.assert_called_once_with("telegram", "200")
        mock_get_profile.assert_called_once_with("acc-200", display_name="Target User")
        self.assertIn('tg://user?id=200', result)
        self.assertIn("Target", result)

    @patch("bot.telegram_bot.systems.commands_logic.AccountsService.get_profile_by_account")
    @patch("bot.telegram_bot.systems.commands_logic.AccountsService.resolve_account_id")
    def test_profile_without_target_uses_caller(self, mock_resolve_account_id, mock_get_profile):
        mock_resolve_account_id.return_value = "acc-100"
        mock_get_profile.return_value = {
            "custom_nick": "Caller",
            "description": "desc",
            "nulls_brawl_id": "NB321",
            "link_status": "linked",
            "nulls_status": "linked",
            "points": 15,
        }

        result = process_profile_command(telegram_user_id=100, display_name="Caller")

        mock_resolve_account_id.assert_called_once_with("telegram", "100")
        mock_get_profile.assert_called_once_with("acc-100", display_name="Caller")
        self.assertIn('tg://user?id=100', result)

    def test_profile_transport_error_logs_explicitly(self):
        with self.assertLogs("bot.telegram_bot.systems.commands_logic", level="ERROR") as captured:
            result = process_profile_command(telegram_user_id=None)

        self.assertEqual(result, "❌ Не удалось определить пользователя Telegram.")
        self.assertIn("transport identity error", captured.output[0])
        self.assertIn("field=telegram_user_id", captured.output[0])
        self.assertIn("action=extract_platform_user_id", captured.output[0])

    def test_helpy_contains_profile_edit(self):
        self.assertIn("/profile_edit", get_helpy_text())
        self.assertIn("/roles", get_helpy_text())
        self.assertIn("/points", get_helpy_text())
        self.assertIn("/balance", get_helpy_text())
        self.assertIn("/tickets", get_helpy_text())
        self.assertIn("/roles_admin / /rolesadmin", get_helpy_text())

    @patch("bot.telegram_bot.systems.commands_logic.RoleManagementService.list_public_roles_catalog")
    def test_roles_catalog_command_renders_description_and_acquire_hint(self, mock_list_roles_grouped):
        mock_list_roles_grouped.return_value = [
            {
                "category": "Турниры",
                "roles": [
                    {
                        "name": "Чемпион",
                        "description": "Победитель сезона",
                        "acquire_method_label": "выдаёт администратор",
                        "acquire_hint": "Выиграть сезонный турнир",
                    }
                ],
            }
        ]

        payload = prepare_roles_catalog_pages()
        result = render_roles_catalog_page(payload["pages"][0])

        self.assertIn("Каталог ролей", result)
        self.assertIn("Что это", result)
        self.assertIn("сейчас показана страница <b>1/1</b>", result)
        self.assertIn("используй кнопки ниже", result.lower())
        self.assertIn("Где смотреть способ получения", result)
        self.assertIn("выдаются вручную", result)
        self.assertIn("Чемпион", result)
        self.assertIn("Победитель сезона", result)
        self.assertIn("выдаёт администратор", result)
        self.assertIn("Выиграть сезонный турнир", result)

    @patch("bot.telegram_bot.systems.commands_logic.RoleManagementService.list_public_roles_catalog")
    def test_roles_catalog_command_uses_placeholder_when_acquire_hint_missing(self, mock_list_roles_grouped):
        mock_list_roles_grouped.return_value = [
            {
                "category": "Общие",
                "roles": [
                    {
                        "name": "Новичок",
                        "description": "",
                        "acquire_method_label": "за баллы",
                        "acquire_hint": "",
                    }
                ],
            }
        ]

        payload = prepare_roles_catalog_pages()
        result = render_roles_catalog_page(payload["pages"][0])

        self.assertIn("Новичок", result)
        self.assertIn("Описание пока не указано администратором", result)
        self.assertIn("за баллы", result)
        self.assertIn("Способ получения пока не указан администратором", result)

    @patch("bot.telegram_bot.systems.commands_logic.RoleManagementService.list_public_roles_catalog")
    def test_process_roles_catalog_command_uses_requested_page(self, mock_list_roles_grouped):
        mock_list_roles_grouped.return_value = [
            {
                "category": "Первая",
                "roles": [{"name": f"R{i}", "description": "", "acquire_method_label": "выдаёт администратор", "acquire_hint": ""} for i in range(1, 9)],
            },
            {
                "category": "Вторая",
                "roles": [{"name": "R9", "description": "", "acquire_method_label": "за баллы", "acquire_hint": ""}],
            },
        ]

        result = process_roles_catalog_command(page=1)

        self.assertIn("сейчас показана страница <b>2/2</b>", result)
        self.assertIn("<b>Вторая</b>", result)

    def test_link_command_restricted_to_private_chat(self):
        result = process_link_command('/link ABC123', telegram_user_id=100, is_private_chat=False)
        self.assertEqual(result, '❌ Команда привязки доступна только в личных сообщениях с ботом.')

    def test_link_discord_command_restricted_to_private_chat(self):
        result = process_link_discord_command(telegram_user_id=100, is_private_chat=False)
        self.assertEqual(result, '❌ Команда привязки доступна только в личных сообщениях с ботом.')


if __name__ == "__main__":
    unittest.main()
