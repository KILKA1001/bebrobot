import unittest
from types import SimpleNamespace
from unittest.mock import patch

from bot.telegram_bot.commands.roles_admin import (
    PendingRolesAdminAction,
    _PENDING_ACTIONS,
    _build_actions_keyboard,
    _build_home_keyboard,
    _build_pick_role_keyboard,
    _build_position_choice_keyboard,
    _build_user_role_categories_keyboard,
    _build_user_role_picker_keyboard,
    _render_home_text,
    _render_fallback_text,
    _render_help_text,
    _render_list_text,
    _render_position_picker_text,
    _render_user_role_flow_text,
    _resolve_telegram_target,
)


class TelegramRolesAdminTargetResolutionTests(unittest.TestCase):
    def test_resolve_target_uses_reply_when_explicit_target_missing(self):
        reply_user = SimpleNamespace(id=777, username="reply_target", full_name="Reply Target", is_bot=False)

        with patch("bot.telegram_bot.commands.roles_admin.AccountsService.resolve_account_id", return_value="acc-777"):
            result = _resolve_telegram_target(
                actor_id=100,
                raw_target=None,
                reply_user=reply_user,
                operation="user_roles",
                source="button",
            )

        self.assertEqual(result["provider_user_id"], "777")
        self.assertEqual(result["label"], "@reply_target")
        self.assertEqual(result["account_id"], "acc-777")

    def test_resolve_target_returns_multiple_error_for_ambiguous_username(self):
        with patch(
            "bot.telegram_bot.commands.roles_admin.AccountsService.resolve_user_lookup",
            return_value={
                "status": "multiple",
                "candidates": [
                    {
                        "provider": "telegram",
                        "provider_user_id": "1",
                        "username": "dup_user",
                        "display_name": "One",
                        "matched_by": "username",
                    },
                    {
                        "provider": "discord",
                        "provider_user_id": "2",
                        "username": "dup_user",
                        "display_name": "Two",
                        "matched_by": "display_name",
                    },
                ],
            },
        ):
            result = _resolve_telegram_target(
                actor_id=100,
                raw_target="@dup_user",
                reply_user=None,
                operation="user_grant",
                source="fallback_text_command",
            )

        self.assertEqual(result["error"], "multiple")
        self.assertIn("Найдено несколько кандидатов", result["message"])
        self.assertIn("telegram | @dup_user | One | id=1 | via=username", result["message"])
        self.assertIn("discord | @dup_user | Two | id=2 | via=display_name", result["message"])

    def test_resolve_target_returns_not_found_for_unknown_username(self):
        with patch(
            "bot.telegram_bot.commands.roles_admin.AccountsService.resolve_user_lookup",
            return_value={"status": "not_found", "candidates": [], "reason": "not_found"},
        ):
            result = _resolve_telegram_target(
                actor_id=100,
                raw_target="missing_user",
                reply_user=None,
                operation="user_revoke",
                source="fallback_text_command",
            )

        self.assertEqual(result["error"], "not_found")
        self.assertIn("локальном реестре", result["message"])
        self.assertIn("/register", result["message"])
        self.assertIn("reply", result["message"])

    def test_resolve_target_supports_cross_provider_prefix(self):
        with patch(
            "bot.telegram_bot.commands.roles_admin.AccountsService.resolve_user_lookup",
            return_value={
                "status": "ok",
                "result": {
                    "account_id": "acc-1",
                    "provider": "discord",
                    "provider_user_id": "555",
                    "username": "discord_target",
                    "display_name": "Discord Target",
                    "matched_by": "discord_username",
                },
                "candidates": [],
            },
        ):
            result = _resolve_telegram_target(
                actor_id=100,
                raw_target="ds:discord_target",
                reply_user=None,
                operation="user_grant",
                source="fallback_text_command",
            )

        self.assertEqual(result["provider"], "discord")
        self.assertEqual(result["provider_user_id"], "555")
        self.assertEqual(result["account_id"], "acc-1")
        self.assertEqual(result["matched_by"], "discord_username")

    def test_resolve_target_supports_telegram_dm_username_lookup(self):
        with patch(
            "bot.telegram_bot.commands.roles_admin.AccountsService.resolve_user_lookup",
            return_value={
                "status": "ok",
                "result": {
                    "account_id": "acc-2",
                    "provider": "telegram",
                    "provider_user_id": "999",
                    "username": "dm_target",
                    "display_name": "DM Target",
                    "matched_by": "username",
                },
                "candidates": [],
            },
        ):
            result = _resolve_telegram_target(
                actor_id=100,
                raw_target="@dm_target",
                reply_user=None,
                operation="user_roles",
                source="fallback_text_command",
            )

        self.assertEqual(result["provider"], "telegram")
        self.assertEqual(result["provider_user_id"], "999")
        self.assertEqual(result["label"], "@dm_target")
        self.assertEqual(result["matched_by"], "username")

    def test_role_delete_picker_hides_external_roles_but_move_keeps_them(self):
        grouped = [
            {
                "category": "General",
                "roles": [
                    {"name": "External", "is_discord_managed": True, "discord_role_id": "1"},
                    {"name": "Custom", "is_discord_managed": False, "discord_role_id": None},
                ],
            }
        ]

        delete_keyboard = _build_pick_role_keyboard(grouped, actor_id=10, operation="role_delete", page=0)
        move_keyboard = _build_pick_role_keyboard(grouped, actor_id=10, operation="role_move", page=0)
        delete_texts = [button.text for row in delete_keyboard.inline_keyboard for button in row]
        move_texts = [button.text for row in move_keyboard.inline_keyboard for button in row]

        self.assertFalse(any("External" in text for text in delete_texts))
        self.assertTrue(any("Custom" in text for text in delete_texts))
        self.assertTrue(any("External" in text for text in move_texts))

    def test_home_keyboard_hides_categories_section_without_permission(self):
        keyboard = _build_home_keyboard(actor_id=10, can_manage_categories=False)

        texts = [button.text for row in keyboard.inline_keyboard for button in row]

        self.assertNotIn("🗂 Категории", texts)
        self.assertIn("🪪 Роли", texts)
        self.assertIn("👥 Пользователи", texts)

    def test_home_keyboard_shows_categories_section_with_permission(self):
        keyboard = _build_home_keyboard(actor_id=10, can_manage_categories=True)

        texts = [button.text for row in keyboard.inline_keyboard for button in row]

        self.assertIn("🗂 Категории", texts)

    def test_actions_keyboard_shows_only_category_actions(self):
        keyboard = _build_actions_keyboard(actor_id=10, section="categories", can_manage_categories=True)

        texts = [button.text for row in keyboard.inline_keyboard for button in row]

        self.assertIn("🗂 Создать категорию", texts)
        self.assertIn("↕️ Порядок категории", texts)
        self.assertIn("🗑 Удалить категорию", texts)
        self.assertNotIn("➕ Создать роль", texts)
        self.assertNotIn("🧾 Роли пользователя", texts)

    def test_actions_keyboard_shows_only_user_actions(self):
        keyboard = _build_actions_keyboard(actor_id=10, section="users", can_manage_categories=False)

        texts = [button.text for row in keyboard.inline_keyboard for button in row]

        self.assertIn("🧾 Роли пользователя", texts)
        self.assertIn("✅ Выдать роль", texts)
        self.assertIn("❌ Снять роль", texts)
        self.assertNotIn("🗂 Создать категорию", texts)
        self.assertNotIn("➕ Создать роль", texts)

    def test_position_picker_renders_all_available_positions(self):
        preview = {
            "insertion_positions": [
                {"position": 0, "human_index": 1, "description": "будет добавлено в начало (#1)"},
                {"position": 1, "human_index": 2, "description": "будет добавлено в конец (#2)"},
            ]
        }

        keyboard = _build_position_choice_keyboard(actor_id=10, operation="role_position", preview=preview)
        texts = [button.text for row in keyboard.inline_keyboard for button in row]

        self.assertIn("#1 — будет добавлено в начало (#1)", texts)
        self.assertIn("#2 — будет добавлено в конец (#2)", texts)

    def test_position_picker_text_explains_default_last_position(self):
        preview = {
            "current_roles": [{"name": "Alpha"}, {"name": "Beta"}],
            "position_description": "будет добавлено в конец (#3)",
        }

        text = _render_position_picker_text(
            mode="move",
            category_name="General",
            preview=preview,
            role_name="Gamma",
        )

        self.assertIn("Если ничего не менять, роль будет добавлена последней", text)
        self.assertIn("• #1. Alpha", text)
        self.assertIn("будет добавлено в конец (#3)", text)

    def test_help_and_fallback_texts_describe_position_parity(self):
        self.assertIn("отдельный экран выбора точной позиции", _render_fallback_text())
        self.assertIn("роль будет добавлена последней", _render_help_text())
        self.assertIn("role_edit_description", _render_fallback_text())
        self.assertIn("role_edit_acquire_hint", _render_fallback_text())
        self.assertIn("Описание роли", _render_help_text())
        self.assertIn("как получить", _render_help_text())

    def test_home_text_explains_hidden_buttons(self):
        text = _render_home_text(hidden_sections=("categories",))

        self.assertIn("Некоторые кнопки скрыты, потому что у вас нет нужных полномочий", text)
        self.assertIn("Категории", text)

    def test_render_list_text_shows_role_description_and_legacy_rows_without_it(self):
        grouped = [
            {
                "category": "General",
                "roles": [
                    {"name": "Alpha", "discord_role_id": "1", "description": "Первое описание"},
                    {"name": "Legacy", "discord_role_id": None, "description": "", "acquire_hint": ""},
                ],
            }
        ]

        text = _render_list_text(grouped, 0)

        self.assertIn("Первое описание", text)
        self.assertIn("Legacy", text)
        self.assertIn("Как получить", text)
        self.assertIn("Способ получения пока не указан администратором", text)

    def test_user_role_flow_text_shows_multi_select_summary_and_continue_hint(self):
        text = _render_user_role_flow_text(
            target_label="@target",
            action="grant",
            selected_roles=["Alpha", "Beta"],
            current_category="General",
        )

        self.assertIn("Будет выдано", text)
        self.assertIn("Alpha", text)
        self.assertIn("Beta", text)
        self.assertIn("Будет снято", text)
        self.assertIn("Уже выбрано ролей: <b>2</b>", text)
        self.assertIn("можно продолжать по другим категориям", text)

    def test_user_role_category_and_picker_keyboards_keep_multi_select_state(self):
        grouped = [
            {"category": "General", "roles": [{"name": "Alpha"}, {"name": "Beta"}]},
            {"category": "Events", "roles": [{"name": "Gamma"}]},
        ]

        categories_keyboard = _build_user_role_categories_keyboard(grouped, actor_id=10, action="grant", selected_roles=["Alpha", "Gamma"])
        picker_keyboard = _build_user_role_picker_keyboard(grouped, actor_id=10, action="grant", category_idx=0, selected_roles=["Alpha", "Gamma"])

        category_texts = [button.text for row in categories_keyboard.inline_keyboard for button in row]
        picker_texts = [button.text for row in picker_keyboard.inline_keyboard for button in row]

        self.assertIn("📂 General [1]", category_texts)
        self.assertIn("📂 Events [1]", category_texts)
        self.assertIn("🚀 Подтвердить пакет (2)", category_texts)
        self.assertIn("✅ Alpha", picker_texts)
        self.assertIn("⬜️ Beta", picker_texts)

    def test_pending_user_role_flow_state_supports_reentering_another_category(self):
        _PENDING_ACTIONS[77] = PendingRolesAdminAction(
            operation="user_role_flow_panel",
            created_at=1.0,
            payload={
                "action": "grant",
                "label": "@target",
                "account_id": "acc-7",
                "selected_roles": ["Alpha"],
            },
        )
        try:
            grouped = [
                {"category": "General", "roles": [{"name": "Alpha"}]},
                {"category": "Events", "roles": [{"name": "Gamma"}]},
            ]

            first_keyboard = _build_user_role_picker_keyboard(grouped, actor_id=77, action="grant", category_idx=0, selected_roles=["Alpha"])
            second_keyboard = _build_user_role_picker_keyboard(grouped, actor_id=77, action="grant", category_idx=1, selected_roles=["Alpha", "Gamma"])
            first_texts = [button.text for row in first_keyboard.inline_keyboard for button in row]
            second_texts = [button.text for row in second_keyboard.inline_keyboard for button in row]

            self.assertIn("✅ Alpha", first_texts)
            self.assertIn("✅ Gamma", second_texts)
            self.assertIn("🗂 К категориям", second_texts)
        finally:
            _PENDING_ACTIONS.pop(77, None)


if __name__ == "__main__":
    unittest.main()
