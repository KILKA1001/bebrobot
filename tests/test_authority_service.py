"""
Назначение: модуль "test authority service" реализует продуктовый контур в зоне Discord/Telegram/общая логика (тесты).
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Discord/Telegram/общая логика (тесты).
"""

import unittest
from unittest.mock import patch

from bot.services.authority_service import AuthorityService
from bot.services.profile_titles import normalize_protected_profile_title, protected_profile_title_canonical_keys


class AuthorityServiceTests(unittest.TestCase):
    @patch("bot.services.authority_service.AccountsService.get_account_titles")
    @patch("bot.services.authority_service.AccountsService.resolve_account_id")
    def test_resolve_authority_from_titles(self, mock_resolve, mock_titles):
        mock_resolve.return_value = "acc-1"
        mock_titles.return_value = ["Участник клубов", "Вице города"]

        result = AuthorityService.resolve_authority("discord", "100")

        self.assertEqual(result.level, 80)
        self.assertEqual(result.rank_weight, 80)
        self.assertEqual(result.account_id, "acc-1")

    @patch("bot.services.authority_service.AccountsService.get_account_titles")
    @patch("bot.services.authority_service.AccountsService.resolve_account_id")
    def test_permission_matrix(self, mock_resolve, mock_titles):
        mock_resolve.return_value = "acc-2"
        mock_titles.return_value = ["Ветеран города"]

        self.assertFalse(AuthorityService.has_command_permission("discord", "200", "points_manage"))
        self.assertFalse(AuthorityService.has_command_permission("discord", "200", "tickets_manage"))
        self.assertTrue(AuthorityService.has_command_permission("discord", "200", "moderation_mute"))
        self.assertTrue(AuthorityService.has_command_permission("discord", "200", "moderation_view_cases"))
        self.assertFalse(AuthorityService.has_command_permission("discord", "200", "moderation_warn"))
        self.assertFalse(AuthorityService.has_command_permission("discord", "200", "moderation_ban"))
        self.assertFalse(AuthorityService.has_command_permission("discord", "200", "moderation_manage_rules"))

        mock_titles.return_value = ["Младший админ"]
        self.assertTrue(AuthorityService.has_command_permission("discord", "200", "moderation_mute"))
        self.assertFalse(AuthorityService.has_command_permission("discord", "200", "moderation_warn"))

        mock_titles.return_value = ["Вице города"]
        self.assertTrue(AuthorityService.has_command_permission("discord", "200", "moderation_warn"))
        self.assertFalse(AuthorityService.has_command_permission("discord", "200", "moderation_ban"))

        mock_titles.return_value = ["Админ"]
        self.assertTrue(AuthorityService.has_command_permission("discord", "200", "moderation_warn"))
        self.assertFalse(AuthorityService.has_command_permission("discord", "200", "moderation_ban"))

        mock_titles.return_value = ["Главный вице"]
        self.assertTrue(AuthorityService.has_command_permission("discord", "200", "moderation_ban"))

        mock_titles.return_value = ["Глава клуба"]
        self.assertTrue(AuthorityService.has_command_permission("discord", "200", "moderation_ban"))

        mock_titles.return_value = ["Оператор"]
        self.assertTrue(AuthorityService.has_command_permission("discord", "200", "moderation_ban"))

    @patch("bot.services.authority_service.AccountsService.get_account_titles")
    @patch("bot.services.authority_service.AccountsService.resolve_account_id")
    def test_operator_is_limited_to_moderation_permissions(self, mock_resolve, mock_titles):
        mock_resolve.return_value = "acc-operator-scope"
        mock_titles.return_value = ["Оператор"]

        self.assertTrue(AuthorityService.has_command_permission("discord", "201", "moderation_mute"))
        self.assertTrue(AuthorityService.has_command_permission("discord", "201", "moderation_warn"))
        self.assertTrue(AuthorityService.has_command_permission("discord", "201", "moderation_ban"))
        self.assertFalse(AuthorityService.has_command_permission("discord", "201", "points_manage"))
        self.assertFalse(AuthorityService.has_command_permission("discord", "201", "bank_manage"))
        self.assertFalse(AuthorityService.has_command_permission("discord", "201", "tickets_manage"))

    @patch("bot.services.authority_service.AccountsService.get_account_titles")
    @patch("bot.services.authority_service.AccountsService.resolve_account_id")
    def test_operator_cannot_manage_roles_via_authority_service(self, mock_resolve, mock_titles):
        mock_resolve.return_value = "acc-operator-role"
        mock_titles.return_value = ["Оператор"]

        self.assertFalse(AuthorityService.can_manage_role("discord", "202", "админ"))

    @patch("bot.services.authority_service.AccountsService.get_account_titles")
    @patch("bot.services.authority_service.AccountsService.resolve_account_id")
    def test_hierarchy_allows_peer_roles_for_head_club_and_main_vice(self, mock_resolve, mock_titles):
        def _resolve(_provider, user_id):
            return f"acc-{user_id}"

        def _titles(account_id):
            if account_id == "acc-1":
                return ["Главный вице"]
            return ["Глава клуба"]

        mock_resolve.side_effect = _resolve
        mock_titles.side_effect = _titles

        self.assertTrue(AuthorityService.can_manage_target("discord", "1", "discord", "2"))

    @patch("bot.services.authority_service.AccountsService.get_account_titles")
    @patch("bot.services.authority_service.AccountsService.resolve_account_id")
    def test_hierarchy_allows_head_club_to_manage_main_vice(self, mock_resolve, mock_titles):
        def _resolve(_provider, user_id):
            return f"acc-{user_id}"

        def _titles(account_id):
            if account_id == "acc-1":
                return ["Глава клуба"]
            return ["Главный вице"]

        mock_resolve.side_effect = _resolve
        mock_titles.side_effect = _titles

        self.assertTrue(AuthorityService.can_manage_target("discord", "1", "discord", "2"))

    @patch("bot.services.authority_service.AccountsService.get_account_titles")
    @patch("bot.services.authority_service.AccountsService.resolve_account_id")
    def test_hierarchy_denies_operator_against_top_peer_roles(self, mock_resolve, mock_titles):
        def _resolve(_provider, user_id):
            return f"acc-{user_id}"

        def _titles(account_id):
            if account_id == "acc-operator":
                return ["Оператор"]
            if account_id == "acc-main-vice":
                return ["Главный вице"]
            return ["Глава клуба"]

        mock_resolve.side_effect = _resolve
        mock_titles.side_effect = _titles

        self.assertFalse(AuthorityService.can_manage_target("discord", "operator", "discord", "main-vice"))
        self.assertFalse(AuthorityService.can_manage_target("discord", "operator", "discord", "head"))

    @patch("bot.services.authority_service.AccountsService.get_account_titles")
    @patch("bot.services.authority_service.AccountsService.resolve_account_id")
    def test_hierarchy_denies_top_peer_roles_against_operator(self, mock_resolve, mock_titles):
        def _resolve(_provider, user_id):
            return f"acc-{user_id}"

        def _titles(account_id):
            if account_id == "acc-head":
                return ["Глава клуба"]
            return ["Оператор"]

        mock_resolve.side_effect = _resolve
        mock_titles.side_effect = _titles

        self.assertFalse(AuthorityService.can_manage_target("discord", "head", "discord", "operator"))

    @patch("bot.services.authority_service.AccountsService.get_account_titles")
    @patch("bot.services.authority_service.AccountsService.resolve_account_id")
    def test_hierarchy_still_prevents_equal_non_peer_roles(self, mock_resolve, mock_titles):
        def _resolve(_provider, user_id):
            return f"acc-{user_id}"

        def _titles(_account_id):
            return ["Вице города"]

        mock_resolve.side_effect = _resolve
        mock_titles.side_effect = _titles

        self.assertFalse(AuthorityService.can_manage_target("discord", "1", "discord", "2"))

    @patch("bot.services.authority_service.AccountsService.get_account_titles")
    @patch("bot.services.authority_service.AccountsService.resolve_account_id")
    def test_hierarchy_prevents_equal_operator_roles(self, mock_resolve, mock_titles):
        def _resolve(_provider, user_id):
            return f"acc-{user_id}"

        def _titles(_account_id):
            return ["Оператор"]

        mock_resolve.side_effect = _resolve
        mock_titles.side_effect = _titles

        self.assertFalse(AuthorityService.can_manage_target("discord", "1", "discord", "2"))

    @patch("bot.services.authority_service.AccountsService.get_account_titles")
    @patch("bot.services.authority_service.AccountsService.resolve_account_id")
    def test_self_manage_allowed_only_for_head_club_and_main_vice(self, mock_resolve, mock_titles):
        mock_resolve.return_value = "acc-self"

        mock_titles.return_value = ["Вице города"]
        self.assertFalse(AuthorityService.can_manage_self("discord", "1"))

        mock_titles.return_value = ["Глава клуба"]
        self.assertTrue(AuthorityService.can_manage_self("discord", "1"))

        mock_titles.return_value = ["Главный вице"]
        self.assertTrue(AuthorityService.can_manage_self("discord", "1"))

    @patch("bot.services.authority_service.AccountsService.get_account_titles")
    @patch("bot.services.authority_service.AccountsService.resolve_account_id")
    def test_can_manage_role_requires_vice_or_above(self, mock_resolve, mock_titles):
        mock_resolve.return_value = "acc-role"
        mock_titles.return_value = ["Ветеран города"]

        self.assertFalse(AuthorityService.can_manage_role("discord", "1", "оператор"))

    @patch("bot.services.authority_service.AccountsService.get_account_titles")
    @patch("bot.services.authority_service.AccountsService.resolve_account_id")
    def test_can_manage_role_allows_head_club_to_manage_high_roles(self, mock_resolve, mock_titles):
        mock_resolve.return_value = "acc-role2"
        mock_titles.return_value = ["Глава клуба"]

        self.assertTrue(AuthorityService.can_manage_role("discord", "1", "глава клуба"))
        self.assertTrue(AuthorityService.can_manage_role("discord", "1", "оператор"))

    @patch("bot.services.authority_service.AccountsService.get_account_titles")
    @patch("bot.services.authority_service.AccountsService.resolve_account_id")
    def test_can_manage_role_allows_main_vice_to_manage_head_role(self, mock_resolve, mock_titles):
        mock_resolve.return_value = "acc-role4"
        mock_titles.return_value = ["Главный вице"]

        self.assertTrue(AuthorityService.can_manage_role("discord", "9", "глава клуба"))

    @patch("bot.services.authority_service.AccountsService.get_account_titles")
    @patch("bot.services.authority_service.AccountsService.resolve_account_id")
    def test_vice_cannot_manage_head_main_vice_or_operator(self, mock_resolve, mock_titles):
        mock_resolve.return_value = "acc-role5"
        mock_titles.return_value = ["Вице города"]

        self.assertFalse(AuthorityService.can_manage_role("telegram", "55", "главный вице"))
        self.assertFalse(AuthorityService.can_manage_role("telegram", "55", "глава клуба"))
        self.assertFalse(AuthorityService.can_manage_role("telegram", "55", "оператор"))

    @patch("bot.services.authority_service.AccountsService.get_account_titles")
    @patch("bot.services.authority_service.AccountsService.resolve_account_id")
    def test_can_manage_role_allows_admin_to_manage_vice_level_roles_but_not_operator(self, mock_resolve, mock_titles):
        mock_resolve.return_value = "acc-role3"
        mock_titles.return_value = ["Админ"]

        self.assertTrue(AuthorityService.can_manage_role("telegram", "42", "вице города"))
        self.assertTrue(AuthorityService.can_manage_role("telegram", "42", "админ"))
        self.assertFalse(AuthorityService.can_manage_role("telegram", "42", "оператор"))

    @patch("bot.services.authority_service.AccountsService.get_account_titles")
    @patch("bot.services.authority_service.AccountsService.resolve_account_id")
    def test_head_clubs_alias_is_treated_as_head_club(self, mock_resolve, mock_titles):
        mock_resolve.return_value = "acc-head-alias"
        mock_titles.return_value = ["Глава клубов"]

        result = AuthorityService.resolve_authority("discord", "500")

        self.assertEqual(result.level, 100)
        self.assertTrue(AuthorityService.can_manage_self("discord", "500"))
        self.assertTrue(AuthorityService.can_manage_role("discord", "500", "глава клуба"))

    @patch("bot.services.authority_service.AccountsService.get_account_titles")
    @patch("bot.services.authority_service.AccountsService.resolve_account_id")
    def test_veteran_can_only_mute_participant(self, mock_resolve, mock_titles):
        def _resolve(_provider, user_id):
            return f"acc-{user_id}"

        def _titles(account_id):
            return ["Ветеран города"] if account_id == "acc-actor" else ["Участник клубов"]

        mock_resolve.side_effect = _resolve
        mock_titles.side_effect = _titles

        mute = AuthorityService.can_apply_moderation_action("discord", "actor", "discord", "target", "mute")
        warn = AuthorityService.can_apply_moderation_action("discord", "actor", "discord", "target", "warn")
        ban = AuthorityService.can_apply_moderation_action("discord", "actor", "discord", "target", "ban")

        self.assertTrue(mute.allowed)
        self.assertFalse(warn.allowed)
        self.assertEqual(warn.message, "Вы можете выдавать только мут участникам")
        self.assertFalse(ban.allowed)
        self.assertEqual(ban.message, "Вы можете выдавать только мут участникам")

    @patch("bot.services.authority_service.AccountsService.get_account_titles")
    @patch("bot.services.authority_service.AccountsService.resolve_account_id")
    def test_junior_admin_can_only_mute_participant(self, mock_resolve, mock_titles):
        def _resolve(_provider, user_id):
            return f"acc-{user_id}"

        def _titles(account_id):
            return ["Младший админ"] if account_id == "acc-actor" else ["Участник клубов"]

        mock_resolve.side_effect = _resolve
        mock_titles.side_effect = _titles

        mute = AuthorityService.can_apply_moderation_action("discord", "actor", "discord", "target", "mute")
        warn = AuthorityService.can_apply_moderation_action("discord", "actor", "discord", "target", "warn")
        ban = AuthorityService.can_apply_moderation_action("discord", "actor", "discord", "target", "ban")

        self.assertTrue(mute.allowed)
        self.assertFalse(warn.allowed)
        self.assertEqual(warn.message, "Вы можете выдавать только мут участникам")
        self.assertFalse(ban.allowed)
        self.assertEqual(ban.message, "Вы можете выдавать только мут участникам")

    @patch("bot.services.authority_service.AccountsService.get_account_titles")
    @patch("bot.services.authority_service.AccountsService.resolve_account_id")
    def test_vice_can_warn_veteran(self, mock_resolve, mock_titles):
        def _resolve(_provider, user_id):
            return f"acc-{user_id}"

        def _titles(account_id):
            return ["Вице города"] if account_id == "acc-actor" else ["Ветеран города"]

        mock_resolve.side_effect = _resolve
        mock_titles.side_effect = _titles

        decision = AuthorityService.can_apply_moderation_action("discord", "actor", "discord", "target", "warn")

        self.assertTrue(decision.allowed)

    @patch("bot.services.authority_service.AccountsService.get_account_titles")
    @patch("bot.services.authority_service.AccountsService.resolve_account_id")
    def test_resolve_authority_adds_chat_member_title_when_user_has_no_titles(self, mock_resolve, mock_titles):
        mock_resolve.return_value = "acc-empty"
        mock_titles.return_value = []

        result = AuthorityService.resolve_authority("discord", "777")

        self.assertEqual(result.level, 0)
        self.assertIn("Участник чата", result.titles)

    @patch("bot.services.authority_service.AccountsService.get_account_titles")
    @patch("bot.services.authority_service.AccountsService.resolve_account_id")
    def test_admin_can_warn_junior_admin(self, mock_resolve, mock_titles):
        def _resolve(_provider, user_id):
            return f"acc-{user_id}"

        def _titles(account_id):
            return ["Админ"] if account_id == "acc-actor" else ["Младший админ"]

        mock_resolve.side_effect = _resolve
        mock_titles.side_effect = _titles

        decision = AuthorityService.can_apply_moderation_action("discord", "actor", "discord", "target", "warn")

        self.assertTrue(decision.allowed)

    @patch("bot.services.authority_service.AccountsService.get_account_titles")
    @patch("bot.services.authority_service.AccountsService.resolve_account_id")
    def test_top_tier_roles_can_ban_allowed_targets(self, mock_resolve, mock_titles):
        cases = {
            "acc-head": ["Глава клуба"],
            "acc-main-vice": ["Главный вице"],
            "acc-operator": ["Оператор"],
            "acc-target": ["Админ"],
        }

        def _resolve(_provider, user_id):
            return f"acc-{user_id}"

        def _titles(account_id):
            return cases[account_id]

        mock_resolve.side_effect = _resolve
        mock_titles.side_effect = _titles

        self.assertTrue(AuthorityService.can_apply_moderation_action("discord", "head", "discord", "target", "ban").allowed)
        self.assertTrue(AuthorityService.can_apply_moderation_action("discord", "main-vice", "discord", "target", "ban").allowed)
        self.assertTrue(AuthorityService.can_apply_moderation_action("discord", "operator", "discord", "target", "ban").allowed)

    @patch("bot.services.authority_service.AccountsService.get_account_titles")
    @patch("bot.services.authority_service.AccountsService.resolve_account_id")
    def test_top_tier_moderation_policy_is_explicit_for_operator_and_peers(self, mock_resolve, mock_titles):
        def _resolve(_provider, user_id):
            return f"acc-{user_id}"

        def _titles(account_id):
            mapping = {
                "acc-head": ["Глава клуба"],
                "acc-main-vice": ["Главный вице"],
                "acc-operator": ["Оператор"],
            }
            return mapping[account_id]

        mock_resolve.side_effect = _resolve
        mock_titles.side_effect = _titles

        self.assertTrue(AuthorityService.can_apply_moderation_action("discord", "head", "discord", "main-vice", "ban").allowed)
        self.assertTrue(AuthorityService.can_apply_moderation_action("discord", "main-vice", "discord", "head", "ban").allowed)
        operator_vs_head = AuthorityService.can_apply_moderation_action("discord", "operator", "discord", "head", "ban")
        head_vs_operator = AuthorityService.can_apply_moderation_action("discord", "head", "discord", "operator", "ban")

        self.assertFalse(operator_vs_head.allowed)
        self.assertEqual(operator_vs_head.deny_reason, "hierarchy_denied")
        self.assertFalse(head_vs_operator.allowed)
        self.assertEqual(head_vs_operator.deny_reason, "hierarchy_denied")

    @patch("bot.services.authority_service.AccountsService.get_account_titles")
    @patch("bot.services.authority_service.AccountsService.resolve_account_id")
    def test_equal_roles_are_denied_for_moderation(self, mock_resolve, mock_titles):
        def _resolve(_provider, user_id):
            return f"acc-{user_id}"

        def _titles(_account_id):
            return ["Админ"]

        mock_resolve.side_effect = _resolve
        mock_titles.side_effect = _titles

        decision = AuthorityService.can_apply_moderation_action("discord", "1", "discord", "2", "warn")

        self.assertFalse(decision.allowed)
        self.assertEqual(decision.deny_reason, "hierarchy_denied")
        self.assertEqual(decision.message, "Нельзя модерировать пользователя с равным или более высоким званием")

    @patch("bot.services.authority_service.AccountsService.get_account_titles")
    @patch("bot.services.authority_service.AccountsService.resolve_account_id")
    def test_ban_message_for_vice(self, mock_resolve, mock_titles):
        def _resolve(_provider, user_id):
            return f"acc-{user_id}"

        def _titles(account_id):
            return ["Вице города"] if account_id == "acc-actor" else ["Участник клубов"]

        mock_resolve.side_effect = _resolve
        mock_titles.side_effect = _titles

        decision = AuthorityService.can_apply_moderation_action("discord", "actor", "discord", "target", "ban")

        self.assertFalse(decision.allowed)
        self.assertEqual(decision.message, "Бан доступен только Главному вице, Главе клуба и Оператору")

    @patch("bot.services.authority_service.AccountsService.get_account_titles")
    @patch("bot.services.authority_service.AccountsService.resolve_account_id")
    def test_unknown_action_returns_explicit_decision(self, mock_resolve, mock_titles):
        def _resolve(_provider, user_id):
            return f"acc-{user_id}"

        def _titles(account_id):
            return ["Админ"] if account_id == "acc-actor" else ["Участник клубов"]

        mock_resolve.side_effect = _resolve
        mock_titles.side_effect = _titles

        decision = AuthorityService.can_apply_moderation_action("discord", "actor", "discord", "target", "freeze")

        self.assertFalse(decision.allowed)
        self.assertEqual(decision.deny_reason, "unknown_action")
        self.assertEqual(decision.message, "Неизвестный тип модерации")

    @patch("bot.services.authority_service.AccountsService.get_account_titles")
    @patch("bot.services.authority_service.AccountsService.resolve_account_id")
    def test_discord_and_telegram_keep_same_deny_message(self, mock_resolve, mock_titles):
        def _resolve(_provider, user_id):
            return f"acc-{user_id}"

        def _titles(account_id):
            return ["Вице города"] if account_id == "acc-actor" else ["Участник клубов"]

        mock_resolve.side_effect = _resolve
        mock_titles.side_effect = _titles

        discord_decision = AuthorityService.can_apply_moderation_action("discord", "actor", "discord", "target", "ban")
        telegram_decision = AuthorityService.can_apply_moderation_action("telegram", "actor", "telegram", "target", "ban")

        self.assertEqual(discord_decision.message, "Бан доступен только Главному вице, Главе клуба и Оператору")
        self.assertEqual(telegram_decision.message, discord_decision.message)
        self.assertEqual(telegram_decision.deny_reason, discord_decision.deny_reason)

    def test_profile_titles_include_all_required_moderation_titles(self):
        required_titles = {
            "Глава клуба",
            "Главный вице",
            "Оператор",
            "Вице города",
            "Админ",
            "Ветеран города",
            "Младший админ",
            "Участник чата",
        }

        canonical_keys = protected_profile_title_canonical_keys()

        self.assertEqual(
            {normalize_protected_profile_title(title) for title in required_titles},
            canonical_keys - {"участник клубов"},
        )


if __name__ == "__main__":
    unittest.main()
