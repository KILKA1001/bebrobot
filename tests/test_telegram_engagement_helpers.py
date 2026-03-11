import unittest

from bot.telegram_bot.commands.engagement import _resolve_profile_name


class TelegramEngagementHelperTests(unittest.TestCase):
    def test_resolve_profile_name_prefers_custom_nick(self):
        result = _resolve_profile_name({"custom_nick": "Кастом"}, "Original Name", 10)
        self.assertEqual(result, "Кастом")

    def test_resolve_profile_name_falls_back_to_original_name(self):
        result = _resolve_profile_name({"custom_nick": "Игрок"}, "Original Name", 10)
        self.assertEqual(result, "Original Name")


if __name__ == "__main__":
    unittest.main()

