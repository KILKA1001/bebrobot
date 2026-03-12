import unittest

from bot.services.gemini_service import _force_guiy_prefix, _is_role_break
from bot.telegram_bot.commands.ai_chat import _is_command_text


class GuiyAIGuardsTests(unittest.TestCase):
    def test_role_break_detects_model_leak(self):
        self.assertTrue(_is_role_break("Я языковая модель, не могу войти в роль"))

    def test_role_break_allows_in_character_answer(self):
        self.assertFalse(_is_role_break("Гуй: Слышь, давай по делу, где мои огурцы?"))

    def test_force_guiy_prefix_adds_prefix(self):
        self.assertEqual(_force_guiy_prefix("Принёс огурцы?"), "Гуй: Принёс огурцы?")

    def test_force_guiy_prefix_keeps_existing_prefix(self):
        self.assertEqual(_force_guiy_prefix("Гуй: уже тут"), "Гуй: уже тут")

    def test_is_command_text_for_known_command(self):
        self.assertTrue(_is_command_text("/points 123"))
        self.assertTrue(_is_command_text("/PROFILE"))

    def test_is_command_text_for_regular_text(self):
        self.assertFalse(_is_command_text("Гуй, привет"))


if __name__ == "__main__":
    unittest.main()
