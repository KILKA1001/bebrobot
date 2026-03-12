import asyncio
import unittest

from unittest.mock import patch

from bot.services.gemini_service import _force_guiy_prefix, _is_role_break, _resolve_candidate_models, generate_guiy_reply
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



    @patch.dict("os.environ", {}, clear=True)
    def test_resolve_models_default_order(self):
        models = _resolve_candidate_models()
        self.assertGreaterEqual(len(models), 3)
        self.assertEqual(models[0], "gemini-2.0-flash")

    @patch.dict("os.environ", {"GEMINI_MODEL": "gemini-2.5-flash", "GEMINI_MODELS": "gemini-2.0-flash-lite, gemini-1.5-flash"}, clear=True)
    def test_resolve_models_prefers_env(self):
        models = _resolve_candidate_models()
        self.assertEqual(models[0], "gemini-2.5-flash")
        self.assertIn("gemini-2.0-flash-lite", models)


    @patch.dict("os.environ", {}, clear=True)
    def test_generate_reply_returns_fallback_when_api_key_missing(self):
        reply = asyncio.run(generate_guiy_reply("Гуй, ты тут?"))
        self.assertIn("Гуй:", reply)
        self.assertIn("GEMINI_API_KEY", reply)

if __name__ == "__main__":
    unittest.main()
