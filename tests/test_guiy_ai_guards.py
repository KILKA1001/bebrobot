import asyncio
import unittest

from unittest.mock import AsyncMock, patch

from bot.services import gemini_service
from bot.services.gemini_service import (
    _extract_retry_after_seconds,
    _force_guiy_prefix,
    _is_hard_quota_exhausted,
    _is_role_break,
    _resolve_candidate_models,
    generate_guiy_reply,
)
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


    def test_extract_retry_after_from_body(self):
        seconds = _extract_retry_after_seconds({}, "Please retry in 34.312858291s.")
        self.assertEqual(seconds, 35)


    def test_extract_retry_after_from_russian_body(self):
        seconds = _extract_retry_after_seconds({}, "Пожалуйста Повторная попытка через 22.030640423 с.")
        self.assertEqual(seconds, 23)

    def test_is_hard_quota_exhausted_detects_zero_limit_payload(self):
        body = (
            'status: "RESOURCE_EXHAUSTED" '
            'message: "You exceeded your current quota" '
            '* Превышена квота для метрики: generativelanguage.googleapis.com/generate_content_free_tier_requests, limit: 0 '
            '* Превышена квота для метрики: generativelanguage.googleapis.com/generate_content_free_tier_input_token_count, limit: 0'
        )
        self.assertTrue(_is_hard_quota_exhausted(body))



    def test_set_gemini_cooldown_caps_soft_quota(self):
        old = gemini_service._GEMINI_COOLDOWN_UNTIL
        now = gemini_service.time.time()
        try:
            gemini_service._GEMINI_COOLDOWN_UNTIL = 0
            gemini_service._set_gemini_cooldown(3600, hard_quota=False)
            delta = int(gemini_service._GEMINI_COOLDOWN_UNTIL - now)
            self.assertLessEqual(delta, 91)
        finally:
            gemini_service._GEMINI_COOLDOWN_UNTIL = old

    def test_set_gemini_cooldown_caps_hard_quota(self):
        old = gemini_service._GEMINI_COOLDOWN_UNTIL
        now = gemini_service.time.time()
        try:
            gemini_service._GEMINI_COOLDOWN_UNTIL = 0
            gemini_service._set_gemini_cooldown(7200, hard_quota=True)
            delta = int(gemini_service._GEMINI_COOLDOWN_UNTIL - now)
            self.assertLessEqual(delta, 901)
            self.assertGreaterEqual(delta, 10)
        finally:
            gemini_service._GEMINI_COOLDOWN_UNTIL = old

    @patch.dict("os.environ", {"GEMINI_API_KEY": "x"}, clear=True)
    @patch("bot.services.gemini_service._generate_with_model_fallback", new_callable=AsyncMock, return_value=None)
    def test_generate_reply_reports_quota_cooldown(self, mock_generate):
        old = gemini_service._GEMINI_COOLDOWN_UNTIL
        try:
            gemini_service._GEMINI_COOLDOWN_UNTIL = 9999999999
            reply = asyncio.run(generate_guiy_reply("Гуй, ты тут?"))
            self.assertIn("лимит Gemini", reply)
            mock_generate.assert_not_called()
        finally:
            gemini_service._GEMINI_COOLDOWN_UNTIL = old

if __name__ == "__main__":
    unittest.main()
