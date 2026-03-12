import asyncio
import unittest

from unittest.mock import AsyncMock, patch

from bot.services import gemini_service
from bot.services.gemini_service import (
    _extract_retry_after_seconds,
    _force_guiy_prefix,
    _inject_user_context,
    _is_father_user,
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

    def test_force_guiy_prefix_keeps_plain_text(self):
        self.assertEqual(_force_guiy_prefix("Принёс огурцы?"), "Принёс огурцы?")

    def test_force_guiy_prefix_removes_existing_prefix(self):
        self.assertEqual(_force_guiy_prefix("Гуй: уже тут"), "уже тут")

    def test_force_guiy_prefix_trims_followup_speaker_blocks(self):
        self.assertEqual(
            _force_guiy_prefix("Гуй: Привет, папочка!\nПользователь: Что делаешь?"),
            "Привет, папочка!",
        )

    def test_is_command_text_for_known_command(self):
        self.assertTrue(_is_command_text("/points 123"))
        self.assertTrue(_is_command_text("/PROFILE"))

    def test_is_command_text_for_regular_text(self):
        self.assertFalse(_is_command_text("Гуй, привет"))




    @patch.dict("os.environ", {"GUIY_FATHER_TELEGRAM_IDS": "100,200"}, clear=True)
    def test_is_father_user_by_provider_id(self):
        self.assertTrue(_is_father_user("telegram", "100"))

    @patch.dict("os.environ", {"GUIY_FATHER_ACCOUNT_IDS": "acc-1"}, clear=True)
    @patch("bot.services.gemini_service.AccountsService.resolve_account_id", return_value="acc-1")
    def test_is_father_user_by_shared_account(self, mock_resolve):
        self.assertTrue(_is_father_user("telegram", "321"))
        mock_resolve.assert_called_once_with("telegram", "321")

    @patch.dict("os.environ", {"GUIY_FATHER_TELEGRAM_IDS": "100"}, clear=True)
    def test_inject_user_context_for_father(self):
        prompt = _inject_user_context("base", provider="telegram", user_id="100")
        self.assertIn("это твой отец Эмочка", prompt)

    @patch.dict("os.environ", {}, clear=True)
    def test_resolve_models_default_order(self):
        models = _resolve_candidate_models()
        self.assertEqual(models, ("gemini-2.5-flash",))


    @patch.dict("os.environ", {"GEMINI_USE_FREE_TIER": "0"}, clear=True)
    def test_resolve_models_still_pinned_when_free_tier_disabled(self):
        models = _resolve_candidate_models()
        self.assertEqual(models, ("gemini-2.5-flash",))

    @patch.dict("os.environ", {"GEMINI_MODEL": "gemini-2.0-flash", "GEMINI_MODELS": "gemini-1.5-flash"}, clear=True)
    def test_resolve_models_ignores_env_overrides(self):
        models = _resolve_candidate_models()
        self.assertEqual(models, ("gemini-2.5-flash",))


    @patch.dict("os.environ", {}, clear=True)
    def test_generate_reply_returns_fallback_when_api_key_missing(self):
        reply = asyncio.run(generate_guiy_reply("Гуй, ты тут?"))
        self.assertNotIn("Гуй:", reply)
        self.assertEqual(reply, "Я очень устал, не мешай мне спать.")


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
        old_hard = gemini_service._GEMINI_HARD_QUOTA_UNTIL
        try:
            gemini_service._GEMINI_COOLDOWN_UNTIL = 9999999999
            reply = asyncio.run(generate_guiy_reply("Гуй, ты тут?"))
            self.assertEqual(reply, "Я очень устал, не мешай мне спать.")
            mock_generate.assert_not_called()
        finally:
            gemini_service._GEMINI_COOLDOWN_UNTIL = old
            gemini_service._GEMINI_HARD_QUOTA_UNTIL = old_hard

    @patch.dict("os.environ", {"GEMINI_API_KEY": "x"}, clear=True)
    @patch("bot.services.gemini_service._generate_with_model_fallback", new_callable=AsyncMock, return_value=None)
    def test_generate_reply_reports_hard_quota_cooldown(self, mock_generate):
        old = gemini_service._GEMINI_COOLDOWN_UNTIL
        old_hard = gemini_service._GEMINI_HARD_QUOTA_UNTIL
        try:
            gemini_service._GEMINI_COOLDOWN_UNTIL = 9999999999
            gemini_service._GEMINI_HARD_QUOTA_UNTIL = 9999999999
            reply = asyncio.run(generate_guiy_reply("Гуй, ты тут?"))
            self.assertEqual(reply, "Я очень устал, не мешай мне спать.")
            mock_generate.assert_not_called()
        finally:
            gemini_service._GEMINI_COOLDOWN_UNTIL = old
            gemini_service._GEMINI_HARD_QUOTA_UNTIL = old_hard

    @patch.dict("os.environ", {"GEMINI_API_KEY": "x"}, clear=True)
    @patch("bot.services.gemini_service.asyncio.sleep", new_callable=AsyncMock)
    @patch("bot.services.gemini_service.random.uniform", return_value=3.4)
    @patch("bot.services.gemini_service._generate_with_model_fallback", new_callable=AsyncMock, return_value="Ответ")
    def test_generate_reply_adds_artificial_delay(self, mock_generate, mock_uniform, mock_sleep):
        reply = asyncio.run(generate_guiy_reply("Гуй, ты тут?"))
        self.assertEqual(reply, "Ответ")
        mock_uniform.assert_called_once_with(3.0, 4.0)
        mock_sleep.assert_awaited_once_with(3.4)
        mock_generate.assert_awaited()

if __name__ == "__main__":
    unittest.main()
