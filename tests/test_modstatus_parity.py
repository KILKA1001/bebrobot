"""
Назначение: модуль "test modstatus parity" реализует продуктовый контур в зоне Discord/Telegram/общая логика (тесты).
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Discord/Telegram/общая логика (тесты).
"""

from pathlib import Path

from bot.services.moderation_service import ModerationService


def test_modstatus_payment_hint_is_same_for_discord_and_telegram():
    discord_source = Path("bot/commands/modstatus.py").read_text(encoding="utf-8")
    telegram_source = Path("bot/telegram_bot/commands/modstatus.py").read_text(encoding="utf-8")
    assert "_PAYMENT_HINT = ModerationService.MODSTATUS_PAYMENT_HINT" in discord_source
    assert "_PAYMENT_HINT = ModerationService.MODSTATUS_PAYMENT_HINT" in telegram_source


def test_render_user_moderation_snapshot_shows_manual_and_auto_fine_states():
    snapshot = {
        "ok": True,
        "profile_name": "Test",
        "active_penalties": [
            {
                "kind": "case_fine",
                "case_id": 101,
                "amount": 10,
                "value": 10,
                "status": "pending",
                "payment_mode": "manual",
                "ends_at": "2026-04-01T00:00:00+00:00",
            },
            {
                "kind": "case_fine",
                "case_id": 102,
                "amount": 5,
                "value": 0,
                "status": "paid",
                "payment_mode": "instant",
                "ends_at": "2026-04-01T00:00:00+00:00",
            },
            {
                "kind": "case_fine",
                "case_id": 103,
                "amount": 8,
                "value": 4,
                "status": "partial",
                "payment_mode": "manual",
                "ends_at": "2026-04-01T00:00:00+00:00",
            },
        ],
        "recent_cases": [],
    }
    text = ModerationService.render_user_moderation_snapshot(snapshot, payment_hint=ModerationService.MODSTATUS_PAYMENT_HINT)
    assert "Статус: ждёт оплаты" in text
    assert "Этот штраф нужно оплатить вручную" in text
    assert "Статус: уже удержан автоматически" in text
    assert "Статус: частично оплачен" in text
