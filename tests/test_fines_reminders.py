"""
Назначение: модуль "test fines reminders" реализует продуктовый контур в зоне Discord/Telegram/общая логика (тесты).
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Discord/Telegram/общая логика (тесты).
"""

import asyncio
from datetime import datetime as real_datetime
from types import SimpleNamespace

from bot.systems import fines_logic


class _FakeBot:
    def __init__(self, members):
        self._members = members

    async def wait_until_ready(self):
        return

    def get_all_members(self):
        return list(self._members)


class _FakeDb:
    def __init__(self, fines, *, has_tracking=True):
        self.fines = fines
        self.has_fine_reminder_tracking = has_tracking
        self.marked = []

    def is_fine_reminder_sent(self, fine, stage):
        field_map = {
            "due_3d": "reminder_3d_sent_at",
            "due_1d": "reminder_1d_sent_at",
            "overdue": "overdue_notice_sent_at",
        }
        return bool((fine or {}).get(field_map[stage]))

    def _get_discord_user_for_account_id(self, account_id):
        return 101 if account_id == "acc-1" else None

    def mark_fine_reminder_sent(self, fine_id, stage):
        self.marked.append((fine_id, stage))
        return True


def test_remind_fines_marks_due_3d_once(monkeypatch):
    sent = []
    fine = {
        "id": 55,
        "account_id": "acc-1",
        "amount": 10,
        "paid_amount": 0,
        "is_paid": False,
        "is_canceled": False,
        "due_date": "2099-01-03T12:00:00+00:00",
    }
    fake_db = _FakeDb([fine], has_tracking=True)
    fake_bot = _FakeBot([SimpleNamespace(id=101)])

    async def _safe_send(user, text):
        sent.append((user.id, text))
        return True

    class _FrozenDateTime:
        @staticmethod
        def now(_tz):
            return real_datetime.fromisoformat("2099-01-01T12:00:00+00:00")

        @staticmethod
        def fromisoformat(value):
            return real_datetime.fromisoformat(value)

    monkeypatch.setattr(fines_logic, "db", fake_db)
    monkeypatch.setattr(fines_logic, "safe_send", _safe_send)
    monkeypatch.setattr(fines_logic, "datetime", _FrozenDateTime)

    asyncio.run(fines_logic.remind_fines(fake_bot))

    assert sent
    assert fake_db.marked == [(55, "due_3d")]


def test_remind_fines_skips_already_sent_stage(monkeypatch):
    sent = []
    fine = {
        "id": 56,
        "account_id": "acc-1",
        "is_paid": False,
        "is_canceled": False,
        "reminder_1d_sent_at": "2099-01-01T10:00:00+00:00",
        "due_date": "2099-01-02T12:00:00+00:00",
    }
    fake_db = _FakeDb([fine], has_tracking=True)
    fake_bot = _FakeBot([SimpleNamespace(id=101)])

    async def _safe_send(user, text):
        sent.append((user.id, text))
        return True

    class _FrozenDateTime:
        @staticmethod
        def now(_tz):
            return real_datetime.fromisoformat("2099-01-01T13:00:00+00:00")

        @staticmethod
        def fromisoformat(value):
            return real_datetime.fromisoformat(value)

    monkeypatch.setattr(fines_logic, "db", fake_db)
    monkeypatch.setattr(fines_logic, "safe_send", _safe_send)
    monkeypatch.setattr(fines_logic, "datetime", _FrozenDateTime)

    asyncio.run(fines_logic.remind_fines(fake_bot))

    assert sent == []
    assert fake_db.marked == []


def test_remind_fines_skips_when_tracking_not_available(monkeypatch):
    sent = []
    fine = {
        "id": 57,
        "account_id": "acc-1",
        "is_paid": False,
        "is_canceled": False,
        "due_date": "2099-01-02T12:00:00+00:00",
    }
    fake_db = _FakeDb([fine], has_tracking=False)
    fake_bot = _FakeBot([SimpleNamespace(id=101)])

    async def _safe_send(user, text):
        sent.append((user.id, text))
        return True

    monkeypatch.setattr(fines_logic, "db", fake_db)
    monkeypatch.setattr(fines_logic, "safe_send", _safe_send)
    monkeypatch.setattr(fines_logic, "_REMINDER_TRACKING_WARNING_LOGGED", False)

    asyncio.run(fines_logic.remind_fines(fake_bot))

    assert sent == []
    assert fake_db.marked == []
