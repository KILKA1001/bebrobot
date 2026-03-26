from bot.telegram_bot.commands.fines import _format_fine_line, _pay_keyboard


def test_format_fine_line_includes_remaining_and_status() -> None:
    fine = {
        "id": 10,
        "amount": 12.5,
        "paid_amount": 2.5,
        "due_date": "2026-05-01T10:00:00+00:00",
        "is_paid": False,
        "is_overdue": False,
        "is_canceled": False,
        "reason": "Тест",
    }
    text = _format_fine_line(fine)
    assert "Штраф #10" in text
    assert "Статус: ⏳ Активен" in text
    assert "Осталось: 10.00" in text
    assert "Причина: Тест" in text


def test_pay_keyboard_has_all_percent_buttons() -> None:
    keyboard = _pay_keyboard(77)
    rows = keyboard.inline_keyboard
    callback_data = [button.callback_data for row in rows for button in row]
    assert "tgfine:pay:77:100" in callback_data
    assert "tgfine:pay:77:50" in callback_data
    assert "tgfine:pay:77:25" in callback_data
