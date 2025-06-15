from bot.data.tournament_db import set_bank_type

# ─────────────────────────────────────────────────────

def validate_and_save_bank(tournament_id: int, bank_type: int, manual_amount: float | None) -> tuple[bool, str]:
    """
    Проверяет корректность выбора банка и сохраняет его.
    Возвращает (успех, текст/ошибка).
    """
    if bank_type not in (1, 2, 3):
        return False, "❌ Неверный тип банка. Используйте 1, 2 или 3."

    if bank_type == 1:
        if manual_amount is None:
            return False, "❌ Укажите сумму при типе 1."
        if manual_amount < 15:
            return False, "❌ Минимальная сумма при типе 1 — 15 баллов."

    # Сохраняем
    success = set_bank_type(tournament_id, bank_type, manual_amount if bank_type == 1 else None)
    if not success:
        return False, "❌ Не удалось сохранить тип банка в базу данных."

    label = {
        1: f"🔹 Тип 1 — Пользователь, {manual_amount:.2f} баллов",
        2: "🔸 Тип 2 — Смешанный (30 баллов, 25% платит игрок)",
        3: "🟣 Тип 3 — Клуб (30 баллов, всё из банка Бебр)"
    }[bank_type]

    return True, f"✅ Выбран {label}"
