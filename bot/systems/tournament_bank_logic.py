from bot.data.tournament_db import set_bank_type
from typing import Literal

BankType = Literal[1, 2, 3, 4]

# ─────────────────────────────────────────────────────

def validate_and_save_bank(tournament_id: int, bank_type: int, manual_amount: float | None) -> tuple[bool, str]:
    """
    Проверяет корректность выбора банка и сохраняет его.
    Возвращает (успех, текст/ошибка).
    """
    if bank_type not in (1, 2, 3, 4):
        return False, "❌ Неверный тип банка. Используйте 1, 2, 3 или 4."

    if bank_type == 1:
        if manual_amount is None:
            return False, "❌ Укажите сумму при типе 1."
        if manual_amount < 15:
            return False, "❌ Минимальная сумма при типе 1 — 15 баллов."
    success = set_bank_type(tournament_id, bank_type, manual_amount if bank_type == 1 else None)
    if not success:
        return False, "❌ Не удалось сохранить тип банка в базу данных."

    # 4-й тип — TEST, никаких списаний и выплат
    if bank_type == 4:
        success = set_bank_type(tournament_id, bank_type, None)
        if not success:
            return False, "❌ Не удалось сохранить TEST-режим в базу."
        return True, "🧪 Выбран TEST-режим — награды не выдаются и баллы не списываются."

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

def calculate_bank(bank_type: BankType, user_balance: float = 0, manual_amount: float = 0) -> tuple[float, float, float]:
    """
    Возвращает (итоговый банк, сколько платит пользователь, сколько банк).
    """
    if bank_type == 1:
        # Тип 1 — пользователь платит 50% от manual_amount (мин. 15)
        if manual_amount < 15:
            raise ValueError("Минимум 15 баллов при типе 1")
        user_part = manual_amount * 0.5
        bank_part = manual_amount - user_part
        return manual_amount, user_part, bank_part

    elif bank_type == 2:
        # Тип 2 — фиксированный банк 30, пользователь платит 25%
        bank = 30.0
        user_part = bank * 0.25
        bank_part = bank - user_part
        return bank, user_part, bank_part

    elif bank_type == 3:
        # Тип 3 — клубный банк 30 за счёт банка
        return 30.0, 0.0, 30.0

    elif bank_type == 4:
        # TEST-режим — ничего никуда не идёт
        return 0.0, 0.0, 0.0

    else:
        raise ValueError("Неверный тип банка")
