from bot.data import db
from typing import Literal
import math

from bot.data.tournament_db import set_bank_type

BankType = Literal[1, 2, 3]  # 1 = пользователь, 2 = смешанный, 3 = клуб

# ─────────────────────────────────────────────────────────────

def calculate_bank(bank_type: BankType, user_balance: float = 0, manual_amount: float = 0) -> tuple[float, float, float]:
    """
    Возвращает кортеж (итоговый банк, сколько платит пользователь, сколько банк)
    """
    if bank_type == 1:
        if manual_amount < 15:
            raise ValueError("Минимум 15 баллов при типе 1")
        user_part = manual_amount * 0.5
        return manual_amount, user_part, manual_amount - user_part
    elif bank_type == 2:
        bank = 30
        user_part = bank * 0.25
        return bank, user_part, bank - user_part
    elif bank_type == 3:
        return 30, 0.0, 30.0
    else:
        raise ValueError("Неверный тип банка")

# ─────────────────────────────────────────────────────────────

def distribute_rewards(
    tournament_id: int,
    bank_total: float,
    first_team_ids: list[int],
    second_team_ids: list[int],
    author_id: int
):
    """
    Делит награды между победителями (баллы и билеты).
    Логика:
    - каждому в 1 команде — 50% банка + золотой билет
    - каждому во 2 команде — 25% банка + обычный билет
    """
    reward_first = bank_total * 0.5
    reward_second = bank_total * 0.25

    for uid in first_team_ids:
        db.add_action(uid, reward_first, f"🏆 1 место в турнире #{tournament_id}", author_id)
        db.give_ticket(uid, "gold", 1, f"🥇 Золотой билет за 1 место (турнир #{tournament_id})", author_id)

    for uid in second_team_ids:
        db.add_action(uid, reward_second, f"🥈 2 место в турнире #{tournament_id}", author_id)
        db.give_ticket(uid, "normal", 1, f"🎟 Обычный билет за 2 место (турнир #{tournament_id})", author_id)

# ─────────────────────────────────────────────────────────────

def charge_bank_contribution(user_id: int, user_amount: float, bank_amount: float, reason: str) -> bool:
    """
    Списывает часть баллов с пользователя и/или банка.
    """
    if user_amount > 0:
        success_user = db.update_scores(user_id, -user_amount)
        if not success_user:
            return False
        db.add_action(user_id, -user_amount, reason, author_id=user_id)

    if bank_amount > 0:
        return db.spend_from_bank(bank_amount, user_id=user_id, reason=reason)

    return True