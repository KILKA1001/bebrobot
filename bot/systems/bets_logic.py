from __future__ import annotations

import math
from bot.data import db
from bot.data import tournament_db


ROUND_COEFFICIENTS = {
    1: (1, 0.25),  # rounds 1-3 (1/16..1/4)
    2: (2, 0.50),  # semifinal
    3: (3, 0.75),  # final
}


def _get_stage(round_no: int, total_rounds: int) -> int:
    """Maps round number to stage index (1, 2, 3)."""
    if total_rounds >= 4 and round_no <= total_rounds - 3:
        return 1
    if round_no == total_rounds - 1:
        return 2
    return 3


def get_min_bet(round_no: int, total_rounds: int) -> int:
    stage = _get_stage(round_no, total_rounds)
    return ROUND_COEFFICIENTS.get(stage, (1, 0.25))[0]


def get_multiplier(round_no: int, total_rounds: int) -> float:
    stage = _get_stage(round_no, total_rounds)
    return ROUND_COEFFICIENTS.get(stage, (1, 0.25))[1]


def place_bet(tournament_id: int, round_no: int, pair_index: int, user_id: int, bet_on: int, amount: float, total_rounds: int) -> tuple[bool, str]:
    min_bet = get_min_bet(round_no, total_rounds)
    if amount < min_bet:
        return False, f"Минимальная ставка для этого раунда: {min_bet}"

    balance = db.db.scores.get(user_id, 0)
    if balance < amount:
        return False, "Недостаточно баллов"

    bet_id = tournament_db.create_bet(tournament_id, round_no, pair_index, user_id, bet_on, amount)
    if bet_id is None:
        return False, "Не удалось создать ставку"

    if not db.db.update_scores(user_id, -amount):
        return False, "Не удалось списать баллы"

    return True, f"Ставка принята. ID {bet_id}"


def payout_bets(tournament_id: int, round_no: int, pair_index: int, winner: int, total_rounds: int) -> None:
    bets = tournament_db.list_bets(tournament_id, round_no)
    for bet in bets:
        if bet.get("pair_index") != pair_index:
            continue
        won = int(bet.get("bet_on")) == winner
        multiplier = get_multiplier(round_no, total_rounds)
        payout = math.floor(float(bet.get("amount")) * (1 + multiplier)) if won else 0
        if payout:
            db.db.update_scores(int(bet["user_id"]), payout)
        tournament_db.close_bet(int(bet["id"]), won, payout)
