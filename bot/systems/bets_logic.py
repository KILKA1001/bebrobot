from __future__ import annotations

import math
from bot.data import db
from bot.data import tournament_db


def _is_test(tournament_id: int) -> bool:
    """Returns True if the tournament uses TEST bank type."""
    info = tournament_db.get_tournament_info(tournament_id) or {}
    try:
        bank_type = int(info.get("bank_type", 1))
    except Exception:
        bank_type = 1
    return bank_type == 4


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


def place_bet(
    tournament_id: int,
    round_no: int,
    pair_index: int,
    user_id: int,
    bet_on: int,
    amount: float,
    total_rounds: int,
) -> tuple[bool, str]:
    min_bet = get_min_bet(round_no, total_rounds)
    if amount < min_bet:
        return False, f"Минимальная ставка для этого раунда: {min_bet}"

    test = _is_test(tournament_id)

    if not test:
        balance = db.scores.get(user_id, 0)
        if balance < amount:
            return False, "Недостаточно баллов"

    bet_id = tournament_db.create_bet(
        tournament_id, round_no, pair_index, user_id, bet_on, amount
    )
    if bet_id is None:
        return False, "Не удалось создать ставку"

    if not test:
        if not db.update_scores(user_id, -amount):
            return False, "Не удалось списать баллы"
        tournament_db.update_bet_bank(tournament_id, amount)

    return True, f"Ставка принята. ID {bet_id}"


def payout_bets(tournament_id: int, round_no: int, pair_index: int, winner_id: int, total_rounds: int) -> None:
    """Выплачивает ставки для указанной пары.

    ``winner_id`` — ID игрока/команды-победителя (0 если ничья).
    """
    bets = tournament_db.list_bets(tournament_id, round_no)
    test = _is_test(tournament_id)
    for bet in bets:
        if bet.get("pair_index") != pair_index:
            continue
        won = int(bet.get("bet_on")) == winner_id
        multiplier = get_multiplier(round_no, total_rounds)
        payout = math.floor(float(bet.get("amount")) * (1 + multiplier)) if won else 0
        if payout and not test:
            tournament_db.update_bet_bank(tournament_id, -payout)
            db.update_scores(int(bet["user_id"]), payout)
        tournament_db.close_bet(int(bet["id"]), won, payout if not test else 0)


def calculate_payout(round_no: int, total_rounds: int, amount: float) -> int:
    """Возвращает сумму выплаты при победе."""
    multiplier = get_multiplier(round_no, total_rounds)
    return math.floor(amount * (1 + multiplier))


def get_user_bets(tournament_id: int, user_id: int) -> list[dict]:
    """Список активных ставок пользователя."""
    return tournament_db.list_user_bets(tournament_id, user_id, open_only=True)


def cancel_bet(bet_id: int) -> tuple[bool, str]:
    bet = tournament_db.get_bet(bet_id)
    if not bet or bet.get("won") is not None:
        return False, "Ставка не найдена или уже завершена"
    if not tournament_db.delete_bet(bet_id):
        return False, "Не удалось удалить ставку"
    test = _is_test(int(bet["tournament_id"]))
    if not test:
        db.update_scores(int(bet["user_id"]), float(bet["amount"]))
        tournament_db.update_bet_bank(int(bet["tournament_id"]), -float(bet["amount"]))
        return True, "Ставка удалена и баллы возвращены"
    return True, "Ставка удалена (тестовый режим)"


def modify_bet(bet_id: int, bet_on: int, amount: float, user_id: int, total_rounds: int) -> tuple[bool, str]:
    bet = tournament_db.get_bet(bet_id)
    if not bet or bet.get("won") is not None:
        return False, "Ставка не найдена или уже завершена"
    min_bet = get_min_bet(int(bet["round"]), total_rounds)
    if amount < min_bet:
        return False, f"Минимальная ставка для этого раунда: {min_bet}"
    diff = amount - float(bet["amount"])
    test = _is_test(int(bet["tournament_id"]))
    if not test:
        balance = db.scores.get(user_id, 0)
        if diff > 0 and balance < diff:
            return False, "Недостаточно баллов"
        if diff != 0 and not db.update_scores(user_id, -diff):
            return False, "Не удалось обновить баланс"
    if not tournament_db.update_bet(bet_id, bet_on, amount):
        return False, "Не удалось изменить ставку"
    if diff != 0 and not test:
        tournament_db.update_bet_bank(int(bet["tournament_id"]), diff)
    return True, "Ставка обновлена" if not test else "Ставка обновлена (тест)"


def get_pair_summary(tournament_id: int, round_no: int, pair_index: int, winner_id: int, total_rounds: int) -> dict:
    """Возвращает статистику ставок на пару.

    ``winner_id`` — ID игрока/команды-победителя (0 если ничья).
    """
    bets = tournament_db.list_bets(tournament_id, round_no)
    total = 0
    won_cnt = 0
    lose_cnt = 0
    total_amount = 0.0
    payout_total = 0.0
    for b in bets:
        if b.get("pair_index") != pair_index:
            continue
        total += 1
        amt = float(b.get("amount", 0))
        total_amount += amt
        if int(b.get("bet_on")) == winner_id:
            won_cnt += 1
            payout_total += calculate_payout(round_no, total_rounds, amt)
        else:
            lose_cnt += 1
    profit = total_amount - payout_total
    return {
        "total": total,
        "won": won_cnt,
        "lost": lose_cnt,
        "profit": profit,
    }


def pair_started(tournament_id: int, round_no: int, pair_index: int) -> bool:
    """Returns True if any match in the pair has a result set."""
    matches = tournament_db.get_matches(tournament_id, round_no)
    if not matches:
        return False

    pairs: dict[int, list[dict]] = {}
    idx_map: dict[tuple[int, int], int] = {}
    idx = 1
    for m in matches:
        key = (int(m["player1_id"]), int(m["player2_id"]))
        if key not in idx_map:
            idx_map[key] = idx
            idx += 1
        pid = idx_map[key]
        pairs.setdefault(pid, []).append(m)

    for m in pairs.get(pair_index, []):
        if m.get("result") is not None:
            return True
    return False


def refund_all_bets(tournament_id: int, admin_id: int | None = None) -> None:
    """Отменяет все ставки турнира и возвращает банк в общий банк Бебр."""
    bets = tournament_db.list_bets(tournament_id)
    for bet in bets:
        cancel_bet(int(bet["id"]))

    remaining = tournament_db.close_bet_bank(tournament_id)
    if remaining > 0:
        db.add_to_bank(remaining)
        db.log_bank_income(
            admin_id or 0,
            remaining,
            f"Возврат банка ставок турнира #{tournament_id}",
        )
