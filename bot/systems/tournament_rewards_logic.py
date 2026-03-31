"""
Назначение: модуль "tournament rewards logic" реализует продуктовый контур в зоне общая логика.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: общая логика.
"""

import logging

from bot.data import db
from bot.data.tournament_db import get_tournament_info
from bot.services import PointsService, TicketsService
from bot.legacy_identity_logging import (
    log_identity_resolve_error,
    log_legacy_identity_fallback_used,
)


logger = logging.getLogger(__name__)


def _resolve_reward_account_id(discord_user_id: int, tournament_id: int, operation: str) -> str | None:
    account_id = db._get_account_id_for_discord_user(discord_user_id)
    if account_id:
        return account_id
    log_identity_resolve_error(
        logger,
        module=__name__,
        handler="distribute_rewards",
        field="discord_user_id",
        action="replace_with_account_id",
        continue_execution=False,
        tournament_id=tournament_id,
        participant_id=discord_user_id,
        operation=operation,
    )
    return None


def _resolve_author_account_id(author_id: int, tournament_id: int, operation: str) -> str | None:
    author_account_id = db._get_account_id_for_discord_user(author_id)
    if author_account_id:
        return author_account_id
    log_identity_resolve_error(
        logger,
        module=__name__,
        handler=operation,
        field="author_id",
        action="replace_with_account_id",
        continue_execution=False,
        tournament_id=tournament_id,
        author_id=author_id,
    )
    return None


def distribute_rewards(
    tournament_id: int,
    bank_total: float,
    first_team_ids: list[int],
    second_team_ids: list[int],
    author_id: int,
):
    """
    Делит награды между победителями (баллы и билеты).
    Логика:
    - каждому в 1 команде — 50% банка + золотой билет
    - каждому во 2 команде — 25% банка + обычный билет
    """
    reward_first = bank_total * 0.5
    reward_second = bank_total * 0.25
    give_tickets = bank_total > 0

    info = get_tournament_info(tournament_id) or {}
    t_name = info.get("name")
    tournament_title = f"{t_name} (#{tournament_id})" if t_name else f"#{tournament_id}"
    author_account_id = _resolve_author_account_id(author_id, tournament_id, "distribute_rewards")
    if not author_account_id:
        return

    for discord_user_id in first_team_ids:
        account_id = _resolve_reward_account_id(discord_user_id, tournament_id, "reward_first_place")
        if not account_id:
            continue
        PointsService.add_points_by_account(
            account_id,
            reward_first,
            f"🏆 1 место в турнире {tournament_title}",
            author_account_id,
        )
        if give_tickets:
            TicketsService.give_ticket_by_account(
                account_id,
                "gold",
                1,
                f"🥇 Золотой билет за 1 место (турнир {tournament_title})",
                author_account_id,
            )

    for discord_user_id in second_team_ids:
        account_id = _resolve_reward_account_id(discord_user_id, tournament_id, "reward_second_place")
        if not account_id:
            continue
        PointsService.add_points_by_account(
            account_id,
            reward_second,
            f"🥈 2 место в турнире {tournament_title}",
            author_account_id,
        )
        if give_tickets:
            TicketsService.give_ticket_by_account(
                account_id,
                "normal",
                1,
                f"🎟 Обычный билет за 2 место (турнир {tournament_title})",
                author_account_id,
            )


def charge_bank_contribution(user_id: int, user_amount: float, bank_amount: float, reason: str) -> bool:
    """
    Списывает часть баллов с пользователя и/или банка.
    """
    account_id = db._get_account_id_for_discord_user(user_id)
    if not account_id:
        log_identity_resolve_error(
            logger,
            module=__name__,
            handler="charge_bank_contribution",
            field="discord_user_id",
            action="replace_with_account_id",
            continue_execution=False,
            user_id=user_id,
            user_amount=user_amount,
            bank_amount=bank_amount,
        )
        return False

    if user_amount > 0:
        success_user = db.update_scores_by_account(account_id, -user_amount, user_id=user_id)
        if not success_user:
            return False
        if not db.add_action_by_account(account_id, -user_amount, reason, account_id):
            log_legacy_identity_fallback_used(
                logger,
                module=__name__,
                handler="charge_bank_contribution",
                field="account_id",
                action="replace_with_single_account_first_transaction",
                continue_execution=False,
                account_id=account_id,
                reason=reason,
            )
            return False

    if bank_amount > 0:
        return db.spend_from_bank(bank_amount, user_id=user_id, reason=reason)

    return True
