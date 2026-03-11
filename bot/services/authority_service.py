import logging
from dataclasses import dataclass

from bot.services.accounts_service import AccountsService

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AuthorityResult:
    level: int
    rank_weight: int
    titles: tuple[str, ...]


TITLE_WEIGHTS: dict[str, int] = {
    "глава клуба": 100,
    "главный вице": 100,
    "вице города": 80,
    "ветеран города": 30,
    "участник клубов": 0,
}

COMMAND_LEVELS: dict[str, int] = {
    "points_manage": 30,
    "fine_create": 30,
    "fine_manage": 80,
    "tournament_manage": 80,
    "tickets_manage": 80,
    "players_manage": 80,
    "bank_manage": 100,
    "monthtop_manage": 100,
    "undo_manage": 100,
}


class AuthorityService:
    @staticmethod
    def resolve_authority(provider: str, provider_user_id: str) -> AuthorityResult:
        try:
            account_id = AccountsService.resolve_account_id(provider, str(provider_user_id))
            if not account_id:
                return AuthorityResult(level=0, rank_weight=0, titles=tuple())
            titles = tuple(AccountsService.get_account_titles(account_id))
            max_weight = 0
            for title in titles:
                weight = TITLE_WEIGHTS.get(str(title).strip().lower(), 0)
                if weight > max_weight:
                    max_weight = weight
            return AuthorityResult(level=max_weight, rank_weight=max_weight, titles=titles)
        except Exception:
            logger.exception(
                "resolve_authority failed provider=%s provider_user_id=%s",
                provider,
                provider_user_id,
            )
            return AuthorityResult(level=0, rank_weight=0, titles=tuple())

    @staticmethod
    def has_command_permission(provider: str, provider_user_id: str, command_key: str) -> bool:
        required_level = COMMAND_LEVELS.get(command_key, 100)
        actor = AuthorityService.resolve_authority(provider, provider_user_id)
        allowed = actor.level >= required_level
        logger.info(
            "authority check provider=%s user_id=%s command_key=%s actor_level=%s required=%s allowed=%s",
            provider,
            provider_user_id,
            command_key,
            actor.level,
            required_level,
            allowed,
        )
        return allowed

    @staticmethod
    def can_manage_target(
        actor_provider: str,
        actor_user_id: str,
        target_provider: str,
        target_user_id: str,
    ) -> bool:
        actor = AuthorityService.resolve_authority(actor_provider, actor_user_id)
        target = AuthorityService.resolve_authority(target_provider, target_user_id)

        allowed = actor.rank_weight > target.rank_weight
        logger.info(
            "authority hierarchy check actor=%s:%s (%s) target=%s:%s (%s) allowed=%s",
            actor_provider,
            actor_user_id,
            actor.rank_weight,
            target_provider,
            target_user_id,
            target.rank_weight,
            allowed,
        )
        return allowed
