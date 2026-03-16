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


ROLE_LEVELS: dict[str, int] = {
    "глава клуба": 100,
    "главный вице": 100,
    "вице города": 80,
    "оператор": 80,
    "ветеран города": 30,
    "участник клубов": 0,
}

MAX_MANAGEABLE_ROLE_LEVEL = 80
MIN_ROLE_MANAGER_LEVEL = 80


COMMAND_LEVELS: dict[str, int] = {
    "points_manage": 80,
    "fine_create": 30,
    "fine_manage": 80,
    "tournament_manage": 80,
    "tickets_manage": 100,
    "players_manage": 80,
    "bank_manage": 100,
    "monthtop_manage": 100,
    "undo_manage": 100,
}


class AuthorityService:
    @staticmethod
    def _normalized_titles(titles: tuple[str, ...]) -> set[str]:
        return {str(title).strip().lower() for title in titles}

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
    def can_manage_self(provider: str, provider_user_id: str) -> bool:
        actor = AuthorityService.resolve_authority(provider, provider_user_id)
        actor_titles = AuthorityService._normalized_titles(actor.titles)
        allowed = bool(actor_titles & {"глава клуба", "главный вице"})
        logger.info(
            "authority self-manage check provider=%s user_id=%s titles=%s allowed=%s",
            provider,
            provider_user_id,
            sorted(actor_titles),
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

        actor_titles = AuthorityService._normalized_titles(actor.titles)
        target_titles = AuthorityService._normalized_titles(target.titles)

        peer_titles = {"глава клуба", "главный вице"}
        actor_is_peer = bool(actor_titles & peer_titles)
        target_is_peer = bool(target_titles & peer_titles)

        allowed = actor.rank_weight > target.rank_weight
        if not allowed and actor.rank_weight == target.rank_weight == 100 and actor_is_peer and target_is_peer:
            allowed = True
        logger.info(
            "authority hierarchy check actor=%s:%s (%s:%s) target=%s:%s (%s:%s) allowed=%s",
            actor_provider,
            actor_user_id,
            actor.rank_weight,
            sorted(actor_titles),
            target_provider,
            target_user_id,
            target.rank_weight,
            sorted(target_titles),
            allowed,
        )
        return allowed


    @staticmethod
    def can_manage_role(actor_provider: str, actor_user_id: str, target_role: str) -> bool:
        actor = AuthorityService.resolve_authority(actor_provider, actor_user_id)
        role_key = str(target_role).strip().lower()
        target_level = ROLE_LEVELS.get(role_key, 0)

        if target_level > MAX_MANAGEABLE_ROLE_LEVEL:
            logger.info(
                "authority role-manage denied: target role above operator actor=%s:%s target_role=%s target_level=%s max=%s",
                actor_provider,
                actor_user_id,
                target_role,
                target_level,
                MAX_MANAGEABLE_ROLE_LEVEL,
            )
            return False

        allowed = actor.level >= MIN_ROLE_MANAGER_LEVEL and actor.level >= target_level
        logger.info(
            "authority role-manage check actor=%s:%s actor_level=%s min_level=%s target_role=%s target_level=%s allowed=%s",
            actor_provider,
            actor_user_id,
            actor.level,
            MIN_ROLE_MANAGER_LEVEL,
            target_role,
            target_level,
            allowed,
        )
        return allowed
