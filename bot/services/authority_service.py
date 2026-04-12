"""
Назначение: модуль "authority service" реализует продуктовый контур в зоне общая логика.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: общая логика.
Доменные операции: операции прав доступа и проверок полномочий.
"""

import logging
from dataclasses import dataclass

from bot.services.accounts_service import AccountsService
from bot.services.profile_titles import normalize_protected_profile_title

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AuthorityResult:
    level: int
    rank_weight: int
    titles: tuple[str, ...]
    account_id: str | None = None


@dataclass(frozen=True)
class ModerationAuthorityDecision:
    allowed: bool
    deny_reason: str | None
    message: str
    actor_account_id: str | None
    target_account_id: str | None
    actor_titles: tuple[str, ...]
    target_titles: tuple[str, ...]
    requested_action: str


TITLE_WEIGHTS: dict[str, int] = {
    "глава клуба": 100,
    "главный вице": 100,
    "оператор": 100,
    "вице города": 80,
    "админ": 80,
    "ветеран города": 30,
    "младший админ": 30,
    "участник клубов": 0,
    "участник чата": 0,
}


ROLE_LEVELS: dict[str, int] = {
    "глава клуба": 100,
    "главный вице": 100,
    "оператор": 100,
    "вице города": 80,
    "админ": 80,
    "ветеран города": 30,
    "младший админ": 30,
    "участник клубов": 0,
    "участник чата": 0,
}

MIN_ROLE_MANAGER_LEVEL = 80
SUPER_ADMIN_ROLE_KEYS = {"глава клуба", "главный вице"}
SUPER_ADMIN_LEVEL = 100
TOP_HIERARCHY_MUTUAL_TITLES = {"глава клуба", "главный вице"}

MODERATION_ACTIONS = {"mute", "warn", "ban"}
MODERATION_MUTE_TITLES = {
    "ветеран города",
    "младший админ",
    "вице города",
    "админ",
    "главный вице",
    "глава клуба",
    "оператор",
}
MODERATION_WARN_TITLES = {
    "вице города",
    "админ",
    "главный вице",
    "глава клуба",
    "оператор",
}
MODERATION_BAN_TITLES = {
    "главный вице",
    "глава клуба",
    "оператор",
}
MODERATION_PERMISSION_TITLES: dict[str, set[str]] = {
    "moderation_mute": MODERATION_MUTE_TITLES,
    "moderation_warn": MODERATION_WARN_TITLES,
    "moderation_ban": MODERATION_BAN_TITLES,
    "moderation_view_cases": MODERATION_MUTE_TITLES,
    "moderation_manage_rules": MODERATION_BAN_TITLES,
}
MODERATION_ACTION_TITLES: dict[str, set[str]] = {
    "mute": MODERATION_MUTE_TITLES,
    "warn": MODERATION_WARN_TITLES,
    "ban": MODERATION_BAN_TITLES,
}

COMMAND_LEVELS: dict[str, int] = {
    "points_manage": 80,
    "fine_create": 30,
    "fine_manage": 80,
    "tournament_manage": 80,
    "tickets_manage": 100,
    "players_manage": 80,
    "bank_manage": 100,
    "undo_manage": 100,
}

FALLBACK_CHAT_MEMBER_TITLE = "Участник чата"


class AuthorityService:
    @staticmethod
    def _effective_titles(raw_titles: list[str] | tuple[str, ...]) -> tuple[str, ...]:
        normalized_existing = {
            normalize_protected_profile_title(title)
            for title in raw_titles
            if str(title).strip()
        }
        if "участник чата" in normalized_existing:
            return tuple(raw_titles)
        has_non_chat_member_title = bool(normalized_existing - {"участник чата"})
        if has_non_chat_member_title:
            return tuple(raw_titles)
        return (*tuple(raw_titles), FALLBACK_CHAT_MEMBER_TITLE)

    @staticmethod
    def _normalized_titles(titles: tuple[str, ...]) -> set[str]:
        return {normalize_protected_profile_title(title) for title in titles if str(title).strip()}

    @staticmethod
    def _moderation_action_message(action_key: str) -> str:
        if action_key == "warn":
            return "Предупреждение доступно только ролям уровня Вице города / Админ и выше"
        if action_key == "ban":
            return "Бан доступен только Главному вице, Главе клуба и Оператору"
        return "Модерация недоступна по вашему званию"

    @staticmethod
    def _build_moderation_decision(
        *,
        allowed: bool,
        deny_reason: str | None,
        message: str,
        actor: AuthorityResult,
        target: AuthorityResult,
        requested_action: str,
    ) -> ModerationAuthorityDecision:
        decision = ModerationAuthorityDecision(
            allowed=allowed,
            deny_reason=deny_reason,
            message=message,
            actor_account_id=actor.account_id,
            target_account_id=target.account_id,
            actor_titles=actor.titles,
            target_titles=target.titles,
            requested_action=requested_action,
        )
        logger.info(
            "moderation authority check actor_account_id=%s target_account_id=%s actor_titles=%s target_titles=%s requested_action=%s allowed=%s deny_reason=%s",
            decision.actor_account_id,
            decision.target_account_id,
            list(decision.actor_titles),
            list(decision.target_titles),
            decision.requested_action,
            decision.allowed,
            decision.deny_reason,
        )
        return decision

    @staticmethod
    def _can_manage_target_authority(
        actor: AuthorityResult,
        target: AuthorityResult,
    ) -> tuple[bool, set[str], set[str]]:
        actor_titles = AuthorityService._normalized_titles(actor.titles)
        target_titles = AuthorityService._normalized_titles(target.titles)

        actor_is_top_peer = bool(actor_titles & TOP_HIERARCHY_MUTUAL_TITLES)
        target_is_top_peer = bool(target_titles & TOP_HIERARCHY_MUTUAL_TITLES)

        allowed = actor.rank_weight > target.rank_weight
        if not allowed and actor.rank_weight == target.rank_weight == 100 and actor_is_top_peer and target_is_top_peer:
            allowed = True
        return allowed, actor_titles, target_titles

    @staticmethod
    def is_super_admin(actor_provider: str, actor_user_id: str) -> bool:
        actor = AuthorityService.resolve_authority(actor_provider, actor_user_id)
        actor_titles = AuthorityService._normalized_titles(actor.titles)
        allowed = bool(actor_titles & SUPER_ADMIN_ROLE_KEYS)
        logger.info(
            "authority super-admin check actor=%s:%s actor_level=%s titles=%s allowed=%s",
            actor_provider,
            actor_user_id,
            actor.level,
            sorted(actor_titles),
            allowed,
        )
        return allowed

    @staticmethod
    def resolve_authority(provider: str, provider_user_id: str) -> AuthorityResult:
        try:
            account_id = AccountsService.resolve_account_id(provider, str(provider_user_id))
            if not account_id:
                return AuthorityResult(level=0, rank_weight=0, titles=tuple(), account_id=None)
            account_id = str(account_id)
            titles = AuthorityService._effective_titles(tuple(AccountsService.get_account_titles(account_id)))
            max_weight = 0
            for title in titles:
                weight = TITLE_WEIGHTS.get(normalize_protected_profile_title(title), 0)
                if weight > max_weight:
                    max_weight = weight
            return AuthorityResult(level=max_weight, rank_weight=max_weight, titles=titles, account_id=account_id)
        except Exception:
            logger.exception(
                "resolve_authority failed provider=%s provider_user_id=%s",
                provider,
                provider_user_id,
            )
            return AuthorityResult(level=0, rank_weight=0, titles=tuple(), account_id=None)

    @staticmethod
    def has_command_permission(provider: str, provider_user_id: str, command_key: str) -> bool:
        actor = AuthorityService.resolve_authority(provider, provider_user_id)
        actor_titles = AuthorityService._normalized_titles(actor.titles)
        is_operator_only = "оператор" in actor_titles and not bool(actor_titles & SUPER_ADMIN_ROLE_KEYS)

        if is_operator_only and command_key not in MODERATION_PERMISSION_TITLES:
            logger.info(
                "authority operator restriction provider=%s user_id=%s command_key=%s actor_level=%s actor_titles=%s allowed=%s",
                provider,
                provider_user_id,
                command_key,
                actor.level,
                sorted(actor_titles),
                False,
            )
            return False

        if command_key in MODERATION_PERMISSION_TITLES:
            allowed_titles = MODERATION_PERMISSION_TITLES[command_key]
            allowed = bool(actor_titles & allowed_titles)
            logger.info(
                "authority permission check provider=%s user_id=%s command_key=%s actor_level=%s actor_titles=%s allowed=%s mode=title_matrix",
                provider,
                provider_user_id,
                command_key,
                actor.level,
                sorted(actor_titles),
                allowed,
            )
            return allowed

        required_level = COMMAND_LEVELS.get(command_key, 100)
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

        allowed, actor_titles, target_titles = AuthorityService._can_manage_target_authority(actor, target)
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
    def can_apply_moderation_action(
        actor_provider: str,
        actor_user_id: str,
        target_provider: str,
        target_user_id: str,
        action: str,
    ) -> ModerationAuthorityDecision:
        requested_action = str(action or "").strip().lower()
        actor = AuthorityService.resolve_authority(actor_provider, actor_user_id)
        target = AuthorityService.resolve_authority(target_provider, target_user_id)
        actor_titles = AuthorityService._normalized_titles(actor.titles)

        if requested_action not in MODERATION_ACTIONS:
            return AuthorityService._build_moderation_decision(
                allowed=False,
                deny_reason="unknown_action",
                message="Неизвестный тип модерации",
                actor=actor,
                target=target,
                requested_action=requested_action,
            )

        allowed_titles = MODERATION_ACTION_TITLES[requested_action]
        if not (actor_titles & allowed_titles):
            message = AuthorityService._moderation_action_message(requested_action)
            if requested_action in {"warn", "ban"} and actor_titles & {"ветеран города", "младший админ"}:
                message = "Вы можете выдавать только мут участникам"
            return AuthorityService._build_moderation_decision(
                allowed=False,
                deny_reason="action_not_permitted",
                message=message,
                actor=actor,
                target=target,
                requested_action=requested_action,
            )

        can_manage_target, _, _ = AuthorityService._can_manage_target_authority(actor, target)
        if not can_manage_target:
            return AuthorityService._build_moderation_decision(
                allowed=False,
                deny_reason="hierarchy_denied",
                message="Нельзя модерировать пользователя с равным или более высоким званием",
                actor=actor,
                target=target,
                requested_action=requested_action,
            )

        return AuthorityService._build_moderation_decision(
            allowed=True,
            deny_reason=None,
            message="Разрешено",
            actor=actor,
            target=target,
            requested_action=requested_action,
        )

    @staticmethod
    def can_manage_role(actor_provider: str, actor_user_id: str, target_role: str) -> bool:
        actor = AuthorityService.resolve_authority(actor_provider, actor_user_id)
        actor_titles = AuthorityService._normalized_titles(actor.titles)
        role_key = normalize_protected_profile_title(target_role)
        target_level = ROLE_LEVELS.get(role_key, 0)
        is_operator_only = "оператор" in actor_titles and not bool(actor_titles & SUPER_ADMIN_ROLE_KEYS)

        if is_operator_only:
            logger.info(
                "authority role-manage denied: operator has moderation-only scope actor=%s:%s actor_level=%s target_role=%s",
                actor_provider,
                actor_user_id,
                actor.level,
                target_role,
            )
            return False

        if role_key in SUPER_ADMIN_ROLE_KEYS and actor.level < SUPER_ADMIN_LEVEL:
            logger.info(
                "authority role-manage denied: super role requires level=%s actor=%s:%s actor_level=%s target_role=%s",
                SUPER_ADMIN_LEVEL,
                actor_provider,
                actor_user_id,
                actor.level,
                target_role,
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

    @staticmethod
    def can_manage_role_categories(actor_provider: str, actor_user_id: str) -> bool:
        actor = AuthorityService.resolve_authority(actor_provider, actor_user_id)
        actor_titles = AuthorityService._normalized_titles(actor.titles)
        allowed = bool(actor_titles & SUPER_ADMIN_ROLE_KEYS)
        logger.info(
            "authority category-manage check actor=%s:%s actor_level=%s titles=%s allowed=%s",
            actor_provider,
            actor_user_id,
            actor.level,
            sorted(actor_titles),
            allowed,
        )
        return allowed
