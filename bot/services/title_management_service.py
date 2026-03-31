"""
Назначение: модуль "title management service" реализует продуктовый контур в зоне общая логика.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: общая логика.
Доменные операции: операции управления титулами и доступностью.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from bot.services.accounts_service import AccountsService
from bot.services.authority_service import AuthorityService, ROLE_LEVELS
from bot.services.profile_titles import normalize_protected_profile_title

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TitleChangeResult:
    ok: bool
    message: str
    titles: tuple[str, ...] = tuple()


class TitleManagementService:
    @staticmethod
    def managed_titles() -> list[tuple[str, str]]:
        unique: dict[str, int] = {}
        for key, weight in ROLE_LEVELS.items():
            normalized = normalize_protected_profile_title(key)
            if not normalized:
                continue
            current = unique.get(normalized)
            if current is None or weight > current:
                unique[normalized] = int(weight)
        ordered = sorted(unique.items(), key=lambda item: (-item[1], item[0]))
        return [(key, key[:1].upper() + key[1:]) for key, _ in ordered]

    @staticmethod
    def is_super_admin(provider: str, provider_user_id: str) -> bool:
        return AuthorityService.is_super_admin(provider, provider_user_id)

    @staticmethod
    def get_target_titles(target_provider: str, target_user_id: str) -> tuple[str, ...]:
        account_id = AccountsService.resolve_account_id(target_provider, target_user_id)
        if not account_id:
            return tuple()
        try:
            return tuple(AccountsService.get_account_titles(account_id))
        except Exception:
            logger.exception(
                "title fetch target titles failed target=%s:%s account_id=%s",
                target_provider,
                target_user_id,
                account_id,
            )
            return tuple()

    @staticmethod
    def apply_title_change(
        *,
        actor_provider: str,
        actor_user_id: str,
        target_provider: str,
        target_user_id: str,
        title_key: str,
        mode: str,
        source: str,
    ) -> TitleChangeResult:
        normalized_mode = str(mode or "").strip().lower()
        normalized_title = normalize_protected_profile_title(title_key)
        if normalized_mode not in {"promote", "demote"}:
            return TitleChangeResult(ok=False, message="❌ Некорректный режим изменения звания.")
        if not normalized_title:
            return TitleChangeResult(ok=False, message="❌ Не удалось определить выбранное звание.")
        if not TitleManagementService.is_super_admin(actor_provider, actor_user_id):
            return TitleChangeResult(ok=False, message="❌ Повышать или понижать звания могут только суперадмины.")

        account_id = AccountsService.resolve_account_id(target_provider, target_user_id)
        if not account_id:
            return TitleChangeResult(ok=False, message="❌ У цели нет привязанного общего аккаунта. Сначала выполните /register и привязку.")

        current_titles = list(AccountsService.get_account_titles(account_id))
        normalized_current = {normalize_protected_profile_title(item): item for item in current_titles if str(item).strip()}
        has_title = normalized_title in normalized_current

        if normalized_mode == "promote":
            if has_title:
                return TitleChangeResult(ok=True, message=f"ℹ️ У пользователя уже есть звание «{normalized_title}».", titles=tuple(current_titles))
            updated = [*current_titles, normalized_title]
            success = AccountsService.save_account_titles(account_id, updated, source=source)
            if not success:
                logger.error(
                    "title promote save failed actor=%s:%s target=%s:%s account_id=%s title=%s source=%s",
                    actor_provider,
                    actor_user_id,
                    target_provider,
                    target_user_id,
                    account_id,
                    normalized_title,
                    source,
                )
                return TitleChangeResult(ok=False, message="❌ Не удалось сохранить повышение звания (подробности в логах).")
            logger.info(
                "title promote applied actor=%s:%s target=%s:%s account_id=%s title=%s source=%s",
                actor_provider,
                actor_user_id,
                target_provider,
                target_user_id,
                account_id,
                normalized_title,
                source,
            )
            return TitleChangeResult(ok=True, message=f"✅ Звание «{normalized_title}» добавлено.", titles=tuple(updated))

        if not has_title:
            return TitleChangeResult(ok=True, message=f"ℹ️ У пользователя нет звания «{normalized_title}».", titles=tuple(current_titles))
        updated = [item for item in current_titles if normalize_protected_profile_title(item) != normalized_title]
        success = AccountsService.save_account_titles(account_id, updated, source=source)
        if not success:
            logger.error(
                "title demote save failed actor=%s:%s target=%s:%s account_id=%s title=%s source=%s",
                actor_provider,
                actor_user_id,
                target_provider,
                target_user_id,
                account_id,
                normalized_title,
                source,
            )
            return TitleChangeResult(ok=False, message="❌ Не удалось сохранить понижение звания (подробности в логах).")
        logger.info(
            "title demote applied actor=%s:%s target=%s:%s account_id=%s title=%s source=%s",
            actor_provider,
            actor_user_id,
            target_provider,
            target_user_id,
            account_id,
            normalized_title,
            source,
        )
        return TitleChangeResult(ok=True, message=f"✅ Звание «{normalized_title}» снято.", titles=tuple(updated))
