"""
Назначение: модуль "guiy admin service" реализует продуктовый контур в зоне общая логика.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: общая логика.
Доменные операции: операции админ-панели GUIY и модераторских действий.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Literal

from bot.data import db
from bot.services.accounts_service import AccountsService
from bot.services.ai_service import _is_father_user


logger = logging.getLogger(__name__)


def _parse_env_id_set(var_name: str) -> set[str]:
    raw = (os.getenv(var_name) or "").strip()
    if not raw:
        return set()
    return {item.strip() for item in raw.split(",") if item.strip()}


GUIY_OWNER_DENIED_MESSAGE = "❌ Команда сейчас недоступна."
GUIY_OWNER_REPLY_REQUIRED_MESSAGE = (
    "ℹ️ Для этого действия ответьте командой на сообщение Гуя."
)
GUIY_OWNER_USAGE_TEXT = (
    "🛠️ Управление Гуем доступно только владельцу.\n"
    "Зачем нужна регистрация профиля Гуя: она создаёт общий аккаунт самого бота, куда сохраняются никнейм, описание, Null's ID и отображаемые роли.\n"
    "Что делает регистрация: находит identity бота по его platform user id и, если записи ещё нет, создаёт её без привязки к владельцу. Повторный запуск безопасен.\n"
    "Что делать дальше: после регистрации откройте редактирование профиля и заполните нужные поля.\n\n"
    "Форматы:\n"
    "• /guiy_owner register_profile — зарегистрировать общий профиль Гуя перед первым редактированием\n"
    "• /guiy_owner say <текст> — отправить новое сообщение от лица Гуя\n"
    "• /guiy_owner reply <текст> — ответить от лица Гуя именно на сообщение Гуя\n"
    "• /guiy_owner profile <поле> | <значение> — изменить профиль Гуя\n"
    "• /guiy_owner cancel — вручную отключить текущий owner-сценарий, если передумали или кажется, что бот завис в режиме ожидания\n"
    "Доступные поля профиля: custom_nick, description, nulls_brawl_id, visible_roles.\n"
    "Если значение нужно очистить, после | оставьте пусто или передайте -"
)
GUIY_OWNER_ALLOWED_PROFILE_FIELDS = {"custom_nick", "description", "nulls_brawl_id", "visible_roles"}


@dataclass(slots=True)
class GuiyProfileBootstrapResult:
    ok: bool
    created: bool
    status: Literal["created", "already_exists", "error"]
    provider: str
    bot_user_id: str | None
    guiy_account_id: str | None
    message: str


def bootstrap_guiy_profile(
    *,
    provider: str | None,
    bot_user_id: str | int | None,
) -> GuiyProfileBootstrapResult:
    normalized_provider = (provider or "").strip().lower()
    normalized_bot_user_id = str(bot_user_id).strip() if bot_user_id is not None else ""

    if normalized_provider not in {"telegram", "discord"} or not normalized_bot_user_id:
        logger.error(
            "guiy profile bootstrap invalid parameters provider=%s bot_user_id=%s",
            normalized_provider or None,
            normalized_bot_user_id or None,
        )
        return GuiyProfileBootstrapResult(
            ok=False,
            created=False,
            status="error",
            provider=normalized_provider,
            bot_user_id=normalized_bot_user_id or None,
            guiy_account_id=None,
            message="❌ Не удалось определить профиль Гуя для регистрации.",
        )

    try:
        existing_account_id = AccountsService.resolve_account_id(normalized_provider, normalized_bot_user_id)
    except Exception:
        logger.exception(
            "guiy profile bootstrap resolve failed provider=%s bot_user_id=%s",
            normalized_provider,
            normalized_bot_user_id,
        )
        return GuiyProfileBootstrapResult(
            ok=False,
            created=False,
            status="error",
            provider=normalized_provider,
            bot_user_id=normalized_bot_user_id,
            guiy_account_id=None,
            message="❌ Не удалось проверить регистрацию профиля Гуя. Проверьте логи и БД.",
        )

    if existing_account_id:
        logger.info(
            "guiy profile bootstrap already registered provider=%s bot_user_id=%s account_id=%s",
            normalized_provider,
            normalized_bot_user_id,
            existing_account_id,
        )
        return GuiyProfileBootstrapResult(
            ok=True,
            created=False,
            status="already_exists",
            provider=normalized_provider,
            bot_user_id=normalized_bot_user_id,
            guiy_account_id=str(existing_account_id),
            message=(
                "✅ Профиль Гуя уже зарегистрирован.\n"
                "Теперь можно открыть редактирование профиля и изменить нужные поля."
            ),
        )

    try:
        success, response = AccountsService.register_identity(normalized_provider, normalized_bot_user_id)
    except Exception:
        logger.exception(
            "guiy profile bootstrap register failed provider=%s bot_user_id=%s",
            normalized_provider,
            normalized_bot_user_id,
        )
        return GuiyProfileBootstrapResult(
            ok=False,
            created=False,
            status="error",
            provider=normalized_provider,
            bot_user_id=normalized_bot_user_id,
            guiy_account_id=None,
            message="❌ Не удалось зарегистрировать профиль Гуя. Проверьте логи и БД.",
        )

    if not success:
        logger.error(
            "guiy profile bootstrap register returned error provider=%s bot_user_id=%s response=%s",
            normalized_provider,
            normalized_bot_user_id,
            response,
        )
        return GuiyProfileBootstrapResult(
            ok=False,
            created=False,
            status="error",
            provider=normalized_provider,
            bot_user_id=normalized_bot_user_id,
            guiy_account_id=None,
            message=(
                "❌ Не удалось зарегистрировать профиль Гуя. "
                f"Причина: {response or 'смотри логи сервиса'}."
            ),
        )

    try:
        registered_account_id = AccountsService.resolve_account_id(normalized_provider, normalized_bot_user_id)
    except Exception:
        logger.exception(
            "guiy profile bootstrap resolve after register failed provider=%s bot_user_id=%s",
            normalized_provider,
            normalized_bot_user_id,
        )
        return GuiyProfileBootstrapResult(
            ok=False,
            created=False,
            status="error",
            provider=normalized_provider,
            bot_user_id=normalized_bot_user_id,
            guiy_account_id=None,
            message="❌ Профиль Гуя зарегистрирован, но не удалось подтвердить account_id. Проверьте логи и БД.",
        )

    if not registered_account_id:
        logger.error(
            "guiy profile bootstrap missing account after register provider=%s bot_user_id=%s register_response=%s",
            normalized_provider,
            normalized_bot_user_id,
            response,
        )
        return GuiyProfileBootstrapResult(
            ok=False,
            created=False,
            status="error",
            provider=normalized_provider,
            bot_user_id=normalized_bot_user_id,
            guiy_account_id=None,
            message="❌ Профиль Гуя зарегистрирован, но account_id не найден. Проверьте логи и БД.",
        )

    logger.info(
        "guiy profile bootstrap registered provider=%s bot_user_id=%s account_id=%s",
        normalized_provider,
        normalized_bot_user_id,
        registered_account_id,
    )
    return GuiyProfileBootstrapResult(
        ok=True,
        created=True,
        status="created",
        provider=normalized_provider,
        bot_user_id=normalized_bot_user_id,
        guiy_account_id=str(registered_account_id),
        message=(
            "✅ Профиль Гуя зарегистрирован.\n"
            "Теперь можно открыть редактирование профиля и изменить нужные поля."
        ),
    )


@dataclass(slots=True)
class GuiyOwnerAccessResult:
    allowed: bool
    actor_provider: str
    actor_user_id: str | None
    resolved_account_id: str | None
    target_message_id: str | None
    requested_action: str
    denial_message: str = GUIY_OWNER_DENIED_MESSAGE


@dataclass(slots=True)
class GuiyOwnerTargetResolution:
    ok: bool
    target_account_id: str | None
    target_provider_user_id: str | None
    message: str


def authorize_guiy_owner_action(
    *,
    actor_provider: str | None,
    actor_user_id: str | int | None,
    requested_action: str,
    target_message_id: str | int | None = None,
) -> GuiyOwnerAccessResult:
    normalized_provider = (actor_provider or "").strip().lower()
    normalized_user_id = str(actor_user_id).strip() if actor_user_id is not None else None
    normalized_target_message_id = str(target_message_id).strip() if target_message_id is not None else None
    resolved_account_id: str | None = None
    allowed = False

    if normalized_provider in {"telegram", "discord"} and normalized_user_id:
        try:
            resolved_account_id = AccountsService.resolve_account_id(normalized_provider, normalized_user_id)
        except Exception:
            logger.exception(
                "guiy owner actor account resolve failed actor_provider=%s actor_user_id=%s target_message_id=%s requested_action=%s",
                normalized_provider,
                normalized_user_id,
                normalized_target_message_id,
                requested_action,
            )

        try:
            allowed = _is_father_user(normalized_provider, normalized_user_id)
        except Exception:
            logger.exception(
                "guiy owner authorization crashed actor_provider=%s actor_user_id=%s resolved_account_id=%s target_message_id=%s requested_action=%s",
                normalized_provider,
                normalized_user_id,
                resolved_account_id,
                normalized_target_message_id,
                requested_action,
            )
            allowed = False

    logger.info(
        "guiy owner access attempt actor_provider=%s actor_user_id=%s resolved_account_id=%s allowed=%s target_message_id=%s requested_action=%s",
        normalized_provider or None,
        normalized_user_id,
        resolved_account_id,
        allowed,
        normalized_target_message_id,
        requested_action,
    )
    return GuiyOwnerAccessResult(
        allowed=allowed,
        actor_provider=normalized_provider,
        actor_user_id=normalized_user_id,
        resolved_account_id=resolved_account_id,
        target_message_id=normalized_target_message_id,
        requested_action=requested_action,
    )


def resolve_guiy_target_account(
    *,
    provider: str | None,
    bot_user_id: str | int | None,
    reply_author_user_id: str | int | None = None,
    explicit_owner_command: bool,
) -> GuiyOwnerTargetResolution:
    normalized_provider = (provider or "").strip().lower()
    normalized_bot_user_id = str(bot_user_id).strip() if bot_user_id is not None else ""
    normalized_reply_author_user_id = str(reply_author_user_id).strip() if reply_author_user_id is not None else ""

    if normalized_provider not in {"telegram", "discord"} or not normalized_bot_user_id:
        logger.error(
            "guiy owner target resolve invalid provider=%s bot_user_id=%s reply_author_user_id=%s explicit_owner_command=%s",
            normalized_provider,
            normalized_bot_user_id or None,
            normalized_reply_author_user_id or None,
            explicit_owner_command,
        )
        return GuiyOwnerTargetResolution(False, None, None, GUIY_OWNER_DENIED_MESSAGE)

    if normalized_reply_author_user_id and normalized_reply_author_user_id != normalized_bot_user_id:
        logger.warning(
            "guiy owner target resolve denied non-guiy reply provider=%s bot_user_id=%s reply_author_user_id=%s explicit_owner_command=%s",
            normalized_provider,
            normalized_bot_user_id,
            normalized_reply_author_user_id,
            explicit_owner_command,
        )
        return GuiyOwnerTargetResolution(False, None, None, GUIY_OWNER_DENIED_MESSAGE)

    if not explicit_owner_command and not normalized_reply_author_user_id:
        logger.info(
            "guiy owner target resolve requires reply provider=%s bot_user_id=%s explicit_owner_command=%s",
            normalized_provider,
            normalized_bot_user_id,
            explicit_owner_command,
        )
        return GuiyOwnerTargetResolution(False, None, None, GUIY_OWNER_REPLY_REQUIRED_MESSAGE)

    try:
        target_account_id = AccountsService.resolve_account_id(normalized_provider, normalized_bot_user_id)
    except Exception:
        logger.exception(
            "guiy owner target account resolve failed provider=%s bot_user_id=%s reply_author_user_id=%s explicit_owner_command=%s",
            normalized_provider,
            normalized_bot_user_id,
            normalized_reply_author_user_id or None,
            explicit_owner_command,
        )
        return GuiyOwnerTargetResolution(False, None, normalized_bot_user_id, GUIY_OWNER_DENIED_MESSAGE)

    if not target_account_id:
        logger.warning(
            "guiy owner target account missing provider=%s bot_user_id=%s reply_author_user_id=%s explicit_owner_command=%s",
            normalized_provider,
            normalized_bot_user_id,
            normalized_reply_author_user_id or None,
            explicit_owner_command,
        )
        return GuiyOwnerTargetResolution(False, None, normalized_bot_user_id, GUIY_OWNER_DENIED_MESSAGE)

    return GuiyOwnerTargetResolution(True, str(target_account_id), normalized_bot_user_id, "")


def parse_guiy_owner_profile_payload(payload: str) -> tuple[str | None, str | None]:
    raw = str(payload or "").strip()
    if not raw or "|" not in raw:
        return None, None
    field_name, value = raw.split("|", 1)
    normalized_field = field_name.strip().lower()
    if normalized_field not in GUIY_OWNER_ALLOWED_PROFILE_FIELDS:
        return None, None
    normalized_value = value.strip()
    if normalized_value == "-":
        normalized_value = ""
    return normalized_field, normalized_value


def resolve_guiy_owner_telegram_ids() -> list[int]:
    owner_ids: set[int] = set()

    for raw_user_id in _parse_env_id_set("GUIY_FATHER_TELEGRAM_IDS"):
        if str(raw_user_id).isdigit():
            owner_ids.add(int(raw_user_id))

    account_ids = _parse_env_id_set("GUIY_FATHER_ACCOUNT_IDS")
    if not account_ids or not db.supabase:
        return sorted(owner_ids)

    try:
        response = (
            db.supabase.table("account_identities")
            .select("account_id,provider,provider_user_id")
            .eq("provider", "telegram")
            .in_("account_id", sorted(account_ids))
            .execute()
        )
        for row in response.data or []:
            provider_user_id = str(row.get("provider_user_id") or "").strip()
            if provider_user_id.isdigit():
                owner_ids.add(int(provider_user_id))
    except Exception:
        logger.exception(
            "guiy owner telegram ids resolve failed account_ids=%s",
            sorted(account_ids),
        )

    return sorted(owner_ids)
