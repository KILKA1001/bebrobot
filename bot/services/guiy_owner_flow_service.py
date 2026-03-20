from __future__ import annotations

from dataclasses import dataclass

from bot.services import AccountsService
from bot.services.guiy_admin_service import (
    GUIY_OWNER_DENIED_MESSAGE,
    GUIY_OWNER_REPLY_REQUIRED_MESSAGE,
    GUIY_OWNER_USAGE_TEXT,
    authorize_guiy_owner_action,
    bootstrap_guiy_profile,
    resolve_guiy_target_account,
)


@dataclass(frozen=True, slots=True)
class GuiyOwnerActionSpec:
    action: str
    title: str
    instruction: str
    requires_text: bool = False
    requires_reply_context: bool = False


@dataclass(frozen=True, slots=True)
class GuiyOwnerProfileFieldSpec:
    field_name: str
    title: str
    instruction: str
    placeholder: str
    max_length: int


@dataclass(slots=True)
class GuiyOwnerFlowResult:
    ok: bool
    selected_action: str
    message: str
    guiy_account_id: str | None = None
    target_message_id: str | None = None
    reply_to_message_id: int | None = None
    outbound_text: str | None = None
    target_provider_user_id: str | None = None


GUIY_OWNER_ACTION_SPECS: dict[str, GuiyOwnerActionSpec] = {
    "say": GuiyOwnerActionSpec(
        action="say",
        title="Написать от Гуя",
        instruction=(
            "Сначала выберите место публикации. После подтверждения Гуй отправит новое сообщение "
            "в выбранную группу или канал. Следующим сообщением отправьте текст, который должен появиться от лица Гуя."
        ),
        requires_text=True,
    ),
    "reply": GuiyOwnerActionSpec(
        action="reply",
        title="Ответить от Гуя",
        instruction=(
            "После подтверждения Гуй ответит именно на выбранное сообщение. "
            "Откройте меню команд ответом на сообщение Гуя и затем отправьте текст ответа."
        ),
        requires_text=True,
        requires_reply_context=True,
    ),
    "profile": GuiyOwnerActionSpec(
        action="profile",
        title="Профиль Гуя",
        instruction=(
            "Выберите поле профиля. После подтверждения бот сохранит новое значение "
            "в общем аккаунте Гуя и оно сразу появится в /profile."
        ),
    ),
    "register_profile": GuiyOwnerActionSpec(
        action="register_profile",
        title="Зарегистрировать профиль Гуя",
        instruction=(
            "Эта кнопка создаёт общий аккаунт для Гуя, если он ещё не зарегистрирован. "
            "Повторное нажатие безопасно: существующий профиль не сломается."
        ),
    ),
    "cancel": GuiyOwnerActionSpec(
        action="cancel",
        title="Отмена",
        instruction="Отменяет текущий сценарий owner-управления и очищает ожидаемые шаги.",
    ),
}

GUIY_OWNER_PROFILE_FIELDS: dict[str, GuiyOwnerProfileFieldSpec] = {
    "custom_nick": GuiyOwnerProfileFieldSpec(
        field_name="custom_nick",
        title="Никнейм",
        instruction=(
            "Введите новый никнейм Гуя. После подтверждения это имя будет показано "
            "в профиле общего аккаунта."
        ),
        placeholder="Например: Гуй Брат",
        max_length=32,
    ),
    "description": GuiyOwnerProfileFieldSpec(
        field_name="description",
        title="Описание",
        instruction=(
            "Введите краткое описание Гуя. После подтверждения этот текст увидят "
            "пользователи в /profile."
        ),
        placeholder="Коротко опишите, кто такой Гуй",
        max_length=100,
    ),
    "nulls_brawl_id": GuiyOwnerProfileFieldSpec(
        field_name="nulls_brawl_id",
        title="Null's ID",
        instruction=(
            "Введите Null's ID Гуя. После подтверждения значение обновится "
            "в карточке профиля."
        ),
        placeholder="Например: #ABCD123",
        max_length=32,
    ),
    "visible_roles": GuiyOwnerProfileFieldSpec(
        field_name="visible_roles",
        title="Отображаемые роли",
        instruction=(
            "Выберите роли, которые будут видны в профиле Гуя. "
            "После сохранения список сразу изменится в профиле."
        ),
        placeholder="Выбор кнопками",
        max_length=255,
    ),
}


def parse_guiy_owner_text_command(raw_args: str | None) -> tuple[str, str]:
    cleaned = str(raw_args or "").strip()
    if not cleaned:
        return "", ""
    parts = cleaned.split(maxsplit=1)
    action = parts[0].strip().lower()
    payload = parts[1].strip() if len(parts) > 1 else ""
    return action, payload


def get_guiy_owner_action_spec(action: str | None) -> GuiyOwnerActionSpec | None:
    return GUIY_OWNER_ACTION_SPECS.get(str(action or "").strip().lower())


def get_guiy_owner_profile_field_spec(field_name: str | None) -> GuiyOwnerProfileFieldSpec | None:
    return GUIY_OWNER_PROFILE_FIELDS.get(str(field_name or "").strip().lower())


def resolve_guiy_profile_catalog(
    *,
    provider: str,
    bot_user_id: str | int,
    display_name: str | None = None,
) -> tuple[dict, list[dict[str, str]], list[str]]:
    profile = AccountsService.get_profile(provider, str(bot_user_id), display_name=display_name) or {}
    roles_by_category = profile.get("roles_by_category") or {}
    catalog: list[dict[str, str]] = []
    for category_name in sorted(roles_by_category.keys(), key=lambda value: str(value).lower()):
        role_names = sorted(
            {
                str(role_name).strip()
                for role_name in (roles_by_category.get(category_name) or [])
                if str(role_name).strip()
            },
            key=lambda value: value.lower(),
        )
        for role_name in role_names:
            catalog.append({"category": str(category_name).strip(), "role": role_name})

    allowed_roles = {str(item.get("role") or "").strip() for item in catalog}
    visible_roles = [
        str(role_name).strip()
        for role_name in profile.get("visible_roles", [])
        if str(role_name).strip() and str(role_name).strip() in allowed_roles
    ][: AccountsService.MAX_VISIBLE_PROFILE_ROLES]
    return profile, catalog, visible_roles


def execute_guiy_owner_flow(
    *,
    provider: str,
    actor_user_id: str | int | None,
    bot_user_id: str | int | None,
    selected_action: str,
    payload: str = "",
    field_name: str | None = None,
    reply_author_user_id: str | int | None = None,
    target_message_id: str | int | None = None,
    explicit_owner_command: bool = True,
) -> GuiyOwnerFlowResult:
    normalized_provider = str(provider or "").strip().lower()
    normalized_action = str(selected_action or "").strip().lower()
    normalized_bot_user_id = str(bot_user_id).strip() if bot_user_id is not None else ""
    normalized_target_message_id = str(target_message_id).strip() if target_message_id is not None else None

    access = authorize_guiy_owner_action(
        actor_provider=normalized_provider,
        actor_user_id=actor_user_id,
        requested_action=normalized_action,
        target_message_id=normalized_target_message_id,
    )
    if not access.allowed:
        return GuiyOwnerFlowResult(
            ok=False,
            selected_action=normalized_action,
            message=GUIY_OWNER_DENIED_MESSAGE,
            target_message_id=normalized_target_message_id,
        )

    if normalized_action == "register_profile":
        bootstrap_result = bootstrap_guiy_profile(
            provider=normalized_provider,
            bot_user_id=normalized_bot_user_id,
        )
        return GuiyOwnerFlowResult(
            ok=bootstrap_result.ok,
            selected_action=normalized_action,
            message=bootstrap_result.message,
            guiy_account_id=bootstrap_result.guiy_account_id,
            target_message_id=normalized_target_message_id,
            target_provider_user_id=normalized_bot_user_id or None,
        )

    target_resolution = resolve_guiy_target_account(
        provider=normalized_provider,
        bot_user_id=normalized_bot_user_id,
        reply_author_user_id=reply_author_user_id,
        explicit_owner_command=explicit_owner_command,
    )
    if not target_resolution.ok:
        return GuiyOwnerFlowResult(
            ok=False,
            selected_action=normalized_action,
            message=target_resolution.message or GUIY_OWNER_DENIED_MESSAGE,
            guiy_account_id=target_resolution.target_account_id,
            target_message_id=normalized_target_message_id,
            target_provider_user_id=target_resolution.target_provider_user_id,
        )

    if normalized_action == "say":
        cleaned_payload = str(payload or "").strip()
        if not cleaned_payload:
            return GuiyOwnerFlowResult(
                ok=False,
                selected_action=normalized_action,
                message=GUIY_OWNER_USAGE_TEXT,
                guiy_account_id=target_resolution.target_account_id,
                target_message_id=normalized_target_message_id,
                target_provider_user_id=target_resolution.target_provider_user_id,
            )
        return GuiyOwnerFlowResult(
            ok=True,
            selected_action=normalized_action,
            message="✅ Гуй сейчас отправит новое сообщение.",
            guiy_account_id=target_resolution.target_account_id,
            target_message_id=normalized_target_message_id,
            outbound_text=cleaned_payload,
            target_provider_user_id=target_resolution.target_provider_user_id,
        )

    if normalized_action == "reply":
        cleaned_payload = str(payload or "").strip()
        if not normalized_target_message_id:
            return GuiyOwnerFlowResult(
                ok=False,
                selected_action=normalized_action,
                message=GUIY_OWNER_REPLY_REQUIRED_MESSAGE,
                guiy_account_id=target_resolution.target_account_id,
                target_message_id=normalized_target_message_id,
                target_provider_user_id=target_resolution.target_provider_user_id,
            )
        if not cleaned_payload:
            return GuiyOwnerFlowResult(
                ok=False,
                selected_action=normalized_action,
                message=GUIY_OWNER_USAGE_TEXT,
                guiy_account_id=target_resolution.target_account_id,
                target_message_id=normalized_target_message_id,
                target_provider_user_id=target_resolution.target_provider_user_id,
            )
        return GuiyOwnerFlowResult(
            ok=True,
            selected_action=normalized_action,
            message="✅ Гуй сейчас ответит на выбранное сообщение.",
            guiy_account_id=target_resolution.target_account_id,
            target_message_id=normalized_target_message_id,
            reply_to_message_id=int(normalized_target_message_id),
            outbound_text=cleaned_payload,
            target_provider_user_id=target_resolution.target_provider_user_id,
        )

    if normalized_action == "profile_update":
        normalized_field_name = str(field_name or "").strip().lower()
        if normalized_field_name not in GUIY_OWNER_PROFILE_FIELDS:
            return GuiyOwnerFlowResult(
                ok=False,
                selected_action=normalized_action,
                message=GUIY_OWNER_USAGE_TEXT,
                guiy_account_id=target_resolution.target_account_id,
                target_message_id=normalized_target_message_id,
                target_provider_user_id=target_resolution.target_provider_user_id,
            )

        success, response = AccountsService.update_profile_field(
            normalized_provider,
            normalized_bot_user_id,
            normalized_field_name,
            str(payload or "").strip(),
        )
        prefix = "✅" if success else "❌"
        return GuiyOwnerFlowResult(
            ok=success,
            selected_action=normalized_action,
            message=(
                f"{prefix} {response}\n"
                "Что изменилось: профиль Гуя в общем аккаунте обновлён; проверьте результат через /profile."
            ),
            guiy_account_id=target_resolution.target_account_id,
            target_message_id=normalized_target_message_id,
            target_provider_user_id=target_resolution.target_provider_user_id,
        )

    return GuiyOwnerFlowResult(
        ok=False,
        selected_action=normalized_action,
        message=GUIY_OWNER_USAGE_TEXT,
        guiy_account_id=target_resolution.target_account_id,
        target_message_id=normalized_target_message_id,
        target_provider_user_id=target_resolution.target_provider_user_id,
    )
