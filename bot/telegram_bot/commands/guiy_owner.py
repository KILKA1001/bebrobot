"""
Назначение: модуль "guiy owner" реализует продуктовый контур в зоне Telegram.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Telegram.
Пользовательский вход: команда /guiy_owner и связанный пользовательский сценарий.
"""

import logging
import time
from dataclasses import dataclass

from aiogram import F, Router
from aiogram.filters import Command, CommandObject
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from bot.services.guiy_admin_service import (
    GUIY_OWNER_DENIED_MESSAGE,
    GUIY_OWNER_REPLY_REQUIRED_MESSAGE,
    GUIY_OWNER_USAGE_TEXT,
)
from bot.services.guiy_owner_flow_service import (
    GUIY_OWNER_ACTION_SPECS,
    GUIY_OWNER_PROFILE_FIELDS,
    execute_guiy_owner_flow,
    get_guiy_owner_action_spec,
    get_guiy_owner_profile_field_spec,
    parse_guiy_owner_text_command,
    resolve_guiy_profile_catalog,
)
from bot.services.guiy_publish_destinations_service import (
    GuiyPublishDestination,
    GuiyPublishDestinationsService,
)
from bot.telegram_bot.identity import persist_telegram_identity_from_user

logger = logging.getLogger(__name__)
router = Router()

PENDING_GUIY_OWNER_TTL_SECONDS = 900
_VISIBLE_ROLES_PAGE_SIZE = 10
_DESTINATIONS_PAGE_SIZE = 8


@dataclass(slots=True)
class PendingGuiyOwnerAction:
    selected_action: str
    bot_user_id: str
    target_message_id: int | None
    reply_author_user_id: str | None
    created_at: float
    target_chat_or_guild: str
    control_chat_id: str | None = None
    selected_field: str | None = None
    target_destination_id: str | None = None
    target_destination_label: str | None = None


_PENDING_GUIY_OWNER_ACTIONS: dict[int, PendingGuiyOwnerAction] = {}
_PENDING_GUIY_OWNER_VISIBLE_ROLES: dict[int, dict[str, object]] = {}
_PENDING_GUIY_OWNER_DESTINATIONS: dict[int, dict[str, object]] = {}


def _log_guiy_owner_info(
    *,
    provider: str,
    actor_user_id: int | str | None,
    selected_action: str,
    target_chat_or_guild: int | str | None,
    target_message_id: int | str | None,
    guiy_account_id: str | None,
    message: str,
) -> None:
    logger.info(
        "%s provider=%s actor_user_id=%s selected_action=%s target_chat_or_guild=%s target_message_id=%s guiy_account_id=%s",
        message,
        provider,
        actor_user_id,
        selected_action,
        target_chat_or_guild,
        target_message_id,
        guiy_account_id,
    )


def _log_guiy_owner_warning(
    *,
    provider: str,
    actor_user_id: int | str | None,
    selected_action: str,
    target_chat_or_guild: int | str | None,
    target_message_id: int | str | None,
    guiy_account_id: str | None,
    message: str,
) -> None:
    logger.warning(
        "%s provider=%s actor_user_id=%s selected_action=%s target_chat_or_guild=%s target_message_id=%s guiy_account_id=%s",
        message,
        provider,
        actor_user_id,
        selected_action,
        target_chat_or_guild,
        target_message_id,
        guiy_account_id,
    )


def _clear_pending_state(actor_user_id: int | None) -> None:
    if actor_user_id is None:
        return
    _PENDING_GUIY_OWNER_ACTIONS.pop(actor_user_id, None)
    _PENDING_GUIY_OWNER_VISIBLE_ROLES.pop(actor_user_id, None)
    _PENDING_GUIY_OWNER_DESTINATIONS.pop(actor_user_id, None)


def _has_any_pending_state(actor_user_id: int | None) -> bool:
    if actor_user_id is None:
        return False
    _get_non_expired_pending_action(actor_user_id)
    return (
        actor_user_id in _PENDING_GUIY_OWNER_ACTIONS
        or actor_user_id in _PENDING_GUIY_OWNER_VISIBLE_ROLES
        or actor_user_id in _PENDING_GUIY_OWNER_DESTINATIONS
    )


async def _cancel_owner_flow_via_message(message: Message) -> None:
    actor_user_id = message.from_user.id if message.from_user else None
    had_pending_state = _has_any_pending_state(actor_user_id)
    _clear_pending_state(actor_user_id)
    _log_guiy_owner_info(
        provider="telegram",
        actor_user_id=actor_user_id,
        selected_action="cancel",
        target_chat_or_guild=message.chat.id if message.chat else None,
        target_message_id=None,
        guiy_account_id=None,
        message="telegram guiy owner flow canceled via text command",
    )
    if had_pending_state:
        await message.answer(
            "✅ <b>Owner-сценарий отключён вручную</b>\n"
            "Режим ожидания очищен. Теперь команды и обычные сообщения снова обрабатываются в штатном режиме.\n"
            "Если захотите запустить сценарий заново — откройте /guiy_owner.",
            parse_mode="HTML",
        )
        return
    await message.answer(
        "ℹ️ <b>Активного owner-сценария нет</b>\n"
        "Сейчас ничего не ждёт ввода. Если нужно открыть owner-меню заново — используйте /guiy_owner.",
        parse_mode="HTML",
    )


def _get_non_expired_pending_action(actor_user_id: int | None) -> PendingGuiyOwnerAction | None:
    if actor_user_id is None:
        return None
    pending = _PENDING_GUIY_OWNER_ACTIONS.get(actor_user_id)
    if not pending:
        return None
    if (time.time() - pending.created_at) > PENDING_GUIY_OWNER_TTL_SECONDS:
        _log_guiy_owner_info(
            provider="telegram",
            actor_user_id=actor_user_id,
            selected_action=pending.selected_action,
            target_chat_or_guild=pending.target_chat_or_guild,
            target_message_id=pending.target_message_id,
            guiy_account_id=None,
            message="telegram guiy owner pending state expired",
        )
        _clear_pending_state(actor_user_id)
        return None
    return pending


def has_pending_guiy_owner_action(actor_user_id: int | None) -> bool:
    return _get_non_expired_pending_action(actor_user_id) is not None


def _is_command_message(message: Message) -> bool:
    text = str(getattr(message, "text", "") or "").strip()
    return bool(text) and text.startswith("/")


def _is_pending_guiy_owner_input_message(message: Message) -> bool:
    actor_user_id = getattr(getattr(message, "from_user", None), "id", None)
    pending = _get_non_expired_pending_action(actor_user_id)
    if pending is None:
        return False
    if _is_command_message(message):
        logger.info(
            "telegram guiy owner pending input ignored because command was received actor_user_id=%s chat_id=%s selected_action=%s control_chat_id=%s",
            actor_user_id,
            getattr(getattr(message, "chat", None), "id", None),
            pending.selected_action,
            pending.control_chat_id,
        )
        return False
    control_chat_id = str(pending.control_chat_id or "").strip()
    if control_chat_id and control_chat_id != str(getattr(getattr(message, "chat", None), "id", "")):
        logger.info(
            "telegram guiy owner pending input ignored because message arrived from another chat actor_user_id=%s chat_id=%s selected_action=%s control_chat_id=%s",
            actor_user_id,
            getattr(getattr(message, "chat", None), "id", None),
            pending.selected_action,
            control_chat_id,
        )
        return False
    return True


def _owner_action_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=GUIY_OWNER_ACTION_SPECS["say"].title, callback_data="guiy_owner:action:say")],
            [InlineKeyboardButton(text=GUIY_OWNER_ACTION_SPECS["reply"].title, callback_data="guiy_owner:action:reply")],
            [InlineKeyboardButton(text=GUIY_OWNER_ACTION_SPECS["profile"].title, callback_data="guiy_owner:action:profile")],
            [InlineKeyboardButton(text=GUIY_OWNER_ACTION_SPECS["register_profile"].title, callback_data="guiy_owner:action:register_profile")],
            [InlineKeyboardButton(text=GUIY_OWNER_ACTION_SPECS["cancel"].title, callback_data="guiy_owner:action:cancel")],
        ]
    )


def _owner_profile_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=GUIY_OWNER_PROFILE_FIELDS["custom_nick"].title, callback_data="guiy_owner:field:custom_nick")],
            [InlineKeyboardButton(text=GUIY_OWNER_PROFILE_FIELDS["description"].title, callback_data="guiy_owner:field:description")],
            [InlineKeyboardButton(text=GUIY_OWNER_PROFILE_FIELDS["nulls_brawl_id"].title, callback_data="guiy_owner:field:nulls_brawl_id")],
            [InlineKeyboardButton(text=GUIY_OWNER_PROFILE_FIELDS["visible_roles"].title, callback_data="guiy_owner:field:visible_roles")],
            [InlineKeyboardButton(text=GUIY_OWNER_ACTION_SPECS["cancel"].title, callback_data="guiy_owner:action:cancel")],
        ]
    )


def _owner_menu_text() -> str:
    return (
        "🛠️ <b>Owner-управление Гуем</b>\n"
        "Выберите действие ниже. После каждого выбора бот коротко объяснит следующий шаг и что изменится после подтверждения.\n\n"
        "• <b>Написать от Гуя</b> — выбрать группу и отправить туда новое сообщение.\n"
        "• <b>Ответить от Гуя</b> — ответить именно на сообщение Гуя, если команда открыта reply-сообщением.\n"
        "• <b>Профиль Гуя</b> — автоматически проверить регистрацию общего аккаунта Гуя и сразу открыть редактирование полей.\n"
        "• <b>Зарегистрировать профиль Гуя</b> — вручную создать общий аккаунт, если хотите сделать это отдельным шагом заранее."
    )


def _owner_profile_intro(registration_message: str | None = None) -> str:
    spec = GUIY_OWNER_ACTION_SPECS["profile"]
    lines = ["👤 <b>Профиль Гуя</b>"]
    if registration_message:
        lines.extend([registration_message, ""])
    lines.extend([
        spec.instruction,
        "",
        "Теперь можно открыть редактирование профиля. Выберите поле, которое хотите изменить.",
    ])
    return "\n".join(lines)


async def _open_profile_menu(
    message: Message,
    *,
    actor_user_id: int | None,
    target_chat_or_guild: int | str | None,
    target_message_id: int | str | None,
    auto_bootstrap: bool,
    log_message: str,
) -> None:
    bot_user = await message.bot.get_me()
    registration_message: str | None = None
    guiy_account_id: str | None = None
    if auto_bootstrap:
        result = execute_guiy_owner_flow(
            provider="telegram",
            actor_user_id=actor_user_id,
            bot_user_id=bot_user.id,
            selected_action="register_profile",
            target_message_id=target_message_id,
        )
        guiy_account_id = result.guiy_account_id
        if not result.ok:
            await message.answer(result.message)
            return
        registration_message = result.message
    _log_guiy_owner_info(
        provider="telegram",
        actor_user_id=actor_user_id,
        selected_action="profile",
        target_chat_or_guild=target_chat_or_guild,
        target_message_id=target_message_id,
        guiy_account_id=guiy_account_id,
        message=log_message,
    )
    await message.answer(
        _owner_profile_intro(registration_message),
        parse_mode="HTML",
        reply_markup=_owner_profile_keyboard(),
    )


def _build_destination_keyboard(
    destinations: list[GuiyPublishDestination],
    *,
    page: int,
    selected_destination_id: str | None,
) -> InlineKeyboardMarkup:
    total_pages = max((len(destinations) - 1) // _DESTINATIONS_PAGE_SIZE + 1, 1)
    safe_page = min(max(page, 0), total_pages - 1)
    start = safe_page * _DESTINATIONS_PAGE_SIZE
    page_items = destinations[start : start + _DESTINATIONS_PAGE_SIZE]
    rows: list[list[InlineKeyboardButton]] = []
    for item in page_items:
        chat_id = str(item.destination_id)
        label = item.title[:48]
        if chat_id == str(selected_destination_id or ""):
            label = f"✅ {label}"[:64]
        rows.append([InlineKeyboardButton(text=label, callback_data=f"guiy_owner_destination:select:{chat_id}")])

    nav: list[InlineKeyboardButton] = []
    if safe_page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"guiy_owner_destination:page:{safe_page - 1}"))
    nav.append(InlineKeyboardButton(text=f"{safe_page + 1}/{total_pages}", callback_data="guiy_owner_destination:noop"))
    if safe_page + 1 < total_pages:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"guiy_owner_destination:page:{safe_page + 1}"))
    rows.append(nav)
    rows.append(
        [
            InlineKeyboardButton(text="✅ Подтвердить", callback_data="guiy_owner_destination:confirm"),
            InlineKeyboardButton(text=GUIY_OWNER_ACTION_SPECS["cancel"].title, callback_data="guiy_owner:action:cancel"),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _build_destination_text(
    destinations: list[GuiyPublishDestination],
    *,
    page: int,
    selected_destination_id: str | None,
) -> str:
    total_pages = max((len(destinations) - 1) // _DESTINATIONS_PAGE_SIZE + 1, 1)
    safe_page = min(max(page, 0), total_pages - 1)
    selected = next(
        (item for item in destinations if item.destination_id == str(selected_destination_id or "").strip()),
        None,
    )
    lines = [
        "📍 <b>Куда писать?</b>",
        (
            "Выберите группу, где бот уже присутствует и недавно видел события. "
            "Это помогает владельцу быстро понять, куда именно уйдёт сообщение от имени Гуя."
        ),
        f"Страница: <b>{safe_page + 1}/{total_pages}</b>",
    ]
    if selected is not None:
        lines.extend(
            [
                "",
                f"✅ Гуй отправит сообщение сюда: <b>{selected.title}</b>",
                f"<i>{selected.subtitle}</i>",
                "После подтверждения следующим сообщением отправьте текст — бот опубликует его в выбранной группе.",
            ]
        )
    else:
        lines.extend(
            [
                "",
                "Пока место не выбрано. Нажмите на нужную группу ниже, затем подтвердите выбор.",
            ]
        )
    return "\n".join(lines)


def _build_visible_roles_keyboard(catalog: list[dict[str, str]], selected_roles: list[str], page: int) -> InlineKeyboardMarkup:
    total_pages = max((len(catalog) - 1) // _VISIBLE_ROLES_PAGE_SIZE + 1, 1)
    safe_page = min(max(page, 0), total_pages - 1)
    start = safe_page * _VISIBLE_ROLES_PAGE_SIZE
    page_items = catalog[start : start + _VISIBLE_ROLES_PAGE_SIZE]
    rows: list[list[InlineKeyboardButton]] = []
    for idx, item in enumerate(page_items):
        role_name = str(item.get("role") or "").strip()
        category = str(item.get("category") or "").strip()
        label = f"{'✅ ' if role_name in selected_roles else ''}{role_name} [{category}]"[:64]
        row_idx = idx // 2
        if len(rows) <= row_idx:
            rows.append([])
        rows[row_idx].append(
            InlineKeyboardButton(text=label, callback_data=f"guiy_owner_visible_roles:toggle:{safe_page}:{idx}")
        )

    nav: list[InlineKeyboardButton] = []
    if safe_page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"guiy_owner_visible_roles:page:{safe_page - 1}"))
    nav.append(InlineKeyboardButton(text=f"{safe_page + 1}/{total_pages}", callback_data="guiy_owner_visible_roles:noop"))
    if safe_page + 1 < total_pages:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"guiy_owner_visible_roles:page:{safe_page + 1}"))
    rows.append(nav)
    rows.append(
        [
            InlineKeyboardButton(text="💾 Сохранить", callback_data="guiy_owner_visible_roles:save"),
            InlineKeyboardButton(text="🧹 Очистить", callback_data="guiy_owner_visible_roles:clear"),
        ]
    )
    rows.append([InlineKeyboardButton(text=GUIY_OWNER_ACTION_SPECS["cancel"].title, callback_data="guiy_owner:action:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _build_visible_roles_text(selected_roles: list[str], page: int, total_pages: int) -> str:
    selected_text = ", ".join(f"<code>{item}</code>" for item in selected_roles) if selected_roles else "—"
    return (
        "🏅 <b>Отображаемые роли</b>\n"
        f"{GUIY_OWNER_PROFILE_FIELDS['visible_roles'].instruction}\n"
        "Нажимайте на роли ниже, затем подтвердите сохранение.\n"
        f"Страница: <b>{page + 1}/{total_pages}</b>\n"
        f"Выбрано ({len(selected_roles)}/3): {selected_text}"
    )


async def _show_owner_menu(message: Message) -> None:
    await message.answer(_owner_menu_text(), parse_mode="HTML", reply_markup=_owner_action_keyboard())


async def _show_destination_picker(
    message: Message,
    *,
    actor_user_id: int,
    bot_user_id: str,
    target_message_id: int | None,
    reply_author_user_id: str | None,
) -> None:
    destinations = GuiyPublishDestinationsService.list_telegram_destinations()
    if not destinations:
        _log_guiy_owner_warning(
            provider="telegram",
            actor_user_id=actor_user_id,
            selected_action="say",
            target_chat_or_guild=message.chat.id if message.chat else None,
            target_message_id=target_message_id,
            guiy_account_id=None,
            message="telegram guiy owner destination list is empty",
        )
        await message.answer(
            "⚠️ <b>Пока нет доступных групп для публикации</b>\n"
            "Чтобы список появился, добавьте бота в нужную группу, дождитесь любого события или сообщения от неё, "
            "а затем откройте /guiy_owner снова.",
            parse_mode="HTML",
        )
        return

    _PENDING_GUIY_OWNER_DESTINATIONS[actor_user_id] = {
        "destinations": destinations,
        "page": 0,
        "selected_destination_id": None,
        "bot_user_id": str(bot_user_id),
        "target_message_id": target_message_id,
        "reply_author_user_id": reply_author_user_id,
        "created_at": time.time(),
        "target_chat_or_guild": str(message.chat.id if message.chat else ""),
    }
    await message.answer(
        _build_destination_text(destinations, page=0, selected_destination_id=None),
        parse_mode="HTML",
        reply_markup=_build_destination_keyboard(destinations, page=0, selected_destination_id=None),
    )


async def _verify_telegram_destination_access(message: Message, pending: PendingGuiyOwnerAction) -> tuple[bool, str, str]:
    destination_id = str(pending.target_destination_id or "").strip()
    destination_label = str(pending.target_destination_label or destination_id or "неизвестный чат")
    if not destination_id:
        return False, "missing_destination", destination_label

    registry_entry = GuiyPublishDestinationsService.get_telegram_destination(destination_id)
    if registry_entry is None:
        _log_guiy_owner_warning(
            provider="telegram",
            actor_user_id=message.from_user.id if message.from_user else None,
            selected_action=pending.selected_action,
            target_chat_or_guild=destination_id,
            target_message_id=pending.target_message_id,
            guiy_account_id=None,
            message="telegram guiy owner selected stale destination missing from registry",
        )
        return False, "stale_destination", destination_label

    try:
        bot_user = await message.bot.get_me()
        member = await message.bot.get_chat_member(int(destination_id), bot_user.id)
    except Exception:
        logger.exception(
            "telegram guiy owner destination access lookup failed provider=%s actor_user_id=%s selected_action=%s target_chat_or_guild=%s target_message_id=%s guiy_account_id=%s",
            "telegram",
            getattr(message.from_user, "id", None),
            pending.selected_action,
            destination_id,
            pending.target_message_id,
            None,
        )
        return False, "access_lookup_failed", destination_label

    status = str(getattr(member, "status", "") or "").strip()
    if status in {"left", "kicked"}:
        GuiyPublishDestinationsService.mark_telegram_chat_inactive(destination_id, reason=f"status={status}")
        _log_guiy_owner_warning(
            provider="telegram",
            actor_user_id=message.from_user.id if message.from_user else None,
            selected_action=pending.selected_action,
            target_chat_or_guild=destination_id,
            target_message_id=pending.target_message_id,
            guiy_account_id=None,
            message="telegram guiy owner send denied: bot is no longer in destination chat",
        )
        return False, "bot_not_in_chat", destination_label

    can_send = getattr(member, "can_send_messages", None)
    if can_send is False:
        _log_guiy_owner_warning(
            provider="telegram",
            actor_user_id=message.from_user.id if message.from_user else None,
            selected_action=pending.selected_action,
            target_chat_or_guild=destination_id,
            target_message_id=pending.target_message_id,
            guiy_account_id=None,
            message="telegram guiy owner send denied: missing send permissions",
        )
        return False, "missing_permissions", destination_label
    return True, "ok", destination_label


async def _run_text_fallback(message: Message, action: str, payload: str) -> None:
    actor_user_id = message.from_user.id if message.from_user else None
    target_message_id = message.reply_to_message.message_id if message.reply_to_message else None
    reply_author_user_id = message.reply_to_message.from_user.id if message.reply_to_message and message.reply_to_message.from_user else None

    if action == "reply" and not target_message_id:
        await message.answer(GUIY_OWNER_REPLY_REQUIRED_MESSAGE)
        return

    try:
        bot_user = await message.bot.get_me()
    except Exception:
        logger.exception(
            "telegram guiy owner failed to resolve bot identity provider=%s actor_user_id=%s selected_action=%s target_chat_or_guild=%s target_message_id=%s guiy_account_id=%s",
            "telegram",
            actor_user_id,
            action,
            message.chat.id if message.chat else None,
            target_message_id,
            None,
        )
        await message.answer(GUIY_OWNER_DENIED_MESSAGE)
        return

    if action == "profile":
        await message.answer(GUIY_OWNER_USAGE_TEXT)
        return

    result = execute_guiy_owner_flow(
        provider="telegram",
        actor_user_id=actor_user_id,
        bot_user_id=bot_user.id,
        selected_action=action,
        payload=payload,
        reply_author_user_id=reply_author_user_id,
        target_message_id=target_message_id,
        explicit_owner_command=True,
    )
    _log_guiy_owner_info(
        provider="telegram",
        actor_user_id=actor_user_id,
        selected_action=action,
        target_chat_or_guild=message.chat.id if message.chat else None,
        target_message_id=target_message_id,
        guiy_account_id=result.guiy_account_id,
        message="telegram guiy owner fallback handled",
    )
    if not result.ok:
        await message.answer(result.message)
        return

    try:
        if action == "register_profile":
            await message.answer(
                _owner_profile_intro(result.message),
                parse_mode="HTML",
                reply_markup=_owner_profile_keyboard(),
            )
            return
        if action == "say":
            await message.answer(result.outbound_text)
            return
        if action == "reply":
            await message.answer(result.outbound_text, reply_to_message_id=result.reply_to_message_id)
            return
    except Exception:
        logger.exception(
            "telegram guiy owner fallback send failed provider=%s actor_user_id=%s selected_action=%s target_chat_or_guild=%s target_message_id=%s guiy_account_id=%s",
            "telegram",
            actor_user_id,
            action,
            message.chat.id if message.chat else None,
            target_message_id,
            result.guiy_account_id,
        )
        await message.answer("❌ Не удалось выполнить действие. Попробуйте позже.")


@router.message(Command("guiy_owner"))
async def guiy_owner_command(message: Message, command: CommandObject) -> None:
    persist_telegram_identity_from_user(message.from_user)
    persist_telegram_identity_from_user(message.reply_to_message.from_user if message.reply_to_message else None)
    action, payload = parse_guiy_owner_text_command(command.args)

    if action == "cancel":
        await _cancel_owner_flow_via_message(message)
        return

    if action in {"say", "reply", "register_profile"}:
        await _run_text_fallback(message, action, payload)
        return

    if action == "profile":
        await message.answer(GUIY_OWNER_USAGE_TEXT)
        return

    await _show_owner_menu(message)


@router.callback_query(F.data == "guiy_owner:action:cancel")
async def guiy_owner_cancel_callback(callback: CallbackQuery) -> None:
    try:
        actor_user_id = callback.from_user.id if callback.from_user else None
        _clear_pending_state(actor_user_id)
        _log_guiy_owner_info(
            provider="telegram",
            actor_user_id=actor_user_id,
            selected_action="cancel",
            target_chat_or_guild=callback.message.chat.id if callback.message and callback.message.chat else None,
            target_message_id=None,
            guiy_account_id=None,
            message="telegram guiy owner flow canceled",
        )
        if callback.message:
            await callback.message.answer(
                "✅ <b>Owner-сценарий отменён</b>\nНичего не изменилось. При необходимости снова откройте /guiy_owner.",
                parse_mode="HTML",
            )
        await callback.answer()
    except Exception:
        logger.exception(
            "telegram guiy owner cancel failed provider=%s actor_user_id=%s selected_action=%s target_chat_or_guild=%s target_message_id=%s guiy_account_id=%s",
            "telegram",
            getattr(callback.from_user, "id", None),
            "cancel",
            callback.message.chat.id if callback.message and callback.message.chat else None,
            None,
            None,
        )
        await callback.answer("Ошибка отмены", show_alert=True)


@router.callback_query(F.data == "guiy_owner:action:profile")
async def guiy_owner_profile_menu_callback(callback: CallbackQuery) -> None:
    try:
        _clear_pending_state(callback.from_user.id if callback.from_user else None)
        if callback.message:
            await _open_profile_menu(
                callback.message,
                actor_user_id=getattr(callback.from_user, "id", None),
                target_chat_or_guild=callback.message.chat.id if callback.message.chat else None,
                target_message_id=getattr(callback.message.reply_to_message, "message_id", None),
                auto_bootstrap=True,
                log_message="telegram guiy owner profile menu opened",
            )
        await callback.answer()
    except Exception:
        logger.exception(
            "telegram guiy owner profile menu failed provider=%s actor_user_id=%s selected_action=%s target_chat_or_guild=%s target_message_id=%s guiy_account_id=%s",
            "telegram",
            getattr(callback.from_user, "id", None),
            "profile",
            callback.message.chat.id if callback.message and callback.message.chat else None,
            None,
            None,
        )
        await callback.answer("Ошибка открытия профиля", show_alert=True)


@router.callback_query(F.data.startswith("guiy_owner:action:"), F.data != "guiy_owner:action:cancel", F.data != "guiy_owner:action:profile")
async def guiy_owner_action_callback(callback: CallbackQuery) -> None:
    actor_user_id = callback.from_user.id if callback.from_user else None
    selected_action = str(callback.data or "").split(":")[-1]
    target_chat_or_guild = callback.message.chat.id if callback.message and callback.message.chat else None
    target_message_id = getattr(callback.message.reply_to_message, "message_id", None) if callback.message else None
    reply_author_user_id = (
        str(callback.message.reply_to_message.from_user.id)
        if callback.message and callback.message.reply_to_message and callback.message.reply_to_message.from_user
        else None
    )

    try:
        if callback.from_user is None or callback.message is None:
            await callback.answer("Не удалось определить контекст", show_alert=True)
            return

        try:
            bot_user = await callback.message.bot.get_me()
        except Exception:
            logger.exception(
                "telegram guiy owner bot identity resolve failed provider=%s actor_user_id=%s selected_action=%s target_chat_or_guild=%s target_message_id=%s guiy_account_id=%s",
                "telegram",
                actor_user_id,
                selected_action,
                target_chat_or_guild,
                target_message_id,
                None,
            )
            await callback.answer("Не удалось получить профиль бота", show_alert=True)
            return

        if selected_action == "register_profile":
            result = execute_guiy_owner_flow(
                provider="telegram",
                actor_user_id=actor_user_id,
                bot_user_id=bot_user.id,
                selected_action="register_profile",
                target_message_id=target_message_id,
                reply_author_user_id=reply_author_user_id,
            )
            _log_guiy_owner_info(
                provider="telegram",
                actor_user_id=actor_user_id,
                selected_action=selected_action,
                target_chat_or_guild=target_chat_or_guild,
                target_message_id=target_message_id,
                guiy_account_id=result.guiy_account_id,
                message="telegram guiy owner register action handled",
            )
            if callback.message:
                if result.ok:
                    await callback.message.answer(
                        _owner_profile_intro(result.message),
                        parse_mode="HTML",
                        reply_markup=_owner_profile_keyboard(),
                    )
                else:
                    await callback.message.answer(result.message)
            await callback.answer("Готово" if result.ok else "Ошибка", show_alert=not result.ok)
            return

        spec = get_guiy_owner_action_spec(selected_action)
        if not spec:
            await callback.answer("Неизвестное действие", show_alert=True)
            return

        if spec.requires_reply_context and target_message_id is None:
            _log_guiy_owner_warning(
                provider="telegram",
                actor_user_id=actor_user_id,
                selected_action=selected_action,
                target_chat_or_guild=target_chat_or_guild,
                target_message_id=target_message_id,
                guiy_account_id=None,
                message="telegram guiy owner reply action requested without reply context",
            )
            await callback.message.answer(
                f"ℹ️ <b>{spec.title}</b>\n{spec.instruction}\n\nСейчас ничего не изменится: откройте /guiy_owner ответом на сообщение Гуя и повторите действие.",
                parse_mode="HTML",
            )
            await callback.answer("Нужно открыть меню ответом на сообщение Гуя", show_alert=True)
            return

        if selected_action == "say":
            _clear_pending_state(callback.from_user.id)
            _log_guiy_owner_info(
                provider="telegram",
                actor_user_id=actor_user_id,
                selected_action=selected_action,
                target_chat_or_guild=target_chat_or_guild,
                target_message_id=target_message_id,
                guiy_account_id=None,
                message="telegram guiy owner destination picker opened",
            )
            await _show_destination_picker(
                callback.message,
                actor_user_id=callback.from_user.id,
                bot_user_id=str(bot_user.id),
                target_message_id=target_message_id,
                reply_author_user_id=reply_author_user_id,
            )
            await callback.answer()
            return

        _PENDING_GUIY_OWNER_ACTIONS[callback.from_user.id] = PendingGuiyOwnerAction(
            selected_action=selected_action,
            bot_user_id=str(bot_user.id),
            target_message_id=target_message_id,
            reply_author_user_id=reply_author_user_id,
            created_at=time.time(),
            target_chat_or_guild=str(target_chat_or_guild),
            control_chat_id=str(target_chat_or_guild),
        )
        _PENDING_GUIY_OWNER_VISIBLE_ROLES.pop(callback.from_user.id, None)
        _log_guiy_owner_info(
            provider="telegram",
            actor_user_id=actor_user_id,
            selected_action=selected_action,
            target_chat_or_guild=target_chat_or_guild,
            target_message_id=target_message_id,
            guiy_account_id=None,
            message="telegram guiy owner action selected",
        )
        await callback.message.answer(
            f"ℹ️ <b>{spec.title}</b>\n{spec.instruction}\n\nСледующий шаг: отправьте одним сообщением текст для подтверждения действия.",
            parse_mode="HTML",
        )
        await callback.answer()
    except Exception:
        logger.exception(
            "telegram guiy owner action callback failed provider=%s actor_user_id=%s selected_action=%s target_chat_or_guild=%s target_message_id=%s guiy_account_id=%s",
            "telegram",
            actor_user_id,
            selected_action,
            target_chat_or_guild,
            target_message_id,
            None,
        )
        await callback.answer("Ошибка выбора действия", show_alert=True)


@router.callback_query(F.data.startswith("guiy_owner_destination:"))
async def guiy_owner_destination_callback(callback: CallbackQuery) -> None:
    actor_user_id = callback.from_user.id if callback.from_user else None
    target_chat_or_guild = callback.message.chat.id if callback.message and callback.message.chat else None
    try:
        if callback.from_user is None or callback.message is None:
            await callback.answer("Не удалось определить контекст", show_alert=True)
            return
        state = _PENDING_GUIY_OWNER_DESTINATIONS.get(callback.from_user.id)
        if not state:
            _log_guiy_owner_warning(
                provider="telegram",
                actor_user_id=actor_user_id,
                selected_action="say",
                target_chat_or_guild=target_chat_or_guild,
                target_message_id=None,
                guiy_account_id=None,
                message="telegram guiy owner destination state missing",
            )
            await callback.answer("Список мест устарел. Откройте /guiy_owner заново.", show_alert=True)
            return
        if (time.time() - float(state.get("created_at") or 0)) > PENDING_GUIY_OWNER_TTL_SECONDS:
            _clear_pending_state(callback.from_user.id)
            await callback.answer("Список мест устарел. Откройте /guiy_owner заново.", show_alert=True)
            return

        destinations = [item for item in state.get("destinations", []) if isinstance(item, GuiyPublishDestination)]
        if not destinations:
            _clear_pending_state(callback.from_user.id)
            await callback.answer("Список мест больше недоступен. Откройте /guiy_owner заново.", show_alert=True)
            return

        action = str(callback.data or "").split(":", 1)[1]
        page = int(state.get("page") or 0)
        selected_destination_id = str(state.get("selected_destination_id") or "").strip() or None
        if action == "noop":
            await callback.answer()
            return
        if action.startswith("page:"):
            raw_page = action.split(":", 1)[1]
            page = int(raw_page) if raw_page.lstrip("-").isdigit() else page
        elif action.startswith("select:"):
            selected_destination_id = action.split(":", 1)[1]
        elif action == "confirm":
            if not selected_destination_id:
                await callback.answer("Сначала выберите группу.", show_alert=True)
                return
            selected = next((item for item in destinations if item.destination_id == selected_destination_id), None)
            if selected is None:
                _log_guiy_owner_warning(
                    provider="telegram",
                    actor_user_id=actor_user_id,
                    selected_action="say",
                    target_chat_or_guild=selected_destination_id,
                    target_message_id=state.get("target_message_id"),
                    guiy_account_id=None,
                    message="telegram guiy owner selected destination disappeared before confirmation",
                )
                await callback.answer("Этот чат больше недоступен. Выберите другой.", show_alert=True)
                return
            pending = PendingGuiyOwnerAction(
                selected_action="say",
                bot_user_id=str(state.get("bot_user_id") or ""),
                target_message_id=state.get("target_message_id"),
                reply_author_user_id=state.get("reply_author_user_id"),
                created_at=time.time(),
                target_chat_or_guild=str(selected.destination_id),
                control_chat_id=str(state.get("target_chat_or_guild") or ""),
                target_destination_id=str(selected.destination_id),
                target_destination_label=selected.display_label,
            )
            _PENDING_GUIY_OWNER_ACTIONS[callback.from_user.id] = pending
            _PENDING_GUIY_OWNER_DESTINATIONS.pop(callback.from_user.id, None)
            _log_guiy_owner_info(
                provider="telegram",
                actor_user_id=actor_user_id,
                selected_action="say",
                target_chat_or_guild=selected.destination_id,
                target_message_id=pending.target_message_id,
                guiy_account_id=None,
                message="telegram guiy owner destination confirmed",
            )
            await callback.message.edit_text(
                "✅ <b>Куда писать?</b>\n"
                f"Гуй отправит сообщение сюда: <b>{selected.title}</b>\n"
                f"<i>{selected.subtitle}</i>\n\n"
                "Следующий шаг: отправьте одним сообщением текст, и бот опубликует его в выбранной группе.",
                parse_mode="HTML",
                reply_markup=None,
            )
            await callback.answer("Группа выбрана")
            return
        else:
            await callback.answer("Неизвестное действие", show_alert=True)
            return

        state["page"] = page
        state["selected_destination_id"] = selected_destination_id
        _PENDING_GUIY_OWNER_DESTINATIONS[callback.from_user.id] = state
        await callback.message.edit_text(
            _build_destination_text(destinations, page=page, selected_destination_id=selected_destination_id),
            parse_mode="HTML",
            reply_markup=_build_destination_keyboard(destinations, page=page, selected_destination_id=selected_destination_id),
        )
        await callback.answer()
    except Exception:
        logger.exception(
            "telegram guiy owner destination callback failed provider=%s actor_user_id=%s selected_action=%s target_chat_or_guild=%s target_message_id=%s guiy_account_id=%s",
            "telegram",
            actor_user_id,
            "say",
            target_chat_or_guild,
            None,
            None,
        )
        await callback.answer("Ошибка выбора группы", show_alert=True)


@router.callback_query(F.data.startswith("guiy_owner:field:"))
async def guiy_owner_field_callback(callback: CallbackQuery) -> None:
    actor_user_id = callback.from_user.id if callback.from_user else None
    field_name = str(callback.data or "").split(":")[-1]
    target_chat_or_guild = callback.message.chat.id if callback.message and callback.message.chat else None
    try:
        if callback.from_user is None or callback.message is None:
            await callback.answer("Не удалось определить контекст", show_alert=True)
            return
        spec = get_guiy_owner_profile_field_spec(field_name)
        if not spec:
            await callback.answer("Неизвестное поле", show_alert=True)
            return

        bot_user = await callback.message.bot.get_me()
        pending = PendingGuiyOwnerAction(
            selected_action="profile_update",
            bot_user_id=str(bot_user.id),
            target_message_id=getattr(callback.message.reply_to_message, "message_id", None),
            reply_author_user_id=(
                str(callback.message.reply_to_message.from_user.id)
                if callback.message.reply_to_message and callback.message.reply_to_message.from_user
                else None
            ),
            created_at=time.time(),
            target_chat_or_guild=str(target_chat_or_guild),
            control_chat_id=str(target_chat_or_guild),
            selected_field=field_name,
        )
        _PENDING_GUIY_OWNER_ACTIONS[callback.from_user.id] = pending

        if field_name == "visible_roles":
            profile, catalog, selected_roles = resolve_guiy_profile_catalog(
                provider="telegram",
                bot_user_id=bot_user.id,
                display_name=getattr(bot_user, "full_name", None),
            )
            guiy_account_id = profile.get("account_id") if isinstance(profile, dict) else None
            if not catalog:
                _log_guiy_owner_warning(
                    provider="telegram",
                    actor_user_id=actor_user_id,
                    selected_action="profile_update",
                    target_chat_or_guild=target_chat_or_guild,
                    target_message_id=pending.target_message_id,
                    guiy_account_id=str(guiy_account_id) if guiy_account_id else None,
                    message="telegram guiy owner visible roles catalog is empty",
                )
                await callback.message.answer(
                    "❌ Для Гуя пока нет доступных ролей. Сначала зарегистрируйте профиль и проверьте /profile_roles."
                )
                await callback.answer()
                return
            _PENDING_GUIY_OWNER_VISIBLE_ROLES[callback.from_user.id] = {
                "catalog": catalog,
                "selected_roles": selected_roles,
                "page": 0,
                "created_at": time.time(),
                "bot_user_id": str(bot_user.id),
                "target_message_id": pending.target_message_id,
                "reply_author_user_id": pending.reply_author_user_id,
            }
            total_pages = max((len(catalog) - 1) // _VISIBLE_ROLES_PAGE_SIZE + 1, 1)
            await callback.message.answer(
                _build_visible_roles_text(selected_roles, 0, total_pages),
                parse_mode="HTML",
                reply_markup=_build_visible_roles_keyboard(catalog, selected_roles, 0),
            )
            await callback.answer()
            return

        _log_guiy_owner_info(
            provider="telegram",
            actor_user_id=actor_user_id,
            selected_action="profile_update",
            target_chat_or_guild=target_chat_or_guild,
            target_message_id=pending.target_message_id,
            guiy_account_id=None,
            message="telegram guiy owner profile field selected",
        )
        await callback.message.answer(
            f"ℹ️ <b>{spec.title}</b>\n{spec.instruction}\n\nСледующий шаг: отправьте новое значение одним сообщением. Чтобы очистить поле, отправьте <code>-</code>.",
            parse_mode="HTML",
        )
        await callback.answer()
    except Exception:
        logger.exception(
            "telegram guiy owner field callback failed provider=%s actor_user_id=%s selected_action=%s target_chat_or_guild=%s target_message_id=%s guiy_account_id=%s",
            "telegram",
            actor_user_id,
            "profile_update",
            target_chat_or_guild,
            None,
            None,
        )
        await callback.answer("Ошибка выбора поля", show_alert=True)


@router.callback_query(F.data.startswith("guiy_owner_visible_roles:"))
async def guiy_owner_visible_roles_callback(callback: CallbackQuery) -> None:
    actor_user_id = callback.from_user.id if callback.from_user else None
    target_chat_or_guild = callback.message.chat.id if callback.message and callback.message.chat else None
    selected_action = "profile_update"
    try:
        if callback.from_user is None:
            await callback.answer("Не удалось определить пользователя", show_alert=True)
            return
        state = _PENDING_GUIY_OWNER_VISIBLE_ROLES.get(callback.from_user.id)
        pending = _get_non_expired_pending_action(callback.from_user.id)
        if not state or not pending:
            _log_guiy_owner_warning(
                provider="telegram",
                actor_user_id=actor_user_id,
                selected_action=selected_action,
                target_chat_or_guild=target_chat_or_guild,
                target_message_id=None,
                guiy_account_id=None,
                message="telegram guiy owner visible roles state missing",
            )
            await callback.answer("Меню ролей устарело. Откройте /guiy_owner заново.", show_alert=True)
            return
        if (time.time() - float(state.get("created_at") or 0)) > PENDING_GUIY_OWNER_TTL_SECONDS:
            _clear_pending_state(callback.from_user.id)
            await callback.answer("Меню ролей устарело. Откройте /guiy_owner заново.", show_alert=True)
            return

        catalog = [item for item in state.get("catalog", []) if isinstance(item, dict)]
        selected_roles = [str(item) for item in state.get("selected_roles", []) if str(item).strip()]
        page = int(state.get("page") or 0)
        action = str(callback.data or "").split(":", 1)[1]

        if action == "noop":
            await callback.answer()
            return
        if action == "clear":
            selected_roles = []
        elif action.startswith("page:"):
            raw_page = action.split(":", 1)[1]
            page = int(raw_page) if raw_page.lstrip("-").isdigit() else page
        elif action.startswith("toggle:"):
            _, page_raw, idx_raw = action.split(":")
            current_page = int(page_raw)
            idx = int(idx_raw)
            total_pages = max((len(catalog) - 1) // _VISIBLE_ROLES_PAGE_SIZE + 1, 1)
            safe_page = min(max(current_page, 0), total_pages - 1)
            start = safe_page * _VISIBLE_ROLES_PAGE_SIZE
            page_items = catalog[start : start + _VISIBLE_ROLES_PAGE_SIZE]
            if idx < 0 or idx >= len(page_items):
                await callback.answer("Роль не найдена", show_alert=True)
                return
            role_name = str(page_items[idx].get("role") or "").strip()
            if role_name in selected_roles:
                selected_roles = [item for item in selected_roles if item != role_name]
            else:
                if len(selected_roles) >= 3:
                    await callback.answer("Можно выбрать не более 3 ролей", show_alert=True)
                    return
                selected_roles.append(role_name)
            page = safe_page
        elif action == "save":
            result = execute_guiy_owner_flow(
                provider="telegram",
                actor_user_id=actor_user_id,
                bot_user_id=state.get("bot_user_id"),
                selected_action="profile_update",
                field_name="visible_roles",
                payload=", ".join(selected_roles),
                reply_author_user_id=state.get("reply_author_user_id"),
                target_message_id=state.get("target_message_id"),
            )
            _log_guiy_owner_info(
                provider="telegram",
                actor_user_id=actor_user_id,
                selected_action=selected_action,
                target_chat_or_guild=target_chat_or_guild,
                target_message_id=state.get("target_message_id"),
                guiy_account_id=result.guiy_account_id,
                message="telegram guiy owner visible roles saved",
            )
            _clear_pending_state(callback.from_user.id)
            if callback.message:
                await callback.message.edit_text(result.message, reply_markup=None)
            await callback.answer("Сохранено" if result.ok else "Ошибка", show_alert=not result.ok)
            return
        else:
            await callback.answer("Неизвестное действие", show_alert=True)
            return

        state["selected_roles"] = selected_roles
        state["page"] = page
        _PENDING_GUIY_OWNER_VISIBLE_ROLES[callback.from_user.id] = state
        total_pages = max((len(catalog) - 1) // _VISIBLE_ROLES_PAGE_SIZE + 1, 1)
        safe_page = min(max(page, 0), total_pages - 1)
        if callback.message:
            await callback.message.edit_text(
                _build_visible_roles_text(selected_roles, safe_page, total_pages),
                parse_mode="HTML",
                reply_markup=_build_visible_roles_keyboard(catalog, selected_roles, safe_page),
            )
        await callback.answer()
    except Exception:
        logger.exception(
            "telegram guiy owner visible roles callback failed provider=%s actor_user_id=%s selected_action=%s target_chat_or_guild=%s target_message_id=%s guiy_account_id=%s",
            "telegram",
            actor_user_id,
            selected_action,
            target_chat_or_guild,
            None,
            None,
        )
        await callback.answer("Ошибка выбора ролей", show_alert=True)


@router.message(F.func(_is_pending_guiy_owner_input_message))
async def guiy_owner_pending_input_handler(message: Message) -> None:
    persist_telegram_identity_from_user(message.from_user)
    actor_user_id = message.from_user.id if message.from_user else None
    target_chat_or_guild = message.chat.id if message.chat else None
    pending = _get_non_expired_pending_action(actor_user_id)
    if not pending:
        _log_guiy_owner_warning(
            provider="telegram",
            actor_user_id=actor_user_id,
            selected_action="pending_input",
            target_chat_or_guild=target_chat_or_guild,
            target_message_id=None,
            guiy_account_id=None,
            message="telegram guiy owner pending handler invoked without state",
        )
        return

    payload = (message.text or "").strip()
    if payload == "-":
        payload = ""

    try:
        result = execute_guiy_owner_flow(
            provider="telegram",
            actor_user_id=actor_user_id,
            bot_user_id=pending.bot_user_id,
            selected_action=pending.selected_action,
            field_name=pending.selected_field,
            payload=payload,
            reply_author_user_id=pending.reply_author_user_id,
            target_message_id=pending.target_message_id,
        )
        _log_guiy_owner_info(
            provider="telegram",
            actor_user_id=actor_user_id,
            selected_action=pending.selected_action,
            target_chat_or_guild=target_chat_or_guild,
            target_message_id=pending.target_message_id,
            guiy_account_id=result.guiy_account_id,
            message="telegram guiy owner pending input processed",
        )
        _clear_pending_state(actor_user_id)
        if not result.ok:
            await message.answer(result.message)
            return
        if pending.selected_action == "say":
            allowed, reason, destination_label = await _verify_telegram_destination_access(message, pending)
            if not allowed:
                _log_guiy_owner_warning(
                    provider="telegram",
                    actor_user_id=actor_user_id,
                    selected_action=pending.selected_action,
                    target_chat_or_guild=pending.target_destination_id or pending.target_chat_or_guild,
                    target_message_id=pending.target_message_id,
                    guiy_account_id=result.guiy_account_id,
                    message=f"telegram guiy owner send blocked reason={reason}",
                )
                await message.answer(
                    "❌ Не удалось отправить сообщение от Гуя.\n"
                    f"Причина: чат больше недоступен или у бота нет прав писать сюда.\n"
                    f"Выбранное место: {destination_label}.\n"
                    "Откройте /guiy_owner заново и выберите актуальную группу."
                )
                return
            try:
                await message.bot.send_message(int(str(pending.target_destination_id)), result.outbound_text)
            except Exception:
                logger.exception(
                    "telegram guiy owner send failed provider=%s actor_user_id=%s selected_action=%s target_chat_or_guild=%s target_message_id=%s guiy_account_id=%s",
                    "telegram",
                    actor_user_id,
                    pending.selected_action,
                    pending.target_destination_id or pending.target_chat_or_guild,
                    pending.target_message_id,
                    result.guiy_account_id,
                )
                GuiyPublishDestinationsService.mark_telegram_chat_inactive(
                    pending.target_destination_id,
                    reason="send_failed",
                )
                _log_guiy_owner_warning(
                    provider="telegram",
                    actor_user_id=actor_user_id,
                    selected_action=pending.selected_action,
                    target_chat_or_guild=pending.target_destination_id or pending.target_chat_or_guild,
                    target_message_id=pending.target_message_id,
                    guiy_account_id=result.guiy_account_id,
                    message="telegram guiy owner send failed after destination selection",
                )
                await message.answer(
                    "❌ Не удалось отправить сообщение от Гуя: бот потерял доступ к группе или у него больше нет прав писать туда.\n"
                    f"Выбранное место: {destination_label}."
                )
                return
            await message.answer(
                "✅ Сообщение отправлено.\n"
                f"Гуй отправил сообщение сюда: {destination_label}."
            )
            return
        if pending.selected_action == "reply":
            await message.answer(result.outbound_text, reply_to_message_id=result.reply_to_message_id)
            await message.answer("ℹ️ Ответ отправлен. Изменение уже видно в выбранной ветке диалога.")
            return
        await message.answer(result.message)
    except Exception:
        logger.exception(
            "telegram guiy owner pending input failed provider=%s actor_user_id=%s selected_action=%s target_chat_or_guild=%s target_message_id=%s guiy_account_id=%s",
            "telegram",
            actor_user_id,
            pending.selected_action,
            target_chat_or_guild,
            pending.target_message_id,
            None,
        )
        _clear_pending_state(actor_user_id)
        await message.answer("❌ Не удалось выполнить действие. Попробуйте позже.")
