import logging
import time

from aiogram import F, Router
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from bot.services import AccountsService
from bot.telegram_bot.systems.commands_logic import (
    get_helpy_text,
    process_link_command,
    process_link_discord_command,
    process_profile_command,
    process_profile_roles_command,
    process_register_command,
)

logger = logging.getLogger(__name__)
router = Router()

_VISIBLE_ROLES_PAGE_SIZE = 10

_EDIT_FIELD_LABELS = {
    "custom_nick": "Никнейм",
    "description": "Описание",
    "nulls_brawl_id": "Null's Brawl ID",
    "visible_roles": "Отображаемые роли",
}
_PENDING_EDIT_FIELD: dict[int, str] = {}


PENDING_PROFILE_EDIT_TTL_SECONDS = 900
_PENDING_EDIT_FIELD_CREATED_AT: dict[int, float] = {}
_PENDING_VISIBLE_ROLES: dict[int, dict[str, object]] = {}


def _has_non_expired_profile_edit(telegram_user_id: int) -> bool:
    field_name = _PENDING_EDIT_FIELD.get(telegram_user_id)
    created_at = _PENDING_EDIT_FIELD_CREATED_AT.get(telegram_user_id)
    if not field_name or created_at is None:
        _PENDING_EDIT_FIELD.pop(telegram_user_id, None)
        _PENDING_EDIT_FIELD_CREATED_AT.pop(telegram_user_id, None)
        return False

    if (time.time() - created_at) > PENDING_PROFILE_EDIT_TTL_SECONDS:
        logger.info(
            "profile_edit pending state expired user_id=%s field=%s ttl_seconds=%s",
            telegram_user_id,
            field_name,
            PENDING_PROFILE_EDIT_TTL_SECONDS,
        )
        _PENDING_EDIT_FIELD.pop(telegram_user_id, None)
        _PENDING_EDIT_FIELD_CREATED_AT.pop(telegram_user_id, None)
        return False

    return True


def has_pending_profile_edit(telegram_user_id: int | None) -> bool:
    if telegram_user_id is None:
        return False
    return _has_non_expired_profile_edit(telegram_user_id)


def _is_chat_send_permissions_error(error: TelegramBadRequest) -> bool:
    return "not enough rights to send" in str(error).lower()


async def _safe_answer(
    message: Message,
    text: str,
    *,
    parse_mode: ParseMode | None = None,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> bool:
    try:
        await message.answer(text, parse_mode=parse_mode, reply_markup=reply_markup)
        return True
    except TelegramBadRequest as error:
        if _is_chat_send_permissions_error(error):
            logger.warning(
                "message send skipped due to missing chat permissions chat_id=%s user_id=%s error=%s",
                message.chat.id,
                message.from_user.id if message.from_user is not None else None,
                error,
            )
            return False
        logger.exception("message send failed chat_id=%s", message.chat.id)
        return False


def _profile_settings_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Изменить никнейм", callback_data="profile_edit:custom_nick")],
            [InlineKeyboardButton(text="📝 Изменить описание", callback_data="profile_edit:description")],
            [InlineKeyboardButton(text="🆔 Изменить Null's ID", callback_data="profile_edit:nulls_brawl_id")],
            [InlineKeyboardButton(text="🏅 Отображаемые роли", callback_data="profile_edit:visible_roles")],
        ]
    )


def _normalize_visible_roles_catalog(roles_by_category: dict[str, list[str]] | None) -> list[dict[str, object]]:
    catalog: list[dict[str, object]] = []
    for category_name in sorted((roles_by_category or {}).keys(), key=lambda value: str(value).lower()):
        role_names = [str(name).strip() for name in (roles_by_category or {}).get(category_name, []) if str(name).strip()]
        role_names = sorted(set(role_names), key=lambda value: value.lower())
        if role_names:
            for role_name in role_names:
                catalog.append({"category": str(category_name).strip() or "Без категории", "role": role_name})
    return catalog


def _get_visible_roles_page(catalog: list[dict[str, object]], page: int) -> tuple[int, int, list[dict[str, object]]]:
    total_pages = max((len(catalog) - 1) // _VISIBLE_ROLES_PAGE_SIZE + 1, 1)
    safe_page = min(max(page, 0), total_pages - 1)
    start = safe_page * _VISIBLE_ROLES_PAGE_SIZE
    return safe_page, total_pages, catalog[start : start + _VISIBLE_ROLES_PAGE_SIZE]


def _build_visible_roles_keyboard(catalog: list[dict[str, object]], selected_roles: list[str], page: int) -> InlineKeyboardMarkup:
    safe_page, total_pages, page_items = _get_visible_roles_page(catalog, page)
    rows: list[list[InlineKeyboardButton]] = []
    for idx, item in enumerate(page_items):
        role_name = str(item.get("role") or "").strip()
        category = str(item.get("category") or "Без категории").strip() or "Без категории"
        prefix = "✅ " if role_name in selected_roles else ""
        label = f"{prefix}{role_name} [{category}]"[:64]
        row_idx = idx // 2
        if len(rows) <= row_idx:
            rows.append([])
        rows[row_idx].append(InlineKeyboardButton(text=label, callback_data=f"profile_visible_roles:toggle:{safe_page}:{idx}"))

    nav: list[InlineKeyboardButton] = []
    if safe_page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"profile_visible_roles:page:{safe_page - 1}"))
    nav.append(InlineKeyboardButton(text=f"{safe_page + 1}/{total_pages}", callback_data="profile_visible_roles:noop"))
    if safe_page + 1 < total_pages:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"profile_visible_roles:page:{safe_page + 1}"))
    rows.append(nav)
    rows.append(
        [
            InlineKeyboardButton(text="💾 Сохранить", callback_data="profile_visible_roles:save"),
            InlineKeyboardButton(text="🧹 Очистить", callback_data="profile_visible_roles:clear"),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _build_visible_roles_text(selected_roles: list[str], page: int, total_pages: int) -> str:
    selected_text = ", ".join(f"<code>{item}</code>" for item in selected_roles) if selected_roles else "—"
    return (
        "🏅 <b>Выбор отображаемых ролей</b>\n"
        "Роли отсортированы по категориям. Нажмите на роли ниже (до 3), затем нажмите <b>Сохранить</b>.\n"
        f"Страница: <b>{page + 1}/{total_pages}</b>\n"
        f"Выбрано ({len(selected_roles)}/{AccountsService.MAX_VISIBLE_PROFILE_ROLES}): {selected_text}"
    )


@router.message(Command("helpy"))
async def helpy_command(message: Message) -> None:
    await message.answer(get_helpy_text())


@router.message(Command("register"))
async def register_command(message: Message) -> None:
    telegram_user_id = message.from_user.id if message.from_user is not None else None
    response = process_register_command(telegram_user_id)
    await message.answer(response)


@router.message(Command("profile"))
async def profile_command(message: Message) -> None:
    telegram_user_id = message.from_user.id if message.from_user is not None else None
    display_name = message.from_user.full_name if message.from_user is not None else None

    target_user = message.reply_to_message.from_user if message.reply_to_message else None
    target_user_id = target_user.id if target_user is not None else telegram_user_id
    target_display_name = target_user.full_name if target_user is not None else display_name

    response = process_profile_command(
        telegram_user_id,
        display_name=display_name,
        target_telegram_user_id=target_user_id,
        target_display_name=target_display_name,
    )

    if target_user_id is None:
        await _safe_answer(message, response)
        return

    reply_markup = None
    if message.chat.type == "private" and telegram_user_id == target_user_id:
        buttons = [[InlineKeyboardButton(text="⚙️ Настройки профиля", callback_data="profile_settings")]]
        reply_markup = InlineKeyboardMarkup(inline_keyboard=buttons)

    async def _send_avatar_caption(user_id: int) -> bool:
        try:
            photos = await message.bot.get_user_profile_photos(user_id, limit=1)
            if photos.total_count > 0 and photos.photos and photos.photos[0]:
                file_id = photos.photos[0][-1].file_id
                await message.answer_photo(
                    photo=file_id,
                    caption=response,
                    parse_mode=ParseMode.HTML,
                    reply_markup=reply_markup,
                )
                return True
        except TelegramBadRequest as error:
            if _is_chat_send_permissions_error(error):
                logger.warning(
                    "photo send skipped due to missing chat permissions chat_id=%s target_user_id=%s error=%s",
                    message.chat.id,
                    user_id,
                    error,
                )
                return False
            logger.exception("failed to send profile avatar due to telegram error user_id=%s", user_id)
            return False
        except Exception:
            logger.exception("failed to send profile avatar user_id=%s", user_id)
            return False
        return False

    if await _send_avatar_caption(target_user_id):
        return

    bot_user = await message.bot.get_me()
    if await _send_avatar_caption(bot_user.id):
        return

    await _safe_answer(message, response, parse_mode=ParseMode.HTML, reply_markup=reply_markup)


@router.message(Command("profile_edit"))
async def profile_edit_command(message: Message) -> None:
    telegram_user_id = message.from_user.id if message.from_user is not None else None
    if message.chat.type != "private":
        await message.answer("❌ Редактирование профиля доступно только в личных сообщениях с ботом.")
        return
    if telegram_user_id is None:
        await message.answer("❌ Не удалось определить пользователя Telegram.")
        return

    await message.answer(
        "⚙️ <b>Настройки профиля</b>\n"
        "Выберите, что хотите изменить. Для ролей используйте точные названия из /profile_roles:",
        parse_mode=ParseMode.HTML,
        reply_markup=_profile_settings_keyboard(),
    )


@router.callback_query(F.data == "profile_settings")
async def profile_settings_callback(callback: CallbackQuery) -> None:
    try:
        if callback.message and callback.message.chat.type != "private":
            await callback.answer("Доступно только в ЛС", show_alert=True)
            return

        await callback.message.answer(
            "⚙️ <b>Настройки профиля</b>\n"
            "Выберите поле для изменения. Для ролей используйте точные названия из /profile_roles:",
            parse_mode=ParseMode.HTML,
            reply_markup=_profile_settings_keyboard(),
        )
        await callback.answer()
    except Exception:
        logger.exception("profile_settings callback failed")
        await callback.answer("Ошибка открытия настроек", show_alert=True)




@router.callback_query(F.data.startswith("profile_edit:"))
async def profile_edit_field_callback(callback: CallbackQuery) -> None:
    try:
        if callback.from_user is None:
            await callback.answer("Не удалось определить пользователя", show_alert=True)
            return
        if callback.message and callback.message.chat.type != "private":
            await callback.answer("Доступно только в ЛС", show_alert=True)
            return

        field_name = str(callback.data).split(":", 1)[1]
        if field_name not in _EDIT_FIELD_LABELS:
            await callback.answer("Неизвестное поле", show_alert=True)
            return

        _PENDING_EDIT_FIELD[callback.from_user.id] = field_name
        _PENDING_EDIT_FIELD_CREATED_AT[callback.from_user.id] = time.time()
        helper_text = "Чтобы очистить поле, отправьте символ <code>-</code>."
        if field_name == "visible_roles":
            display_name = callback.from_user.full_name if callback.from_user is not None else None
            profile_data = AccountsService.get_profile("telegram", str(callback.from_user.id), display_name=display_name)
            roles_by_category = (profile_data or {}).get("roles_by_category") or {}
            catalog = _normalize_visible_roles_catalog(roles_by_category)
            visible_roles = [str(name).strip() for name in (profile_data or {}).get("visible_roles", []) if str(name).strip()]

            if not catalog:
                await callback.message.answer("❌ Нет доступных ролей для выбора. Проверьте /profile_roles.")
                await callback.answer()
                return

            allowed_names = {str(item.get("role") or "").strip() for item in catalog}
            selected_roles = [
                role_name
                for role_name in visible_roles
                if role_name in allowed_names
            ][: AccountsService.MAX_VISIBLE_PROFILE_ROLES]
            safe_page, total_pages, _ = _get_visible_roles_page(catalog, 0)

            _PENDING_VISIBLE_ROLES[callback.from_user.id] = {
                "catalog": catalog,
                "selected_roles": selected_roles,
                "page": safe_page,
                "created_at": time.time(),
            }
            _PENDING_EDIT_FIELD.pop(callback.from_user.id, None)
            _PENDING_EDIT_FIELD_CREATED_AT.pop(callback.from_user.id, None)
            await callback.message.answer(
                _build_visible_roles_text(selected_roles, safe_page, total_pages),
                parse_mode=ParseMode.HTML,
                reply_markup=_build_visible_roles_keyboard(catalog, selected_roles, safe_page),
            )
            await callback.answer()
            return

        await callback.message.answer(
            f"✍️ Введите новое значение для поля <b>{_EDIT_FIELD_LABELS[field_name]}</b>.\n{helper_text}",
            parse_mode=ParseMode.HTML,
        )
        await callback.answer()
    except Exception:
        logger.exception("profile_edit field callback failed callback_data=%s", callback.data)
        await callback.answer("Ошибка выбора поля", show_alert=True)


@router.callback_query(F.data.startswith("profile_visible_roles:"))
async def profile_visible_roles_callback(callback: CallbackQuery) -> None:
    try:
        if callback.from_user is None:
            await callback.answer("Не удалось определить пользователя", show_alert=True)
            return

        state = _PENDING_VISIBLE_ROLES.get(callback.from_user.id)
        if not state:
            await callback.answer("Меню выбора ролей устарело. Откройте заново.", show_alert=True)
            return
        created_at = float(state.get("created_at") or 0)
        if created_at and (time.time() - created_at) > PENDING_PROFILE_EDIT_TTL_SECONDS:
            _PENDING_VISIBLE_ROLES.pop(callback.from_user.id, None)
            logger.info("profile_visible_roles state expired user_id=%s", callback.from_user.id)
            await callback.answer("Меню выбора ролей устарело. Откройте заново.", show_alert=True)
            return

        catalog = [item for item in state.get("catalog", []) if isinstance(item, dict)]
        selected_roles = [str(item) for item in state.get("selected_roles", []) if str(item).strip()]
        page = int(state.get("page") or 0)

        callback_data = str(callback.data or "")
        prefix = "profile_visible_roles:"
        if not callback_data.startswith(prefix):
            logger.error(
                "profile_visible_roles callback invalid prefix user_id=%s callback_data=%s",
                callback.from_user.id,
                callback_data,
            )
            await callback.answer("Неизвестное действие", show_alert=True)
            return

        action = callback_data[len(prefix) :]
        if not action:
            logger.error(
                "profile_visible_roles callback empty action user_id=%s callback_data=%s",
                callback.from_user.id,
                callback_data,
            )
            await callback.answer("Неизвестное действие", show_alert=True)
            return
        if action == "save":
            value = ", ".join(selected_roles)
            success, payload = AccountsService.update_profile_field("telegram", str(callback.from_user.id), "visible_roles", value)
            _PENDING_VISIBLE_ROLES.pop(callback.from_user.id, None)
            prefix = "✅" if success else "❌"
            if callback.message:
                await callback.message.edit_text(f"{prefix} {payload}", reply_markup=None)
            await callback.answer("Сохранено" if success else "Ошибка", show_alert=not success)
            return

        if action == "noop":
            await callback.answer()
            return

        if action == "clear":
            selected_roles = []
        elif action.startswith("page:"):
            target_page_raw = action.split(":", 1)[1]
            target_page = int(target_page_raw) if target_page_raw.lstrip("-").isdigit() else page
            page, _, _ = _get_visible_roles_page(catalog, target_page)
        elif action.startswith("toggle:"):
            payload = action.split(":")
            if len(payload) < 3:
                await callback.answer("Некорректный выбор", show_alert=True)
                return
            page_raw = payload[1]
            idx_raw = payload[2]
            try:
                current_page = int(page_raw)
                idx = int(idx_raw)
            except ValueError:
                await callback.answer("Некорректный выбор", show_alert=True)
                return
            safe_page, _, page_items = _get_visible_roles_page(catalog, current_page)
            if idx < 0 or idx >= len(page_items):
                await callback.answer("Роль не найдена", show_alert=True)
                return
            role_name = str(page_items[idx].get("role") or "").strip()
            page = safe_page
            if role_name in selected_roles:
                selected_roles = [item for item in selected_roles if item != role_name]
            else:
                if len(selected_roles) >= AccountsService.MAX_VISIBLE_PROFILE_ROLES:
                    await callback.answer(
                        f"Можно выбрать не более {AccountsService.MAX_VISIBLE_PROFILE_ROLES} ролей",
                        show_alert=True,
                    )
                    return
                selected_roles.append(role_name)
        else:
            await callback.answer("Неизвестное действие", show_alert=True)
            return

        state["selected_roles"] = selected_roles
        state["page"] = page
        _PENDING_VISIBLE_ROLES[callback.from_user.id] = state

        safe_page, total_pages, _ = _get_visible_roles_page(catalog, page)
        if callback.message:
            await callback.message.edit_text(
                _build_visible_roles_text(selected_roles, safe_page, total_pages),
                parse_mode=ParseMode.HTML,
                reply_markup=_build_visible_roles_keyboard(catalog, selected_roles, safe_page),
            )
        await callback.answer()
    except Exception:
        logger.exception("profile_visible_roles callback failed callback_data=%s", callback.data)
        await callback.answer("Ошибка выбора ролей", show_alert=True)


@router.message(F.chat.type == "private", F.from_user, F.from_user.id.func(has_pending_profile_edit))
async def profile_edit_value_handler(message: Message) -> None:
    if not _has_non_expired_profile_edit(message.from_user.id):
        logger.warning(
            "profile_edit handler invoked without pending field user_id=%s chat_id=%s",
            message.from_user.id,
            message.chat.id if message.chat else None,
        )
        return

    pending_field = _PENDING_EDIT_FIELD.get(message.from_user.id)
    try:
        value = (message.text or "").strip()
        if value == "-":
            value = ""

        success, payload = AccountsService.update_profile_field(
            "telegram",
            str(message.from_user.id),
            pending_field,
            value,
        )

        _PENDING_EDIT_FIELD.pop(message.from_user.id, None)
        _PENDING_EDIT_FIELD_CREATED_AT.pop(message.from_user.id, None)
        prefix = "✅" if success else "❌"
        await message.answer(f"{prefix} {payload}")

        if success:
            profile_text = process_profile_command(
                telegram_user_id=message.from_user.id,
                display_name=message.from_user.full_name,
            )
            await message.answer(
                profile_text,
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="⚙️ Настройки профиля", callback_data="profile_settings")]]
                ),
            )
    except Exception:
        logger.exception("profile_edit value handler failed user_id=%s", message.from_user.id)
        _PENDING_EDIT_FIELD.pop(message.from_user.id, None)
        _PENDING_EDIT_FIELD_CREATED_AT.pop(message.from_user.id, None)
        await message.answer("❌ Ошибка обновления профиля. Попробуйте позже.")


@router.message(Command("profile_roles"))
async def profile_roles_command(message: Message) -> None:
    telegram_user_id = message.from_user.id if message.from_user is not None else None
    display_name = message.from_user.full_name if message.from_user is not None else None

    target_user = message.reply_to_message.from_user if message.reply_to_message else None
    target_user_id = target_user.id if target_user is not None else telegram_user_id
    target_display_name = target_user.full_name if target_user is not None else display_name

    response = process_profile_roles_command(
        telegram_user_id,
        display_name=display_name,
        target_telegram_user_id=target_user_id,
        target_display_name=target_display_name,
    )
    await message.answer(response, parse_mode=ParseMode.HTML)


@router.message(Command("link"))
async def link_command(message: Message) -> None:
    telegram_user_id = message.from_user.id if message.from_user is not None else None
    is_private_chat = message.chat.type == "private"
    response = process_link_command(message.text or "", telegram_user_id, is_private_chat=is_private_chat)
    await message.answer(response)


@router.message(Command("link_discord"))
async def link_discord_command(message: Message) -> None:
    telegram_user_id = message.from_user.id if message.from_user is not None else None
    is_private_chat = message.chat.type == "private"
    response = process_link_discord_command(telegram_user_id, is_private_chat=is_private_chat)
    await message.answer(response)
