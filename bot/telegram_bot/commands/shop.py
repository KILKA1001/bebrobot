import logging
import time
from dataclasses import dataclass

from aiogram import F, Router
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from bot.telegram_bot.identity import persist_telegram_identity_from_user
from bot.services import AuthorityService, RoleManagementService
from bot.services.shop_service import (
    SHOP_PAGE_SIZE,
    SHOP_TEXT_ACQUIRE_HINT_PLACEHOLDER,
    SHOP_TEXT_CARD_HINT,
    SHOP_TEXT_CATEGORIES_HINT,
    SHOP_TEXT_CONFIRM_PURCHASE,
    SHOP_TEXT_ITEM_NOT_FOUND,
    SHOP_TEXT_ITEM_PLACEHOLDER,
    SHOP_TEXT_LIST_HINT,
    SHOP_TEXT_PROTECTED_FAILURE,
    build_shop_render_payload,
    check_shop_profile_access,
    find_shop_item,
    get_shop_catalog_items,
    get_shop_page_slice,
    purchase_shop_item,
)

logger = logging.getLogger(__name__)
router = Router()
_SHOP_ADMIN_PENDING_TTL_SECONDS = 900


@dataclass
class PendingShopAdminAction:
    action: str
    role_name: str
    created_at: float


_SHOP_ADMIN_PENDING_ACTIONS: dict[int, PendingShopAdminAction] = {}

SHOP_OPEN_PROMPT_TEXT = "Откройте магазин в личных сообщениях, я уже отправил вам инструкцию."
DM_FALLBACK_TEXT = (
    "❌ Не удалось отправить инструкцию в личные сообщения.\n"
    "Откройте чат с ботом и нажмите <b>Start</b> / <b>Начать</b>, затем снова отправьте <code>/shop</code>."
)


def _build_shop_keyboard(items, page: int) -> InlineKeyboardMarkup:
    page_data = get_shop_page_slice(items, page, page_size=SHOP_PAGE_SIZE)
    rows: list[list[InlineKeyboardButton]] = []
    for idx in range(0, len(page_data.items), 4):
        row_items = page_data.items[idx : idx + 4]
        rows.append(
            [
                InlineKeyboardButton(
                    text=item.short_name,
                    callback_data=f"shop:item:{item.shop_item_id}:{page_data.page}",
                )
                for item in row_items
            ]
        )

    rows.append(
        [
            InlineKeyboardButton(text="⬅️ Назад", callback_data=f"shop:page:{max(page_data.page - 1, 0)}:{page_data.page}"),
            InlineKeyboardButton(text=f"Стр. {page_data.page + 1}/{page_data.total_pages}", callback_data="shop:noop"),
            InlineKeyboardButton(
                text="➡️ Вперёд",
                callback_data=f"shop:page:{min(page_data.page + 1, page_data.total_pages - 1)}:{page_data.page}",
            ),
            InlineKeyboardButton(text="Обновить", callback_data=f"shop:refresh:{page_data.page}"),
        ]
    )
    rows.append([InlineKeyboardButton(text="К категориям", callback_data="shop:categories")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _build_item_card_keyboard(*, shop_item_id: str, page: int, price_points: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Купить", callback_data=f"shop:buy:{shop_item_id}:{page}:{int(price_points)}"),
                InlineKeyboardButton(text="Назад в магазин", callback_data=f"shop:back:{page}"),
            ]
        ]
    )


def _build_confirm_keyboard(*, shop_item_id: str, page: int, price_points: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Подтвердить покупку", callback_data=f"shop:confirm:{shop_item_id}:{page}:{int(price_points)}"),
                InlineKeyboardButton(text="Отмена", callback_data=f"shop:item:{shop_item_id}:{page}"),
            ]
        ]
    )


def _shop_text(account_id: str | None, page: int, total_pages: int) -> str:
    logger.info(
        "ux_action_hint_shown event=ux_action_hint_shown screen=shop_list provider=telegram account_id=%s",
        account_id,
    )
    return (
        "🛒 <b>Магазин — Роли</b>\n"
        "Выберите роль из списка.\n"
        f"{SHOP_TEXT_LIST_HINT}\n\n"
        f"Страница: <b>{page + 1}/{total_pages}</b>"
    )


def _shop_categories_text(account_id: str | None) -> str:
    payload = build_shop_render_payload(account_id)
    logger.info(
        "ux_screen_open event=ux_screen_open screen=shop_categories provider=telegram account_id=%s",
        account_id,
    )
    logger.info(
        "ux_action_hint_shown event=ux_action_hint_shown screen=shop_categories provider=telegram account_id=%s",
        account_id,
    )
    return (
        "🛒 <b>Магазин</b>\n"
        f"Баланс: <b>{payload.points} баллов</b>\n"
        f"{SHOP_TEXT_CATEGORIES_HINT}"
    )


def _build_categories_keyboard() -> InlineKeyboardMarkup:
    return _build_categories_keyboard_with_admin(False)


def _build_categories_keyboard_with_admin(is_superadmin: bool) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text="Роли", callback_data="shop:category:roles")]]
    if is_superadmin:
        rows.append([InlineKeyboardButton(text="⚙️ Настройка магазина", callback_data="shop:admin:entry")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _shop_is_superadmin(user_id: int) -> bool:
    return AuthorityService.is_super_admin("telegram", str(user_id))


def _item_card_text(item, account_id: str | None) -> str:
    payload = build_shop_render_payload(account_id)
    description = item.description or SHOP_TEXT_ITEM_PLACEHOLDER
    acquire_hint = item.acquire_hint or SHOP_TEXT_ACQUIRE_HINT_PLACEHOLDER
    price_line = f"Цена: <b>{item.price_points} баллов</b>"
    if item.is_sale_active and item.sale_price_points is not None:
        price_line = (
            f"Цена: <b>{item.price_points} баллов</b> (акция)\n"
            f"Базовая цена: <s>{item.base_price_points} баллов</s>"
        )
    return (
        f"🛒 <b>{payload.title}</b>\n"
        f"Баланс: <b>{payload.points} баллов</b>\n\n"
        f"<b>{item.role_name}</b>\n"
        f"Категория: <b>{item.category}</b>\n"
        f"{price_line}\n"
        f"Описание: {description}\n"
        f"Как получить: {acquire_hint}\n"
        f"{SHOP_TEXT_CARD_HINT}"
    )


def _item_confirm_text(item, account_id: str | None) -> str:
    return (
        f"{_item_card_text(item, account_id)}\n\n"
        f"{SHOP_TEXT_CONFIRM_PURCHASE}"
    )


def _admin_actions_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="➕ Добавить товар на витрину", callback_data="shop:admin_action:add")],
        [InlineKeyboardButton(text="➖ Убрать товар с витрины", callback_data="shop:admin_action:remove")],
        [InlineKeyboardButton(text="💳 Изменить цену", callback_data="shop:admin_action:price")],
        [InlineKeyboardButton(text="↕️ Изменить позицию", callback_data="shop:admin_action:position")],
        [InlineKeyboardButton(text="⏱ Вкл/выкл акцию", callback_data="shop:admin_action:sale")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="shop:categories")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _shop_admin_roles() -> list[str]:
    # Для админ-настройки магазина показываем все продаваемые роли,
    # даже если роль скрыта из публичного каталога /roles.
    # Иначе возникает ложная ситуация "нет ролей с признаком продаваемости",
    # когда is_sellable=true в БД, но show_in_roles_catalog=false.
    grouped = RoleManagementService.list_roles_grouped(
        log_context="shop:telegram:admin_roles",
    ) or []
    roles: list[str] = []
    total_roles = 0
    blocked_non_sellable = 0
    for category in grouped:
        for role in list(category.get("roles") or []):
            total_roles += 1
            if not bool(role.get("is_sellable")):
                blocked_non_sellable += 1
                continue
            role_name = str(role.get("role") or "").strip()
            if role_name:
                roles.append(role_name)
    if not roles:
        logger.error(
            "shop_admin_roles_empty_after_sellable_filter provider=telegram grouped_categories=%s total_roles=%s blocked_non_sellable=%s",
            len(grouped),
            total_roles,
            blocked_non_sellable,
        )
    else:
        logger.info(
            "shop_admin_roles_loaded provider=telegram grouped_categories=%s total_roles=%s sellable_roles=%s blocked_non_sellable=%s",
            len(grouped),
            total_roles,
            len(roles),
            blocked_non_sellable,
        )
    return roles


def _build_shop_admin_role_picker(action: str, roles: list[str]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for idx, role_name in enumerate(roles[:20]):
        rows.append([InlineKeyboardButton(text=f"🎭 {role_name}"[:64], callback_data=f"shop:admin_pick_role:{action}:{idx}")])
    rows.append([InlineKeyboardButton(text="⬅️ К действиям", callback_data="shop:admin_category:roles")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@router.message(Command("shop"))
async def shop_command(message: Message) -> None:
    persist_telegram_identity_from_user(message.from_user)
    if message.from_user is None:
        logger.error("shop_actor_missing provider=telegram source=unknown")
        return

    source = "dm" if message.chat.type == "private" else "group"
    logger.info(
        "shop_flow_received provider=telegram source=%s actor_user_id=%s chat_id=%s",
        source,
        message.from_user.id,
        message.chat.id if message.chat else None,
    )

    profile_check = check_shop_profile_access("telegram", message.from_user.id, register_command="/register")
    if not profile_check.ok:
        await message.answer(profile_check.user_message or "Сначала создайте профиль и повторите команду /shop.", parse_mode="HTML")
        return

    text = _shop_categories_text(profile_check.account_id)
    is_superadmin = _shop_is_superadmin(message.from_user.id)
    reply_markup = _build_categories_keyboard_with_admin(is_superadmin)
    logger.info(
        "shop_category_screen_open provider=telegram actor_user_id=%s account_id=%s",
        message.from_user.id,
        profile_check.account_id,
    )

    if message.chat.type == "private":
        try:
            await message.answer(text, parse_mode="HTML", reply_markup=reply_markup)
        except Exception as error:  # noqa: BLE001
            logger.exception(
                "shop_category_screen_render_error provider=telegram actor_user_id=%s source=dm_open error=%s",
                message.from_user.id,
                error,
            )
            await message.answer(SHOP_TEXT_PROTECTED_FAILURE, parse_mode="HTML")
        return

    await message.answer(SHOP_OPEN_PROMPT_TEXT)
    logger.info("shop_flow_group_notice_sent provider=telegram source=group actor_user_id=%s", message.from_user.id)
    try:
        await message.bot.send_message(
            chat_id=message.from_user.id,
            text=text,
            parse_mode="HTML",
            reply_markup=reply_markup,
        )
    except (TelegramForbiddenError, TelegramBadRequest) as error:
        logger.exception("shop_dm_transfer_error provider=telegram actor_user_id=%s dm_sent=false error=%s", message.from_user.id, error)
        logger.error(
            "ux_fallback_shown event=ux_fallback_shown screen=shop_dm_transfer provider=telegram actor_user_id=%s",
            message.from_user.id,
        )
        await message.answer(DM_FALLBACK_TEXT, parse_mode="HTML")


@router.callback_query(F.data.startswith("shop:"))
async def shop_callback(callback: CallbackQuery) -> None:
    if callback.from_user is None or callback.message is None:
        logger.error("shop_pagination_error provider=telegram reason=missing_callback_context data=%s", callback.data)
        return

    profile_check = check_shop_profile_access("telegram", callback.from_user.id, register_command="/register")
    if not profile_check.ok:
        await callback.answer("Сначала создайте профиль через /register.", show_alert=True)
        return

    items = get_shop_catalog_items(log_context="shop:telegram:callback")
    data = str(callback.data or "")
    parts = data.split(":")

    try:
        if len(parts) >= 2 and parts[1] == "noop":
            await callback.answer()
            return

        if len(parts) >= 3 and parts[1] == "admin" and parts[2] == "entry":
            if not _shop_is_superadmin(callback.from_user.id):
                logger.warning(
                    "shop_admin_denied_not_superadmin provider=telegram actor_user_id=%s action=entry",
                    callback.from_user.id,
                )
                await callback.answer("Недостаточно прав", show_alert=True)
                return
            logger.info("shop_admin_entry_open provider=telegram actor_user_id=%s", callback.from_user.id)
            await callback.message.edit_text(
                "⚙️ <b>Настройка магазина</b>\n\nШаг 1/2: выберите категорию.\nШаг 2/2: выберите действие.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="Роли", callback_data="shop:admin_category:roles")],
                        [InlineKeyboardButton(text="⬅️ Назад", callback_data="shop:categories")],
                    ]
                ),
            )
            await callback.answer()
            return

        if len(parts) >= 3 and parts[1] == "admin_category":
            if not _shop_is_superadmin(callback.from_user.id):
                logger.warning(
                    "shop_admin_denied_not_superadmin provider=telegram actor_user_id=%s action=category_select",
                    callback.from_user.id,
                )
                await callback.answer("Недостаточно прав", show_alert=True)
                return
            logger.info("shop_admin_category_select provider=telegram actor_user_id=%s category=%s", callback.from_user.id, parts[2])
            await callback.message.edit_text(
                "⚙️ <b>Настройка магазина</b>\n\nШаг 1/2: категория выбрана.\nШаг 2/2: выберите, что изменить на витрине.",
                parse_mode="HTML",
                reply_markup=_admin_actions_keyboard(),
            )
            await callback.answer()
            return

        if len(parts) >= 3 and parts[1] == "admin_action":
            if not _shop_is_superadmin(callback.from_user.id):
                logger.warning(
                    "shop_admin_denied_not_superadmin provider=telegram actor_user_id=%s action=action_select",
                    callback.from_user.id,
                )
                await callback.answer("Недостаточно прав", show_alert=True)
                return
            action = parts[2]
            action_help: dict[str, str] = {
                "add": "➕ Добавить товар на витрину",
                "remove": "➖ Убрать товар с витрины",
                "price": "💳 Изменить цену",
                "position": "↕️ Изменить позицию",
                "sale": "⏱ Вкл/выкл акцию",
            }
            if action not in action_help:
                logger.error(
                    "shop_admin_action_unknown provider=telegram actor_user_id=%s action=%s callback_data=%s",
                    callback.from_user.id,
                    action,
                    callback.data,
                )
                await callback.answer("Неизвестное действие. Попробуйте снова.", show_alert=True)
                return
            action_title = action_help[action]
            logger.info("shop_admin_action_selected provider=telegram actor_user_id=%s action=%s", callback.from_user.id, action)
            roles = _shop_admin_roles()
            if not roles:
                logger.error(
                    "shop_admin_roles_empty provider=telegram actor_user_id=%s action=%s",
                    callback.from_user.id,
                    action,
                )
                await callback.answer("Нет ролей с признаком продаваемости. Включите продаваемость в /roles_admin.", show_alert=True)
                return
            try:
                await callback.message.edit_text(
                    (
                        "⚙️ <b>Настройка магазина</b>\n\n"
                        f"Выбрано действие: <b>{action_title}</b>\n"
                        "Шаг 1/2: выберите роль кнопкой ниже.\n"
                        "Шаг 2/2: бот подскажет, что ввести для завершения изменения."
                    ),
                    parse_mode="HTML",
                    reply_markup=_build_shop_admin_role_picker(action, roles),
                )
            except Exception as error:  # noqa: BLE001
                logger.exception(
                    "shop_admin_action_render_error provider=telegram actor_user_id=%s action=%s error=%s",
                    callback.from_user.id,
                    action,
                    error,
                )
                await callback.answer(SHOP_TEXT_PROTECTED_FAILURE, show_alert=True)
                return
            await callback.answer("Инструкция обновлена ниже 👇")
            return

        if len(parts) >= 4 and parts[1] == "admin_pick_role":
            if not _shop_is_superadmin(callback.from_user.id):
                logger.warning(
                    "shop_admin_denied_not_superadmin provider=telegram actor_user_id=%s action=pick_role",
                    callback.from_user.id,
                )
                await callback.answer("Недостаточно прав", show_alert=True)
                return
            action = parts[2]
            idx_raw = parts[3] if len(parts) > 3 else ""
            if not idx_raw.isdigit():
                logger.error(
                    "shop_admin_role_pick_invalid_index provider=telegram actor_user_id=%s action=%s index=%s",
                    callback.from_user.id,
                    action,
                    idx_raw,
                )
                await callback.answer("Не удалось определить роль. Выберите снова.", show_alert=True)
                return
            roles = _shop_admin_roles()
            idx = int(idx_raw)
            if idx < 0 or idx >= len(roles):
                logger.error(
                    "shop_admin_role_pick_out_of_range provider=telegram actor_user_id=%s action=%s index=%s roles_count=%s",
                    callback.from_user.id,
                    action,
                    idx,
                    len(roles),
                )
                await callback.answer("Список ролей обновился, выберите снова.", show_alert=True)
                return
            role_name = roles[idx]
            _SHOP_ADMIN_PENDING_ACTIONS[callback.from_user.id] = PendingShopAdminAction(
                action=action,
                role_name=role_name,
                created_at=time.time(),
            )
            logger.info(
                "shop_admin_role_selected provider=telegram actor_user_id=%s action=%s role_name=%s",
                callback.from_user.id,
                action,
                role_name,
            )
            if action == "remove":
                ok = RoleManagementService.deactivate_shop_role_item(
                    role_name,
                    actor_provider="telegram",
                    actor_user_id=callback.from_user.id,
                    source="shop_admin_buttons",
                )
                _SHOP_ADMIN_PENDING_ACTIONS.pop(callback.from_user.id, None)
                await callback.message.edit_text(
                    "✅ Роль убрана с витрины." if ok else "❌ Не удалось убрать роль с витрины (смотри логи).",
                    reply_markup=_admin_actions_keyboard(),
                )
                await callback.answer()
                return
            action_prompts = {
                "add": "Введите: <code>цена | [позиция]</code>\nПример: <code>150 | 3</code>",
                "price": "Введите новую цену:\nПример: <code>250</code>",
                "position": "Введите новую позицию:\nПример: <code>2</code>",
                "sale": (
                    "Введите: <code>цена_акции | YYYY-MM-DDTHH:MM | YYYY-MM-DDTHH:MM</code>\n"
                    "или <code>off</code>, чтобы выключить акцию."
                ),
            }
            await callback.message.edit_text(
                (
                    "⚙️ <b>Настройка магазина</b>\n\n"
                    f"Действие: <b>{action}</b>\n"
                    f"Роль: <b>{role_name}</b>\n\n"
                    f"{action_prompts.get(action, 'Введите параметры сообщением.')}"
                ),
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="⬅️ К действиям", callback_data="shop:admin_category:roles")]]
                ),
            )
            await callback.answer("Ожидаю параметры сообщением")
            return

        if len(parts) >= 3 and parts[1] == "category":
            category = parts[2]
            if category != "roles":
                await callback.answer("Эта категория пока недоступна.", show_alert=True)
                return
            logger.info(
                "shop_category_selected provider=telegram actor_user_id=%s account_id=%s category=roles",
                callback.from_user.id,
                profile_check.account_id,
            )
            items = get_shop_catalog_items(log_context="shop:telegram:category_roles")
            page_data = get_shop_page_slice(items, 0, page_size=SHOP_PAGE_SIZE)
            try:
                await callback.message.edit_text(
                    _shop_text(profile_check.account_id, page_data.page, page_data.total_pages),
                    parse_mode="HTML",
                    reply_markup=_build_shop_keyboard(items, page_data.page),
                )
            except Exception as error:  # noqa: BLE001
                logger.exception(
                    "shop_list_screen_render_error provider=telegram actor_user_id=%s action=category_select error=%s",
                    callback.from_user.id,
                    error,
                )
                logger.exception(
                    "ux_render_error event=ux_render_error screen=shop_list provider=telegram actor_user_id=%s error=%s",
                    callback.from_user.id,
                    error,
                )
                await callback.answer(SHOP_TEXT_PROTECTED_FAILURE, show_alert=True)
                return
            await callback.answer()
            return

        if len(parts) >= 2 and parts[1] == "categories":
            logger.info(
                "shop_back_to_categories provider=telegram actor_user_id=%s account_id=%s source=manual",
                callback.from_user.id,
                profile_check.account_id,
            )
            try:
                await callback.message.edit_text(
                    _shop_categories_text(profile_check.account_id),
                    parse_mode="HTML",
                    reply_markup=_build_categories_keyboard_with_admin(_shop_is_superadmin(callback.from_user.id)),
                )
            except Exception as error:  # noqa: BLE001
                logger.exception(
                    "shop_category_screen_render_error provider=telegram actor_user_id=%s source=manual error=%s",
                    callback.from_user.id,
                    error,
                )
                await callback.answer(SHOP_TEXT_PROTECTED_FAILURE, show_alert=True)
                return
            await callback.answer()
            return

        if len(parts) >= 4 and parts[1] == "item":
            shop_item_id = parts[2]
            page = int(parts[3])
            item = find_shop_item(items, shop_item_id)
            if not item:
                logger.error("shop_pagination_error provider=telegram reason=item_not_found actor_user_id=%s shop_item_id=%s", callback.from_user.id, shop_item_id)
                await callback.answer(SHOP_TEXT_ITEM_NOT_FOUND, show_alert=True)
                return
            logger.info(
                "shop_item_click provider=telegram actor_user_id=%s account_id=%s shop_item_id=%s page=%s",
                callback.from_user.id,
                profile_check.account_id,
                shop_item_id,
                page + 1,
            )
            try:
                await callback.message.edit_text(
                    _item_card_text(item, profile_check.account_id),
                    parse_mode="HTML",
                    reply_markup=_build_item_card_keyboard(shop_item_id=shop_item_id, page=page, price_points=item.price_points),
                )
            except Exception as error:  # noqa: BLE001
                logger.exception(
                    "shop_card_screen_render_error provider=telegram actor_user_id=%s shop_item_id=%s error=%s",
                    callback.from_user.id,
                    shop_item_id,
                    error,
                )
                await callback.answer(SHOP_TEXT_PROTECTED_FAILURE, show_alert=True)
                return
            await callback.answer()
            return

        if len(parts) >= 5 and parts[1] == "buy":
            shop_item_id = parts[2]
            page = int(parts[3])
            item = find_shop_item(items, shop_item_id)
            if not item:
                await callback.answer(SHOP_TEXT_ITEM_NOT_FOUND, show_alert=True)
                return
            try:
                await callback.message.edit_text(
                    _item_confirm_text(item, profile_check.account_id),
                    parse_mode="HTML",
                    reply_markup=_build_confirm_keyboard(shop_item_id=shop_item_id, page=page, price_points=item.price_points),
                )
            except Exception as error:  # noqa: BLE001
                logger.exception(
                    "shop_confirm_screen_render_error provider=telegram actor_user_id=%s shop_item_id=%s error=%s",
                    callback.from_user.id,
                    shop_item_id,
                    error,
                )
                await callback.answer(SHOP_TEXT_PROTECTED_FAILURE, show_alert=True)
                return
            await callback.answer()
            return

        if len(parts) >= 5 and parts[1] == "confirm":
            shop_item_id = parts[2]
            page = int(parts[3])
            expected_price_points = int(parts[4])
            result = purchase_shop_item(
                account_id=str(profile_check.account_id or ""),
                shop_item_id=shop_item_id,
                actor_provider="telegram",
                actor_user_id=callback.from_user.id,
                expected_price_points=expected_price_points,
            )
            if not result.ok:
                logger.warning(
                    "shop_purchase_reject provider=telegram actor_user_id=%s account_id=%s shop_item_id=%s reason=%s",
                    callback.from_user.id,
                    profile_check.account_id,
                    shop_item_id,
                    result.reason,
                )
                await callback.answer(result.message, show_alert=True)
                item = find_shop_item(items, shop_item_id)
                if item:
                    try:
                        await callback.message.edit_text(
                            _item_card_text(item, profile_check.account_id),
                            parse_mode="HTML",
                            reply_markup=_build_item_card_keyboard(shop_item_id=shop_item_id, page=page, price_points=item.price_points),
                        )
                    except Exception as error:  # noqa: BLE001
                        logger.exception(
                            "shop_card_screen_render_error provider=telegram actor_user_id=%s action=purchase_reject shop_item_id=%s error=%s",
                            callback.from_user.id,
                            shop_item_id,
                            error,
                        )
                return
            page_data = get_shop_page_slice(items, 0, page_size=SHOP_PAGE_SIZE)
            try:
                await callback.message.edit_text(
                    f"{_shop_categories_text(profile_check.account_id)}\n\n{result.message}",
                    parse_mode="HTML",
                    reply_markup=_build_categories_keyboard_with_admin(_shop_is_superadmin(callback.from_user.id)),
                )
            except Exception as error:  # noqa: BLE001
                logger.exception(
                    "shop_category_screen_render_error provider=telegram actor_user_id=%s source=purchase_success error=%s",
                    callback.from_user.id,
                    error,
                )
                await callback.answer(SHOP_TEXT_PROTECTED_FAILURE, show_alert=True)
                return
            logger.info(
                "shop_back_to_categories provider=telegram actor_user_id=%s account_id=%s source=purchase_success",
                callback.from_user.id,
                profile_check.account_id,
            )
            await callback.answer("Покупка завершена")
            return

        if len(parts) >= 3 and parts[1] == "back":
            target_page = int(parts[2])
            page_data = get_shop_page_slice(items, target_page, page_size=SHOP_PAGE_SIZE)
            try:
                await callback.message.edit_text(
                    _shop_text(profile_check.account_id, page_data.page, page_data.total_pages),
                    parse_mode="HTML",
                    reply_markup=_build_shop_keyboard(items, page_data.page),
                )
            except Exception as error:  # noqa: BLE001
                logger.exception(
                    "shop_list_screen_render_error provider=telegram actor_user_id=%s action=back_from_card error=%s",
                    callback.from_user.id,
                    error,
                )
                await callback.answer(SHOP_TEXT_PROTECTED_FAILURE, show_alert=True)
                return
            await callback.answer()
            return

        if len(parts) >= 4 and parts[1] == "page":
            target_page = int(parts[2])
            from_page = int(parts[3])
            page_data = get_shop_page_slice(items, target_page, page_size=SHOP_PAGE_SIZE)
            logger.info(
                "shop_page_switch provider=telegram actor_user_id=%s account_id=%s from_page=%s to_page=%s total_pages=%s",
                callback.from_user.id,
                profile_check.account_id,
                from_page + 1,
                page_data.page + 1,
                page_data.total_pages,
            )
            try:
                await callback.message.edit_text(
                    _shop_text(profile_check.account_id, page_data.page, page_data.total_pages),
                    parse_mode="HTML",
                    reply_markup=_build_shop_keyboard(items, page_data.page),
                )
            except Exception as error:  # noqa: BLE001
                logger.exception(
                    "shop_list_screen_render_error provider=telegram actor_user_id=%s action=page_switch error=%s",
                    callback.from_user.id,
                    error,
                )
                await callback.answer(SHOP_TEXT_PROTECTED_FAILURE, show_alert=True)
                return
            await callback.answer()
            return

        if len(parts) >= 3 and parts[1] == "refresh":
            target_page = int(parts[2])
            page_data = get_shop_page_slice(items, target_page, page_size=SHOP_PAGE_SIZE)
            logger.info(
                "shop_page_switch provider=telegram actor_user_id=%s account_id=%s from_page=%s to_page=%s total_pages=%s action=refresh",
                callback.from_user.id,
                profile_check.account_id,
                target_page + 1,
                page_data.page + 1,
                page_data.total_pages,
            )
            try:
                await callback.message.edit_text(
                    _shop_text(profile_check.account_id, page_data.page, page_data.total_pages),
                    parse_mode="HTML",
                    reply_markup=_build_shop_keyboard(items, page_data.page),
                )
            except Exception as error:  # noqa: BLE001
                logger.exception(
                    "shop_list_screen_render_error provider=telegram actor_user_id=%s action=refresh error=%s",
                    callback.from_user.id,
                    error,
                )
                await callback.answer(SHOP_TEXT_PROTECTED_FAILURE, show_alert=True)
                return
            await callback.answer("Обновлено")
            return
    except Exception as error:  # noqa: BLE001
        logger.exception("shop_error_screen_render_error provider=telegram actor_user_id=%s data=%s error=%s", callback.from_user.id, data, error)
        await callback.answer(SHOP_TEXT_PROTECTED_FAILURE, show_alert=True)
        return

    logger.error("shop_pagination_error provider=telegram reason=unknown_callback data=%s", data)
    await callback.answer("Неизвестное действие, обновите страницу.", show_alert=True)


@router.message(F.from_user, F.text)
async def shop_admin_pending_input(message: Message) -> None:
    if message.from_user is None:
        return
    pending = _SHOP_ADMIN_PENDING_ACTIONS.get(message.from_user.id)
    if pending is None:
        return
    if not _shop_is_superadmin(message.from_user.id):
        _SHOP_ADMIN_PENDING_ACTIONS.pop(message.from_user.id, None)
        logger.warning("shop_admin_pending_denied provider=telegram actor_user_id=%s", message.from_user.id)
        return
    if (time.time() - pending.created_at) > _SHOP_ADMIN_PENDING_TTL_SECONDS:
        _SHOP_ADMIN_PENDING_ACTIONS.pop(message.from_user.id, None)
        await message.answer("⌛ Сессия настройки магазина истекла. Откройте /shop заново.")
        return

    text = str(message.text or "").strip()
    role_name = pending.role_name
    action = pending.action
    try:
        if action == "add":
            parts = [p.strip() for p in text.split("|")]
            if not parts or not parts[0].lstrip("-").isdigit():
                await message.answer("❌ Формат: <code>цена | [позиция]</code>", parse_mode="HTML")
                return
            price = int(parts[0])
            position = int(parts[1]) if len(parts) > 1 and parts[1].lstrip("-").isdigit() else None
            ok = RoleManagementService.upsert_shop_role_item(
                role_name=role_name,
                base_price_points=price,
                display_position=position,
                actor_provider="telegram",
                actor_user_id=message.from_user.id,
                source="shop_admin_buttons",
            )
            await message.answer("✅ Роль добавлена на витрину магазина." if ok else "❌ Не удалось добавить роль на витрину (смотри логи).")
        elif action == "price":
            if not text.lstrip("-").isdigit():
                await message.answer("❌ Введите цену числом.")
                return
            ok = RoleManagementService.upsert_shop_role_item(
                role_name=role_name,
                base_price_points=int(text),
                actor_provider="telegram",
                actor_user_id=message.from_user.id,
                source="shop_admin_buttons",
            )
            await message.answer("✅ Цена обновлена." if ok else "❌ Не удалось обновить цену (смотри логи).")
        elif action == "position":
            if not text.lstrip("-").isdigit():
                await message.answer("❌ Введите позицию числом.")
                return
            current_shop = RoleManagementService.get_shop_role_item(role_name) or {}
            ok = RoleManagementService.upsert_shop_role_item(
                role_name=role_name,
                base_price_points=int(current_shop.get("base_price_points") or 0),
                display_position=int(text),
                actor_provider="telegram",
                actor_user_id=message.from_user.id,
                source="shop_admin_buttons",
            )
            await message.answer("✅ Позиция обновлена." if ok else "❌ Не удалось обновить позицию (смотри логи).")
        elif action == "sale":
            if text.lower() == "off":
                ok = RoleManagementService.upsert_shop_role_item(
                    role_name=role_name,
                    base_price_points=int((RoleManagementService.get_shop_role_item(role_name) or {}).get("base_price_points") or 0),
                    sale_price_points=None,
                    sale_starts_at=None,
                    sale_ends_at=None,
                    actor_provider="telegram",
                    actor_user_id=message.from_user.id,
                    source="shop_admin_buttons",
                )
                await message.answer("✅ Акция выключена." if ok else "❌ Не удалось выключить акцию (смотри логи).")
            else:
                parts = [p.strip() for p in text.split("|")]
                if len(parts) < 3 or not parts[0].lstrip("-").isdigit():
                    await message.answer("❌ Формат: <code>цена_акции | YYYY-MM-DDTHH:MM | YYYY-MM-DDTHH:MM</code>", parse_mode="HTML")
                    return
                ok = RoleManagementService.upsert_shop_role_item(
                    role_name=role_name,
                    base_price_points=int((RoleManagementService.get_shop_role_item(role_name) or {}).get("base_price_points") or 0),
                    sale_price_points=int(parts[0]),
                    sale_starts_at=parts[1],
                    sale_ends_at=parts[2],
                    actor_provider="telegram",
                    actor_user_id=message.from_user.id,
                    source="shop_admin_buttons",
                )
                await message.answer("✅ Акция сохранена." if ok else "❌ Не удалось обновить акцию (смотри логи).")
        else:
            logger.error("shop_admin_pending_unknown_action provider=telegram actor_user_id=%s action=%s", message.from_user.id, action)
            await message.answer("❌ Неизвестное действие. Откройте /shop заново.")
            return
        _SHOP_ADMIN_PENDING_ACTIONS.pop(message.from_user.id, None)
    except Exception as error:  # noqa: BLE001
        logger.exception(
            "shop_admin_pending_apply_failed provider=telegram actor_user_id=%s action=%s role_name=%s error=%s",
            message.from_user.id,
            action,
            role_name,
            error,
        )
        await message.answer("❌ Не удалось применить изменение (смотри логи).")
