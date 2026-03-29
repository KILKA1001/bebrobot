import logging

from aiogram import F, Router
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from bot.telegram_bot.identity import persist_telegram_identity_from_user
from bot.systems.shop_logic import (
    SHOP_PAGE_SIZE,
    SHOP_TEXT_ACQUIRE_HINT_PLACEHOLDER,
    SHOP_TEXT_CONFIRM_PURCHASE,
    SHOP_TEXT_ITEM_NOT_FOUND,
    SHOP_TEXT_ITEM_PLACEHOLDER,
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
    payload = build_shop_render_payload(account_id)
    return f"{payload.telegram_text}\n\nСтраница: <b>{page + 1}/{total_pages}</b>"


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
        f"Как получить: {acquire_hint}"
    )


def _item_confirm_text(item, account_id: str | None) -> str:
    return (
        f"{_item_card_text(item, account_id)}\n\n"
        f"{SHOP_TEXT_CONFIRM_PURCHASE}"
    )


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

    items = get_shop_catalog_items(log_context="shop:telegram")
    page_data = get_shop_page_slice(items, 0, page_size=SHOP_PAGE_SIZE)
    reply_markup = _build_shop_keyboard(items, 0)
    text = _shop_text(profile_check.account_id, page_data.page, page_data.total_pages)

    if message.chat.type == "private":
        await message.answer(text, parse_mode="HTML", reply_markup=reply_markup)
        logger.info(
            "shop_page_open provider=telegram actor_user_id=%s account_id=%s page=%s total_pages=%s page_size=%s",
            message.from_user.id,
            profile_check.account_id,
            page_data.page + 1,
            page_data.total_pages,
            SHOP_PAGE_SIZE,
        )
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
        logger.info(
            "shop_page_open provider=telegram actor_user_id=%s account_id=%s page=%s total_pages=%s page_size=%s",
            message.from_user.id,
            profile_check.account_id,
            page_data.page + 1,
            page_data.total_pages,
            SHOP_PAGE_SIZE,
        )
    except (TelegramForbiddenError, TelegramBadRequest) as error:
        logger.exception("shop_dm_transfer_error provider=telegram actor_user_id=%s dm_sent=false error=%s", message.from_user.id, error)
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
            await callback.message.edit_text(
                _item_card_text(item, profile_check.account_id),
                parse_mode="HTML",
                reply_markup=_build_item_card_keyboard(shop_item_id=shop_item_id, page=page, price_points=item.price_points),
            )
            await callback.answer()
            return

        if len(parts) >= 5 and parts[1] == "buy":
            shop_item_id = parts[2]
            page = int(parts[3])
            item = find_shop_item(items, shop_item_id)
            if not item:
                await callback.answer(SHOP_TEXT_ITEM_NOT_FOUND, show_alert=True)
                return
            await callback.message.edit_text(
                _item_confirm_text(item, profile_check.account_id),
                parse_mode="HTML",
                reply_markup=_build_confirm_keyboard(shop_item_id=shop_item_id, page=page, price_points=item.price_points),
            )
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
                    await callback.message.edit_text(
                        _item_card_text(item, profile_check.account_id),
                        parse_mode="HTML",
                        reply_markup=_build_item_card_keyboard(shop_item_id=shop_item_id, page=page, price_points=item.price_points),
                    )
                return
            page_data = get_shop_page_slice(items, 0, page_size=SHOP_PAGE_SIZE)
            await callback.message.edit_text(
                f"{_shop_text(profile_check.account_id, page_data.page, page_data.total_pages)}\n\n{result.message}",
                parse_mode="HTML",
                reply_markup=_build_shop_keyboard(items, page_data.page),
            )
            await callback.answer("Покупка завершена")
            return

        if len(parts) >= 3 and parts[1] == "back":
            target_page = int(parts[2])
            page_data = get_shop_page_slice(items, target_page, page_size=SHOP_PAGE_SIZE)
            await callback.message.edit_text(
                _shop_text(profile_check.account_id, page_data.page, page_data.total_pages),
                parse_mode="HTML",
                reply_markup=_build_shop_keyboard(items, page_data.page),
            )
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
            await callback.message.edit_text(
                _shop_text(profile_check.account_id, page_data.page, page_data.total_pages),
                parse_mode="HTML",
                reply_markup=_build_shop_keyboard(items, page_data.page),
            )
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
            await callback.message.edit_text(
                _shop_text(profile_check.account_id, page_data.page, page_data.total_pages),
                parse_mode="HTML",
                reply_markup=_build_shop_keyboard(items, page_data.page),
            )
            await callback.answer("Обновлено")
            return
    except Exception as error:  # noqa: BLE001
        logger.exception("shop_pagination_error provider=telegram actor_user_id=%s data=%s error=%s", callback.from_user.id, data, error)
        await callback.answer(SHOP_TEXT_PROTECTED_FAILURE, show_alert=True)
        return

    logger.error("shop_pagination_error provider=telegram reason=unknown_callback data=%s", data)
    await callback.answer("Неизвестное действие, обновите страницу.", show_alert=True)
