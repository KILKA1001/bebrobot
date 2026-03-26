from __future__ import annotations

import logging

from aiogram import F, Router
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from bot.data import db
from bot.services import AccountsService, FinesService

logger = logging.getLogger(__name__)
router = Router()


def _format_fine_line(fine: dict) -> str:
    fine_id = fine.get("id")
    amount = float(fine.get("amount") or 0)
    paid_amount = float(fine.get("paid_amount") or 0)
    due_date = str(fine.get("due_date") or "—")
    status = "✅ Оплачен" if fine.get("is_paid") else "⏳ Активен"
    if fine.get("is_overdue"):
        status = "⚠️ Просрочен"
    if fine.get("is_canceled"):
        status = "🚫 Отменён"
    remaining = max(0.0, amount - paid_amount)
    reason = str(fine.get("reason") or "Без причины")
    return (
        f"📌 Штраф #{fine_id}\n"
        f"Статус: {status}\n"
        f"Сумма: {amount:.2f}\n"
        f"Оплачено: {paid_amount:.2f}\n"
        f"Осталось: {remaining:.2f}\n"
        f"Срок: {due_date}\n"
        f"Причина: {reason}"
    )


def _pay_keyboard(fine_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="💸 Оплатить 100%", callback_data=f"tgfine:pay:{fine_id}:100"),
                InlineKeyboardButton(text="🌗 50%", callback_data=f"tgfine:pay:{fine_id}:50"),
            ],
            [InlineKeyboardButton(text="🌘 25%", callback_data=f"tgfine:pay:{fine_id}:25")],
        ]
    )


async def send_legacy_fines_panel(*, message: Message, telegram_user_id: int) -> None:
    actor_id = str(telegram_user_id)
    account_id = AccountsService.resolve_account_id("telegram", actor_id)
    if not account_id:
        logger.warning("telegram legacy fines panel account unresolved actor_id=%s chat_id=%s", actor_id, message.chat.id)
        await message.answer("❌ Сначала привяжите общий аккаунт, затем повторите /modstatus.")
        return

    fines = FinesService.get_user_fines_by_account(str(account_id), active_only=True)
    if not fines:
        await message.answer(
            "✅ У вас нет активных legacy-штрафов.\n"
            "Для просмотра активных наказаний и кейсов используйте /modstatus."
        )
        return

    await message.answer(
        "💳 Панель оплаты legacy-штрафов из /modstatus.\n"
        "Выберите кнопку оплаты под нужным штрафом.\n"
        "Если штраф уже списан автоматически в кейсе /rep, повторно платить не нужно."
    )
    for fine in fines:
        fine_id = int(fine.get("id") or 0)
        if fine_id <= 0:
            logger.warning("telegram legacy fines panel skip invalid fine id actor_id=%s fine=%s", actor_id, fine)
            continue
        await message.answer(_format_fine_line(fine), reply_markup=_pay_keyboard(fine_id))


@router.callback_query(F.data.startswith("tgfine:pay:"))
async def myfines_pay_callback(callback: CallbackQuery) -> None:
    if not callback.from_user:
        return
    data_parts = str(callback.data or "").split(":")
    if len(data_parts) != 4:
        await callback.answer("❌ Некорректная кнопка оплаты.", show_alert=True)
        return
    _, action, fine_id_raw, percent_raw = data_parts
    if action != "pay":
        await callback.answer("❌ Некорректное действие.", show_alert=True)
        return
    if not fine_id_raw.isdigit() or percent_raw not in {"25", "50", "100"}:
        await callback.answer("❌ Некорректные параметры оплаты.", show_alert=True)
        return

    fine_id = int(fine_id_raw)
    percent = float(percent_raw) / 100.0
    actor_id = str(callback.from_user.id)
    account_id = AccountsService.resolve_account_id("telegram", actor_id)
    if not account_id:
        logger.warning("telegram myfines pay account unresolved actor_id=%s fine_id=%s", actor_id, fine_id)
        await callback.answer("❌ Сначала привяжите общий аккаунт.", show_alert=True)
        return

    fine = db.get_fine_by_id(fine_id)
    if not fine:
        logger.warning("telegram myfines pay fine not found actor_id=%s fine_id=%s", actor_id, fine_id)
        await callback.answer("❌ Штраф не найден.", show_alert=True)
        return
    if str(fine.get("account_id") or "") != str(account_id):
        logger.warning(
            "telegram myfines pay denied чужой штраф actor_id=%s account_id=%s fine_id=%s fine_account_id=%s",
            actor_id,
            account_id,
            fine_id,
            fine.get("account_id"),
        )
        await callback.answer("❌ Можно оплачивать только свои штрафы.", show_alert=True)
        return
    if fine.get("is_paid") or fine.get("is_canceled"):
        await callback.answer("ℹ️ Этот штраф уже закрыт.", show_alert=True)
        return

    amount = float(fine.get("amount") or 0)
    paid_amount = float(fine.get("paid_amount") or 0)
    remaining = max(0.0, amount - paid_amount)
    pay_amount = round(remaining * percent, 2)
    if pay_amount <= 0:
        await callback.answer("ℹ️ По этому штрафу уже ничего не осталось оплачивать.", show_alert=True)
        return

    try:
        ok = db.record_payment_by_account(
            account_id=str(account_id),
            fine_id=fine_id,
            amount=pay_amount,
            author_account_id=str(account_id),
        )
    except Exception:
        logger.exception("telegram myfines pay failed actor_id=%s fine_id=%s amount=%s", actor_id, fine_id, pay_amount)
        await callback.answer("❌ Ошибка оплаты. Подробности записаны в консоль.", show_alert=True)
        return

    if not ok:
        logger.error("telegram myfines pay returned false actor_id=%s fine_id=%s amount=%s", actor_id, fine_id, pay_amount)
        await callback.answer("❌ Не удалось записать оплату. Попробуйте позже.", show_alert=True)
        return

    refreshed_fine = db.get_fine_by_id(fine_id) or fine
    refreshed_text = _format_fine_line(refreshed_fine)
    await callback.answer(f"✅ Оплата {pay_amount:.2f} баллов записана.", show_alert=True)
    if callback.message:
        reply_markup = None if refreshed_fine.get("is_paid") or refreshed_fine.get("is_canceled") else _pay_keyboard(fine_id)
        await callback.message.edit_text(refreshed_text, reply_markup=reply_markup)
