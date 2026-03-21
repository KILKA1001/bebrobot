import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from aiogram import F, Router
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from bot.data import db
from bot.services import AccountsService, AuthorityService, PointsService, TicketsService
from bot.services.profile_titles import normalize_protected_profile_title
from bot.telegram_bot.identity import persist_telegram_identity_from_user
from bot.utils.blocking_io import run_blocking_io

logger = logging.getLogger(__name__)
router = Router()


@dataclass
class PendingAction:
    domain: str
    operation: str
    target_provider_user_id: str
    actor_provider_user_id: str
    created_at: float = field(default_factory=time.time)
    chat_id: Optional[int] = None
    flow_message_id: Optional[int] = None


_PENDING_ACTIONS: dict[int, PendingAction] = {}


PENDING_ACTION_TTL_SECONDS = 600


def _is_pending_action_expired(pending: PendingAction) -> bool:
    return (time.time() - pending.created_at) > PENDING_ACTION_TTL_SECONDS


def has_pending_action(telegram_user_id: int | None) -> bool:
    if telegram_user_id is None:
        return False

    pending = _PENDING_ACTIONS.get(telegram_user_id)
    if not pending:
        return False

    if _is_pending_action_expired(pending):
        logger.info(
            "pending action expired user_id=%s domain=%s operation=%s ttl_seconds=%s",
            telegram_user_id,
            pending.domain,
            pending.operation,
            PENDING_ACTION_TTL_SECONDS,
        )
        _PENDING_ACTIONS.pop(telegram_user_id, None)
        return False

    return True


def _can_manage_tickets(actor_titles: tuple[str, ...], actor_level: int) -> bool:
    normalized = {normalize_protected_profile_title(title) for title in actor_titles if str(title).strip()}
    if "глава клуба" in normalized or "главный вице" in normalized:
        return True
    return actor_level >= 100


def _can_manage_points(actor_level: int) -> bool:
    return actor_level >= 80


def _can_manage_own_engagement(actor_titles: tuple[str, ...]) -> bool:
    normalized = {normalize_protected_profile_title(title) for title in actor_titles if str(title).strip()}
    return bool(normalized & {"глава клуба", "главный вице"})


def _parse_callback_payload(raw_data: str) -> tuple[str, str, str | None] | None:
    parts = str(raw_data or "").split(":")
    if len(parts) == 4:
        _, action, target_id, owner_id = parts
        return action, target_id, owner_id
    if len(parts) == 3:
        _, action, target_id = parts
        return action, target_id, None
    return None


async def _respond_in_flow(
    message: Message,
    pending: PendingAction | None,
    text: str,
    *,
    parse_mode: ParseMode | None = None,
) -> None:
    if pending and pending.chat_id and pending.flow_message_id:
        try:
            await message.bot.edit_message_text(
                text=text,
                chat_id=pending.chat_id,
                message_id=pending.flow_message_id,
                parse_mode=parse_mode,
            )
            return
        except Exception:
            logger.exception(
                "failed to edit engagement flow message chat_id=%s message_id=%s actor_id=%s",
                pending.chat_id,
                pending.flow_message_id,
                message.from_user.id if message.from_user else None,
            )
    await message.answer(text, parse_mode=parse_mode)



async def _guard_callback_actor(callback: CallbackQuery, owner_id: str | None) -> bool:
    persist_telegram_identity_from_user(callback.from_user)
    if callback.from_user is None:
        await callback.answer("Ошибка пользователя", show_alert=True)
        return False

    if owner_id and str(callback.from_user.id) != str(owner_id):
        logger.warning(
            "engagement callback denied foreign actor callback_data=%s actor_id=%s owner_id=%s",
            callback.data,
            callback.from_user.id,
            owner_id,
        )
        await callback.answer("Чушка, не суй свой пятак в чужой пердак")
        return False
    return True


def _parse_target_arg(message: Message) -> int | None:
    def _extract_user_id_from_entities(source_message: Message | None) -> int | None:
        if source_message is None:
            return None

        for entity in source_message.entities or []:
            if entity.type == "text_mention" and entity.user is not None:
                return int(entity.user.id)
            if entity.type == "text_link" and entity.url:
                url = str(entity.url)
                if url.startswith("tg://user?id="):
                    raw_user_id = url.split("=", 1)[1]
                    if raw_user_id.isdigit():
                        return int(raw_user_id)
        return None

    if message.reply_to_message:
        quoted_target_id = _extract_user_id_from_entities(message.reply_to_message)
        if quoted_target_id is not None:
            return quoted_target_id

        if message.reply_to_message.from_user:
            reply_user = message.reply_to_message.from_user
            if not reply_user.is_bot:
                return int(reply_user.id)
            logger.warning(
                "points/tickets target reply points to bot, trying command text fallback actor_id=%s chat_id=%s reply_user_id=%s",
                message.from_user.id if message.from_user else None,
                message.chat.id if message.chat else None,
                reply_user.id,
            )

    text = (message.text or "").strip()
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        return message.from_user.id if message.from_user else None

    candidate = parts[1].strip()
    if candidate.isdigit():
        return int(candidate)

    mentioned_target_id = _extract_user_id_from_entities(message)
    if mentioned_target_id is not None:
        return mentioned_target_id

    return None


def _build_points_keyboard(target_id: int, actor_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="ℹ️ Что делает команда", callback_data=f"points:help:{target_id}:{actor_id}")],
            [InlineKeyboardButton(text="➕ Начислить баллы", callback_data=f"points:add:{target_id}:{actor_id}")],
            [InlineKeyboardButton(text="➖ Снять баллы", callback_data=f"points:remove:{target_id}:{actor_id}")],
        ]
    )


def _build_tickets_keyboard(target_id: int, actor_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="ℹ️ Что делает команда", callback_data=f"tickets:help:{target_id}:{actor_id}")],
            [InlineKeyboardButton(text="🎟️ + Обычные", callback_data=f"tickets:add_normal:{target_id}:{actor_id}")],
            [InlineKeyboardButton(text="🎟️ - Обычные", callback_data=f"tickets:remove_normal:{target_id}:{actor_id}")],
            [InlineKeyboardButton(text="🪙 + Золотые", callback_data=f"tickets:add_gold:{target_id}:{actor_id}")],
            [InlineKeyboardButton(text="🪙 - Золотые", callback_data=f"tickets:remove_gold:{target_id}:{actor_id}")],
        ]
    )


def _get_score_snapshot(account_id: str) -> tuple[float, int, int]:
    if not db.supabase:
        return 0.0, 0, 0
    try:
        row = (
            db.supabase.table("scores")
            .select("points,tickets_normal,tickets_gold")
            .eq("account_id", str(account_id))
            .limit(1)
            .execute()
        )
        if row.data:
            data = row.data[0]
            return float(data.get("points") or 0), int(data.get("tickets_normal") or 0), int(data.get("tickets_gold") or 0)
    except Exception:
        logger.exception("failed to read score snapshot account_id=%s", account_id)
    return 0.0, 0, 0


@router.message(Command("balance"))
async def balance_command(message: Message) -> None:
    try:
        persist_telegram_identity_from_user(message.from_user)
        if not message.from_user:
            await message.answer("❌ Не удалось определить пользователя Telegram.")
            return

        target_id = _parse_target_arg(message)
        if target_id is None:
            await message.answer("❌ Не удалось определить цель. Используйте ответ на сообщение или id пользователя.")
            return

        profile = await run_blocking_io(
            "telegram.balance.get_profile",
            AccountsService.get_profile,
            "telegram",
            str(target_id),
            logger=logger,
        )
        if not profile:
            await message.answer("❌ Профиль не найден. Сначала выполните /register")
            return

        points, tickets_normal, tickets_gold = await run_blocking_io(
            "telegram.balance.score_snapshot",
            _get_score_snapshot,
            profile["account_id"],
            logger=logger,
        )
        await message.answer(
            "💰 <b>Баланс пользователя</b>\n"
            f"Пользователь: <a href=\"tg://user?id={target_id}\">{profile['custom_nick']}</a>\n"
            f"Баллы: <b>{points:.2f}</b>\n"
            f"Билеты: 🎟️ <b>{tickets_normal}</b> / 🪙 <b>{tickets_gold}</b>",
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        logger.exception(
            "balance command failed actor_id=%s chat_id=%s text=%s",
            message.from_user.id if message.from_user else None,
            message.chat.id if message.chat else None,
            message.text,
        )
        await message.answer("❌ Ошибка получения баланса.")


@router.message(Command("points"))
async def points_menu_command(message: Message) -> None:
    try:
        persist_telegram_identity_from_user(message.from_user)
        if not message.from_user:
            await message.answer("❌ Не удалось определить пользователя Telegram.")
            return
        actor_id = str(message.from_user.id)
        target_id = _parse_target_arg(message)
        if target_id is None:
            await message.answer("❌ Не удалось определить цель. Используйте ответ на сообщение или id пользователя.")
            return

        authority = await run_blocking_io(
            "telegram.points.resolve_authority",
            AuthorityService.resolve_authority,
            "telegram",
            actor_id,
            logger=logger,
        )
        if not _can_manage_points(authority.level):
            await message.answer("Недоступно по вашему званию.")
            return

        if str(target_id) == actor_id:
            if not _can_manage_own_engagement(authority.titles):
                logger.warning("tickets menu self-edit denied actor_id=%s", actor_id)
                await message.answer("❌ Нельзя редактировать себя. Доступно только Главе клуба и Главному вице.")
                return
        elif not await run_blocking_io(
            "telegram.points.can_manage_target",
            AuthorityService.can_manage_target,
            "telegram",
            actor_id,
            "telegram",
            str(target_id),
            logger=logger,
        ):
            await message.answer("❌ Нельзя взаимодействовать с пользователем с равным/более высоким званием.")
            return

        profile = await run_blocking_io(
            "telegram.points.get_profile",
            AccountsService.get_profile,
            "telegram",
            str(target_id),
            logger=logger,
        )
        if not profile:
            await message.answer("❌ Целевой пользователь не зарегистрирован в системе.")
            return

        points, tickets_normal, tickets_gold = await run_blocking_io(
            "telegram.points.score_snapshot",
            _get_score_snapshot,
            profile["account_id"],
            logger=logger,
        )
        await message.answer(
            "🎛️ <b>Меню баллов</b>\n"
            f"Пользователь: <a href=\"tg://user?id={target_id}\">{profile['custom_nick']}</a>\n"
            f"Текущий баланс: <b>{points:.2f}</b>\n"
            f"Билеты: 🎟️ {tickets_normal} / 🪙 {tickets_gold}\n\n"
            "Выберите действие. Для любого изменения причина обязательна.",
            parse_mode=ParseMode.HTML,
            reply_markup=_build_points_keyboard(target_id, int(actor_id)),
        )
    except Exception:
        logger.exception("points menu command failed")
        await message.answer("❌ Ошибка открытия меню баллов.")


@router.message(Command("tickets"))
async def tickets_menu_command(message: Message) -> None:
    try:
        persist_telegram_identity_from_user(message.from_user)
        if not message.from_user:
            await message.answer("❌ Не удалось определить пользователя Telegram.")
            return
        actor_id = str(message.from_user.id)
        target_id = _parse_target_arg(message)
        if target_id is None:
            await message.answer("❌ Не удалось определить цель. Используйте ответ на сообщение или id пользователя.")
            return

        authority = await run_blocking_io(
            "telegram.tickets.resolve_authority",
            AuthorityService.resolve_authority,
            "telegram",
            actor_id,
            logger=logger,
        )
        if not _can_manage_tickets(authority.titles, authority.level):
            await message.answer("Недоступно по вашему званию.")
            return

        if str(target_id) == actor_id:
            if not _can_manage_own_engagement(authority.titles):
                logger.warning("points menu self-edit denied actor_id=%s", actor_id)
                await message.answer("❌ Нельзя редактировать себя. Доступно только Главе клуба и Главному вице.")
                return
        elif not await run_blocking_io(
            "telegram.tickets.can_manage_target",
            AuthorityService.can_manage_target,
            "telegram",
            actor_id,
            "telegram",
            str(target_id),
            logger=logger,
        ):
            await message.answer("❌ Нельзя взаимодействовать с пользователем с равным/более высоким званием.")
            return

        profile = await run_blocking_io(
            "telegram.tickets.get_profile",
            AccountsService.get_profile,
            "telegram",
            str(target_id),
            logger=logger,
        )
        if not profile:
            await message.answer("❌ Целевой пользователь не зарегистрирован в системе.")
            return

        points, tickets_normal, tickets_gold = await run_blocking_io(
            "telegram.tickets.score_snapshot",
            _get_score_snapshot,
            profile["account_id"],
            logger=logger,
        )
        await message.answer(
            "🎟️ <b>Меню билетов</b>\n"
            f"Пользователь: <a href=\"tg://user?id={target_id}\">{profile['custom_nick']}</a>\n"
            f"Баллы: <b>{points:.2f}</b>\n"
            f"Текущие билеты: 🎟️ <b>{tickets_normal}</b> / 🪙 <b>{tickets_gold}</b>\n\n"
            "Выберите действие. Для любого изменения причина обязательна.",
            parse_mode=ParseMode.HTML,
            reply_markup=_build_tickets_keyboard(target_id, int(actor_id)),
        )
    except Exception:
        logger.exception("tickets menu command failed")
        await message.answer("❌ Ошибка открытия меню билетов.")


@router.callback_query(F.data.startswith("points:"))
async def points_callback(callback: CallbackQuery) -> None:
    try:
        persist_telegram_identity_from_user(callback.from_user)
        if not callback.from_user:
            await callback.answer("Ошибка пользователя", show_alert=True)
            return

        payload = _parse_callback_payload(str(callback.data))
        if payload is None:
            logger.error("points callback got malformed payload=%s", callback.data)
            await callback.answer("Ошибка меню баллов", show_alert=True)
            return
        action, target_id, owner_id = payload
        if not await _guard_callback_actor(callback, owner_id):
            return
        actor_id = str(callback.from_user.id)

        authority = await run_blocking_io(
            "telegram.points_callback.resolve_authority",
            AuthorityService.resolve_authority,
            "telegram",
            actor_id,
            logger=logger,
        )
        if not _can_manage_points(authority.level):
            logger.warning("points callback denied by authority actor_id=%s action=%s", actor_id, action)
            await callback.answer("Недоступно по вашему званию.", show_alert=True)
            return

        if target_id == actor_id:
            if not _can_manage_own_engagement(authority.titles):
                logger.warning("points callback self-edit denied actor_id=%s", actor_id)
                await callback.answer("Нельзя редактировать себя.", show_alert=True)
                return
        elif not await run_blocking_io(
            "telegram.points_callback.can_manage_target",
            AuthorityService.can_manage_target,
            "telegram",
            actor_id,
            "telegram",
            str(target_id),
            logger=logger,
        ):
            logger.warning("points callback denied by hierarchy actor_id=%s target_id=%s", actor_id, target_id)
            await callback.answer("Нельзя взаимодействовать с равным/старшим званием.", show_alert=True)
            return

        if action == "help":
            await callback.answer(
                "ℹ️ Формат: число | причина\nПример: 25 | За победу в турнире",
                show_alert=True,
            )
            return

        flow_message_id = callback.message.message_id if callback.message else None
        flow_chat_id = callback.message.chat.id if callback.message and callback.message.chat else None
        _PENDING_ACTIONS[callback.from_user.id] = PendingAction(
            domain="points",
            operation=action,
            target_provider_user_id=target_id,
            actor_provider_user_id=actor_id,
            chat_id=flow_chat_id,
            flow_message_id=flow_message_id,
        )
        if callback.message is not None:
            try:
                await callback.message.edit_text(
                    "Введите данные в формате: <code>число | причина</code>.\n"
                    "Причина обязательна, без неё изменение не выполнится.",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                logger.exception("points callback failed to edit flow prompt actor_id=%s", actor_id)
                await callback.message.answer(
                    "Введите данные в формате: <code>число | причина</code>.\n"
                    "Причина обязательна, без неё изменение не выполнится.",
                    parse_mode=ParseMode.HTML,
                )
        await callback.answer()
    except Exception:
        logger.exception("points callback failed callback_data=%s", callback.data)
        await callback.answer("Ошибка меню баллов", show_alert=True)


@router.callback_query(F.data.startswith("tickets:"))
async def tickets_callback(callback: CallbackQuery) -> None:
    try:
        persist_telegram_identity_from_user(callback.from_user)
        if not callback.from_user:
            await callback.answer("Ошибка пользователя", show_alert=True)
            return

        payload = _parse_callback_payload(str(callback.data))
        if payload is None:
            logger.error("tickets callback got malformed payload=%s", callback.data)
            await callback.answer("Ошибка меню билетов", show_alert=True)
            return
        action, target_id, owner_id = payload
        if not await _guard_callback_actor(callback, owner_id):
            return
        actor_id = str(callback.from_user.id)

        authority = await run_blocking_io(
            "telegram.tickets_callback.resolve_authority",
            AuthorityService.resolve_authority,
            "telegram",
            actor_id,
            logger=logger,
        )
        if not _can_manage_tickets(authority.titles, authority.level):
            logger.warning("tickets callback denied by authority actor_id=%s action=%s", actor_id, action)
            await callback.answer("Недоступно по вашему званию.", show_alert=True)
            return

        if target_id == actor_id:
            if not _can_manage_own_engagement(authority.titles):
                logger.warning("tickets callback self-edit denied actor_id=%s", actor_id)
                await callback.answer("Нельзя редактировать себя.", show_alert=True)
                return
        elif not await run_blocking_io(
            "telegram.tickets_callback.can_manage_target",
            AuthorityService.can_manage_target,
            "telegram",
            actor_id,
            "telegram",
            str(target_id),
            logger=logger,
        ):
            logger.warning("tickets callback denied by hierarchy actor_id=%s target_id=%s", actor_id, target_id)
            await callback.answer("Нельзя взаимодействовать с равным/старшим званием.", show_alert=True)
            return

        if action == "help":
            await callback.answer(
                "ℹ️ Напишите только причину. Команда добавит или спишет ровно 1 билет.",
                show_alert=True,
            )
            return

        flow_message_id = callback.message.message_id if callback.message else None
        flow_chat_id = callback.message.chat.id if callback.message and callback.message.chat else None
        _PENDING_ACTIONS[callback.from_user.id] = PendingAction(
            domain="tickets",
            operation=action,
            target_provider_user_id=target_id,
            actor_provider_user_id=actor_id,
            chat_id=flow_chat_id,
            flow_message_id=flow_message_id,
        )
        if callback.message is not None:
            try:
                await callback.message.edit_text(
                    "Введите только причину изменения.\n"
                    "Команда добавит или спишет ровно 1 билет.",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                logger.exception("tickets callback failed to edit flow prompt actor_id=%s", actor_id)
                await callback.message.answer(
                    "Введите только причину изменения.\n"
                    "Команда добавит или спишет ровно 1 билет.",
                    parse_mode=ParseMode.HTML,
                )
        await callback.answer()
    except Exception:
        logger.exception("tickets callback failed callback_data=%s", callback.data)
        await callback.answer("Ошибка меню билетов", show_alert=True)


@router.message(F.from_user, F.from_user.id.func(has_pending_action))
async def pending_action_handler(message: Message) -> None:
    persist_telegram_identity_from_user(message.from_user)
    if not has_pending_action(message.from_user.id):
        logger.warning(
            "pending action handler invoked without pending state user_id=%s chat_id=%s",
            message.from_user.id,
            message.chat.id if message.chat else None,
        )
        return

    pending = _PENDING_ACTIONS.get(message.from_user.id)
    try:
        authority = await run_blocking_io(
            "telegram.pending_action.resolve_authority",
            AuthorityService.resolve_authority,
            "telegram",
            str(message.from_user.id),
            logger=logger,
        )
        if pending.domain == "points" and not _can_manage_points(authority.level):
            logger.warning("pending points action denied by authority actor_id=%s", message.from_user.id)
            await message.answer("❌ Недостаточно полномочий для редактирования баллов.")
            _PENDING_ACTIONS.pop(message.from_user.id, None)
            return
        if pending.domain == "tickets" and not _can_manage_tickets(authority.titles, authority.level):
            logger.warning("pending tickets action denied by authority actor_id=%s", message.from_user.id)
            await message.answer("❌ Недостаточно полномочий для редактирования билетов.")
            _PENDING_ACTIONS.pop(message.from_user.id, None)
            return
        if str(pending.target_provider_user_id) == str(message.from_user.id):
            if not _can_manage_own_engagement(authority.titles):
                logger.warning(
                    "pending action self-edit denied actor_id=%s domain=%s",
                    message.from_user.id,
                    pending.domain,
                )
                await message.answer("❌ Нельзя редактировать себя. Доступно только Главе клуба и Главному вице.")
                _PENDING_ACTIONS.pop(message.from_user.id, None)
                return
        elif not await run_blocking_io(
            "telegram.pending_action.can_manage_target",
            AuthorityService.can_manage_target,
            "telegram",
            str(message.from_user.id),
            "telegram",
            str(pending.target_provider_user_id),
            logger=logger,
        ):
            logger.warning(
                "pending action denied by hierarchy actor_id=%s target_id=%s domain=%s",
                message.from_user.id,
                pending.target_provider_user_id,
                pending.domain,
            )
            await message.answer("❌ Нельзя выполнять действие для пользователя с равным/более высоким званием.")
            _PENDING_ACTIONS.pop(message.from_user.id, None)
            return

        raw = (message.text or "").strip()

        if pending.domain == "points":
            if "|" not in raw:
                await _respond_in_flow(message, pending, "❌ Неверный формат. Используйте: число | причина")
                return
            amount_raw, reason_raw = [part.strip() for part in raw.split("|", 1)]
            if not reason_raw:
                await _respond_in_flow(message, pending, "❌ Причина обязательна. Изменение отменено.")
                _PENDING_ACTIONS.pop(message.from_user.id, None)
                return
            amount = float(amount_raw.replace(",", "."))
            if amount <= 0:
                await _respond_in_flow(message, pending, "❌ Количество баллов должно быть больше 0.")
                return
            if pending.operation == "add":
                ok = await run_blocking_io(
                    "telegram.pending_action.add_points",
                    PointsService.add_points_by_identity,
                    "telegram",
                    pending.target_provider_user_id,
                    amount,
                    reason_raw,
                    int(pending.actor_provider_user_id),
                    logger=logger,
                )
                action_text = "начислены"
            else:
                ok = await run_blocking_io(
                    "telegram.pending_action.remove_points",
                    PointsService.remove_points_by_identity,
                    "telegram",
                    pending.target_provider_user_id,
                    amount,
                    reason_raw,
                    int(pending.actor_provider_user_id),
                    logger=logger,
                )
                action_text = "списаны"
            if not ok:
                await _respond_in_flow(message, pending, "❌ Не удалось обновить баллы. Проверьте привязку аккаунта.")
            else:
                await _respond_in_flow(message, pending, f"✅ Баллы успешно {action_text}: {amount:.2f}. Причина: {reason_raw}")

        elif pending.domain == "tickets":
            reason_raw = raw
            if not reason_raw:
                await _respond_in_flow(message, pending, "❌ Причина обязательна. Изменение отменено.")
                _PENDING_ACTIONS.pop(message.from_user.id, None)
                return
            amount = 1
            mapping = {
                "add_normal": ("normal", True),
                "remove_normal": ("normal", False),
                "add_gold": ("gold", True),
                "remove_gold": ("gold", False),
            }
            ticket_type, is_add = mapping[pending.operation]
            if is_add:
                ok = await run_blocking_io(
                    "telegram.pending_action.give_ticket",
                    TicketsService.give_ticket_by_identity,
                    "telegram",
                    pending.target_provider_user_id,
                    ticket_type,
                    amount,
                    reason_raw,
                    int(pending.actor_provider_user_id),
                    logger=logger,
                )
                verb = "начислены"
            else:
                ok = await run_blocking_io(
                    "telegram.pending_action.remove_ticket",
                    TicketsService.remove_ticket_by_identity,
                    "telegram",
                    pending.target_provider_user_id,
                    ticket_type,
                    amount,
                    reason_raw,
                    int(pending.actor_provider_user_id),
                    logger=logger,
                )
                verb = "списаны"

            if not ok:
                await _respond_in_flow(message, pending, "❌ Не удалось обновить билеты. Проверьте привязку аккаунта.")
            else:
                await _respond_in_flow(message, pending, f"✅ Билеты успешно {verb}: {amount}. Причина: {reason_raw}")

        _PENDING_ACTIONS.pop(message.from_user.id, None)
    except ValueError:
        logger.exception("pending action parse failed user_id=%s text=%s", message.from_user.id, message.text)
        await _respond_in_flow(message, pending, "❌ Ошибка формата количества. Проверьте число.")
    except Exception:
        logger.exception("pending action failed user_id=%s", message.from_user.id)
        await _respond_in_flow(message, pending, "❌ Ошибка выполнения операции.")
        _PENDING_ACTIONS.pop(message.from_user.id, None)
