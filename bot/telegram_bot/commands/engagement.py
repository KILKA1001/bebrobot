import logging
import time
from dataclasses import dataclass, field

from aiogram import F, Router
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from bot.data import db
from bot.services import AccountsService, AuthorityService, PointsService, TicketsService

logger = logging.getLogger(__name__)
router = Router()


@dataclass
class PendingAction:
    domain: str
    operation: str
    target_provider_user_id: str
    actor_provider_user_id: str
    created_at: float = field(default_factory=time.time)


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
    normalized = {str(title).strip().lower() for title in actor_titles}
    if "глава клуба" in normalized or "главный вице" in normalized:
        return True
    return actor_level >= 100


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


def _build_points_keyboard(target_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="ℹ️ Что делает команда", callback_data=f"points:help:{target_id}")],
            [InlineKeyboardButton(text="➕ Начислить баллы", callback_data=f"points:add:{target_id}")],
            [InlineKeyboardButton(text="➖ Снять баллы", callback_data=f"points:remove:{target_id}")],
        ]
    )


def _build_tickets_keyboard(target_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="ℹ️ Что делает команда", callback_data=f"tickets:help:{target_id}")],
            [InlineKeyboardButton(text="🎟️ + Обычные", callback_data=f"tickets:add_normal:{target_id}")],
            [InlineKeyboardButton(text="🎟️ - Обычные", callback_data=f"tickets:remove_normal:{target_id}")],
            [InlineKeyboardButton(text="🪙 + Золотые", callback_data=f"tickets:add_gold:{target_id}")],
            [InlineKeyboardButton(text="🪙 - Золотые", callback_data=f"tickets:remove_gold:{target_id}")],
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


@router.message(Command("points"))
async def points_menu_command(message: Message) -> None:
    try:
        if not message.from_user:
            await message.answer("❌ Не удалось определить пользователя Telegram.")
            return
        actor_id = str(message.from_user.id)
        target_id = _parse_target_arg(message)
        if target_id is None:
            await message.answer("❌ Не удалось определить цель. Используйте ответ на сообщение или id пользователя.")
            return

        authority = AuthorityService.resolve_authority("telegram", actor_id)
        if authority.level < 30:
            await message.answer("Недоступно по вашему званию.")
            return

        if str(target_id) != actor_id and not AuthorityService.can_manage_target("telegram", actor_id, "telegram", str(target_id)):
            await message.answer("❌ Нельзя взаимодействовать с пользователем с равным/более высоким званием.")
            return

        profile = AccountsService.get_profile("telegram", str(target_id))
        if not profile:
            await message.answer("❌ Целевой пользователь не зарегистрирован в системе.")
            return

        points, tickets_normal, tickets_gold = _get_score_snapshot(profile["account_id"])
        await message.answer(
            "🎛️ <b>Меню баллов</b>\n"
            f"Пользователь: <a href=\"tg://user?id={target_id}\">{profile['custom_nick']}</a>\n"
            f"Текущий баланс: <b>{points:.2f}</b>\n"
            f"Билеты: 🎟️ {tickets_normal} / 🪙 {tickets_gold}\n\n"
            "Выберите действие. Для любого изменения причина обязательна.",
            parse_mode=ParseMode.HTML,
            reply_markup=_build_points_keyboard(target_id),
        )
    except Exception:
        logger.exception("points menu command failed")
        await message.answer("❌ Ошибка открытия меню баллов.")


@router.message(Command("tickets"))
async def tickets_menu_command(message: Message) -> None:
    try:
        if not message.from_user:
            await message.answer("❌ Не удалось определить пользователя Telegram.")
            return
        actor_id = str(message.from_user.id)
        target_id = _parse_target_arg(message)
        if target_id is None:
            await message.answer("❌ Не удалось определить цель. Используйте ответ на сообщение или id пользователя.")
            return

        authority = AuthorityService.resolve_authority("telegram", actor_id)
        if not _can_manage_tickets(authority.titles, authority.level):
            await message.answer("Недоступно по вашему званию.")
            return

        if str(target_id) != actor_id and not AuthorityService.can_manage_target("telegram", actor_id, "telegram", str(target_id)):
            await message.answer("❌ Нельзя взаимодействовать с пользователем с равным/более высоким званием.")
            return

        profile = AccountsService.get_profile("telegram", str(target_id))
        if not profile:
            await message.answer("❌ Целевой пользователь не зарегистрирован в системе.")
            return

        points, tickets_normal, tickets_gold = _get_score_snapshot(profile["account_id"])
        await message.answer(
            "🎟️ <b>Меню билетов</b>\n"
            f"Пользователь: <a href=\"tg://user?id={target_id}\">{profile['custom_nick']}</a>\n"
            f"Баллы: <b>{points:.2f}</b>\n"
            f"Текущие билеты: 🎟️ <b>{tickets_normal}</b> / 🪙 <b>{tickets_gold}</b>\n\n"
            "Выберите действие. Для любого изменения причина обязательна.",
            parse_mode=ParseMode.HTML,
            reply_markup=_build_tickets_keyboard(target_id),
        )
    except Exception:
        logger.exception("tickets menu command failed")
        await message.answer("❌ Ошибка открытия меню билетов.")


@router.callback_query(F.data.startswith("points:"))
async def points_callback(callback: CallbackQuery) -> None:
    try:
        if not callback.from_user:
            await callback.answer("Ошибка пользователя", show_alert=True)
            return

        _, action, target_raw = str(callback.data).split(":", 2)
        target_id = str(target_raw)
        actor_id = str(callback.from_user.id)

        if action == "help":
            await callback.message.answer(
                "ℹ️ <b>Подробно о меню баллов</b>\n"
                "➕ Начислить — добавляет баллы в общий аккаунт пользователя.\n"
                "➖ Снять — уменьшает баллы (если хватает).\n"
                "Формат ответа после выбора действия: <code>число | причина</code>.\n"
                "Пример: <code>25 | За победу в турнире</code>",
                parse_mode=ParseMode.HTML,
            )
            await callback.answer()
            return

        _PENDING_ACTIONS[callback.from_user.id] = PendingAction(
            domain="points",
            operation=action,
            target_provider_user_id=target_id,
            actor_provider_user_id=actor_id,
        )
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
        if not callback.from_user:
            await callback.answer("Ошибка пользователя", show_alert=True)
            return

        _, action, target_raw = str(callback.data).split(":", 2)
        target_id = str(target_raw)
        actor_id = str(callback.from_user.id)

        if action == "help":
            await callback.message.answer(
                "ℹ️ <b>Подробно о меню билетов</b>\n"
                "🎟️/🪙 кнопки позволяют начислять или списывать обычные и золотые билеты.\n"
                "Формат ответа после выбора действия: <code>количество | причина</code>.\n"
                "Пример: <code>2 | Награда за ивент</code>",
                parse_mode=ParseMode.HTML,
            )
            await callback.answer()
            return

        _PENDING_ACTIONS[callback.from_user.id] = PendingAction(
            domain="tickets",
            operation=action,
            target_provider_user_id=target_id,
            actor_provider_user_id=actor_id,
        )
        await callback.message.answer(
            "Введите данные в формате: <code>количество | причина</code>.\n"
            "Причина обязательна, без неё изменение не выполнится.",
            parse_mode=ParseMode.HTML,
        )
        await callback.answer()
    except Exception:
        logger.exception("tickets callback failed callback_data=%s", callback.data)
        await callback.answer("Ошибка меню билетов", show_alert=True)


@router.message(F.from_user, F.from_user.id.func(has_pending_action))
async def pending_action_handler(message: Message) -> None:
    if not has_pending_action(message.from_user.id):
        logger.warning(
            "pending action handler invoked without pending state user_id=%s chat_id=%s",
            message.from_user.id,
            message.chat.id if message.chat else None,
        )
        return

    pending = _PENDING_ACTIONS.get(message.from_user.id)
    try:
        raw = (message.text or "").strip()
        if "|" not in raw:
            await message.answer("❌ Неверный формат. Используйте: число | причина")
            return
        amount_raw, reason_raw = [part.strip() for part in raw.split("|", 1)]
        if not reason_raw:
            await message.answer("❌ Причина обязательна. Изменение отменено.")
            _PENDING_ACTIONS.pop(message.from_user.id, None)
            return

        if pending.domain == "points":
            amount = float(amount_raw.replace(",", "."))
            if amount <= 0:
                await message.answer("❌ Количество баллов должно быть больше 0.")
                return
            if pending.operation == "add":
                ok = PointsService.add_points_by_identity(
                    "telegram", pending.target_provider_user_id, amount, reason_raw, int(pending.actor_provider_user_id)
                )
                action_text = "начислены"
            else:
                ok = PointsService.remove_points_by_identity(
                    "telegram", pending.target_provider_user_id, amount, reason_raw, int(pending.actor_provider_user_id)
                )
                action_text = "списаны"
            if not ok:
                await message.answer("❌ Не удалось обновить баллы. Проверьте привязку аккаунта.")
            else:
                await message.answer(f"✅ Баллы успешно {action_text}: {amount:.2f}. Причина: {reason_raw}")

        elif pending.domain == "tickets":
            amount = int(amount_raw)
            if amount <= 0:
                await message.answer("❌ Количество билетов должно быть больше 0.")
                return
            mapping = {
                "add_normal": ("normal", True),
                "remove_normal": ("normal", False),
                "add_gold": ("gold", True),
                "remove_gold": ("gold", False),
            }
            ticket_type, is_add = mapping[pending.operation]
            if is_add:
                ok = TicketsService.give_ticket_by_identity(
                    "telegram", pending.target_provider_user_id, ticket_type, amount, reason_raw, int(pending.actor_provider_user_id)
                )
                verb = "начислены"
            else:
                ok = TicketsService.remove_ticket_by_identity(
                    "telegram", pending.target_provider_user_id, ticket_type, amount, reason_raw, int(pending.actor_provider_user_id)
                )
                verb = "списаны"

            if not ok:
                await message.answer("❌ Не удалось обновить билеты. Проверьте привязку аккаунта.")
            else:
                await message.answer(f"✅ Билеты успешно {verb}: {amount}. Причина: {reason_raw}")

        _PENDING_ACTIONS.pop(message.from_user.id, None)
    except ValueError:
        logger.exception("pending action parse failed user_id=%s text=%s", message.from_user.id, message.text)
        await message.answer("❌ Ошибка формата количества. Проверьте число.")
    except Exception:
        logger.exception("pending action failed user_id=%s", message.from_user.id)
        await message.answer("❌ Ошибка выполнения операции.")
        _PENDING_ACTIONS.pop(message.from_user.id, None)
