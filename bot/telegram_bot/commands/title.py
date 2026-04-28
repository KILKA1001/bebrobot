"""
Назначение: модуль "title" реализует продуктовый контур в зоне Telegram.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Telegram.
Пользовательский вход: команда /title и связанный пользовательский сценарий.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from aiogram import F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from bot.services.title_management_service import TitleManagementService
from bot.telegram_bot.commands.roles_admin import _resolve_telegram_target, _user_without_account_message
from bot.telegram_bot.identity import persist_telegram_identity_from_user

logger = logging.getLogger(__name__)
router = Router()

_PENDING_TTL_SECONDS = 300


@dataclass
class PendingTitleFlow:
    actor_id: int
    target_provider: str
    target_user_id: str
    target_label: str
    mode: str
    target_titles: tuple[str, ...]
    created_at: float


_PENDING: dict[int, PendingTitleFlow] = {}


async def _safe_edit_title_panel(
    callback: CallbackQuery,
    *,
    actor_id: int,
    flow: PendingTitleFlow,
    text: str,
) -> None:
    try:
        await callback.message.edit_text(
            text,
            reply_markup=_build_title_keyboard(actor_id, mode=flow.mode, target_titles=flow.target_titles),
        )
    except TelegramBadRequest as error:
        if "message is not modified" in str(error).lower():
            logger.warning(
                "telegram title panel edit skipped unchanged actor_id=%s target_provider=%s target_user_id=%s mode=%s",
                actor_id,
                flow.target_provider,
                flow.target_user_id,
                flow.mode,
            )
            return
        raise


def _build_title_keyboard(
    actor_id: int,
    *,
    mode: str,
    target_titles: tuple[str, ...] = tuple(),
) -> InlineKeyboardMarkup:
    selected_titles = {str(item or "").strip().casefold() for item in target_titles if str(item or "").strip()}
    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(text="⬆️ Повысить", callback_data=f"title:{actor_id}:mode:promote"),
            InlineKeyboardButton(text="⬇️ Понизить", callback_data=f"title:{actor_id}:mode:demote"),
        ]
    ]
    for key, label in TitleManagementService.managed_titles():
        marker = "✅ " if key.casefold() in selected_titles else ""
        rows.append([InlineKeyboardButton(text=f"{marker}{label}"[:64], callback_data=f"title:{actor_id}:apply:{key}")])
    rows.append([InlineKeyboardButton(text="🔄 Обновить", callback_data=f"title:{actor_id}:refresh")])
    suffix = "повышение" if mode == "promote" else "понижение"
    rows.append([InlineKeyboardButton(text=f"Текущий режим: {suffix}", callback_data=f"title:{actor_id}:noop")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _expired(flow: PendingTitleFlow) -> bool:
    return time.monotonic() - flow.created_at > _PENDING_TTL_SECONDS


@router.message(Command("title"))
async def title_command(message: Message) -> None:
    if not message.from_user:
        return
    persist_telegram_identity_from_user(message.from_user)
    actor_id = int(message.from_user.id)
    logger.info(
        "ux_screen_open event=ux_screen_open screen=title_admin provider=telegram actor_user_id=%s chat_id=%s",
        actor_id,
        message.chat.id,
    )
    if not TitleManagementService.is_super_admin("telegram", str(actor_id)):
        await message.answer(
            "❌ Команда доступна только суперадминам.\n"
            "Что делать сейчас: попросите суперадмина выполнить изменение.\n"
            "Что будет дальше: после выдачи прав вы сможете управлять званиями через /title."
        )
        return

    command_text = str(message.text or "").strip()
    parts = command_text.split(maxsplit=1)
    raw_target = parts[1].strip() if len(parts) > 1 else ""
    if not raw_target and message.reply_to_message and message.reply_to_message.from_user:
        target = _resolve_telegram_target(
            actor_id=actor_id,
            raw_target=None,
            reply_user=message.reply_to_message.from_user,
            operation="title",
            source="group" if message.chat.type != "private" else "private",
        )
    else:
        target = _resolve_telegram_target(
            actor_id=actor_id,
            raw_target=raw_target,
            reply_user=None,
            operation="title",
            source="group" if message.chat.type != "private" else "private",
        )

    if not target:
        await message.answer(
            "❌ Укажи пользователя: /title @username (или reply на сообщение цели).\n"
            "После запуска: выбери режим (повысить/понизить), затем нужное звание кнопкой."
        )
        return
    if target.get("error"):
        await message.answer(str(target.get("message") or "❌ Не удалось определить пользователя."))
        return
    if not str(target.get("account_id") or "").strip():
        logger.warning(
            "telegram title command blocked target without linked account actor_id=%s target_provider=%s target_user_id=%s source=%s",
            actor_id,
            target.get("provider"),
            target.get("provider_user_id"),
            "group" if message.chat.type != "private" else "private",
        )
        await message.answer(
            _user_without_account_message()
            + "\n\n"
            + "Что нужно сделать пользователю:\n"
            + "1) Выполнить /register в личных сообщениях боту.\n"
            + "2) Завершить привязку аккаунта.\n"
            + "3) После этого повторить команду /title."
        )
        return

    flow = PendingTitleFlow(
        actor_id=actor_id,
        target_provider=str(target.get("provider") or "telegram"),
        target_user_id=str(target.get("provider_user_id") or ""),
        target_label=str(target.get("label") or target.get("provider_user_id") or "пользователь"),
        mode="promote",
        target_titles=TitleManagementService.get_target_titles(
            str(target.get("provider") or "telegram"),
            str(target.get("provider_user_id") or ""),
        ),
        created_at=time.monotonic(),
    )
    _PENDING[actor_id] = flow

    await message.answer(
        "🛠️ Управление званием\n"
        "1) Выбери режим: повышение или понижение.\n"
        "2) Нажми на звание из списка.\n"
        "✅ возле звания = у пользователя оно уже есть.\n"
        "Команда /title объединяет оба сценария в одном интерфейсе.",
        reply_markup=_build_title_keyboard(actor_id, mode=flow.mode, target_titles=flow.target_titles),
    )


@router.callback_query(F.data.startswith("title:"))
async def title_callbacks(callback: CallbackQuery) -> None:
    if not callback.from_user or not callback.message:
        return
    actor_id = int(callback.from_user.id)
    raw = str(callback.data or "")
    parts = raw.split(":", maxsplit=3)
    if len(parts) < 3:
        await callback.answer("❌ Некорректные данные кнопки.", show_alert=True)
        return
    owner_id = int(parts[1]) if parts[1].isdigit() else -1
    action = parts[2]
    value = parts[3] if len(parts) > 3 else ""

    if owner_id != actor_id:
        await callback.answer("❌ Эти кнопки открыты для другого администратора.", show_alert=True)
        return

    flow = _PENDING.get(actor_id)
    if not flow or _expired(flow):
        _PENDING.pop(actor_id, None)
        await callback.answer("⌛ Панель устарела. Запусти /title снова.", show_alert=True)
        return

    if action == "noop":
        await callback.answer()
        return

    if action in {"mode", "refresh"}:
        if action == "mode" and value in {"promote", "demote"}:
            flow.mode = value
        flow.target_titles = TitleManagementService.get_target_titles(flow.target_provider, flow.target_user_id)
        _PENDING[actor_id] = flow
        mode_label = "повышение" if flow.mode == "promote" else "понижение"
        await _safe_edit_title_panel(
            callback,
            actor_id=actor_id,
            flow=flow,
            text=(
                "🛠️ Управление званием\n"
            f"Пользователь: {flow.target_label}\n"
            f"Режим: {mode_label}\n"
            "Выбери звание кнопкой ниже.\n"
                "✅ возле звания = у пользователя оно уже есть."
            ),
        )
        await callback.answer("✅ Режим обновлён.")
        return

    if action != "apply":
        await callback.answer("❌ Неизвестное действие.", show_alert=True)
        return

    try:
        result = TitleManagementService.apply_title_change(
            actor_provider="telegram",
            actor_user_id=str(actor_id),
            target_provider=flow.target_provider,
            target_user_id=flow.target_user_id,
            title_key=value,
            mode=flow.mode,
            source="telegram_title_command",
        )
    except Exception:
        logger.exception(
            "telegram title command failed actor_id=%s target_provider=%s target_user_id=%s mode=%s title=%s",
            actor_id,
            flow.target_provider,
            flow.target_user_id,
            flow.mode,
            value,
        )
        await callback.answer("❌ Не удалось изменить звание. Подробности в консоли.", show_alert=True)
        return

    mode_label = "повышение" if flow.mode == "promote" else "понижение"
    flow.target_titles = tuple(result.titles)
    _PENDING[actor_id] = flow
    await _safe_edit_title_panel(
        callback,
        actor_id=actor_id,
        flow=flow,
        text=(
            f"{result.message}\n"
        f"Пользователь: {flow.target_label}\n"
        f"Режим: {mode_label}\n"
        f"Текущие звания: {', '.join(result.titles) if result.titles else 'нет'}\n\n"
            "Можно продолжить в этой же панели: выбери другой режим или другое звание."
        ),
    )
    await callback.answer("✅ Готово" if result.ok else "⚠️ Операция не применена", show_alert=not result.ok)
