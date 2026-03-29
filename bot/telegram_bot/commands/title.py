from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from bot.services.title_management_service import TitleManagementService
from bot.telegram_bot.commands.roles_admin import _resolve_telegram_target
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
    created_at: float


_PENDING: dict[int, PendingTitleFlow] = {}


def _build_title_keyboard(actor_id: int, *, mode: str) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(text="⬆️ Повысить", callback_data=f"title:{actor_id}:mode:promote"),
            InlineKeyboardButton(text="⬇️ Понизить", callback_data=f"title:{actor_id}:mode:demote"),
        ]
    ]
    for key, label in TitleManagementService.managed_titles():
        rows.append([InlineKeyboardButton(text=label[:64], callback_data=f"title:{actor_id}:apply:{key}")])
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
    if not TitleManagementService.is_super_admin("telegram", str(actor_id)):
        await message.answer("❌ Повышать или понижать звания могут только суперадмины.")
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

    flow = PendingTitleFlow(
        actor_id=actor_id,
        target_provider=str(target.get("provider") or "telegram"),
        target_user_id=str(target.get("provider_user_id") or ""),
        target_label=str(target.get("label") or target.get("provider_user_id") or "пользователь"),
        mode="promote",
        created_at=time.monotonic(),
    )
    _PENDING[actor_id] = flow

    await message.answer(
        "🛠️ Управление званием\n"
        "1) Выбери режим: повышение или понижение.\n"
        "2) Нажми на звание из списка.\n"
        "Команда /title объединяет оба сценария в одном интерфейсе.",
        reply_markup=_build_title_keyboard(actor_id, mode=flow.mode),
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
            _PENDING[actor_id] = flow
        mode_label = "повышение" if flow.mode == "promote" else "понижение"
        await callback.message.edit_text(
            "🛠️ Управление званием\n"
            f"Пользователь: {flow.target_label}\n"
            f"Режим: {mode_label}\n"
            "Выбери звание кнопкой ниже.",
            reply_markup=_build_title_keyboard(actor_id, mode=flow.mode),
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
    await callback.message.edit_text(
        f"{result.message}\n"
        f"Пользователь: {flow.target_label}\n"
        f"Режим: {mode_label}\n"
        f"Текущие звания: {', '.join(result.titles) if result.titles else 'нет'}\n\n"
        "Можно продолжить в этой же панели: выбери другой режим или другое звание.",
        reply_markup=_build_title_keyboard(actor_id, mode=flow.mode),
    )
    await callback.answer("✅ Готово" if result.ok else "⚠️ Операция не применена", show_alert=not result.ok)
