import logging

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from bot.services import AuthorityService, RoleManagementService

logger = logging.getLogger(__name__)
router = Router()

_ROLES_PAGE_SIZE = 5
_MAX_ROLE_BUTTONS = 8


def _parse_target_arg(message: Message) -> int | None:
    if message.reply_to_message and message.reply_to_message.from_user and not message.reply_to_message.from_user.is_bot:
        return int(message.reply_to_message.from_user.id)
    parts = (message.text or "").strip().split()
    if len(parts) < 2:
        return None
    return int(parts[1]) if parts[1].isdigit() else None


def _normalize_page(page: int, total_items: int, page_size: int) -> int:
    if total_items <= 0:
        return 0
    max_page = max((total_items - 1) // page_size, 0)
    return min(max(page, 0), max_page)


def _build_home_keyboard(actor_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📋 Категории и роли", callback_data=f"roles_admin:{actor_id}:list:0")],
            [InlineKeyboardButton(text="ℹ️ Что делает каждая функция", callback_data=f"roles_admin:{actor_id}:help")],
            [InlineKeyboardButton(text="🔄 Обновить", callback_data=f"roles_admin:{actor_id}:home")],
        ]
    )


def _build_list_keyboard(grouped: list[dict], actor_id: int, page: int) -> InlineKeyboardMarkup:
    safe_page = _normalize_page(page, len(grouped), _ROLES_PAGE_SIZE)
    start = safe_page * _ROLES_PAGE_SIZE
    page_items = grouped[start : start + _ROLES_PAGE_SIZE]

    rows: list[list[InlineKeyboardButton]] = []
    for idx, item in enumerate(page_items):
        role_count = len(item.get("roles", []))
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"📂 {item['category']} ({role_count})"[:64],
                    callback_data=f"roles_admin:{actor_id}:category:{safe_page}:{idx}",
                )
            ]
        )

    nav: list[InlineKeyboardButton] = []
    if safe_page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"roles_admin:{actor_id}:list:{safe_page - 1}"))
    nav.append(InlineKeyboardButton(text="🔄", callback_data=f"roles_admin:{actor_id}:list:{safe_page}"))
    if (safe_page + 1) * _ROLES_PAGE_SIZE < len(grouped):
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"roles_admin:{actor_id}:list:{safe_page + 1}"))
    rows.append(nav)
    rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data=f"roles_admin:{actor_id}:home")])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def _build_category_keyboard(actor_id: int, page: int, category_idx: int, roles_count: int) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text="⬅️ Назад к списку", callback_data=f"roles_admin:{actor_id}:list:{page}")],
        [InlineKeyboardButton(text="🗑 Удалить категорию", callback_data=f"roles_admin:{actor_id}:delete_category:{page}:{category_idx}")],
    ]
    for role_idx in range(min(roles_count, _MAX_ROLE_BUTTONS)):
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"🗑 Роль #{role_idx + 1}",
                    callback_data=f"roles_admin:{actor_id}:delete_role:{page}:{category_idx}:{role_idx}",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data=f"roles_admin:{actor_id}:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _render_home_text() -> str:
    return (
        "🛠 <b>Панель управления ролями</b>\n\n"
        "Все обновления идут в <b>одном сообщении</b> через кнопки.\n\n"
        "Быстрые действия через команду (если нужно точное имя):\n"
        "<code>/roles_admin category_create &lt;name&gt; [position]</code>\n"
        "<code>/roles_admin role_create &lt;name&gt; &lt;category&gt; [discord_role_id]</code>\n"
        "<code>/roles_admin role_move &lt;name&gt; &lt;category&gt; [position]</code>\n"
        "<code>/roles_admin user_roles [reply|telegram_id]</code>\n"
        "<code>/roles_admin user_grant &lt;telegram_id&gt; &lt;role_name&gt;</code>\n"
        "<code>/roles_admin user_revoke &lt;telegram_id&gt; &lt;role_name&gt;</code>\n"
        "\nНужны пояснения по функциям? Нажми кнопку <b>ℹ️ Что делает каждая функция</b>."
    )




def _render_help_text() -> str:
    return (
        "ℹ️ <b>Что делает /roles_admin</b>\n\n"
        "<b>Категории</b>\n"
        "• <code>category_create &lt;name&gt; [position]</code> — создать/обновить категорию.\n"
        "• <code>category_delete &lt;name&gt;</code> — удалить категорию (роли уйдут в 'Без категории').\n\n"
        "<b>Роли</b>\n"
        "• <code>role_create &lt;name&gt; &lt;category&gt; [discord_role_id]</code> — добавить роль в каталог.\n"
        "• <code>role_move &lt;name&gt; &lt;category&gt; [position]</code> — переместить роль в другую категорию.\n"
        "• <code>role_delete &lt;name&gt;</code> — удалить роль из каталога.\n\n"
        "<b>Роли пользователей</b>\n"
        "• <code>user_roles [reply|telegram_id]</code> — показать роли пользователя.\n"
        "• <code>user_grant &lt;telegram_id&gt; &lt;role_name&gt;</code> — выдать роль в БД.\n"
        "• <code>user_revoke &lt;telegram_id&gt; &lt;role_name&gt;</code> — снять роль в БД.\n\n"
        "<b>Кнопки в панели</b>\n"
        "• 'Категории и роли' — просмотр списка и переход по категориям.\n"
        "• Внутри категории доступны кнопки удаления категории/ролей.\n"
        "• Все экраны обновляются в одном сообщении без спама."
    )

def _render_list_text(grouped: list[dict], page: int) -> str:
    safe_page = _normalize_page(page, len(grouped), _ROLES_PAGE_SIZE)
    start = safe_page * _ROLES_PAGE_SIZE
    page_items = grouped[start : start + _ROLES_PAGE_SIZE]
    total_pages = max((len(grouped) - 1) // _ROLES_PAGE_SIZE + 1, 1)

    lines = [f"🧩 <b>Роли по категориям</b> (стр. {safe_page + 1}/{total_pages})"]
    if not page_items:
        lines.append("\n📭 Категории отсутствуют.")
    for item in page_items:
        lines.append(f"\n<b>{item['category']}</b>")
        roles = item.get("roles", [])
        if not roles:
            lines.append("• —")
            continue
        for role in roles:
            suffix = f" (Discord ID: {role['discord_role_id']})" if role.get("discord_role_id") else ""
            lines.append(f"• {role['name']}{suffix}")

    lines.append("\nНажми на категорию ниже для действий (удаление категории/ролей).")
    return "\n".join(lines)


def _resolve_category(grouped: list[dict], page: int, category_idx: int) -> dict | None:
    safe_page = _normalize_page(page, len(grouped), _ROLES_PAGE_SIZE)
    start = safe_page * _ROLES_PAGE_SIZE
    page_items = grouped[start : start + _ROLES_PAGE_SIZE]
    if category_idx < 0 or category_idx >= len(page_items):
        return None
    return page_items[category_idx]


async def _ensure_roles_admin(message: Message) -> bool:
    if not message.from_user:
        await message.answer("❌ Не удалось определить пользователя Telegram.")
        return False
    authority = AuthorityService.resolve_authority("telegram", str(message.from_user.id))
    if authority.level < 80:
        await message.answer("❌ Недостаточно полномочий для управления ролями.")
        return False
    return True


@router.message(Command("roles_admin"))
async def roles_admin_command(message: Message) -> None:
    try:
        if not await _ensure_roles_admin(message):
            return

        text = (message.text or "").strip()
        parts = text.split()
        if len(parts) < 2:
            if not message.from_user:
                await message.answer("❌ Не удалось определить пользователя Telegram.")
                return
            await message.answer(_render_home_text(), parse_mode="HTML", reply_markup=_build_home_keyboard(message.from_user.id))
            return

        subcommand = parts[1].lower()
        args = parts[2:]

        if subcommand == "help":
            await message.answer(_render_help_text(), parse_mode="HTML")
            return

        if subcommand == "list":
            grouped = RoleManagementService.list_roles_grouped()
            if not grouped:
                await message.answer("📭 Список ролей пуст или БД недоступна.")
                return
            await message.answer(_render_list_text(grouped, 0), parse_mode="HTML")
            return

        if subcommand == "category_create" and len(args) >= 1:
            position = int(args[-1]) if len(args) > 1 and args[-1].isdigit() else 0
            name = " ".join(args[:-1] if len(args) > 1 and args[-1].isdigit() else args)
            ok = RoleManagementService.create_category(name, position)
            await message.answer("✅ Категория сохранена." if ok else "❌ Не удалось создать категорию (смотри логи).")
            return

        if subcommand == "category_delete" and len(args) >= 1:
            ok = RoleManagementService.delete_category(" ".join(args))
            await message.answer("✅ Категория удалена." if ok else "❌ Не удалось удалить категорию (смотри логи).")
            return

        if subcommand == "role_create" and len(args) >= 2:
            role_name = args[0]
            category = args[1]
            discord_role_id = args[2] if len(args) >= 3 else None
            ok = RoleManagementService.create_role(role_name, category, discord_role_id=discord_role_id)
            await message.answer("✅ Роль создана." if ok else "❌ Не удалось создать роль (смотри логи).")
            return

        if subcommand == "role_delete" and len(args) >= 1:
            ok = RoleManagementService.delete_role(args[0])
            await message.answer("✅ Роль удалена." if ok else "❌ Не удалось удалить роль (смотри логи).")
            return

        if subcommand == "role_move" and len(args) >= 2:
            position = int(args[2]) if len(args) >= 3 and args[2].isdigit() else 0
            ok = RoleManagementService.move_role(args[0], args[1], position)
            await message.answer("✅ Роль перемещена." if ok else "❌ Не удалось переместить роль (смотри логи).")
            return

        if subcommand == "user_roles":
            target_id = _parse_target_arg(message)
            if target_id is None:
                await message.answer("❌ Укажите telegram_id вторым аргументом или сделайте reply на сообщение пользователя.")
                return
            roles = RoleManagementService.get_user_roles("telegram", str(target_id))
            if not roles:
                await message.answer("📭 У пользователя нет ролей.")
                return
            lines = [f"🧾 Роли пользователя {target_id}:"]
            for role in roles:
                lines.append(f"• {role['name']} ({role.get('category') or 'Без категории'})")
            await message.answer("\n".join(lines))
            return

        if subcommand in {"user_grant", "user_revoke"} and len(args) >= 2 and args[0].isdigit():
            target_id = args[0]
            role_name = " ".join(args[1:])
            if subcommand == "user_grant":
                role_info = RoleManagementService.get_role(role_name)
                category = role_info.get("category_name") if role_info else None
                ok = RoleManagementService.assign_user_role("telegram", target_id, role_name, category=category)
                await message.answer("✅ Роль выдана в БД." if ok else "❌ Не удалось выдать роль (смотри логи).")
            else:
                ok = RoleManagementService.revoke_user_role("telegram", target_id, role_name)
                await message.answer("✅ Роль снята в БД." if ok else "❌ Не удалось снять роль (смотри логи).")
            return

        await message.answer("❌ Неверная команда или аргументы. Напишите /roles_admin для панели управления.")
    except Exception:
        logger.exception(
            "roles_admin command failed actor_id=%s text=%s",
            message.from_user.id if message.from_user else None,
            message.text,
        )
        await message.answer("❌ Ошибка выполнения команды ролей.")


@router.callback_query(F.data.startswith("roles_admin:"))
async def roles_admin_callback(callback: CallbackQuery) -> None:
    try:
        if not callback.data or not callback.from_user or not callback.message:
            await callback.answer("Некорректный callback", show_alert=True)
            return

        parts = callback.data.split(":")
        if len(parts) < 3:
            await callback.answer("Некорректный callback", show_alert=True)
            return

        owner_id = int(parts[1]) if parts[1].isdigit() else 0
        if owner_id != callback.from_user.id:
            logger.warning(
                "roles_admin callback denied foreign actor callback_data=%s actor_id=%s owner_id=%s",
                callback.data,
                callback.from_user.id,
                owner_id,
            )
            await callback.answer("Эта панель открыта другим администратором.", show_alert=True)
            return

        action = parts[2]
        grouped = RoleManagementService.list_roles_grouped() or []

        if action == "help":
            await callback.message.edit_text(
                _render_help_text(),
                parse_mode="HTML",
                reply_markup=_build_home_keyboard(owner_id),
            )
            await callback.answer()
            return

        if action == "home":
            await callback.message.edit_text(
                _render_home_text(),
                parse_mode="HTML",
                reply_markup=_build_home_keyboard(owner_id),
            )
            await callback.answer()
            return

        if action == "list":
            page = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
            await callback.message.edit_text(
                _render_list_text(grouped, page),
                parse_mode="HTML",
                reply_markup=_build_list_keyboard(grouped, owner_id, page),
            )
            await callback.answer()
            return

        if action == "category":
            page = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
            category_idx = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else -1
            category_item = _resolve_category(grouped, page, category_idx)
            if not category_item:
                await callback.answer("Категория не найдена, обновите список.", show_alert=True)
                return

            roles = category_item.get("roles", [])
            lines = [f"📂 <b>{category_item['category']}</b>"]
            if not roles:
                lines.append("\n• В категории нет ролей")
            else:
                for idx, role in enumerate(roles[:_MAX_ROLE_BUTTONS], start=1):
                    suffix = f" (Discord ID: {role['discord_role_id']})" if role.get("discord_role_id") else ""
                    lines.append(f"\n{idx}. {role['name']}{suffix}")
                if len(roles) > _MAX_ROLE_BUTTONS:
                    lines.append(f"\n… и ещё {len(roles) - _MAX_ROLE_BUTTONS} ролей (удаляй через /roles_admin role_delete)")

            await callback.message.edit_text(
                "\n".join(lines),
                parse_mode="HTML",
                reply_markup=_build_category_keyboard(owner_id, page, category_idx, len(roles)),
            )
            await callback.answer()
            return

        if action == "delete_category":
            page = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
            category_idx = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else -1
            category_item = _resolve_category(grouped, page, category_idx)
            if not category_item:
                await callback.answer("Категория не найдена, обновите список.", show_alert=True)
                return

            ok = RoleManagementService.delete_category(category_item["category"])
            if not ok:
                await callback.answer("Не удалось удалить категорию (смотри логи).", show_alert=True)
                return

            grouped_after = RoleManagementService.list_roles_grouped() or []
            await callback.message.edit_text(
                _render_list_text(grouped_after, page),
                parse_mode="HTML",
                reply_markup=_build_list_keyboard(grouped_after, owner_id, page),
            )
            await callback.answer("Категория удалена")
            return

        if action == "delete_role":
            page = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
            category_idx = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else -1
            role_idx = int(parts[5]) if len(parts) > 5 and parts[5].isdigit() else -1
            category_item = _resolve_category(grouped, page, category_idx)
            roles = category_item.get("roles", []) if category_item else []
            if not category_item or role_idx < 0 or role_idx >= len(roles):
                await callback.answer("Роль не найдена, обновите список.", show_alert=True)
                return

            role_name = roles[role_idx]["name"]
            ok = RoleManagementService.delete_role(role_name)
            if not ok:
                await callback.answer("Не удалось удалить роль (смотри логи).", show_alert=True)
                return

            grouped_after = RoleManagementService.list_roles_grouped() or []
            refreshed_item = _resolve_category(grouped_after, page, category_idx)
            if refreshed_item:
                refreshed_roles = refreshed_item.get("roles", [])
                lines = [f"📂 <b>{refreshed_item['category']}</b>"]
                if not refreshed_roles:
                    lines.append("\n• В категории нет ролей")
                else:
                    for idx, role in enumerate(refreshed_roles[:_MAX_ROLE_BUTTONS], start=1):
                        suffix = f" (Discord ID: {role['discord_role_id']})" if role.get("discord_role_id") else ""
                        lines.append(f"\n{idx}. {role['name']}{suffix}")
                await callback.message.edit_text(
                    "\n".join(lines),
                    parse_mode="HTML",
                    reply_markup=_build_category_keyboard(owner_id, page, category_idx, len(refreshed_roles)),
                )
            else:
                await callback.message.edit_text(
                    _render_list_text(grouped_after, page),
                    parse_mode="HTML",
                    reply_markup=_build_list_keyboard(grouped_after, owner_id, page),
                )
            await callback.answer(f"Роль {role_name} удалена")
            return

        await callback.answer("Неизвестное действие", show_alert=True)
    except Exception:
        logger.exception(
            "roles_admin callback failed callback_data=%s actor_id=%s",
            callback.data,
            callback.from_user.id if callback.from_user else None,
        )
        await callback.answer("Ошибка в панели ролей (смотри логи).", show_alert=True)
