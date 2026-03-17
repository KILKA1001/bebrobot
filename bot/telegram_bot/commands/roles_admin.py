import logging
import time
from dataclasses import dataclass

from aiogram import F, Router
from aiogram.exceptions import TelegramConflictError, TelegramNetworkError
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from bot.data import db
from bot.services import AccountsService, AuthorityService, RoleManagementService

logger = logging.getLogger(__name__)
router = Router()

_ROLES_PAGE_SIZE = 5
_MAX_ROLE_BUTTONS = 8
_PENDING_TTL_SECONDS = 300


@dataclass
class PendingRolesAdminAction:
    operation: str
    created_at: float
    payload: dict[str, str] | None = None


_PENDING_ACTIONS: dict[int, PendingRolesAdminAction] = {}


async def _sync_linked_discord_role(provider_user_id: str, role_name: str, *, revoke: bool) -> None:
    try:
        account_id = AccountsService.resolve_account_id("telegram", str(provider_user_id))
        if not account_id or not db.supabase:
            return
        role_info = RoleManagementService.get_role(role_name)
        discord_role_id = str((role_info or {}).get("discord_role_id") or "").strip()
        if not discord_role_id:
            return
        identity_resp = (
            db.supabase.table("account_identities")
            .select("provider_user_id")
            .eq("account_id", str(account_id))
            .eq("provider", "discord")
            .limit(1)
            .execute()
        )
        if not identity_resp.data:
            return
        discord_user_id = int(identity_resp.data[0].get("provider_user_id") or 0)
        if not discord_user_id:
            return

        from bot.commands.base import bot as discord_bot

        if not getattr(discord_bot, "guilds", None):
            logger.warning("discord sync skipped: bot guilds unavailable target_telegram_id=%s", provider_user_id)
            return
        for guild in discord_bot.guilds:
            member = guild.get_member(discord_user_id)
            guild_role = guild.get_role(int(discord_role_id))
            if not member or not guild_role:
                continue
            try:
                if revoke:
                    await member.remove_roles(guild_role, reason=f"telegram roles_admin revoke by {provider_user_id}")
                else:
                    await member.add_roles(guild_role, reason=f"telegram roles_admin grant by {provider_user_id}")
            except Exception:
                logger.exception(
                    "telegram roles_admin discord sync failed discord_user_id=%s role_id=%s revoke=%s guild_id=%s",
                    discord_user_id,
                    discord_role_id,
                    revoke,
                    guild.id,
                )
            return
        logger.warning(
            "telegram roles_admin discord sync target not found discord_user_id=%s role_id=%s revoke=%s",
            discord_user_id,
            discord_role_id,
            revoke,
        )
    except Exception:
        logger.exception("telegram roles_admin discord sync crashed target_telegram_id=%s role=%s revoke=%s", provider_user_id, role_name, revoke)



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
            [InlineKeyboardButton(text="⚡ Действия кнопками", callback_data=f"roles_admin:{actor_id}:actions")],
            [InlineKeyboardButton(text="ℹ️ Что делает каждая функция", callback_data=f"roles_admin:{actor_id}:help")],
            [InlineKeyboardButton(text="🔄 Обновить", callback_data=f"roles_admin:{actor_id}:home")],
        ]
    )


def _build_actions_keyboard(actor_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🗂 Создать категорию", callback_data=f"roles_admin:{actor_id}:start:category_create")],
            [InlineKeyboardButton(text="↕️ Порядок категории", callback_data=f"roles_admin:{actor_id}:start:category_order")],
            [InlineKeyboardButton(text="🗑 Удалить категорию", callback_data=f"roles_admin:{actor_id}:start:category_delete")],
            [InlineKeyboardButton(text="➕ Создать роль", callback_data=f"roles_admin:{actor_id}:start:role_create")],
            [InlineKeyboardButton(text="🚚 Переместить роль", callback_data=f"roles_admin:{actor_id}:start:role_move")],
            [InlineKeyboardButton(text="🔢 Порядок роли", callback_data=f"roles_admin:{actor_id}:start:role_order")],
            [InlineKeyboardButton(text="🗑 Удалить роль", callback_data=f"roles_admin:{actor_id}:start:role_delete")],
            [InlineKeyboardButton(text="🧾 Роли пользователя", callback_data=f"roles_admin:{actor_id}:start:user_roles")],
            [InlineKeyboardButton(text="✅ Выдать роль", callback_data=f"roles_admin:{actor_id}:start:user_grant")],
            [InlineKeyboardButton(text="❌ Снять роль", callback_data=f"roles_admin:{actor_id}:start:user_revoke")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data=f"roles_admin:{actor_id}:home")],
        ]
    )


def _render_actions_text() -> str:
    return (
        "⚡ <b>Действия кнопками</b>\n\n"
        "Нажми кнопку, затем отправь параметры <b>в следующем сообщении</b>.\n"
        "Разделитель параметров: <code>|</code>.\n"
        "Для отмены ввода отправь: <code>отмена</code>."
    )


def _operation_hint(operation: str) -> str:
    hints = {
        "category_create": "Отправь: <code>Название категории | position(опционально)</code>",
        "category_order": "Отправь: <code>Название категории | position</code>",
        "category_delete": "Отправь: <code>Название категории</code>",
        "role_create": "Отправь: <code>Название роли | Категория | discord_role_id(опц) | position(опц)</code>",
        "role_move": "Отправь: <code>Название роли | Категория | position(опц)</code>",
        "role_order": "Отправь: <code>Название роли | Категория | position</code>",
        "role_delete": "Отправь: <code>Название роли</code>",
        "user_roles": "Отправь: <code>telegram_id</code> или сделай reply на сообщение пользователя",
        "user_grant": "Отправь: <code>telegram_id | Название роли</code>",
        "user_revoke": "Отправь: <code>telegram_id | Название роли</code>",
    }
    return hints.get(operation, "Неизвестная операция")


def _parse_pipe_args(raw: str) -> list[str]:
    return [part.strip() for part in raw.split("|") if part.strip()]


def _is_pending_action_expired(pending: PendingRolesAdminAction) -> bool:
    return (time.time() - pending.created_at) > _PENDING_TTL_SECONDS


def has_pending_roles_admin_action(telegram_user_id: int | None) -> bool:
    if not telegram_user_id:
        return False
    pending = _PENDING_ACTIONS.get(telegram_user_id)
    if not pending:
        return False
    if _is_pending_action_expired(pending):
        logger.info(
            "roles_admin pending action expired user_id=%s operation=%s ttl_seconds=%s",
            telegram_user_id,
            pending.operation,
            _PENDING_TTL_SECONDS,
        )
        _PENDING_ACTIONS.pop(telegram_user_id, None)
        return False
    return True




def _flatten_roles(grouped: list[dict]) -> list[dict[str, str]]:
    flattened: list[dict[str, str]] = []
    for item in grouped:
        category = str(item.get("category") or "Без категории")
        for role in item.get("roles", []):
            role_name = str(role.get("name") or "").strip()
            if role_name:
                flattened.append({"role": role_name, "category": category})
    return flattened


def _build_pick_category_keyboard(grouped: list[dict], actor_id: int, operation: str) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for idx, item in enumerate(grouped[:20]):
        rows.append([
            InlineKeyboardButton(
                text=f"📂 {item['category']}"[:64],
                callback_data=f"roles_admin:{actor_id}:pick_category:{operation}:{idx}",
            )
        ])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"roles_admin:{actor_id}:actions")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _build_pick_role_keyboard(grouped: list[dict], actor_id: int, operation: str, page: int = 0) -> InlineKeyboardMarkup:
    flattened = _flatten_roles(grouped)
    page_size = 8
    safe_page = _normalize_page(page, len(flattened), page_size)
    start = safe_page * page_size
    items = flattened[start : start + page_size]
    rows: list[list[InlineKeyboardButton]] = []
    for idx, item in enumerate(items):
        rows.append([
            InlineKeyboardButton(
                text=f"🎭 {item['role']} [{item['category']}]"[:64],
                callback_data=f"roles_admin:{actor_id}:pick_role:{operation}:{safe_page}:{idx}",
            )
        ])
    nav: list[InlineKeyboardButton] = []
    if safe_page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"roles_admin:{actor_id}:pick_role_page:{operation}:{safe_page - 1}"))
    nav.append(InlineKeyboardButton(text="🔄", callback_data=f"roles_admin:{actor_id}:pick_role_page:{operation}:{safe_page}"))
    if (safe_page + 1) * page_size < len(flattened):
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"roles_admin:{actor_id}:pick_role_page:{operation}:{safe_page + 1}"))
    rows.append(nav)
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"roles_admin:{actor_id}:actions")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _build_position_choice_keyboard(actor_id: int, operation: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⏫ В начало", callback_data=f"roles_admin:{actor_id}:set_position:{operation}:start")],
            [InlineKeyboardButton(text="⏬ В конец", callback_data=f"roles_admin:{actor_id}:set_position:{operation}:end")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"roles_admin:{actor_id}:actions")],
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


def _build_category_keyboard(
    actor_id: int,
    page: int,
    category_idx: int,
    roles_count: int,
    *,
    can_manage_categories: bool,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text="⬅️ Назад к списку", callback_data=f"roles_admin:{actor_id}:list:{page}")],
    ]
    if can_manage_categories:
        rows.append(
            [InlineKeyboardButton(text="🗑 Удалить категорию", callback_data=f"roles_admin:{actor_id}:delete_category:{page}:{category_idx}")]
        )
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
        "Управление через <b>кнопки</b> в разделе <b>⚡ Действия кнопками</b>.\n"
        "Командные подкоманды оставлены как резерв:\n"
        "<code>/roles_admin category_create &lt;name&gt; [position]</code>\n"
        "<code>/roles_admin category_order &lt;name&gt; &lt;position&gt;</code>\n"
        "<code>/roles_admin role_create &lt;name&gt; &lt;category&gt; [discord_role_id] [position]</code>\n"
        "<code>/roles_admin role_move &lt;name&gt; &lt;category&gt; [position]</code>\n"
        "<code>/roles_admin role_order &lt;role_name&gt; &lt;category&gt; &lt;position&gt;</code>\n"
        "<code>/roles_admin user_roles [reply|telegram_id]</code>\n"
        "<code>/roles_admin user_grant &lt;telegram_id&gt; &lt;role_name&gt;</code>\n"
        "<code>/roles_admin user_revoke &lt;telegram_id&gt; &lt;role_name&gt;</code>\n"
        "\nНужны пояснения по функциям? Нажми кнопку <b>ℹ️ Что делает каждая функция</b>."
    )




def _render_help_text() -> str:
    return (
        "ℹ️ <b>Что делает /roles_admin</b>\n\n"
        "<b>Категории</b>\n"
        "• <code>category_create &lt;name&gt; [position]</code> — создать/обновить категорию (<b>только Глава клуба/Главный вице</b>).\n"
        "• <code>category_order &lt;name&gt; &lt;position&gt;</code> — выставить порядок категории (<b>только Глава клуба/Главный вице</b>).\n"
        "• <code>category_delete &lt;name&gt;</code> — удалить категорию (роли уйдут в 'Без категории', <b>только Глава клуба/Главный вице</b>).\n\n"
        "<b>Роли</b>\n"
        "• <code>role_create &lt;name&gt; &lt;category&gt; [discord_role_id] [position]</code> — добавить роль в каталог.\n"
        "• <code>role_move &lt;name&gt; &lt;category&gt; [position]</code> — переместить роль в другую категорию.\n"
        "• <code>role_order &lt;role_name&gt; &lt;category&gt; &lt;position&gt;</code> — выставить очередь роли в категории.\n"
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


async def _safe_callback_answer(
    callback: CallbackQuery,
    text: str | None = None,
    *,
    show_alert: bool = False,
) -> None:
    try:
        await callback.answer(text, show_alert=show_alert)
    except Exception:
        logger.exception(
            "roles_admin callback answer failed callback_data=%s actor_id=%s text=%s show_alert=%s",
            callback.data,
            callback.from_user.id if callback.from_user else None,
            text,
            show_alert,
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
        lines.append(f"\n• <i>{item['category']}</i>")
        roles = item.get("roles", [])
        if not roles:
            lines.append("  • —")
            continue
        for role in roles:
            suffix = f" (Discord ID: {role['discord_role_id']})" if role.get("discord_role_id") else ""
            lines.append(f"  • {role['name']}{suffix}")

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


def _can_manage_categories(provider: str, provider_user_id: str) -> bool:
    return AuthorityService.can_manage_role_categories(provider, provider_user_id)


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


        if subcommand == "list":
            grouped = RoleManagementService.list_roles_grouped()
            if not grouped:
                await message.answer("📭 Список ролей пуст или БД недоступна.")
                return
            await message.answer(_render_list_text(grouped, 0), parse_mode="HTML")
            return

        if subcommand == "category_create" and len(args) >= 1:
            if not message.from_user or not _can_manage_categories("telegram", str(message.from_user.id)):
                await message.answer("❌ Категориями может управлять только Глава клуба или Главный вице.")
                return
            position = int(args[-1]) if len(args) > 1 and args[-1].isdigit() else 0
            name = " ".join(args[:-1] if len(args) > 1 and args[-1].isdigit() else args)
            ok = RoleManagementService.create_category(name, position)
            await message.answer("✅ Категория сохранена." if ok else "❌ Не удалось создать категорию (смотри логи).")
            return

        if subcommand == "category_order" and len(args) >= 2:
            if not message.from_user or not _can_manage_categories("telegram", str(message.from_user.id)):
                await message.answer("❌ Категориями может управлять только Глава клуба или Главный вице.")
                return
            position_raw = args[-1]
            name = " ".join(args[:-1]).strip()
            if not position_raw.lstrip("-").isdigit() or not name:
                await message.answer("❌ Формат: /roles_admin category_order <name> <position>")
                return
            ok = RoleManagementService.create_category(name, int(position_raw))
            await message.answer("✅ Порядок категории обновлён." if ok else "❌ Не удалось обновить порядок категории (смотри логи).")
            return

        if subcommand == "category_delete" and len(args) >= 1:
            if not message.from_user or not _can_manage_categories("telegram", str(message.from_user.id)):
                await message.answer("❌ Категориями может управлять только Глава клуба или Главный вице.")
                return
            ok = RoleManagementService.delete_category(" ".join(args))
            await message.answer("✅ Категория удалена." if ok else "❌ Не удалось удалить категорию (смотри логи).")
            return

        if subcommand == "role_create" and len(args) >= 2:
            role_name = args[0]
            category = args[1]
            discord_role_id = args[2] if len(args) >= 3 else None
            position = int(args[3]) if len(args) >= 4 and args[3].lstrip("-").isdigit() else 0
            ok = RoleManagementService.create_role(
                role_name,
                category,
                discord_role_id=discord_role_id,
                position=position,
            )
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

        if subcommand == "role_order" and len(args) >= 3:
            role_name = args[0]
            category = args[1]
            position_raw = args[2]
            if not position_raw.lstrip("-").isdigit():
                await message.answer("❌ Формат: /roles_admin role_order <role_name> <category> <position>")
                return
            ok = RoleManagementService.move_role(role_name, category, int(position_raw))
            await message.answer("✅ Очередность роли обновлена." if ok else "❌ Не удалось обновить очередь роли (смотри логи).")
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
                if ok:
                    await _sync_linked_discord_role(target_id, role_name, revoke=False)
                await message.answer("✅ Роль выдана в БД." if ok else "❌ Не удалось выдать роль (смотри логи).")
            else:
                ok = RoleManagementService.revoke_user_role("telegram", target_id, role_name)
                if ok:
                    await _sync_linked_discord_role(target_id, role_name, revoke=True)
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
        actor_can_manage_categories = _can_manage_categories("telegram", str(callback.from_user.id))
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

        if action == "actions":
            await callback.message.edit_text(
                _render_actions_text(),
                parse_mode="HTML",
                reply_markup=_build_actions_keyboard(owner_id),
            )
            await callback.answer()
            return

        if action == "start":
            operation = parts[3] if len(parts) > 3 else ""
            if operation.startswith("category_") and not actor_can_manage_categories:
                await callback.answer("Категориями может управлять только Глава клуба или Главный вице.", show_alert=True)
                return
            button_ops = {"category_order", "category_delete", "role_move", "role_order", "role_delete"}
            if operation in {"category_order", "category_delete"}:
                await callback.message.edit_text(
                    "Выберите категорию:",
                    reply_markup=_build_pick_category_keyboard(grouped, owner_id, operation),
                )
                await callback.answer()
                return
            if operation in {"role_move", "role_order", "role_delete"}:
                await callback.message.edit_text(
                    "Выберите роль:",
                    reply_markup=_build_pick_role_keyboard(grouped, owner_id, operation, 0),
                )
                await callback.answer()
                return
            _PENDING_ACTIONS[callback.from_user.id] = PendingRolesAdminAction(operation=operation, created_at=time.time())
            await callback.answer("Ожидаю ввод параметров", show_alert=True)
            await callback.message.reply(_operation_hint(operation), parse_mode="HTML")
            return

        if action == "pick_role_page":
            operation = parts[3] if len(parts) > 3 else ""
            page = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else 0
            await callback.message.edit_text(
                "Выберите роль:",
                reply_markup=_build_pick_role_keyboard(grouped, owner_id, operation, page),
            )
            await callback.answer()
            return

        if action == "pick_category":
            operation = parts[3] if len(parts) > 3 else ""
            category_idx = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else -1
            if category_idx < 0 or category_idx >= len(grouped):
                await callback.answer("Категория не найдена", show_alert=True)
                return
            category_name = str(grouped[category_idx]["category"])
            if operation == "category_delete":
                ok = RoleManagementService.delete_category(category_name)
                await callback.answer("Категория удалена" if ok else "Не удалось удалить категорию", show_alert=not ok)
                grouped_after = RoleManagementService.list_roles_grouped() or []
                await callback.message.edit_text(_render_actions_text(), parse_mode="HTML", reply_markup=_build_actions_keyboard(owner_id))
                return
            if operation == "category_order":
                _PENDING_ACTIONS[callback.from_user.id] = PendingRolesAdminAction(
                    operation="category_order_pick_position",
                    created_at=time.time(),
                    payload={"category": category_name},
                )
                await callback.message.edit_text(
                    f"Выбрана категория: <b>{category_name}</b>\nВыберите новую позицию:",
                    parse_mode="HTML",
                    reply_markup=_build_position_choice_keyboard(owner_id, "category_order"),
                )
                await callback.answer()
                return
            if operation in {"role_move_target", "role_order_target"}:
                pending = _PENDING_ACTIONS.get(callback.from_user.id)
                if not pending or not pending.payload or not pending.payload.get("role"):
                    await callback.answer("Сессия устарела, начните заново", show_alert=True)
                    return
                pending.operation = "role_pick_position"
                pending.payload["category"] = category_name
                pending.payload["mode"] = "move" if operation == "role_move_target" else "order"
                pending.created_at = time.time()
                _PENDING_ACTIONS[callback.from_user.id] = pending
                await callback.message.edit_text(
                    f"Роль: <b>{pending.payload['role']}</b>\nКатегория: <b>{category_name}</b>\nВыберите позицию:",
                    parse_mode="HTML",
                    reply_markup=_build_position_choice_keyboard(owner_id, "role_position"),
                )
                await callback.answer()
                return

        if action == "pick_role":
            operation = parts[3] if len(parts) > 3 else ""
            page = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else 0
            role_idx = int(parts[5]) if len(parts) > 5 and parts[5].isdigit() else -1
            flattened = _flatten_roles(grouped)
            page_size = 8
            safe_page = _normalize_page(page, len(flattened), page_size)
            item_index = safe_page * page_size + role_idx
            if role_idx < 0 or item_index >= len(flattened):
                await callback.answer("Роль не найдена", show_alert=True)
                return
            role_name = flattened[item_index]["role"]
            if operation == "role_delete":
                ok = RoleManagementService.delete_role(role_name)
                await callback.answer(f"Роль {role_name} удалена" if ok else "Не удалось удалить роль", show_alert=not ok)
                await callback.message.edit_text(_render_actions_text(), parse_mode="HTML", reply_markup=_build_actions_keyboard(owner_id))
                return
            if operation in {"role_move", "role_order"}:
                _PENDING_ACTIONS[callback.from_user.id] = PendingRolesAdminAction(
                    operation="role_pick_category",
                    created_at=time.time(),
                    payload={"role": role_name, "mode": "move" if operation == "role_move" else "order"},
                )
                next_operation = "role_move_target" if operation == "role_move" else "role_order_target"
                await callback.message.edit_text(
                    f"Роль: <b>{role_name}</b>\nВыберите целевую категорию:",
                    parse_mode="HTML",
                    reply_markup=_build_pick_category_keyboard(grouped, owner_id, next_operation),
                )
                await callback.answer()
                return

        if action == "set_position":
            op = parts[3] if len(parts) > 3 else ""
            value = parts[4] if len(parts) > 4 else ""
            pending = _PENDING_ACTIONS.get(callback.from_user.id)
            if op == "category_order":
                if not pending or pending.operation != "category_order_pick_position" or not pending.payload:
                    await callback.answer("Сессия устарела, начните заново", show_alert=True)
                    return
                category_name = pending.payload.get("category", "")
                new_pos = 0 if value == "start" else max(len(grouped) - 1, 0)
                ok = RoleManagementService.create_category(category_name, new_pos)
                _PENDING_ACTIONS.pop(callback.from_user.id, None)
                await callback.answer("Порядок категории обновлён" if ok else "Не удалось обновить порядок", show_alert=not ok)
                await callback.message.edit_text(_render_actions_text(), parse_mode="HTML", reply_markup=_build_actions_keyboard(owner_id))
                return
            if op == "role_position":
                if not pending or pending.operation != "role_pick_position" or not pending.payload:
                    await callback.answer("Сессия устарела, начните заново", show_alert=True)
                    return
                role_name = pending.payload.get("role", "")
                category_name = pending.payload.get("category", "")
                category_item = next((item for item in grouped if str(item.get("category")) == category_name), None)
                total_roles = len((category_item or {}).get("roles", []))
                new_pos = 0 if value == "start" else total_roles
                ok = RoleManagementService.move_role(role_name, category_name, new_pos)
                _PENDING_ACTIONS.pop(callback.from_user.id, None)
                await callback.answer("Позиция роли обновлена" if ok else "Не удалось обновить позицию роли", show_alert=not ok)
                await callback.message.edit_text(_render_actions_text(), parse_mode="HTML", reply_markup=_build_actions_keyboard(owner_id))
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
                reply_markup=_build_category_keyboard(
                    owner_id,
                    page,
                    category_idx,
                    len(roles),
                    can_manage_categories=actor_can_manage_categories,
                ),
            )
            await callback.answer()
            return

        if action == "delete_category":
            if not actor_can_manage_categories:
                logger.warning(
                    "roles_admin category delete denied callback_data=%s actor_id=%s",
                    callback.data,
                    callback.from_user.id,
                )
                await callback.answer("Категориями может управлять только Глава клуба или Главный вице.", show_alert=True)
                return
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
                    reply_markup=_build_category_keyboard(
                        owner_id,
                        page,
                        category_idx,
                        len(refreshed_roles),
                        can_manage_categories=actor_can_manage_categories,
                    ),
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
    except (TelegramNetworkError, TelegramConflictError):
        logger.exception(
            "roles_admin callback transport failed (telegram runtime/session issue) callback_data=%s actor_id=%s",
            callback.data,
            callback.from_user.id if callback.from_user else None,
        )
        await _safe_callback_answer(
            callback,
            "Сеть Telegram недоступна или идёт перезапуск polling. Попробуйте ещё раз через пару секунд.",
            show_alert=True,
        )
    except Exception:
        logger.exception(
            "roles_admin callback failed callback_data=%s actor_id=%s",
            callback.data,
            callback.from_user.id if callback.from_user else None,
        )
        await _safe_callback_answer(callback, "Ошибка в панели ролей (смотри логи).", show_alert=True)


@router.message(F.from_user, F.from_user.id.func(has_pending_roles_admin_action))
async def roles_admin_pending_action_handler(message: Message) -> None:
    if not message.from_user:
        return
    pending = _PENDING_ACTIONS.get(message.from_user.id)
    if not pending:
        logger.warning(
            "roles_admin pending handler invoked without state user_id=%s chat_id=%s",
            message.from_user.id,
            message.chat.id if message.chat else None,
        )
        return

    text = (message.text or "").strip()
    if not text:
        await message.answer("❌ Пустой ввод. " + _operation_hint(pending.operation), parse_mode="HTML")
        return
    if text.lower() in {"отмена", "cancel"}:
        _PENDING_ACTIONS.pop(message.from_user.id, None)
        await message.answer("🟡 Операция отменена.")
        return

    try:
        if pending.operation.startswith("category_") and not _can_manage_categories("telegram", str(message.from_user.id)):
            await message.answer("❌ Категориями может управлять только Глава клуба или Главный вице.")
            _PENDING_ACTIONS.pop(message.from_user.id, None)
            return

        args = _parse_pipe_args(text)
        op = pending.operation
        if op == "category_create":
            if not args:
                await message.answer("❌ Формат: Название | position(опц)")
                return
            pos = int(args[1]) if len(args) > 1 and args[1].lstrip("-").isdigit() else 0
            ok = RoleManagementService.create_category(args[0], pos)
            await message.answer("✅ Категория сохранена." if ok else "❌ Не удалось создать категорию (смотри логи).")
        elif op == "category_order":
            if len(args) < 2 or not args[1].lstrip("-").isdigit():
                await message.answer("❌ Формат: Название | position")
                return
            ok = RoleManagementService.create_category(args[0], int(args[1]))
            await message.answer("✅ Порядок категории обновлён." if ok else "❌ Не удалось обновить порядок категории (смотри логи).")
        elif op == "category_delete":
            if not args:
                await message.answer("❌ Формат: Название")
                return
            ok = RoleManagementService.delete_category(args[0])
            await message.answer("✅ Категория удалена." if ok else "❌ Не удалось удалить категорию (смотри логи).")
        elif op == "role_create":
            if len(args) < 2:
                await message.answer("❌ Формат: Роль | Категория | discord_role_id(опц) | position(опц)")
                return
            discord_role_id = args[2] if len(args) > 2 else None
            pos = int(args[3]) if len(args) > 3 and args[3].lstrip("-").isdigit() else 0
            ok = RoleManagementService.create_role(args[0], args[1], discord_role_id=discord_role_id, position=pos)
            await message.answer("✅ Роль создана." if ok else "❌ Не удалось создать роль (смотри логи).")
        elif op == "role_move":
            if len(args) < 2:
                await message.answer("❌ Формат: Роль | Категория | position(опц)")
                return
            pos = int(args[2]) if len(args) > 2 and args[2].lstrip("-").isdigit() else 0
            ok = RoleManagementService.move_role(args[0], args[1], pos)
            await message.answer("✅ Роль перемещена." if ok else "❌ Не удалось переместить роль (смотри логи).")
        elif op == "role_order":
            if len(args) < 3 or not args[2].lstrip("-").isdigit():
                await message.answer("❌ Формат: Роль | Категория | position")
                return
            ok = RoleManagementService.move_role(args[0], args[1], int(args[2]))
            await message.answer("✅ Очередность роли обновлена." if ok else "❌ Не удалось обновить очередь роли (смотри логи).")
        elif op == "role_delete":
            if not args:
                await message.answer("❌ Формат: Название роли")
                return
            ok = RoleManagementService.delete_role(args[0])
            await message.answer("✅ Роль удалена." if ok else "❌ Не удалось удалить роль (смотри логи).")
        elif op == "user_roles":
            if message.reply_to_message and message.reply_to_message.from_user:
                target_id = str(message.reply_to_message.from_user.id)
            elif args and args[0].isdigit():
                target_id = args[0]
            else:
                await message.answer("❌ Формат: telegram_id или reply на пользователя")
                return
            roles = RoleManagementService.get_user_roles("telegram", target_id)
            if not roles:
                await message.answer("📭 У пользователя нет ролей.")
            else:
                lines = [f"🧾 Роли пользователя {target_id}:"]
                for role in roles:
                    lines.append(f"• {role['name']} ({role.get('category') or 'Без категории'})")
                await message.answer("\n".join(lines))
        elif op in {"user_grant", "user_revoke"}:
            if len(args) < 2 or not args[0].isdigit():
                await message.answer("❌ Формат: telegram_id | Название роли")
                return
            if op == "user_grant":
                role_info = RoleManagementService.get_role(args[1])
                category = role_info.get("category_name") if role_info else None
                ok = RoleManagementService.assign_user_role("telegram", args[0], args[1], category=category)
                if ok:
                    await _sync_linked_discord_role(args[0], args[1], revoke=False)
                await message.answer("✅ Роль выдана в БД." if ok else "❌ Не удалось выдать роль (смотри логи).")
            else:
                ok = RoleManagementService.revoke_user_role("telegram", args[0], args[1])
                if ok:
                    await _sync_linked_discord_role(args[0], args[1], revoke=True)
                await message.answer("✅ Роль снята в БД." if ok else "❌ Не удалось снять роль (смотри логи).")
        else:
            logger.warning("roles_admin pending unknown operation user_id=%s operation=%s", message.from_user.id, op)
            await message.answer("❌ Неизвестная операция. Откройте панель заново: /roles_admin")
    except Exception:
        logger.exception(
            "roles_admin pending action failed user_id=%s operation=%s text=%s",
            message.from_user.id,
            pending.operation,
            message.text,
        )
        await message.answer("❌ Ошибка выполнения операции (смотри логи).")
    finally:
        _PENDING_ACTIONS.pop(message.from_user.id, None)
