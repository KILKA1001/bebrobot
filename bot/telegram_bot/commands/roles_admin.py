import logging

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from bot.services import AuthorityService, RoleManagementService

logger = logging.getLogger(__name__)
router = Router()


def _parse_target_arg(message: Message) -> int | None:
    if message.reply_to_message and message.reply_to_message.from_user and not message.reply_to_message.from_user.is_bot:
        return int(message.reply_to_message.from_user.id)
    parts = (message.text or "").strip().split()
    if len(parts) < 2:
        return None
    return int(parts[1]) if parts[1].isdigit() else None


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
            await message.answer(
                "Использование:\n"
                "/roles_admin list\n"
                "/roles_admin category_create <name> [position]\n"
                "/roles_admin category_delete <name>\n"
                "/roles_admin role_create <name> <category> [discord_role_id]\n"
                "/roles_admin role_delete <name>\n"
                "/roles_admin role_move <name> <category> [position]\n"
                "/roles_admin user_roles [reply|telegram_id]\n"
                "/roles_admin user_grant <telegram_id> <role_name>\n"
                "/roles_admin user_revoke <telegram_id> <role_name>"
            )
            return

        subcommand = parts[1].lower()
        args = parts[2:]

        if subcommand == "list":
            grouped = RoleManagementService.list_roles_grouped()
            if not grouped:
                await message.answer("📭 Список ролей пуст или БД недоступна.")
                return
            lines = ["🧩 Роли по категориям:"]
            for item in grouped:
                lines.append(f"\n<b>{item['category']}</b>")
                if not item["roles"]:
                    lines.append("• —")
                    continue
                for role in item["roles"]:
                    suffix = f" (Discord ID: {role['discord_role_id']})" if role.get("discord_role_id") else ""
                    lines.append(f"• {role['name']}{suffix}")
            await message.answer("\n".join(lines), parse_mode="HTML")
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

        await message.answer("❌ Неверная команда или аргументы. Напишите /roles_admin для справки.")
    except Exception:
        logger.exception(
            "roles_admin command failed actor_id=%s text=%s",
            message.from_user.id if message.from_user else None,
            message.text,
        )
        await message.answer("❌ Ошибка выполнения команды ролей.")
