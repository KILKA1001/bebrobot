import logging
from html import escape

from bot.legacy_identity_logging import (
    log_identity_resolve_error,
    log_transport_identity_error,
)
from bot.services import AccountsService, AuthorityService, RoleManagementService
from bot.services.profile_titles import normalize_protected_profile_title
from bot.services.role_management_service import USER_ACQUIRE_HINT_PLACEHOLDER
from bot.services.shop_service import build_shop_prompt_text
from bot.services.ux_texts import compose_three_block_message
from bot.systems.roles_catalog_shared import (
    ROLES_CATALOG_FOOTER_TEXT,
    ROLES_CATALOG_TITLE,
    build_roles_catalog_intro_lines,
    format_roles_catalog_category_title,
    prepare_public_roles_catalog_pages,
    build_role_visual_tags,
)


logger = logging.getLogger(__name__)
ROLE_DESCRIPTION_PLACEHOLDER = "Описание пока не указано администратором"
ROLES_CATALOG_LOAD_ERROR_TEXT = (
    "❌ Не удалось загрузить каталог ролей. Попробуйте позже; если ошибка повторится, откройте /helpy "
    "или сообщите администратору, что не открывается /roles."
)
ROLES_CATALOG_EMPTY_TEXT = (
    "📭 <b>Каталог ролей пока пуст.</b>\n"
    "Когда администраторы добавят роли, здесь появятся категории, описания и инструкция по получению."
)


_PUBLIC_HELP_COMMANDS: tuple[str, ...] = (
    "/register — создать общий аккаунт (делается один раз, затем открываются профиль, модерация и роли).",
    "/profile — открыть профиль общего аккаунта: звания, баллы, статус привязки и видимые роли.",
    "/profile_roles — показать роли пользователя по категориям, чтобы быстро проверить текущий набор ролей.",
    "/profile_edit — открыть меню редактирования профиля (ник, описание и другие доступные поля).",
    "/link <код> — привязать Telegram к аккаунту по коду из Discord (команда работает в личке).",
    "/link_discord — получить код для обратной привязки Discord к текущему общему аккаунту.",
    "/roles — открыть каталог ролей с описанием, способом получения и подсказкой «как получить».",
    "/balance [reply|id] — посмотреть баланс: свой или выбранного пользователя (через reply/id).",
    "/modstatus — единый статус по модерации: активные наказания, предупреждения, последние кейсы и штрафы к оплате.",
    "/shop — открыть магазин с кнопкой «Открыть магазин» (лучше запускать в личных сообщениях).",
    "/helpy — показать это меню со списком доступных вам команд и короткими пояснениями.",
)

_POINTS_HELP_LINE = "/points [reply|id] — меню управления баллами: выбрать пользователя и действие без ручного ввода сложных команд"
_TICKETS_HELP_LINE = "/tickets [reply|id] — меню управления билетами: выдача и списание через пошаговый интерфейс"
_ROLES_ADMIN_HELP_LINE = (
    "/roles_admin / /rolesadmin — вход в панель ролей. Дальше используйте кнопки внутри экрана."
)
_TITLE_HELP_LINE = "/title @username (или reply) — единая кнопочная команда для повышения/понижения звания; сначала выбирается режим, затем конкретное звание"
_REP_HELP_LINE = "/rep — модерация по шагам: выбери цель через reply/@username, выбери нарушение кнопками, проверь preview (предупреждения, активное наказание, следующий шаг); наказание вручную вводить не нужно; себя и старшее/равное звание выбрать нельзя"
_MODSTATUS_HELP_LINE = "/modstatus — показать свои активные наказания, предупреждения, последние кейсы и штрафы; оплата доступна кнопкой «Оплатить штраф» прямо в этом экране"


def _can_manage_points(actor_level: int) -> bool:
    return actor_level >= 80


def _can_manage_tickets(actor_titles: tuple[str, ...], actor_level: int) -> bool:
    normalized = {normalize_protected_profile_title(title) for title in actor_titles if str(title).strip()}
    if {"глава клуба", "главный вице"} & normalized:
        return True
    return actor_level >= 100


def _build_helpy_text(*, actor_level: int = 0, actor_titles: tuple[str, ...] = tuple(), can_use_rep: bool = False) -> str:
    lines = ["📚 Список команд:", *_PUBLIC_HELP_COMMANDS]

    privileged_lines: list[str] = []
    if _can_manage_points(actor_level):
        privileged_lines.append(_POINTS_HELP_LINE)
    if _can_manage_tickets(actor_titles, actor_level):
        privileged_lines.append(_TICKETS_HELP_LINE)
    if actor_level >= 80:
        privileged_lines.append(_ROLES_ADMIN_HELP_LINE)
    if {"глава клуба", "главный вице"} & {normalize_protected_profile_title(title) for title in actor_titles if str(title).strip()}:
        privileged_lines.append(_TITLE_HELP_LINE)
    privileged_lines.append(_MODSTATUS_HELP_LINE)
    if can_use_rep:
        privileged_lines.append(_REP_HELP_LINE)

    if privileged_lines:
        lines.append("")
        lines.append("🔐 Дополнительно доступно по вашему званию:")
        lines.extend(privileged_lines)

    return "\n".join(lines)


def get_helpy_text(telegram_user_id: int | None = None) -> str:
    if telegram_user_id is None:
        return _build_helpy_text()

    try:
        authority = AuthorityService.resolve_authority("telegram", str(telegram_user_id))
        can_use_rep = AuthorityService.has_command_permission("telegram", str(telegram_user_id), "moderation_mute")
    except Exception:
        logger.exception("telegram help authority resolve failed actor_id=%s", telegram_user_id)
        return _build_helpy_text()

    return _build_helpy_text(actor_level=authority.level, actor_titles=authority.titles, can_use_rep=can_use_rep)


def prepare_roles_catalog_pages() -> dict[str, object]:
    try:
        grouped = RoleManagementService.list_public_roles_catalog(log_context="/roles")
    except Exception:
        logger.exception("roles catalog render failed command=/roles source=telegram_user_command")
        return {"status": "error", "pages": [], "message": ROLES_CATALOG_LOAD_ERROR_TEXT}

    if not grouped:
        return {"status": "empty", "pages": [], "message": ROLES_CATALOG_EMPTY_TEXT}

    return {
        "status": "ok",
        "pages": prepare_public_roles_catalog_pages(grouped, max_roles_per_page=8, log_context="telegram:/roles"),
        "message": "",
    }


def render_roles_catalog_page(page_data: dict[str, object]) -> str:
    current_page = int(page_data.get("page_index") or 0) + 1
    total_pages = int(page_data.get("total_pages") or 1)
    parts = [f"🏅 <b>{escape(ROLES_CATALOG_TITLE.replace('🏅 ', ''))}</b>"]
    for line in build_roles_catalog_intro_lines(current_page=current_page, total_pages=total_pages):
        label, value = line.split(": ", maxsplit=1)
        if label == "Страница":
            current_marker = escape(f"{current_page}/{total_pages}")
            parts.append(f"<b>{escape(label)}:</b> сейчас показана страница <b>{current_marker}</b>.")
            continue
        parts.append(f"<b>{escape(label)}:</b> {escape(value)}")

    for item in page_data.get("sections") or []:
        category = escape(format_roles_catalog_category_title(item))
        parts.append(f"\n<b>{category}</b>")
        roles = item.get("items") or []
        if not roles:
            parts.append("• Пока нет ролей")
            continue
        for role in roles:
            role_name = escape(str(role.get("name") or "Без названия"))
            description = escape(str(role.get("description") or "").strip() or ROLE_DESCRIPTION_PLACEHOLDER)
            acquire_method = escape(str(role.get("acquire_method_label") or "Не указан").strip())
            acquire_hint = escape(str(role.get("acquire_hint") or "").strip() or USER_ACQUIRE_HINT_PLACEHOLDER)
            visual_tags = escape(build_role_visual_tags(role))
            parts.append(
                f"\n• <b>{role_name}</b>\n"
                f"Метки: <code>{visual_tags}</code>\n"
                f"Описание: {description}\n"
                f"Способ получения: {acquire_method}\n"
                f"Как получить: {acquire_hint}"
            )
    parts.append(f"\n{escape(ROLES_CATALOG_FOOTER_TEXT)}")
    return "\n".join(parts)


def process_roles_catalog_command(page: int = 0) -> str:
    payload = prepare_roles_catalog_pages()
    if payload["status"] != "ok":
        return str(payload["message"])

    pages = list(payload.get("pages") or [])
    safe_page = min(max(int(page), 0), len(pages) - 1)
    return render_roles_catalog_page(pages[safe_page])


def process_register_command(telegram_user_id: int | None) -> str:
    if telegram_user_id is None:
        log_transport_identity_error(
            logger,
            module=__name__,
            handler="process_register_command",
            field="telegram_user_id",
            action="extract_platform_user_id",
            continue_execution=False,
        )
        return compose_three_block_message(
            what="Не получилось определить ваш Telegram-профиль.",
            now="Закройте и откройте чат с ботом, затем повторите /register.",
            next_step="Бот создаст общий профиль и откроет остальные команды.",
            emoji="❌",
        )

    from bot.systems.linking_logic import register_telegram_account

    success, payload = register_telegram_account(telegram_user_id)
    if success:
        return compose_three_block_message(
            what="Профиль создан.",
            now="Откройте /profile, чтобы проверить данные.",
            next_step="Станут доступны магазин, роли и модерационные экраны.",
            emoji="✅",
        )
    return compose_three_block_message(
        what="Профиль пока не создан.",
        now="Повторите /register через минуту.",
        next_step=f"Если ошибка повторяется, передайте администратору текст: {payload}",
        emoji="❌",
    )


def process_shop_command() -> str:
    return build_shop_prompt_text()


def process_profile_command(
    telegram_user_id: int | None,
    display_name: str | None = None,
    target_telegram_user_id: int | None = None,
    target_display_name: str | None = None,
) -> str:
    lookup_user_id = target_telegram_user_id or telegram_user_id
    lookup_display_name = target_display_name or display_name

    if lookup_user_id is None:
        log_transport_identity_error(
            logger,
            module=__name__,
            handler="process_profile_command",
            field="telegram_user_id",
            action="extract_platform_user_id",
            continue_execution=False,
        )
        return "❌ Не удалось определить пользователя Telegram."

    account_id = AccountsService.resolve_account_id("telegram", str(lookup_user_id))
    if not account_id:
        log_identity_resolve_error(
            logger,
            module=__name__,
            handler="process_profile_command",
            field="telegram_user_id",
            action="resolve_account_id",
            continue_execution=False,
            provider="telegram",
            provider_user_id=lookup_user_id,
        )
        return compose_three_block_message(
            what="Профиль ещё не создан.",
            now="Отправьте /register в личном чате с ботом.",
            next_step="После регистрации команда /profile откроет ваш профиль.",
            emoji="❌",
        )

    data = AccountsService.get_profile_by_account(account_id, display_name=lookup_display_name)
    if not data:
        return compose_three_block_message(
            what="Профиль ещё не создан.",
            now="Отправьте /register в личном чате с ботом.",
            next_step="После регистрации команда /profile откроет ваш профиль.",
            emoji="❌",
        )

    title_name = escape(data["custom_nick"])
    target_platform_name = (lookup_display_name or "").strip()
    if target_platform_name and target_platform_name != data["custom_nick"]:
        title_name = f"{title_name} ({escape(target_platform_name)})"
    safe_description = escape(data["description"][:100])
    safe_nulls_id = escape(data["nulls_brawl_id"])
    safe_link_status = escape(data["link_status"])
    safe_nulls_status = escape(data["nulls_status"])
    safe_points = escape(str(data["points"]))
    safe_titles_text = escape(str(data.get("titles_text") or "Нет званий"))
    visible_roles = data.get("visible_roles") or []
    safe_roles_text = "\n".join(f"• {escape(str(role))}" for role in visible_roles) if visible_roles else "Нет назначенных ролей"

    return (
        "👤 <b><a href=\"tg://user?id={telegram_user_id}\">{title_name}</a></b>\n"
        "━━━━━━━━━━━━━━\n"
        "<b>Общая информация</b>\n"
        "Звания: {safe_titles_text}\n"
        "Айди в Null's Brawl: <code>{safe_nulls_id}</code> ({safe_nulls_status})\n"
        "Баллы: {safe_points}\n"
        "━━━━━━━━━━━━━━\n"
        "<b>Описание</b>\n"
        "{safe_description}\n"
        "━━━━━━━━━━━━━━\n"
        "<b>Дополнительная информация</b>\n"
        "🔗 TG ↔ DC: {safe_link_status}\n"
        "━━━━━━━━━━━━━━\n"
        "<b>Роли</b>\n"
        "{safe_roles_text}"
    ).format(
        telegram_user_id=lookup_user_id,
        title_name=title_name,
        safe_nulls_id=safe_nulls_id,
        safe_description=safe_description,
        safe_link_status=safe_link_status,
        safe_nulls_status=safe_nulls_status,
        safe_points=safe_points,
        safe_titles_text=safe_titles_text,
        safe_roles_text=safe_roles_text,
    )


def process_link_command(
    message_text: str,
    telegram_user_id: int | None,
    is_private_chat: bool = True,
) -> str:
    if not is_private_chat:
        return compose_three_block_message(
            what="Привязка работает только в личном чате.",
            now="Откройте личные сообщения с ботом и повторите команду.",
            next_step="Бот примет код и свяжет аккаунты Telegram и Discord.",
            emoji="❌",
        )

    text = (message_text or "").strip()
    parts = text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        return "Использование: /link <код>"

    if telegram_user_id is None:
        log_transport_identity_error(
            logger,
            module=__name__,
            handler="process_link_command",
            field="telegram_user_id",
            action="extract_platform_user_id",
            continue_execution=False,
        )
        return "Не удалось определить пользователя Telegram."

    code = parts[1].strip()
    from bot.telegram_bot.link_handler import handle_link_command

    success, payload = handle_link_command(telegram_user_id, code)
    prefix = "✅" if success else "❌"
    return f"{prefix} {payload}"


def process_link_discord_command(
    telegram_user_id: int | None,
    is_private_chat: bool = True,
) -> str:
    if not is_private_chat:
        return "❌ Команда привязки доступна только в личных сообщениях с ботом."

    if telegram_user_id is None:
        log_transport_identity_error(
            logger,
            module=__name__,
            handler="process_link_discord_command",
            field="telegram_user_id",
            action="extract_platform_user_id",
            continue_execution=False,
        )
        return "Не удалось определить пользователя Telegram."

    from bot.systems.linking_logic import issue_telegram_discord_link_code

    success, payload = issue_telegram_discord_link_code(telegram_user_id)
    if not success:
        return f"❌ {payload}"

    return (
        "🔗 Код привязки Discord сгенерирован.\n"
        f"Код: `{payload}`\n"
        f"Срок действия: {AccountsService.LINK_TTL_MINUTES} минут.\n"
        "Используйте в Discord: `/link <код>`"
    )


def process_profile_roles_command(
    telegram_user_id: int | None,
    display_name: str | None = None,
    target_telegram_user_id: int | None = None,
    target_display_name: str | None = None,
) -> str:
    lookup_user_id = target_telegram_user_id or telegram_user_id
    lookup_display_name = target_display_name or display_name

    if lookup_user_id is None:
        log_transport_identity_error(
            logger,
            module=__name__,
            handler="process_profile_roles_command",
            field="telegram_user_id",
            action="extract_platform_user_id",
            continue_execution=False,
        )
        return "❌ Не удалось определить пользователя Telegram."

    account_id = AccountsService.resolve_account_id("telegram", str(lookup_user_id))
    if not account_id:
        log_identity_resolve_error(
            logger,
            module=__name__,
            handler="process_profile_roles_command",
            field="telegram_user_id",
            action="resolve_account_id",
            continue_execution=False,
            provider="telegram",
            provider_user_id=lookup_user_id,
        )
        return "❌ Профиль не найден. Сначала выполните /register"

    data = AccountsService.get_profile_by_account(account_id, display_name=lookup_display_name)
    if not data:
        return "❌ Профиль не найден. Сначала выполните /register"

    roles_by_category = data.get("roles_by_category") or {}
    if not roles_by_category:
        return "🏅 <b>Роли пользователя</b>\nНет назначенных ролей"

    parts = ["🏅 <b>Роли пользователя</b>"]
    for category in sorted(roles_by_category.keys()):
        role_names = roles_by_category.get(category) or []
        if not role_names:
            continue
        rendered = "\n".join(f"• {escape(str(role_name))}" for role_name in role_names)
        parts.append(f"\n<b>{escape(str(category))}</b>\n{rendered}")

    return "\n".join(parts)
