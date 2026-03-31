"""
Назначение: модуль "roles catalog shared" реализует продуктовый контур в зоне общая логика.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: общая логика.
"""

import copy
import logging
from typing import Any


logger = logging.getLogger(__name__)

ROLES_CATALOG_TITLE = "🏅 Каталог ролей"
ROLES_CATALOG_ERROR_TEXT = "❌ Ошибка открытия каталога ролей (смотри логи)."
ROLES_CATALOG_EMPTY_TEXT = (
    "📭 Каталог ролей пока пуст.\n"
    "Когда администраторы добавят роли, здесь появятся категории, описания и инструкция по получению."
)
ROLES_CATALOG_FOOTER_TEXT = (
    "Если хочешь получить роль, ориентируйся на блок «Как получить» и при необходимости уточняй условия у администратора."
)
ROLES_CATALOG_DEFAULT_PAGE_SIZE = 8
ROLES_CATALOG_DESCRIPTION_WARNING_LENGTH = 700
ROLES_CATALOG_ACQUIRE_HINT_WARNING_LENGTH = 400
ROLES_CATALOG_EMPTY_CATEGORY_PLACEHOLDER = "Без категории"
ROLES_CATALOG_EMPTY_ROLE_PLACEHOLDER = "Без названия"


def build_roles_catalog_intro_lines(*, current_page: int, total_pages: int) -> list[str]:
    return [
        "Что это: здесь собраны все пользовательские роли по категориям — чтобы быстро понять, за что отвечает каждая роль.",
        f"Страница: сейчас показана страница {current_page}/{total_pages}.",
        "Как листать: используй кнопки ниже — ⬅️ назад, 🔄 обновить каталог, ➡️ вперёд.",
        "Где смотреть способ получения: у каждой роли есть строки Способ получения и Как получить.",
        "Как читать статус: роли со способом выдаёт администратор обычно выдаются вручную, а роли с пометками вроде автоматически, за баллы или через активность приходят автоматически после нужного условия.",
        "Как читать оформление: в Discord цвет роли берётся из самой роли сервера, а в Telegram показываются текстовые метки роли.",
    ]


def build_role_visual_tags(role: dict[str, Any]) -> str:
    tags: list[str] = []
    discord_role_id = str(role.get("discord_role_id") or "").strip()
    if discord_role_id:
        tags.append("DISCORD")
    else:
        tags.append("ЛОКАЛЬНАЯ")
    if bool(role.get("is_privileged_discord_role")):
        tags.append("ПРИВИЛЕГИЯ")
    return " • ".join(tags)


def _normalized_position(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _normalized_text(value: Any, *, placeholder: str, anomaly_message: str, log_context: str) -> str:
    normalized = str(value or "").strip()
    if normalized:
        return normalized
    logger.warning("%s log_context=%s placeholder=%s", anomaly_message, log_context, placeholder)
    return placeholder


def _role_sort_key(role: dict[str, Any]) -> tuple[Any, ...]:
    return (
        _normalized_position(role.get("position")),
        str(role.get("name") or "").strip().casefold(),
        str(role.get("discord_role_id") or "").strip(),
        str(role.get("acquire_method_label") or "").strip().casefold(),
        str(role.get("description") or "").strip().casefold(),
        str(role.get("acquire_hint") or "").strip().casefold(),
    )


def _category_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (
        _normalized_position(item.get("position")),
        str(item.get("category") or "").strip().casefold(),
    )


def _normalize_role(role: dict[str, Any], *, category_name: str, log_context: str) -> dict[str, Any]:
    normalized_role = copy.deepcopy(role)
    normalized_role["name"] = _normalized_text(
        normalized_role.get("name"),
        placeholder=ROLES_CATALOG_EMPTY_ROLE_PLACEHOLDER,
        anomaly_message="roles catalog anomaly: empty role name",
        log_context=f"{log_context}:{category_name}",
    )
    description = str(normalized_role.get("description") or "").strip()
    acquire_hint = str(normalized_role.get("acquire_hint") or "").strip()
    if len(description) > ROLES_CATALOG_DESCRIPTION_WARNING_LENGTH:
        logger.warning(
            "roles catalog anomaly: description too long log_context=%s category=%s role=%s description_len=%s limit=%s",
            log_context,
            category_name,
            normalized_role["name"],
            len(description),
            ROLES_CATALOG_DESCRIPTION_WARNING_LENGTH,
        )
    if len(acquire_hint) > ROLES_CATALOG_ACQUIRE_HINT_WARNING_LENGTH:
        logger.warning(
            "roles catalog anomaly: acquire_hint too long log_context=%s category=%s role=%s acquire_hint_len=%s limit=%s",
            log_context,
            category_name,
            normalized_role["name"],
            len(acquire_hint),
            ROLES_CATALOG_ACQUIRE_HINT_WARNING_LENGTH,
        )
    return normalized_role


def _normalize_category(item: dict[str, Any], *, log_context: str) -> dict[str, Any] | None:
    category_name = _normalized_text(
        item.get("category"),
        placeholder=ROLES_CATALOG_EMPTY_CATEGORY_PLACEHOLDER,
        anomaly_message="roles catalog anomaly: empty category name",
        log_context=log_context,
    )
    roles = [_normalize_role(role, category_name=category_name, log_context=log_context) for role in item.get("roles") or []]
    roles = sorted(roles, key=_role_sort_key)
    if not roles:
        logger.info(
            "roles catalog empty category hidden log_context=%s category=%s",
            log_context,
            category_name,
        )
        return None
    return {
        "category": category_name,
        "position": _normalized_position(item.get("position")),
        "items": roles,
    }


def prepare_public_roles_catalog_pages(
    grouped: list[dict[str, Any]] | None,
    *,
    max_roles_per_page: int = ROLES_CATALOG_DEFAULT_PAGE_SIZE,
    log_context: str = "/roles",
) -> list[dict[str, Any]]:
    page_size = max(int(max_roles_per_page or ROLES_CATALOG_DEFAULT_PAGE_SIZE), 1)
    normalized_categories = [
        normalized
        for normalized in (
            _normalize_category(item or {}, log_context=log_context)
            for item in sorted(grouped or [], key=_category_sort_key)
        )
        if normalized is not None
    ]

    pages: list[dict[str, Any]] = []
    current_sections: list[dict[str, Any]] = []
    current_role_count = 0

    def flush_page() -> None:
        nonlocal current_sections, current_role_count
        if not current_sections:
            return
        pages.append({
            "sections": current_sections,
            "role_count": current_role_count,
        })
        current_sections = []
        current_role_count = 0

    for category in normalized_categories:
        category_name = str(category.get("category") or ROLES_CATALOG_EMPTY_CATEGORY_PLACEHOLDER)
        items = list(category.get("items") or [])
        category_item_count = len(items)
        if category_item_count <= 0:
            logger.error(
                "roles catalog anomaly: category normalized without items log_context=%s category=%s",
                log_context,
                category_name,
            )
            continue

        if category_item_count > page_size:
            flush_page()
            section_count = max((category_item_count + page_size - 1) // page_size, 1)
            for chunk_start in range(0, category_item_count, page_size):
                chunk_items = items[chunk_start : chunk_start + page_size]
                if len(chunk_items) > page_size:
                    logger.error(
                        "roles catalog anomaly: category does not fit page after split log_context=%s category=%s chunk_size=%s page_size=%s total_items=%s",
                        log_context,
                        category_name,
                        len(chunk_items),
                        page_size,
                        category_item_count,
                    )
                section_index = chunk_start // page_size
                pages.append(
                    {
                        "sections": [
                            {
                                "category": category_name,
                                "items": chunk_items,
                                "item_count": len(chunk_items),
                                "category_item_count": category_item_count,
                                "section_index": section_index,
                                "section_count": section_count,
                                "is_category_continuation": section_index > 0,
                                "continues_on_next_page": section_index + 1 < section_count,
                            }
                        ],
                        "role_count": len(chunk_items),
                    }
                )
            continue

        if current_sections and current_role_count + category_item_count > page_size:
            flush_page()

        current_sections.append(
            {
                "category": category_name,
                "items": items,
                "item_count": category_item_count,
                "category_item_count": category_item_count,
                "section_index": 0,
                "section_count": 1,
                "is_category_continuation": False,
                "continues_on_next_page": False,
            }
        )
        current_role_count += category_item_count

    flush_page()

    total_pages = len(pages)
    normalized_pages: list[dict[str, Any]] = []
    for page_index, page in enumerate(pages):
        normalized_pages.append(
            {
                "page_index": page_index,
                "total_pages": total_pages,
                "sections": list(page.get("sections") or []),
                "role_count": int(page.get("role_count") or 0),
                "section_count": len(page.get("sections") or []),
            }
        )
    return normalized_pages


def format_roles_catalog_category_title(item: dict[str, Any]) -> str:
    category = str(item.get("category") or ROLES_CATALOG_EMPTY_CATEGORY_PLACEHOLDER)
    section_index = max(int(item.get("section_index") or 0), 0)
    section_count = max(int(item.get("section_count") or 1), 1)
    display_index = section_index + 1
    if section_count <= 1:
        return category
    if display_index <= 1:
        return f"{category} (часть {display_index}/{section_count})"
    return f"{category} (продолжение {display_index}/{section_count})"
