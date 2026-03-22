from typing import Any


ROLES_CATALOG_TITLE = "🏅 Каталог ролей"
ROLES_CATALOG_ERROR_TEXT = "❌ Ошибка открытия каталога ролей (смотри логи)."
ROLES_CATALOG_EMPTY_TEXT = (
    "📭 Каталог ролей пока пуст.\n"
    "Когда администраторы добавят роли, здесь появятся категории, описания и инструкция по получению."
)
ROLES_CATALOG_FOOTER_TEXT = (
    "Если хочешь получить роль, ориентируйся на блок «Как получить» и при необходимости уточняй условия у администратора."
)


def build_roles_catalog_intro_lines(*, current_page: int, total_pages: int) -> list[str]:
    return [
        "Что это: здесь собраны все пользовательские роли по категориям — чтобы быстро понять, за что отвечает каждая роль.",
        f"Страница: сейчас показана страница {current_page}/{total_pages}.",
        "Как листать: используй кнопки ниже — ⬅️ назад, 🔄 обновить каталог, ➡️ вперёд.",
        "Где смотреть способ получения: у каждой роли есть строки Способ получения и Как получить.",
        "Как читать статус: роли со способом выдаёт администратор обычно выдаются вручную, а роли с пометками вроде автоматически, за баллы или через активность приходят автоматически после нужного условия.",
    ]


def format_roles_catalog_category_title(item: dict[str, Any]) -> str:
    category = str(item.get("category") or "Без категории")
    chunk_index = max(int(item.get("category_chunk_index") or 1), 1)
    chunk_total = max(int(item.get("category_chunk_total") or 1), 1)
    if chunk_total <= 1:
        return category
    if chunk_index <= 1:
        return f"{category} (часть {chunk_index}/{chunk_total})"
    return f"{category} (продолжение {chunk_index}/{chunk_total})"
