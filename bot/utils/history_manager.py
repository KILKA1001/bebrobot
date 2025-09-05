from enum import Enum
import discord
from .points import format_points


# Типы действий с баллами
class ActionType(Enum):
    ADD = "Начисление"  # Добавление баллов
    REMOVE = "Снятие"  # Снятие баллов
    UNDO = "Отмена"  # Отмена последнего действия


# Класс для хранения информации о действии с баллами
class HistoryEntry:
    def __init__(
        self,
        points: float,
        reason: str,
        author_id: int,
        timestamp: str,
        action_type: ActionType,
    ):
        self.points = points  # Количество баллов
        self.reason = reason  # Причина изменения
        self.author_id = author_id  # ID автора действия
        self.timestamp = timestamp  # Временная метка
        self.action_type = action_type  # Тип действия


# Форматирование истории действий для отображения
def format_history_embed(
    entries: list, member_name: str, page: int, total_entries: int
) -> discord.Embed:
    entries_per_page = 5  # Количество записей на странице
    total_pages = (total_entries + entries_per_page - 1) // entries_per_page

    # Создание основного embed сообщения
    embed = discord.Embed(
        title=f"📜 История баллов — {member_name}",
        color=discord.Color.blue(),
        description=f"Страница {page}/{total_pages}",
    )

    # Обработка каждой записи в истории
    for entry in entries:
        if isinstance(entry, dict):
            # Получение данных из записи
            points = entry.get("points", 0)
            action_type = ActionType.ADD if points >= 0 else ActionType.REMOVE
            if "is_undo" in entry:
                action_type = ActionType.UNDO

            # Форматирование деталей записи
            timestamp = entry.get("timestamp", "Неизвестно")
            reason = entry.get("reason", "Без причины")
            author_id = entry.get("author_id", None)

            # Форматирование отображения баллов
            sign = "+" if points >= 0 else ""
            title = f"{action_type.value} {sign}{format_points(points)} баллов"

            # Определяем отображение автора: для системных действий не
            # отображаем упоминание пользователя, чтобы избежать выводов вида
            # ``<@None>`` или ``<@0>``.
            author_display = f"<@{author_id}>" if author_id else "Система"

            # Формирование текста записи
            value = (
                f"📝 Причина: {reason}\n"
                f"👤 Автор: {author_display}\n"
                f"🕒 Дата: {timestamp}"
            )

            # Выбор emoji в зависимости от типа действия
            color_emoji = "🟢" if points >= 0 else "🔴"
            if action_type == ActionType.UNDO:
                color_emoji = "⚪"

            # Добавление записи в embed
            embed.add_field(
                name=f"{color_emoji} {title}", value=value, inline=False
            )

    # Добавление информации о общем количестве записей
    embed.set_footer(text=f"Всего записей: {total_entries}")
    return embed
