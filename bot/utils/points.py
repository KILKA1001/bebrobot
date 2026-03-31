"""
Назначение: модуль "points" реализует продуктовый контур в зоне общая логика.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: общая логика.
"""

def format_points(points: float) -> str:
    """Return a human-friendly representation of points.

    The number is rounded to two decimals and trailing zeros are removed.
    """
    formatted = f"{points:.2f}"
    formatted = formatted.rstrip("0").rstrip(".")
    return formatted
