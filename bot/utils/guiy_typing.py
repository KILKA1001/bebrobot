"""
Назначение: модуль "guiy typing" реализует продуктовый контур в зоне общая логика.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: общая логика.
"""

_MIN_TYPING_DELAY_SECONDS = 1.2
_MAX_TYPING_DELAY_SECONDS = 9.0
_CHARS_PER_SECOND = 26.0
_TYPING_DELAY_EXTRA_SECONDS = 1.0


def calculate_typing_delay_seconds(text: str) -> float:
    return calculate_typing_delay_details(text)["typing_delay_final"]


def calculate_typing_delay_details(text: str) -> dict[str, float | int]:
    normalized = (text or "").strip()
    if not normalized:
        return {
            "typing_delay_base": round(_MIN_TYPING_DELAY_SECONDS, 2),
            "typing_delay_final": round(_MIN_TYPING_DELAY_SECONDS, 2),
            "reply_len": 0,
        }

    # Более длинный ответ = более длинная имитация печати.
    estimated = len(normalized) / _CHARS_PER_SECOND + _TYPING_DELAY_EXTRA_SECONDS
    bounded = max(_MIN_TYPING_DELAY_SECONDS, min(estimated, _MAX_TYPING_DELAY_SECONDS))
    return {
        "typing_delay_base": round(estimated, 2),
        "typing_delay_final": round(bounded, 2),
        "reply_len": len(normalized),
    }
