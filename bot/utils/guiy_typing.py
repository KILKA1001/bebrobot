_MIN_TYPING_DELAY_SECONDS = 1.2
_MAX_TYPING_DELAY_SECONDS = 9.0
_CHARS_PER_SECOND = 26.0


def calculate_typing_delay_seconds(text: str) -> float:
    normalized = (text or "").strip()
    if not normalized:
        return _MIN_TYPING_DELAY_SECONDS

    # Более длинный ответ = более длинная имитация печати.
    estimated = len(normalized) / _CHARS_PER_SECOND
    bounded = max(_MIN_TYPING_DELAY_SECONDS, min(estimated, _MAX_TYPING_DELAY_SECONDS))
    return round(bounded, 2)
