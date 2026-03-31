"""
Назначение: модуль "guiy trigger" реализует продуктовый контур в зоне общая логика.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: общая логика.
"""

import re


_GUIY_ALIASES = ("гуй", "guiy")
_GUIY_VARIANTS = _GUIY_ALIASES + ("гуя", "гую", "гуем", "гуе")
_LETTER_OR_DIGIT_PATTERN = re.compile(r"[\w\d]", re.UNICODE)


def is_guiy_name_trigger(text: str) -> bool:
    """Return True when text explicitly calls Guiy by name in any message position."""
    normalized = (text or "").casefold().strip()
    if not normalized:
        return False

    for variant in _GUIY_VARIANTS:
        for match in re.finditer(re.escape(variant), normalized):
            start, end = match.span()
            left_char = normalized[start - 1] if start > 0 else ""
            right_char = normalized[end] if end < len(normalized) else ""

            left_ok = not left_char or not _LETTER_OR_DIGIT_PATTERN.match(left_char)
            right_ok = not right_char or not _LETTER_OR_DIGIT_PATTERN.match(right_char)
            if left_ok and right_ok:
                return True

    return False
