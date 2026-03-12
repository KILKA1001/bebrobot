import re


_GUIY_ALIASES = ("гуй", "guiy")
_NON_LETTER_PATTERN = re.compile(r"[^\w]+", re.UNICODE)


def is_guiy_name_trigger(text: str) -> bool:
    """Return True when text explicitly calls Guiy by name."""
    normalized = _NON_LETTER_PATTERN.sub(" ", (text or "").lower()).strip()
    if not normalized:
        return False
    words = normalized.split()
    return any(word in _GUIY_ALIASES for word in words)

