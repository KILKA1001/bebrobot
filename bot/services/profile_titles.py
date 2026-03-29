from __future__ import annotations

PROTECTED_PROFILE_TITLES: tuple[str, ...] = (
    "Глава клуба",
    "Главный вице",
    "Оператор",
    "Вице города",
    "Админ",
    "Ветеран города",
    "Младший админ",
    "Участник клубов",
    "Участник чата",
)

PROTECTED_PROFILE_TITLE_ALIASES: dict[str, str] = {
    "глава клубов": "глава клуба",
}


def normalize_protected_profile_title(name: str | None) -> str:
    normalized = str(name or "").strip().lower()
    canonical_keys = protected_profile_title_canonical_keys()
    if normalized in canonical_keys:
        return normalized
    return PROTECTED_PROFILE_TITLE_ALIASES.get(normalized, normalized)


def normalized_protected_profile_titles() -> set[str]:
    return protected_profile_title_canonical_keys() | set(PROTECTED_PROFILE_TITLE_ALIASES)


def protected_profile_title_canonical_keys() -> set[str]:
    return {title.strip().lower() for title in PROTECTED_PROFILE_TITLES if str(title).strip()}


def is_protected_profile_title(name: str | None) -> bool:
    return normalize_protected_profile_title(name) in protected_profile_title_canonical_keys()
