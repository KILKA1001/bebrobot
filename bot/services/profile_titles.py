from __future__ import annotations

PROTECTED_PROFILE_TITLES: tuple[str, ...] = (
    "Глава клуба",
    "Главный вице",
    "Вице города",
    "Ветеран города",
    "Участник клубов",
)

PROTECTED_PROFILE_TITLE_ALIASES: dict[str, str] = {
    "глава клуба": "глава клуба",
    "глава клубов": "глава клуба",
    "главный вице": "главный вице",
    "вице города": "вице города",
    "ветеран города": "ветеран города",
    "участник клубов": "участник клубов",
}


def normalize_protected_profile_title(name: str | None) -> str:
    normalized = str(name or "").strip().lower()
    return PROTECTED_PROFILE_TITLE_ALIASES.get(normalized, normalized)


def normalized_protected_profile_titles() -> set[str]:
    return set(PROTECTED_PROFILE_TITLE_ALIASES)


def protected_profile_title_canonical_keys() -> set[str]:
    return set(PROTECTED_PROFILE_TITLE_ALIASES.values())


def is_protected_profile_title(name: str | None) -> bool:
    return normalize_protected_profile_title(name) in protected_profile_title_canonical_keys()
