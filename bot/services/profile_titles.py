from __future__ import annotations

PROTECTED_PROFILE_TITLES: tuple[str, ...] = (
    "Глава клуба",
    "Главный вице",
    "Вице города",
    "Ветеран города",
    "Участник клубов",
)


def normalized_protected_profile_titles() -> set[str]:
    return {title.strip().lower() for title in PROTECTED_PROFILE_TITLES if str(title).strip()}


def is_protected_profile_title(name: str | None) -> bool:
    return str(name or "").strip().lower() in normalized_protected_profile_titles()
