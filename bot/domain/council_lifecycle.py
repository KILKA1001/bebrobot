"""
Единые коды жизненного цикла для Совета.
Используются и в Telegram, и в Discord, чтобы исключить расхождения по статусам.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# Созыв (term) lifecycle.
TERM_STATUS_DRAFT = "draft"
TERM_STATUS_PENDING_LAUNCH_CONFIRMATION = "pending_launch_confirmation"
TERM_STATUS_ACTIVE = "active"
TERM_STATUS_ARCHIVED = "archived"
TERM_STATUS_CANCELLED = "cancelled"
TERM_STATUS_VALUES: tuple[str, ...] = (
    TERM_STATUS_DRAFT,
    TERM_STATUS_PENDING_LAUNCH_CONFIRMATION,
    TERM_STATUS_ACTIVE,
    TERM_STATUS_ARCHIVED,
    TERM_STATUS_CANCELLED,
)

# Выборы (election) lifecycle.
ELECTION_STATUS_DRAFT = "draft"
ELECTION_STATUS_NOMINATION = "nomination"
ELECTION_STATUS_VOTING = "voting"
ELECTION_STATUS_COMPLETED = "completed"
ELECTION_STATUS_CANCELLED = "cancelled"
ELECTION_STATUS_VALUES: tuple[str, ...] = (
    ELECTION_STATUS_DRAFT,
    ELECTION_STATUS_NOMINATION,
    ELECTION_STATUS_VOTING,
    ELECTION_STATUS_COMPLETED,
    ELECTION_STATUS_CANCELLED,
)

# Вопрос (question) lifecycle.
QUESTION_STATUS_DRAFT = "draft"
QUESTION_STATUS_DISCUSSION = "discussion"
QUESTION_STATUS_VOTING = "voting"
QUESTION_STATUS_DECIDED = "decided"
QUESTION_STATUS_ARCHIVED = "archived"
QUESTION_STATUS_VALUES: tuple[str, ...] = (
    QUESTION_STATUS_DRAFT,
    QUESTION_STATUS_DISCUSSION,
    QUESTION_STATUS_VOTING,
    QUESTION_STATUS_DECIDED,
    QUESTION_STATUS_ARCHIVED,
)

MAX_COUNCIL_TEXT_LEN = 1000


def is_valid_lifecycle_status(status: str, *, lifecycle: str) -> bool:
    value = (status or "").strip().lower()
    if lifecycle == "term":
        return value in TERM_STATUS_VALUES
    if lifecycle == "election":
        return value in ELECTION_STATUS_VALUES
    if lifecycle == "question":
        return value in QUESTION_STATUS_VALUES
    logger.error("Unknown lifecycle passed to is_valid_lifecycle_status lifecycle=%s", lifecycle)
    return False


def validate_council_text_length(text: str | None, *, field_name: str) -> tuple[bool, str | None]:
    cleaned = (text or "").strip()
    if len(cleaned) <= MAX_COUNCIL_TEXT_LEN:
        return True, None
    logger.error(
        "Council text is too long field=%s actual_len=%s max_len=%s",
        field_name,
        len(cleaned),
        MAX_COUNCIL_TEXT_LEN,
    )
    return False, f"Текст поля «{field_name}» должен быть не длиннее {MAX_COUNCIL_TEXT_LEN} символов."
