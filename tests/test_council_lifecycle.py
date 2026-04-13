from bot.domain.council_lifecycle import (
    MAX_COUNCIL_TEXT_LEN,
    ELECTION_STATUS_VALUES,
    QUESTION_STATUS_VALUES,
    TERM_STATUS_VALUES,
    is_valid_lifecycle_status,
    validate_council_text_length,
)


def test_term_election_question_status_values_are_stable():
    assert TERM_STATUS_VALUES == (
        "draft",
        "pending_launch_confirmation",
        "active",
        "archived",
        "cancelled",
    )
    assert ELECTION_STATUS_VALUES == (
        "draft",
        "nomination",
        "voting",
        "completed",
        "cancelled",
    )
    assert QUESTION_STATUS_VALUES == (
        "draft",
        "discussion",
        "voting",
        "decided",
        "archived",
    )


def test_status_validator_for_all_lifecycles():
    assert is_valid_lifecycle_status("active", lifecycle="term")
    assert is_valid_lifecycle_status("voting", lifecycle="election")
    assert is_valid_lifecycle_status("decided", lifecycle="question")
    assert not is_valid_lifecycle_status("unknown", lifecycle="term")
    assert not is_valid_lifecycle_status("active", lifecycle="missing")


def test_council_text_length_validation():
    valid, err = validate_council_text_length("x" * MAX_COUNCIL_TEXT_LEN, field_name="Вопрос")
    assert valid is True
    assert err is None

    valid, err = validate_council_text_length("x" * (MAX_COUNCIL_TEXT_LEN + 1), field_name="Предложение")
    assert valid is False
    assert "1000" in (err or "")
