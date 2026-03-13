from bot.utils import conversation_activity as activity


def test_should_thread_reply_false_for_single_user():
    key = "telegram:test-single"
    assert activity.should_thread_reply(key, 111, window_seconds=300) is False
    assert activity.should_thread_reply(key, 111, window_seconds=300) is False


def test_should_thread_reply_true_for_multiple_users():
    key = "telegram:test-multi"
    assert activity.should_thread_reply(key, 111, window_seconds=300) is False
    assert activity.should_thread_reply(key, 222, window_seconds=300) is True
    assert activity.should_thread_reply(key, 111, window_seconds=300) is True
