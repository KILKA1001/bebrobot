from __future__ import annotations

import threading
import time
from collections import defaultdict, deque


# Keeps recent non-bot speakers by conversation to decide whether reply threading
# should be enabled to reduce ambiguity in busy chats.
_ACTIVITY: dict[str, deque[tuple[float, int]]] = defaultdict(deque)
_LOCK = threading.Lock()


def should_thread_reply(
    conversation_key: str,
    user_id: int | None,
    *,
    window_seconds: int = 120,
    min_distinct_users: int = 2,
) -> bool:
    """Return True when recent activity includes multiple distinct users.

    This function records the current user activity and then checks how many
    distinct users have spoken recently in the conversation.
    """
    if user_id is None:
        return False

    now = time.monotonic()
    cutoff = now - max(1, window_seconds)

    with _LOCK:
        bucket = _ACTIVITY[conversation_key]
        bucket.append((now, user_id))

        while bucket and bucket[0][0] < cutoff:
            bucket.popleft()

        distinct_users = {uid for _, uid in bucket}
        return len(distinct_users) >= max(1, min_distinct_users)
