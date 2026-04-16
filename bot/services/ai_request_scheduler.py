"""
Назначение: модуль планировщика AI-запросов с общей очередью для Telegram и Discord.
Ответственность: справедливая обработка запросов и единый worker, выполняющий generate_guiy_reply.
Где используется: Telegram и Discord AI-ветки.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from bot.services.ai_service import generate_guiy_reply


logger = logging.getLogger(__name__)


DEFAULT_AI_SCHEDULER_MAX_CONCURRENCY = 2
DEFAULT_AI_SCHEDULER_PER_CHAT_QUANTUM = 1
DEFAULT_AI_SCHEDULER_MAX_QUEUE_PER_CHAT = 30
DEFAULT_AI_SCHEDULER_REQUEST_TIMEOUT_SEC = 120.0


def _read_int_env(name: str, default: int, *, minimum: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None or not raw_value.strip():
        return default
    try:
        return max(minimum, int(raw_value.strip()))
    except ValueError:
        logger.exception(
            "ai scheduler invalid int env, fallback to default env_name=%s env_value=%s default=%s minimum=%s",
            name,
            raw_value,
            default,
            minimum,
        )
        return default


def _read_float_env(name: str, default: float, *, minimum: float) -> float:
    raw_value = os.getenv(name)
    if raw_value is None or not raw_value.strip():
        return default
    try:
        return max(minimum, float(raw_value.strip()))
    except ValueError:
        logger.exception(
            "ai scheduler invalid float env, fallback to default env_name=%s env_value=%s default=%s minimum=%s",
            name,
            raw_value,
            default,
            minimum,
        )
        return default


AI_SCHEDULER_MAX_CONCURRENCY = _read_int_env(
    "AI_SCHEDULER_MAX_CONCURRENCY",
    DEFAULT_AI_SCHEDULER_MAX_CONCURRENCY,
    minimum=1,
)
AI_SCHEDULER_PER_CHAT_QUANTUM = _read_int_env(
    "AI_SCHEDULER_PER_CHAT_QUANTUM",
    DEFAULT_AI_SCHEDULER_PER_CHAT_QUANTUM,
    minimum=1,
)
AI_SCHEDULER_MAX_QUEUE_PER_CHAT = _read_int_env(
    "AI_SCHEDULER_MAX_QUEUE_PER_CHAT",
    DEFAULT_AI_SCHEDULER_MAX_QUEUE_PER_CHAT,
    minimum=1,
)
AI_SCHEDULER_REQUEST_TIMEOUT_SEC = _read_float_env(
    "AI_SCHEDULER_REQUEST_TIMEOUT_SEC",
    DEFAULT_AI_SCHEDULER_REQUEST_TIMEOUT_SEC,
    minimum=5.0,
)


@dataclass(slots=True)
class _QueuedRequest:
    platform: str
    conversation_id: str
    user_id: str
    payload: dict[str, Any]
    enqueued_at: float
    future: asyncio.Future[str | None]
    request_id: str


@dataclass(slots=True)
class _ConversationBucket:
    by_user: dict[str, deque[_QueuedRequest]] = field(default_factory=dict)
    user_order: deque[str] = field(default_factory=deque)
    last_user_id: str | None = None
    size: int = 0

    def add(self, item: _QueuedRequest) -> None:
        queue = self.by_user.get(item.user_id)
        if queue is None:
            queue = deque()
            self.by_user[item.user_id] = queue
            self.user_order.append(item.user_id)
        queue.append(item)
        self.size += 1

    def pop_next(self) -> _QueuedRequest | None:
        if self.size <= 0 or not self.user_order:
            return None

        selected_user: str | None = None
        if len(self.user_order) > 1 and self.last_user_id is not None:
            for user in self.user_order:
                if user != self.last_user_id and self.by_user.get(user):
                    selected_user = user
                    break

        if selected_user is None:
            for user in self.user_order:
                if self.by_user.get(user):
                    selected_user = user
                    break

        if selected_user is None:
            return None

        queue = self.by_user.get(selected_user)
        if not queue:
            return None

        item = queue.popleft()
        self.size -= 1
        self.last_user_id = selected_user

        if not queue:
            self.by_user.pop(selected_user, None)
            try:
                self.user_order.remove(selected_user)
            except ValueError:
                pass
        else:
            while self.user_order and self.user_order[0] != selected_user:
                self.user_order.rotate(-1)
            self.user_order.rotate(-1)

        return item


class AIRequestScheduler:
    def __init__(self) -> None:
        self._max_concurrency = AI_SCHEDULER_MAX_CONCURRENCY
        self._per_chat_quantum = AI_SCHEDULER_PER_CHAT_QUANTUM
        self._max_queue_per_chat = AI_SCHEDULER_MAX_QUEUE_PER_CHAT
        self._request_timeout_sec = AI_SCHEDULER_REQUEST_TIMEOUT_SEC

        self._lock = asyncio.Lock()
        self._condition = asyncio.Condition(self._lock)
        self._conversation_buckets: dict[str, _ConversationBucket] = {}
        self._conversation_order: deque[str] = deque()
        self._active_conversation: str | None = None
        self._active_conversation_budget = 0

        self._workers: list[asyncio.Task[Any]] = []
        self._request_seq = 0
        self._total_queue_len = 0
        self._platform_counts: dict[str, int] = {"telegram": 0, "discord": 0}

    async def start(self) -> None:
        async with self._lock:
            if self._workers:
                return
            for index in range(self._max_concurrency):
                task = asyncio.create_task(self._worker_loop(index + 1), name=f"ai_scheduler_worker_{index + 1}")
                self._workers.append(task)
            logger.info(
                "ai scheduler started max_concurrency=%s per_chat_quantum=%s max_queue_per_chat=%s",
                self._max_concurrency,
                self._per_chat_quantum,
                self._max_queue_per_chat,
            )

    async def enqueue(self, *, platform: str, conversation_id: str | int | None, user_id: str | int | None, payload: dict[str, Any]) -> str | None:
        await self.start()

        normalized_platform = platform if platform in {"telegram", "discord"} else "unknown"
        conversation_key = str(conversation_id) if conversation_id is not None else "unknown"
        sender_key = str(user_id) if user_id is not None else "unknown"
        now = time.monotonic()
        loop = asyncio.get_running_loop()
        future: asyncio.Future[str | None] = loop.create_future()

        async with self._condition:
            bucket = self._conversation_buckets.get(conversation_key)
            if bucket is None:
                bucket = _ConversationBucket()
                self._conversation_buckets[conversation_key] = bucket
                self._conversation_order.append(conversation_key)

            if bucket.size >= self._max_queue_per_chat:
                logger.warning(
                    "ai scheduler dropped request reason=max_queue_per_chat platform=%s conversation_id=%s user_id=%s queue_len=%s per_chat_len=%s platform_parity=%s",
                    normalized_platform,
                    conversation_key,
                    sender_key,
                    self._total_queue_len,
                    bucket.size,
                    self._platform_parity(),
                )
                return None

            self._request_seq += 1
            request_id = f"{normalized_platform}:{conversation_key}:{self._request_seq}"
            item = _QueuedRequest(
                platform=normalized_platform,
                conversation_id=conversation_key,
                user_id=sender_key,
                payload=payload,
                enqueued_at=now,
                future=future,
                request_id=request_id,
            )
            bucket.add(item)
            self._total_queue_len += 1
            self._platform_counts[normalized_platform] = self._platform_counts.get(normalized_platform, 0) + 1

            logger.info(
                "ai scheduler enqueue request_id=%s platform=%s conversation_id=%s user_id=%s queue_len=%s per_chat_len=%s platform_parity=%s",
                request_id,
                normalized_platform,
                conversation_key,
                sender_key,
                self._total_queue_len,
                bucket.size,
                self._platform_parity(),
            )
            self._condition.notify()

        return await future

    async def _worker_loop(self, worker_id: int) -> None:
        logger.info("ai scheduler worker started worker_id=%s", worker_id)
        while True:
            item = await self._dequeue_next()
            if item is None:
                continue

            started_at = time.monotonic()
            wait_ms = int((started_at - item.enqueued_at) * 1000)
            logger.info(
                "ai scheduler dequeue worker_id=%s request_id=%s platform=%s conversation_id=%s user_id=%s queue_len=%s wait_ms=%s platform_parity=%s",
                worker_id,
                item.request_id,
                item.platform,
                item.conversation_id,
                item.user_id,
                self._total_queue_len,
                wait_ms,
                self._platform_parity(),
            )

            try:
                reply = await asyncio.wait_for(
                    generate_guiy_reply(
                        str(item.payload.get("text") or ""),
                        provider=item.platform,
                        user_id=item.user_id,
                        conversation_id=item.conversation_id,
                        media_inputs=item.payload.get("media_inputs"),
                    ),
                    timeout=self._request_timeout_sec,
                )
                processing_ms = int((time.monotonic() - started_at) * 1000)
                logger.info(
                    "ai scheduler processed worker_id=%s request_id=%s platform=%s conversation_id=%s user_id=%s processing_ms=%s wait_ms=%s platform_parity=%s",
                    worker_id,
                    item.request_id,
                    item.platform,
                    item.conversation_id,
                    item.user_id,
                    processing_ms,
                    wait_ms,
                    self._platform_parity(),
                )
                if not item.future.done():
                    item.future.set_result(reply)
            except asyncio.TimeoutError:
                logger.error(
                    "ai scheduler timeout worker_id=%s request_id=%s platform=%s conversation_id=%s user_id=%s timeout_sec=%s wait_ms=%s platform_parity=%s",
                    worker_id,
                    item.request_id,
                    item.platform,
                    item.conversation_id,
                    item.user_id,
                    self._request_timeout_sec,
                    wait_ms,
                    self._platform_parity(),
                )
                if not item.future.done():
                    item.future.set_result(None)
            except Exception:
                logger.exception(
                    "ai scheduler worker failed worker_id=%s request_id=%s platform=%s conversation_id=%s user_id=%s wait_ms=%s",
                    worker_id,
                    item.request_id,
                    item.platform,
                    item.conversation_id,
                    item.user_id,
                    wait_ms,
                )
                if not item.future.done():
                    item.future.set_result(None)

    async def _dequeue_next(self) -> _QueuedRequest | None:
        async with self._condition:
            while self._total_queue_len <= 0:
                await self._condition.wait()

            conversation_key = self._pick_next_conversation()
            if conversation_key is None:
                return None

            bucket = self._conversation_buckets.get(conversation_key)
            if bucket is None:
                return None

            item = bucket.pop_next()
            if item is None:
                return None

            self._total_queue_len = max(0, self._total_queue_len - 1)
            self._platform_counts[item.platform] = max(0, self._platform_counts.get(item.platform, 0) - 1)

            if bucket.size <= 0:
                self._conversation_buckets.pop(conversation_key, None)
                try:
                    self._conversation_order.remove(conversation_key)
                except ValueError:
                    pass
                if self._active_conversation == conversation_key:
                    self._active_conversation = None
                    self._active_conversation_budget = 0

            return item

    def _pick_next_conversation(self) -> str | None:
        if not self._conversation_order:
            self._active_conversation = None
            self._active_conversation_budget = 0
            return None

        if (
            self._active_conversation
            and self._active_conversation_budget > 0
            and self._active_conversation in self._conversation_buckets
            and self._conversation_buckets[self._active_conversation].size > 0
        ):
            self._active_conversation_budget -= 1
            return self._active_conversation

        for _ in range(len(self._conversation_order)):
            candidate = self._conversation_order[0]
            self._conversation_order.rotate(-1)
            bucket = self._conversation_buckets.get(candidate)
            if bucket and bucket.size > 0:
                self._active_conversation = candidate
                self._active_conversation_budget = max(0, self._per_chat_quantum - 1)
                return candidate

        self._active_conversation = None
        self._active_conversation_budget = 0
        return None

    def _platform_parity(self) -> str:
        return f"telegram:{self._platform_counts.get('telegram', 0)},discord:{self._platform_counts.get('discord', 0)}"


_SCHEDULER: AIRequestScheduler | None = None
_SCHEDULER_LOCK = asyncio.Lock()


async def get_ai_request_scheduler() -> AIRequestScheduler:
    global _SCHEDULER
    if _SCHEDULER is not None:
        return _SCHEDULER

    async with _SCHEDULER_LOCK:
        if _SCHEDULER is None:
            _SCHEDULER = AIRequestScheduler()
        return _SCHEDULER


async def enqueue_ai_request(
    *,
    platform: str,
    conversation_id: str | int | None,
    user_id: str | int | None,
    payload: dict[str, Any],
) -> str | None:
    scheduler = await get_ai_request_scheduler()
    return await scheduler.enqueue(
        platform=platform,
        conversation_id=conversation_id,
        user_id=user_id,
        payload=payload,
    )
