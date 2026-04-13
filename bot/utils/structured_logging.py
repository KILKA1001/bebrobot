from __future__ import annotations

import json
import logging
from uuid import uuid4


def generate_request_id() -> str:
    return uuid4().hex[:12]


def log_critical_event(
    logger: logging.Logger,
    *,
    level: int,
    operation_code: str,
    reason: str,
    platform: str,
    user_id: str | int | None,
    entity_type: str,
    entity_id: str | int | None,
    correlation_id: str | None = None,
    request_id: str | None = None,
    **extra_fields: object,
) -> tuple[str, str]:
    request_id = request_id or generate_request_id()
    correlation_id = correlation_id or request_id
    payload: dict[str, object] = {
        "operation_code": operation_code,
        "reason": reason,
        "platform": platform,
        "user_id": str(user_id) if user_id is not None else None,
        "entity_type": entity_type,
        "entity_id": str(entity_id) if entity_id is not None else None,
        "correlation_id": correlation_id,
        "request_id": request_id,
    }
    payload.update(extra_fields)
    logger.log(level, json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return correlation_id, request_id
