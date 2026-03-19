import logging
import re
from typing import Any

import discord

_CLOUDFLARE_RAY_RE = re.compile(r"ray id:\s*([a-z0-9-]+)", re.IGNORECASE)
_RETRY_AFTER_RE = re.compile(
    r"\bretry(?:_|-|\s)after\b\s*[:=]?\s*(\d+(?:\.\d+)?)\s*(ms|milliseconds?|s|sec|seconds?)?",
    re.IGNORECASE,
)
_SCRIPT_STYLE_RE = re.compile(r"<(script|style)\b[^>]*>.*?</\1>", re.IGNORECASE | re.DOTALL)
_TAG_RE = re.compile(r"<[^>]+>")
_WHITESPACE_RE = re.compile(r"\s+")
_HTML_MARKER_RE = re.compile(r"</?(?:html|head|body|script|style|title|div|span|p|section|header|footer|iframe)\b", re.IGNORECASE)


def _response_header(exc: discord.HTTPException, name: str) -> str | None:
    response = getattr(exc, "response", None)
    if response is None:
        return None
    value = response.headers.get(name) or response.headers.get(name.lower())
    return value.strip() if isinstance(value, str) and value.strip() else None


def _looks_like_html(text: str) -> bool:
    return bool(text and (_HTML_MARKER_RE.search(text) or "<!doctype html" in text.lower()))


def _normalize_plain_text(text: str | None) -> str:
    if not text:
        return ""
    cleaned = _SCRIPT_STYLE_RE.sub(" ", text)
    cleaned = _TAG_RE.sub(" ", cleaned)
    cleaned = cleaned.replace("\\n", " ").replace("\n", " ")
    return _WHITESPACE_RE.sub(" ", cleaned).strip()


def extract_retry_after_seconds(exc: discord.HTTPException) -> float | None:
    retry_after_attr = getattr(exc, "retry_after", None)
    if isinstance(retry_after_attr, (int, float)) and retry_after_attr > 0:
        return float(retry_after_attr)

    retry_after_header = _response_header(exc, "Retry-After")
    if retry_after_header:
        try:
            return float(retry_after_header)
        except ValueError:
            pass

    text = getattr(exc, "text", "") or ""
    if _looks_like_html(text):
        return None

    match = _RETRY_AFTER_RE.search(_normalize_plain_text(text))
    if not match:
        return None

    retry_after = float(match.group(1))
    unit = (match.group(2) or "").lower()
    if unit.startswith("ms"):
        retry_after /= 1000
    return retry_after if retry_after > 0 else None


def summarize_http_error_text(text: str | None, *, limit: int = 220) -> str:
    cleaned = _normalize_plain_text(text)
    if not cleaned:
        return ""
    if len(cleaned) <= limit:
        return cleaned
    return f"{cleaned[: limit - 1]}…"


def extract_cloudflare_ray_id(exc: discord.HTTPException) -> str | None:
    header_ray = _response_header(exc, "CF-Ray")
    if header_ray:
        return header_ray

    text = getattr(exc, "text", "") or ""
    match = _CLOUDFLARE_RAY_RE.search(text)
    if match:
        return match.group(1)
    return None


def is_cloudflare_rate_limited_http_exception(exc: discord.HTTPException) -> bool:
    """Detect Cloudflare IP-level bans (1015) returned by Discord edge/proxy."""
    if getattr(exc, "status", None) not in {403, 429}:
        return False

    error_text = (getattr(exc, "text", "") or "").lower()
    if "cloudflare" in error_text and "1015" in error_text:
        return True
    if "access denied" in error_text and "rate limited" in error_text:
        return True
    if "you are being rate limited" in error_text and "discord.com" in error_text:
        return True

    server = (_response_header(exc, "server") or "").lower()
    return "cloudflare" in server and ("rate limit" in error_text or "access denied" in error_text)


def is_transient_rate_limit_error(exc: Exception) -> bool:
    if isinstance(exc, discord.HTTPException):
        return getattr(exc, "status", None) == 429 or is_cloudflare_rate_limited_http_exception(exc)

    error_text = str(exc).lower()
    return "429" in error_text or "rate limit" in error_text or "cloudflare" in error_text


def build_http_exception_log_context(
    exc: discord.HTTPException,
    *,
    stage: str,
    operation_id: str | None = None,
    **extra: Any,
) -> dict[str, Any]:
    context: dict[str, Any] = {
        "stage": stage,
        "operation_id": operation_id,
        "status": getattr(exc, "status", None),
        "code": getattr(exc, "code", None),
        "cloudflare_1015": is_cloudflare_rate_limited_http_exception(exc),
        "retry_after": extract_retry_after_seconds(exc),
        "cf_ray": extract_cloudflare_ray_id(exc),
        "server": _response_header(exc, "server"),
        "error_excerpt": summarize_http_error_text(getattr(exc, "text", "") or ""),
    }
    context.update(extra)
    return context


def log_discord_http_exception(
    message: str,
    exc: discord.HTTPException,
    *,
    stage: str,
    operation_id: str | None = None,
    **extra: Any,
) -> None:
    logging.error(
        "%s | %s",
        message,
        build_http_exception_log_context(exc, stage=stage, operation_id=operation_id, **extra),
    )
