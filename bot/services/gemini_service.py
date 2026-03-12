import logging
import os

import aiohttp


logger = logging.getLogger(__name__)


GEMINI_API_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "gemini-1.5-flash:generateContent"
)

DEFAULT_GUIY_SYSTEM_PROMPT = (
    "Ты персонаж по имени Гуй. "
    "Всегда отвечай от лица Гуя на русском языке. "
    "Характер: немного напористый и немного токсичный, но без оскорблений, угроз и хейта. "
    "Лор: Гуй — сын Эмочки (Эмочка его папа), который очень его любит, "
    "но иногда может наказать за непослушание. "
    "У Гуя есть братья с именами из трёх букв, которые начинаются на 'Г' и заканчиваются на 'й', "
    "например Гий, Гай, Гой. "
    "Гуй знает, что у брата Гия есть комната с прослушкой, будто он агент ФБР. "
    "Гуй обожает огурцы и считает, что без них жить нельзя. "
    "Гуй любит путешествовать и отдыхать в Испании на берегу Средиземного моря, "
    "попивая огуречный смузи. "
    "Гуй наивный и непослушный, но старается помогать отцу в работе. "
    "Гуй понимает, что работает ботом и получает за это огурцы в оплату. "
    "Если информации не хватает, не выдумывай факты про реальных людей. "
    "Отвечай кратко и по делу."
)


def _build_system_prompt() -> str:
    custom_prompt = (os.getenv("GUIY_SYSTEM_PROMPT") or "").strip()
    extra_lore = (os.getenv("GUIY_EXTRA_LORE") or "").strip()

    if custom_prompt:
        base_prompt = custom_prompt
    else:
        base_prompt = DEFAULT_GUIY_SYSTEM_PROMPT

    if extra_lore:
        return f"{base_prompt}\n\nДополнительный лор:\n{extra_lore}"

    return base_prompt


async def generate_guiy_reply(user_text: str) -> str | None:
    api_key = (os.getenv("GEMINI_API_KEY") or "").strip()
    if not api_key:
        logger.warning("GEMINI_API_KEY is empty, skip ai reply")
        return None

    system_prompt = _build_system_prompt()
    payload = {
        "system_instruction": {"parts": [{"text": system_prompt}]},
        "contents": [{"role": "user", "parts": [{"text": user_text}]}],
        "generationConfig": {
            "temperature": 0.9,
            "maxOutputTokens": 220,
            "topP": 0.95,
        },
    }

    try:
        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                GEMINI_API_URL,
                params={"key": api_key},
                json=payload,
            ) as resp:
                if resp.status >= 400:
                    body = await resp.text()
                    logger.error("Gemini API request failed status=%s body=%s", resp.status, body[:1000])
                    return None

                data = await resp.json()
    except Exception:
        logger.exception("Gemini request crashed")
        return None

    try:
        candidates = data.get("candidates") or []
        for candidate in candidates:
            content = candidate.get("content") or {}
            for part in content.get("parts") or []:
                text = (part.get("text") or "").strip()
                if text:
                    return text
    except Exception:
        logger.exception("Gemini response parse failed data=%s", str(data)[:1500])
        return None

    logger.warning("Gemini returned empty candidates: %s", str(data)[:1000])
    return None

