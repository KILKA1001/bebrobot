"""
Назначение: модуль "guiy publish destinations service" реализует продуктовый контур в зоне общая логика.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: общая логика.
Доменные операции: операции целевых каналов публикации GUIY.
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Any

import discord

from bot.data import db

logger = logging.getLogger(__name__)

_TELEGRAM_REGISTRY_TABLE = "bot_chat_registry"
_TELEGRAM_TRACKED_TYPES = {"group", "supergroup", "channel"}


@dataclass(frozen=True, slots=True)
class GuiyPublishDestination:
    provider: str
    destination_id: str
    title: str
    subtitle: str
    destination_type: str
    guild_id: str | None = None
    channel_id: str | None = None
    chat_id: str | None = None

    @property
    def display_label(self) -> str:
        if self.subtitle:
            return f"{self.title} — {self.subtitle}"
        return self.title


class GuiyPublishDestinationsService:
    @staticmethod
    def list_discord_destinations(bot: Any) -> list[GuiyPublishDestination]:
        guilds = list(getattr(bot, "guilds", []) or [])
        if not guilds:
            logger.warning("discord guiy publish destinations empty: bot has no guilds")
            return []

        destinations: list[GuiyPublishDestination] = []
        bot_user = getattr(bot, "user", None)
        bot_user_id = getattr(bot_user, "id", None)
        for guild in guilds:
            guild_id = str(getattr(guild, "id", "") or "").strip()
            guild_name = str(getattr(guild, "name", "") or "Без названия сервера").strip()
            me = getattr(guild, "me", None)
            if me is None and bot_user_id is not None and hasattr(guild, "get_member"):
                me = guild.get_member(bot_user_id)
            if me is None:
                logger.warning(
                    "discord guiy publish destinations skipped guild without bot member guild_id=%s guild_name=%s",
                    guild_id,
                    guild_name,
                )
                continue

            channels = list(getattr(guild, "text_channels", []) or [])
            channels.extend(list(getattr(guild, "threads", []) or []))
            for channel in channels:
                if not GuiyPublishDestinationsService._can_write_to_discord_channel(channel, me):
                    continue
                channel_id = str(getattr(channel, "id", "") or "").strip()
                channel_name = str(getattr(channel, "name", "") or channel_id or "unknown-channel").strip()
                channel_kind = "thread" if isinstance(channel, discord.Thread) else "text"
                destinations.append(
                    GuiyPublishDestination(
                        provider="discord",
                        destination_id=f"{guild_id}:{channel_id}",
                        title=f"#{channel_name}",
                        subtitle=guild_name,
                        destination_type=channel_kind,
                        guild_id=guild_id,
                        channel_id=channel_id,
                    )
                )

        destinations.sort(key=lambda item: (item.subtitle.lower(), item.title.lower(), item.destination_id))
        logger.info("discord guiy publish destinations collected count=%s", len(destinations))
        return destinations

    @staticmethod
    def _can_write_to_discord_channel(channel: Any, me: Any) -> bool:
        try:
            permissions = channel.permissions_for(me)
        except Exception:
            logger.exception(
                "discord guiy publish destinations permissions lookup failed channel_id=%s guild_id=%s",
                getattr(channel, "id", None),
                getattr(getattr(channel, "guild", None), "id", None),
            )
            return False
        if not getattr(permissions, "view_channel", False):
            return False
        if isinstance(channel, discord.Thread):
            return bool(getattr(permissions, "send_messages_in_threads", False) or getattr(permissions, "send_messages", False))
        return bool(getattr(permissions, "send_messages", False))

    @staticmethod
    def resolve_discord_destination(bot: Any, destination_id: str | None) -> GuiyPublishDestination | None:
        normalized = str(destination_id or "").strip()
        if not normalized or ":" not in normalized:
            return None
        guild_id, channel_id = normalized.split(":", 1)
        guild = None
        if hasattr(bot, "get_guild"):
            try:
                guild = bot.get_guild(int(guild_id))
            except Exception:
                guild = None
        if guild is None:
            return None
        channel = guild.get_channel(int(channel_id)) if hasattr(guild, "get_channel") else None
        if channel is None and hasattr(bot, "get_channel"):
            channel = bot.get_channel(int(channel_id))
        if channel is None:
            return None
        return GuiyPublishDestination(
            provider="discord",
            destination_id=normalized,
            title=f"#{getattr(channel, 'name', channel_id)}",
            subtitle=str(getattr(guild, "name", "Без названия сервера") or "Без названия сервера"),
            destination_type="thread" if isinstance(channel, discord.Thread) else "text",
            guild_id=str(getattr(guild, "id", guild_id)),
            channel_id=str(getattr(channel, "id", channel_id)),
        )

    @staticmethod
    def discord_destination_is_writable(bot: Any, destination_id: str | None) -> tuple[bool, str, Any | None, GuiyPublishDestination | None]:
        destination = GuiyPublishDestinationsService.resolve_discord_destination(bot, destination_id)
        if destination is None:
            return False, "channel_missing", None, None
        guild = bot.get_guild(int(destination.guild_id)) if getattr(bot, "get_guild", None) and destination.guild_id else None
        channel = None
        if guild is not None and hasattr(guild, "get_channel") and destination.channel_id:
            channel = guild.get_channel(int(destination.channel_id))
        if channel is None and hasattr(bot, "get_channel") and destination.channel_id:
            channel = bot.get_channel(int(destination.channel_id))
        if channel is None:
            return False, "channel_missing", None, destination
        me = getattr(guild, "me", None) if guild is not None else None
        bot_user = getattr(bot, "user", None)
        bot_user_id = getattr(bot_user, "id", None)
        if me is None and guild is not None and bot_user_id is not None and hasattr(guild, "get_member"):
            me = guild.get_member(bot_user_id)
        if me is None:
            return False, "bot_missing_from_guild", channel, destination
        if not GuiyPublishDestinationsService._can_write_to_discord_channel(channel, me):
            return False, "missing_permissions", channel, destination
        return True, "ok", channel, destination

    @staticmethod
    def list_telegram_destinations() -> list[GuiyPublishDestination]:
        if not getattr(db, "supabase", None):
            logger.warning("telegram guiy publish destinations unavailable: supabase is not configured")
            return []
        try:
            response = (
                db.supabase.table(_TELEGRAM_REGISTRY_TABLE)
                .select("provider,chat_id,chat_title,chat_type,last_seen_at")
                .eq("provider", "telegram")
                .eq("is_active", True)
                .in_("chat_type", list(_TELEGRAM_TRACKED_TYPES))
                .order("last_seen_at", desc=True)
                .limit(100)
                .execute()
            )
        except Exception:
            logger.exception("telegram guiy publish destinations lookup failed")
            return []

        destinations: list[GuiyPublishDestination] = []
        for row in list(getattr(response, "data", None) or []):
            chat_id = str(row.get("chat_id") or "").strip()
            if not chat_id:
                continue
            chat_title = str(row.get("chat_title") or "").strip() or f"Чат {chat_id}"
            chat_type = str(row.get("chat_type") or "").strip() or "unknown"
            last_seen_at = str(row.get("last_seen_at") or "").strip()
            subtitle = f"{chat_type}" + (f" · last seen {last_seen_at}" if last_seen_at else "")
            destinations.append(
                GuiyPublishDestination(
                    provider="telegram",
                    destination_id=chat_id,
                    title=chat_title,
                    subtitle=subtitle,
                    destination_type=chat_type,
                    chat_id=chat_id,
                )
            )
        logger.info("telegram guiy publish destinations collected count=%s", len(destinations))
        return destinations

    @staticmethod
    def get_telegram_destination(chat_id: str | int | None) -> GuiyPublishDestination | None:
        normalized_chat_id = str(chat_id or "").strip()
        if not normalized_chat_id or not getattr(db, "supabase", None):
            return None
        try:
            response = (
                db.supabase.table(_TELEGRAM_REGISTRY_TABLE)
                .select("provider,chat_id,chat_title,chat_type,last_seen_at,is_active")
                .eq("provider", "telegram")
                .eq("chat_id", normalized_chat_id)
                .limit(1)
                .execute()
            )
        except Exception:
            logger.exception("telegram guiy publish destination lookup failed chat_id=%s", normalized_chat_id)
            return None
        rows = list(getattr(response, "data", None) or [])
        if not rows:
            return None
        row = rows[0]
        if not row.get("is_active", True):
            return None
        return GuiyPublishDestination(
            provider="telegram",
            destination_id=normalized_chat_id,
            title=str(row.get("chat_title") or "").strip() or f"Чат {normalized_chat_id}",
            subtitle=str(row.get("chat_type") or "").strip() or "unknown",
            destination_type=str(row.get("chat_type") or "").strip() or "unknown",
            chat_id=normalized_chat_id,
        )

    @staticmethod
    def register_telegram_chat(*, chat_id: str | int | None, chat_title: str | None, chat_type: str | None, is_active: bool = True) -> None:
        normalized_chat_id = str(chat_id or "").strip()
        normalized_chat_type = str(chat_type or "").strip()
        normalized_chat_title = str(chat_title or "").strip() or None
        if not normalized_chat_id or not normalized_chat_type:
            return
        logger.info(
            "telegram bot chat registry seen chat_id=%s chat_title=%s chat_type=%s is_active=%s",
            normalized_chat_id,
            normalized_chat_title,
            normalized_chat_type,
            is_active,
        )
        if not getattr(db, "supabase", None):
            logger.warning(
                "telegram bot chat registry skipped write: supabase is not configured chat_id=%s chat_title=%s chat_type=%s",
                normalized_chat_id,
                normalized_chat_title,
                normalized_chat_type,
            )
            return
        payload = {
            "provider": "telegram",
            "chat_id": normalized_chat_id,
            "chat_title": normalized_chat_title,
            "chat_type": normalized_chat_type,
            "is_active": bool(is_active),
        }
        try:
            db.supabase.table(_TELEGRAM_REGISTRY_TABLE).upsert(payload, on_conflict="provider,chat_id").execute()
        except Exception:
            logger.exception(
                "telegram bot chat registry write failed chat_id=%s chat_title=%s chat_type=%s is_active=%s",
                normalized_chat_id,
                normalized_chat_title,
                normalized_chat_type,
                is_active,
            )

    @staticmethod
    def mark_telegram_chat_inactive(chat_id: str | int | None, *, reason: str) -> None:
        normalized_chat_id = str(chat_id or "").strip()
        if not normalized_chat_id:
            return
        logger.warning("telegram bot chat registry mark inactive chat_id=%s reason=%s", normalized_chat_id, reason)
        if not getattr(db, "supabase", None):
            return
        try:
            db.supabase.table(_TELEGRAM_REGISTRY_TABLE).update({"is_active": False}).eq("provider", "telegram").eq("chat_id", normalized_chat_id).execute()
        except Exception:
            logger.exception(
                "telegram bot chat registry mark inactive failed chat_id=%s reason=%s",
                normalized_chat_id,
                reason,
            )
