import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import discord
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError

from bot.data import db


logger = logging.getLogger(__name__)


class ModerationNotificationsService:
    EVENT_MUTE_STARTED = "mute_started"
    EVENT_MUTE_EXPIRING = "mute_expiring"
    EVENT_MUTE_ENDED = "mute_ended"
    EVENT_FINE_CREATED = "fine_created"
    EVENT_FINE_DUE_SOON = "fine_due_soon"
    EVENT_FINE_OVERDUE = "fine_overdue"
    EVENT_FINE_PAID = "fine_paid"

    TABLE_NAME = "moderation_notification_deliveries"

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _safe_int(value: Any) -> int | None:
        try:
            return int(str(value))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _parse_dt(raw_value: Any) -> datetime | None:
        if not raw_value:
            return None
        try:
            parsed = datetime.fromisoformat(str(raw_value))
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed
        except Exception:
            return None

    @staticmethod
    def _delivery_key(*, event_type: str, provider: str, delivery_chat_id: Any, case_id: Any = None, fine_id: Any = None, mute_id: Any = None) -> str:
        return f"{provider}:{event_type}:case={case_id or 'na'}:fine={fine_id or 'na'}:mute={mute_id or 'na'}:chat={delivery_chat_id or 'na'}"

    @staticmethod
    def _notification_already_sent(dedupe_key: str) -> bool:
        if not db.supabase:
            return False
        try:
            rows = (
                db.supabase.table(ModerationNotificationsService.TABLE_NAME)
                .select("id")
                .eq("dedupe_key", dedupe_key)
                .limit(1)
                .execute()
                .data
                or []
            )
            return bool(rows)
        except Exception:
            logger.exception("notification dedupe lookup failed dedupe_key=%s", dedupe_key)
            return False

    @staticmethod
    def _store_delivery_fact(
        *,
        case_id: Any = None,
        fine_id: Any = None,
        mute_id: Any = None,
        notification_type: str,
        provider: str,
        delivery_chat_id: Any,
        dedupe_key: str,
    ) -> None:
        if not db.supabase:
            logger.error("notification delivery persist skipped: supabase is not initialized")
            return
        payload = {
            "case_id": case_id,
            "fine_id": fine_id,
            "mute_id": mute_id,
            "notification_type": notification_type,
            "provider": provider,
            "delivery_chat_id": str(delivery_chat_id) if delivery_chat_id is not None else None,
            "sent_at": ModerationNotificationsService._now_iso(),
            "dedupe_key": dedupe_key,
        }
        try:
            db.supabase.table(ModerationNotificationsService.TABLE_NAME).insert(payload).execute()
        except Exception:
            logger.exception(
                "notification delivery persist failed provider=%s chat_id=%s target_account_id=%s case_id=%s mute_id=%s fine_id=%s",
                provider,
                delivery_chat_id,
                None,
                case_id,
                mute_id,
                fine_id,
            )

    @staticmethod
    def _resolve_identity(account_id: str | None, provider: str) -> str | None:
        if not db.supabase or not account_id:
            return None
        try:
            rows = (
                db.supabase.table("account_identities")
                .select("provider_user_id")
                .eq("account_id", str(account_id))
                .eq("provider", provider)
                .limit(1)
                .execute()
                .data
                or []
            )
            if rows:
                return str(rows[0].get("provider_user_id") or "").strip() or None
        except Exception:
            logger.exception(
                "notification identity resolve failed provider=%s chat_id=%s target_account_id=%s case_id=%s mute_id=%s fine_id=%s",
                provider,
                None,
                account_id,
                None,
                None,
                None,
            )
        return None

    @staticmethod
    async def _send_discord(bot: discord.Client, chat_id: int, text: str) -> bool:
        channel = bot.get_channel(chat_id)
        if channel is None:
            try:
                channel = await bot.fetch_channel(chat_id)
            except Exception:
                return False
        try:
            await channel.send(text)
            return True
        except Exception:
            return False

    @staticmethod
    async def _send_discord_dm(bot: discord.Client, user_id: int, text: str) -> bool:
        try:
            user = bot.get_user(user_id) or await bot.fetch_user(user_id)
            if not user:
                return False
            await user.send(text)
            return True
        except discord.Forbidden:
            return False
        except Exception:
            return False

    @staticmethod
    async def _send_telegram(bot: Any, chat_id: int, text: str) -> bool:
        try:
            await bot.send_message(chat_id=chat_id, text=text)
            return True
        except (TelegramForbiddenError, TelegramBadRequest):
            return False
        except Exception:
            return False

    @staticmethod
    async def dispatch_notification(
        *,
        runtime_bot: Any,
        provider: str,
        target_account_id: str | None,
        event_type: str,
        message_text: str,
        case_id: Any = None,
        fine_id: Any = None,
        mute_id: Any = None,
        source_chat_id: Any = None,
        requires_chat_delivery: bool = True,
        allow_dm_delivery: bool = True,
    ) -> None:
        normalized_provider = str(provider or "").strip().lower()
        chat_id = ModerationNotificationsService._safe_int(source_chat_id)

        if requires_chat_delivery and chat_id is not None:
            dedupe_key = ModerationNotificationsService._delivery_key(
                event_type=event_type,
                provider=normalized_provider,
                delivery_chat_id=chat_id,
                case_id=case_id,
                fine_id=fine_id,
                mute_id=mute_id,
            )
            if not ModerationNotificationsService._notification_already_sent(dedupe_key):
                try:
                    delivered = False
                    if normalized_provider == "discord" and isinstance(runtime_bot, discord.Client):
                        delivered = await ModerationNotificationsService._send_discord(runtime_bot, chat_id, message_text)
                    elif normalized_provider == "telegram" and runtime_bot is not None:
                        delivered = await ModerationNotificationsService._send_telegram(runtime_bot, chat_id, message_text)
                    if delivered:
                        ModerationNotificationsService._store_delivery_fact(
                            case_id=case_id,
                            fine_id=fine_id,
                            mute_id=mute_id,
                            notification_type=event_type,
                            provider=normalized_provider,
                            delivery_chat_id=chat_id,
                            dedupe_key=dedupe_key,
                        )
                except Exception:
                    logger.exception(
                        "notification delivery failed provider=%s chat_id=%s target_account_id=%s case_id=%s mute_id=%s fine_id=%s",
                        normalized_provider,
                        chat_id,
                        target_account_id,
                        case_id,
                        mute_id,
                        fine_id,
                    )

        if not allow_dm_delivery:
            return

        provider_user_id = ModerationNotificationsService._resolve_identity(target_account_id, normalized_provider)
        recipient_id = ModerationNotificationsService._safe_int(provider_user_id)
        if recipient_id is None:
            logger.warning(
                "notification dm skipped identity not found provider=%s chat_id=%s target_account_id=%s case_id=%s mute_id=%s fine_id=%s",
                normalized_provider,
                chat_id,
                target_account_id,
                case_id,
                mute_id,
                fine_id,
            )
            return

        dm_dedupe_key = ModerationNotificationsService._delivery_key(
            event_type=event_type,
            provider=normalized_provider,
            delivery_chat_id=f"dm:{recipient_id}",
            case_id=case_id,
            fine_id=fine_id,
            mute_id=mute_id,
        )
        if ModerationNotificationsService._notification_already_sent(dm_dedupe_key):
            return

        try:
            delivered_dm = False
            if normalized_provider == "discord" and isinstance(runtime_bot, discord.Client):
                delivered_dm = await ModerationNotificationsService._send_discord_dm(runtime_bot, recipient_id, message_text)
            elif normalized_provider == "telegram" and runtime_bot is not None:
                delivered_dm = await ModerationNotificationsService._send_telegram(runtime_bot, recipient_id, message_text)
            if delivered_dm:
                ModerationNotificationsService._store_delivery_fact(
                    case_id=case_id,
                    fine_id=fine_id,
                    mute_id=mute_id,
                    notification_type=event_type,
                    provider=normalized_provider,
                    delivery_chat_id=f"dm:{recipient_id}",
                    dedupe_key=dm_dedupe_key,
                )
            else:
                logger.warning(
                    "notification dm unavailable provider=%s chat_id=%s target_account_id=%s case_id=%s mute_id=%s fine_id=%s",
                    normalized_provider,
                    recipient_id,
                    target_account_id,
                    case_id,
                    mute_id,
                    fine_id,
                )
        except Exception:
            logger.exception(
                "notification dm failed provider=%s chat_id=%s target_account_id=%s case_id=%s mute_id=%s fine_id=%s",
                normalized_provider,
                recipient_id,
                target_account_id,
                case_id,
                mute_id,
                fine_id,
            )

    @staticmethod
    def build_mute_text(*, reason: str, ends_at: str | None, status_hint: str) -> str:
        until_text = "до отдельного уведомления" if not ends_at else ends_at
        return (
            "🔇 Модерационное уведомление\n"
            f"За что выдан мут: {reason}\n"
            f"Срок мута: {until_text}\n"
            "Что делать дальше: соблюдайте правила, дождитесь окончания и при необходимости обратитесь к модератору.\n"
            f"Где смотреть статус: {status_hint}"
        )

    @staticmethod
    def build_fine_text(*, reason: str, due_date: str | None, amount_text: str, status_hint: str) -> str:
        due_line = due_date or "дата не указана"
        return (
            "💸 Уведомление о штрафе\n"
            f"За что штраф: {reason}\n"
            f"Оплатить до: {due_line}\n"
            f"Сумма: {amount_text}\n"
            "Что делать дальше: оплатите штраф вовремя, чтобы избежать просрочки и дополнительных ограничений.\n"
            f"Где смотреть статус: {status_hint}"
        )

    @staticmethod
    async def reconcile_mutes(runtime_bot: discord.Client) -> None:
        if not db.supabase:
            return
        now = datetime.now(timezone.utc)
        try:
            active_rows = (
                db.supabase.table("moderation_mutes")
                .select("id,case_id,account_id,reason_text,ends_at,is_active")
                .eq("is_active", True)
                .execute()
                .data
                or []
            )
        except Exception:
            logger.exception(
                "mute reconciliation query failed provider=%s chat_id=%s target_account_id=%s case_id=%s mute_id=%s fine_id=%s",
                "discord",
                None,
                None,
                None,
                None,
                None,
            )
            return

        for row in active_rows:
            mute_id = row.get("id")
            case_id = row.get("case_id")
            target_account_id = str(row.get("account_id") or "") or None
            ends_at = ModerationNotificationsService._parse_dt(row.get("ends_at"))
            if ends_at is None:
                continue

            if now + timedelta(minutes=15) >= ends_at > now:
                await ModerationNotificationsService.dispatch_notification(
                    runtime_bot=runtime_bot,
                    provider="discord",
                    target_account_id=target_account_id,
                    event_type=ModerationNotificationsService.EVENT_MUTE_EXPIRING,
                    message_text=ModerationNotificationsService.build_mute_text(
                        reason=str(row.get("reason_text") or "Нарушение правил"),
                        ends_at=ends_at.isoformat(),
                        status_hint="/modstatus",
                    ),
                    case_id=case_id,
                    mute_id=mute_id,
                    source_chat_id=None,
                    requires_chat_delivery=False,
                    allow_dm_delivery=True,
                )

            if ends_at > now:
                continue

            try:
                db.supabase.table("moderation_mutes").update(
                    {"is_active": False}
                ).eq("id", mute_id).eq("is_active", True).execute()
            except Exception:
                logger.exception(
                    "mute reconciliation deactivate failed provider=%s chat_id=%s target_account_id=%s case_id=%s mute_id=%s fine_id=%s",
                    "discord",
                    None,
                    target_account_id,
                    case_id,
                    mute_id,
                    None,
                )
                continue

            case_row = None
            try:
                rows = (
                    db.supabase.table("moderation_cases")
                    .select("source_platform,source_chat_id")
                    .eq("id", case_id)
                    .limit(1)
                    .execute()
                    .data
                    or []
                )
                case_row = rows[0] if rows else None
            except Exception:
                logger.exception(
                    "mute reconciliation case lookup failed provider=%s chat_id=%s target_account_id=%s case_id=%s mute_id=%s fine_id=%s",
                    "discord",
                    None,
                    target_account_id,
                    case_id,
                    mute_id,
                    None,
                )

            provider = str((case_row or {}).get("source_platform") or "discord").lower()
            source_chat_id = (case_row or {}).get("source_chat_id")
            await ModerationNotificationsService.dispatch_notification(
                runtime_bot=runtime_bot,
                provider=provider,
                target_account_id=target_account_id,
                event_type=ModerationNotificationsService.EVENT_MUTE_ENDED,
                message_text=(
                    "✅ Мут завершён\n"
                    f"За что был мут: {str(row.get('reason_text') or 'Нарушение правил')}\n"
                    f"Срок истёк: {ends_at.isoformat()}\n"
                    "Что делать дальше: продолжайте общение без нарушений.\n"
                    "Где смотреть статус: /modstatus"
                ),
                case_id=case_id,
                mute_id=mute_id,
                source_chat_id=source_chat_id,
                requires_chat_delivery=True,
                allow_dm_delivery=True,
            )

    @staticmethod
    async def mute_reconciliation_loop(runtime_bot: discord.Client) -> None:
        while True:
            try:
                await ModerationNotificationsService.reconcile_mutes(runtime_bot)
            except Exception:
                logger.exception(
                    "mute reconciliation loop failed provider=%s chat_id=%s target_account_id=%s case_id=%s mute_id=%s fine_id=%s",
                    "discord",
                    None,
                    None,
                    None,
                    None,
                    None,
                )
            await asyncio.sleep(60)
