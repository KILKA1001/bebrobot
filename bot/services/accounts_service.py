import logging
import secrets
import string
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

from bot.data import db

logger = logging.getLogger(__name__)


class AccountsService:
    """Общий сервис аккаунтов/identity/linking без привязки к Discord API."""

    LINK_CODE_LEN = 8
    LINK_TTL_MINUTES = 10
    MAX_ATTEMPTS = 5

    @staticmethod
    def resolve_account_id(provider: str, provider_user_id: str) -> Optional[str]:
        if not db.supabase:
            return None
        try:
            response = (
                db.supabase.table("account_identities")
                .select("account_id")
                .eq("provider", provider)
                .eq("provider_user_id", str(provider_user_id))
                .limit(1)
                .execute()
            )
            if response.data:
                return response.data[0].get("account_id")
        except Exception as e:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("identity_resolve_errors")
            logger.warning("resolve_account_id failed (%s:%s): %s", provider, provider_user_id, e)
        return None

    @staticmethod
    def resolve_telegram_account_id(telegram_user_id: int) -> Optional[str]:
        return AccountsService.resolve_account_id("telegram", str(telegram_user_id))

    @staticmethod
    def _generate_link_code(length: int = LINK_CODE_LEN) -> str:
        alphabet = string.ascii_uppercase + string.digits
        return "".join(secrets.choice(alphabet) for _ in range(length))

    @staticmethod
    def issue_discord_telegram_link_code(discord_user_id: int) -> Tuple[bool, str]:
        """DS -> one-time code для TG /link <code>."""
        if not db.supabase:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("link_issue_fail")
            return False, "База данных недоступна"

        discord_account_id = AccountsService.resolve_account_id("discord", str(discord_user_id))
        if not discord_account_id:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("link_issue_fail")
            return False, "Discord account identity не найден"

        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(minutes=AccountsService.LINK_TTL_MINUTES)
        code = AccountsService._generate_link_code()

        payload = {
            "code": code,
            "account_id": discord_account_id,
            "discord_user_id": str(discord_user_id),
            "created_at": now.isoformat(),
            "expires_at": expires_at.isoformat(),
            "is_used": False,
            "attempts": 0,
        }

        try:
            db.supabase.table("account_link_codes").insert(payload).execute()
            if hasattr(db, "_inc_metric"):
                db._inc_metric("link_issue_success")
            logger.info("link_code_issued discord_user_id=%s", discord_user_id)
            return True, code
        except Exception as e:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("link_issue_fail")
            logger.error("issue_discord_telegram_link_code failed: %s", e)
            return False, "Не удалось создать код привязки"

    @staticmethod
    def consume_telegram_link_code(telegram_user_id: int, code: str) -> Tuple[bool, str]:
        """TG /link <code> -> bind telegram identity to account."""
        if not db.supabase:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("link_consume_fail")
            return False, "База данных недоступна"
        code = (code or "").strip().upper()
        if not code:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("link_consume_fail")
            return False, "Пустой код"

        try:
            lookup = (
                db.supabase.table("account_link_codes")
                .select("*")
                .eq("code", code)
                .limit(1)
                .execute()
            )
            if not lookup.data:
                
                if hasattr(db, "_inc_metric"):
                    db._inc_metric("link_consume_fail")
                return False, "Код не найден"

            row = lookup.data[0]
            now = datetime.now(timezone.utc)
            expires_at = datetime.fromisoformat(row["expires_at"].replace("Z", "+00:00"))
            attempts = int(row.get("attempts", 0) or 0)

            if row.get("is_used"):
                
                if hasattr(db, "_inc_metric"):
                    db._inc_metric("link_consume_fail")
                return False, "Код уже использован"
            if now > expires_at:
                
                if hasattr(db, "_inc_metric"):
                    db._inc_metric("link_consume_fail")
                return False, "Срок действия кода истёк"
            if attempts >= AccountsService.MAX_ATTEMPTS:
                
                if hasattr(db, "_inc_metric"):
                    db._inc_metric("link_consume_fail")
                return False, "Превышено число попыток"

            # one-time attempt accounting
            db.supabase.table("account_link_codes").update({"attempts": attempts + 1}).eq("code", code).execute()

            account_id = row.get("account_id")
            if not account_id:
                
                if hasattr(db, "_inc_metric"):
                    db._inc_metric("link_consume_fail")
                return False, "Код не содержит account_id"

            identity_payload = {
                "account_id": account_id,
                "provider": "telegram",
                "provider_user_id": str(telegram_user_id),
            }
            db.supabase.table("account_identities").upsert(identity_payload).execute()

            db.supabase.table("account_link_codes").update(
                {
                    "is_used": True,
                    "used_at": now.isoformat(),
                    "used_by_provider": "telegram",
                    "used_by_provider_user_id": str(telegram_user_id),
                }
            ).eq("code", code).execute()

            if hasattr(db, "_inc_metric"):
                db._inc_metric("link_consume_success")
            logger.info("link_code_consumed telegram_user_id=%s account_id=%s", telegram_user_id, account_id)
            return True, "Аккаунт успешно привязан"
        except Exception as e:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("link_consume_fail")
            logger.error("consume_telegram_link_code failed: %s", e)
            return False, "Ошибка привязки"
