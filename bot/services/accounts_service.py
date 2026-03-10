import logging
import secrets
import string
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

from bot.data import db

logger = logging.getLogger(__name__)


class AccountsService:
    """Общий сервис аккаунтов/identity/linking без привязки к API мессенджеров."""

    LINK_CODE_LEN = 8
    LINK_TTL_MINUTES = 10
    MAX_ATTEMPTS = 5
    LINK_CODE_GENERATION_ATTEMPTS = 3

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
    def _generate_link_code(length: int = LINK_CODE_LEN) -> str:
        alphabet = string.ascii_uppercase + string.digits
        return "".join(secrets.choice(alphabet) for _ in range(length))

    @staticmethod
    def _create_account() -> Optional[str]:
        if not db.supabase:
            return None

        try:
            created = db.supabase.table("accounts").insert({}, returning="representation").execute()
        except TypeError:
            created = db.supabase.table("accounts").insert({}).execute()
        except Exception as e:
            logger.error("create account failed: %s", e)
            return None

        if created.data:
            account_id = created.data[0].get("id")
            if account_id:
                return str(account_id)

        # Fallback for RLS/minimal return mode: use explicit uuid and retry insert.
        account_id = str(uuid.uuid4())
        try:
            db.supabase.table("accounts").insert({"id": account_id}, returning="minimal").execute()
            return account_id
        except TypeError:
            db.supabase.table("accounts").insert({"id": account_id}).execute()
            return account_id
        except Exception as e:
            logger.error("create account fallback failed: %s", e)
            return None

    @staticmethod
    def register_identity(provider: str, provider_user_id: str) -> Tuple[bool, str]:
        if not db.supabase:
            return False, "База данных недоступна"

        provider = (provider or "").strip().lower()
        provider_user_id = str(provider_user_id or "").strip()
        if provider not in ("discord", "telegram") or not provider_user_id:
            return False, "Некорректные параметры регистрации"

        existing_account_id = AccountsService.resolve_account_id(provider, provider_user_id)
        if existing_account_id:
            return True, f"Уже зарегистрирован. account_id: {existing_account_id}"

        account_id = AccountsService._create_account()
        if not account_id:
            return False, "Не удалось создать общий аккаунт"

        payload = {
            "account_id": account_id,
            "provider": provider,
            "provider_user_id": provider_user_id,
        }
        try:
            db.supabase.table("account_identities").insert(payload).execute()
            return True, f"Регистрация завершена. account_id: {account_id}"
        except Exception as e:
            logger.error("register identity failed (%s:%s): %s", provider, provider_user_id, e)
            return False, "Не удалось создать привязку identity"

    @staticmethod
    def issue_link_code(source_provider: str, source_provider_user_id: str, target_provider: str) -> Tuple[bool, str]:
        if not db.supabase:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("link_issue_fail")
            return False, "База данных недоступна"

        source_provider = (source_provider or "").strip().lower()
        target_provider = (target_provider or "").strip().lower()
        source_provider_user_id = str(source_provider_user_id or "").strip()

        if source_provider not in ("discord", "telegram") or target_provider not in ("discord", "telegram"):
            return False, "Некорректные параметры провайдеров"
        if source_provider == target_provider:
            return False, "Нельзя привязать аккаунт к тому же провайдеру"

        account_id = AccountsService.resolve_account_id(source_provider, source_provider_user_id)
        if not account_id:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("link_issue_fail")
            return False, "Сначала зарегистрируйтесь в боте"

        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(minutes=AccountsService.LINK_TTL_MINUTES)

        for attempt in range(1, AccountsService.LINK_CODE_GENERATION_ATTEMPTS + 1):
            code = AccountsService._generate_link_code()
            payload = {
                "code": code,
                "account_id": account_id,
                "source_provider": source_provider,
                "source_provider_user_id": source_provider_user_id,
                "target_provider": target_provider,
                "created_at": now.isoformat(),
                "expires_at": expires_at.isoformat(),
                "is_used": False,
                "attempts": 0,
            }
            try:
                db.supabase.table("account_link_codes").insert(payload, returning="minimal").execute()
                if hasattr(db, "_inc_metric"):
                    db._inc_metric("link_issue_success")
                return True, code
            except TypeError:
                try:
                    db.supabase.table("account_link_codes").insert(payload).execute()
                    if hasattr(db, "_inc_metric"):
                        db._inc_metric("link_issue_success")
                    return True, code
                except Exception as nested_e:
                    e = nested_e
            except Exception as e:
                pass

            error_text = str(e).lower()
            is_duplicate_code = "duplicate key" in error_text and "account_link_codes" in error_text
            if is_duplicate_code and attempt < AccountsService.LINK_CODE_GENERATION_ATTEMPTS:
                continue

            if hasattr(db, "_inc_metric"):
                db._inc_metric("link_issue_fail")
            logger.error("issue_link_code failed: %s", e)
            return False, "Не удалось создать код привязки"

        return False, "Не удалось создать код привязки"

    @staticmethod
    def consume_link_code(target_provider: str, target_provider_user_id: str, code: str) -> Tuple[bool, str]:
        if not db.supabase:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("link_consume_fail")
            return False, "База данных недоступна"

        target_provider = (target_provider or "").strip().lower()
        target_provider_user_id = str(target_provider_user_id or "").strip()
        code = (code or "").strip().upper()

        if target_provider not in ("discord", "telegram") or not target_provider_user_id:
            return False, "Некорректные параметры привязки"
        if not code:
            return False, "Пустой код"

        try:
            lookup = db.supabase.table("account_link_codes").select("*").eq("code", code).limit(1).execute()
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

            expected_target = row.get("target_provider")
            if expected_target and expected_target != target_provider:
                return False, f"Этот код предназначен для {expected_target}"

            db.supabase.table("account_link_codes").update({"attempts": attempts + 1}).eq("code", code).execute()

            account_id = row.get("account_id")
            if not account_id:
                return False, "Код не содержит account_id"

            identity_payload = {
                "account_id": account_id,
                "provider": target_provider,
                "provider_user_id": target_provider_user_id,
            }
            db.supabase.table("account_identities").upsert(identity_payload).execute()

            db.supabase.table("account_link_codes").update(
                {
                    "is_used": True,
                    "used_at": now.isoformat(),
                    "used_by_provider": target_provider,
                    "used_by_provider_user_id": target_provider_user_id,
                }
            ).eq("code", code).execute()

            if hasattr(db, "_inc_metric"):
                db._inc_metric("link_consume_success")
            return True, "Аккаунт успешно привязан"
        except Exception as e:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("link_consume_fail")
            logger.error("consume_link_code failed: %s", e)
            return False, "Ошибка привязки"

    @staticmethod
    def issue_discord_telegram_link_code(discord_user_id: int) -> Tuple[bool, str]:
        return AccountsService.issue_link_code("discord", str(discord_user_id), "telegram")

    @staticmethod
    def consume_telegram_link_code(telegram_user_id: int, code: str) -> Tuple[bool, str]:
        return AccountsService.consume_link_code("telegram", str(telegram_user_id), code)

    @staticmethod
    def consume_discord_link_code(discord_user_id: int, code: str) -> Tuple[bool, str]:
        return AccountsService.consume_link_code("discord", str(discord_user_id), code)

    @staticmethod
    def issue_telegram_discord_link_code(telegram_user_id: int) -> Tuple[bool, str]:
        return AccountsService.issue_link_code("telegram", str(telegram_user_id), "discord")

    @staticmethod
    def get_profile(provider: str, provider_user_id: str, display_name: Optional[str] = None) -> Optional[dict]:
        account_id = AccountsService.resolve_account_id(provider, provider_user_id)
        if not account_id or not db.supabase:
            return None

        identities = []
        try:
            response = (
                db.supabase.table("account_identities")
                .select("provider,provider_user_id")
                .eq("account_id", account_id)
                .execute()
            )
            identities = response.data or []
        except Exception as e:
            logger.warning("get_profile identities failed for %s: %s", account_id, e)

        discord_id = None
        telegram_id = None
        for identity in identities:
            if identity.get("provider") == "discord":
                discord_id = identity.get("provider_user_id")
            if identity.get("provider") == "telegram":
                telegram_id = identity.get("provider_user_id")

        custom_nick = display_name or "Пользователь"
        description = "Описание не заполнено"
        nulls_id = "—"
        nulls_status = "Не подтвержден (заглушка)"

        return {
            "account_id": account_id,
            "custom_nick": custom_nick,
            "description": description,
            "discord_id": discord_id,
            "telegram_id": telegram_id,
            "link_status": "Привязан" if discord_id and telegram_id else "Не привязан",
            "nulls_id": nulls_id,
            "nulls_status": nulls_status,
        }

    @staticmethod
    def unlink_identity(provider: str, provider_user_id: str) -> Tuple[bool, str]:
        if not db.supabase:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("unlink_fail")
            return False, "База данных недоступна"

        provider = (provider or "").strip().lower()
        provider_user_id = str(provider_user_id or "").strip()
        if not provider or not provider_user_id:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("unlink_fail")
            return False, "Некорректные параметры unlink"

        try:
            result = (
                db.supabase.table("account_identities")
                .delete()
                .eq("provider", provider)
                .eq("provider_user_id", provider_user_id)
                .execute()
            )
            if not result.data:
                if hasattr(db, "_inc_metric"):
                    db._inc_metric("unlink_fail")
                return False, "Связь не найдена"

            if hasattr(db, "_inc_metric"):
                db._inc_metric("unlink_success")
            logger.info("identity_unlinked provider=%s provider_user_id=%s", provider, provider_user_id)
            return True, "Связь удалена"
        except Exception as e:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("unlink_fail")
            logger.error("unlink_identity failed (%s:%s): %s", provider, provider_user_id, e)
            return False, "Ошибка unlink"
