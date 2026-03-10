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
    LINK_CODES_TABLES = ("account_link_codes", "link_tokens")

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
    def _format_db_error(error: Exception) -> str:
        """Return readable Supabase/PostgREST error details for logs."""
        if error is None:
            return "unknown error"
        parts = [str(error)]
        for attr in ("message", "details", "hint", "code"):
            value = getattr(error, attr, None)
            if value:
                parts.append(f"{attr}={value}")
        return " | ".join(p for p in parts if p)

    @staticmethod
    def _is_unique_violation(error: Exception) -> bool:
        return str(getattr(error, "code", "")) == "23505" or "duplicate key value" in str(error).lower()


    @staticmethod
    def _rebind_account_id(from_account_id: str, to_account_id: str) -> None:
        if not from_account_id or not to_account_id or str(from_account_id) == str(to_account_id):
            return

        db.supabase.table("account_identities").update({"account_id": to_account_id}).eq("account_id", from_account_id).execute()

        for table_name in ("scores", "actions", "ticket_actions", "bank_history", "fines", "fine_payments"):
            try:
                db.supabase.table(table_name).update({"account_id": to_account_id}).eq("account_id", from_account_id).execute()
            except Exception as e:
                logger.debug(
                    "rebind_account_id skipped table=%s from_account_id=%s to_account_id=%s error=%s",
                    table_name,
                    from_account_id,
                    to_account_id,
                    AccountsService._format_db_error(e),
                )

        try:
            db.supabase.table("accounts").delete().eq("id", from_account_id).execute()
        except Exception as e:
            logger.debug(
                "rebind_account_id accounts cleanup skipped from_account_id=%s to_account_id=%s error=%s",
                from_account_id,
                to_account_id,
                AccountsService._format_db_error(e),
            )

    @staticmethod
    def _create_account() -> Optional[str]:
        if not db.supabase:
            return None

        try:
            created = db.supabase.table("accounts").insert({}, returning="representation").execute()
            if created.data:
                account_id = created.data[0].get("id")
                if account_id:
                    return str(account_id)
        except TypeError:
            try:
                created = db.supabase.table("accounts").insert({}).execute()
                if created.data:
                    account_id = created.data[0].get("id")
                    if account_id:
                        return str(account_id)
            except Exception as e:
                logger.error("create account failed (legacy): %s", e)
        except Exception as e:
            logger.error("create account failed: %s", e)

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
            return True, "Уже зарегистрирован"

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
            return True, "Регистрация завершена"
        except Exception as e:
            logger.error("register identity failed (%s:%s): %s", provider, provider_user_id, e)
            return False, "Не удалось создать привязку identity"

    @staticmethod
    def _insert_link_code_with_fallback(payload: dict, source_provider: str, source_provider_user_id: str) -> bool:
        payload_variants = [
            payload,
            {
                "code": payload["code"],
                "account_id": payload["account_id"],
                "discord_user_id": source_provider_user_id if source_provider == "discord" else None,
                "created_at": payload["created_at"],
                "expires_at": payload["expires_at"],
                "is_used": payload["is_used"],
                "attempts": payload["attempts"],
            },
            {
                "code": payload["code"],
                "account_id": payload["account_id"],
                "created_at": payload["created_at"],
                "expires_at": payload["expires_at"],
                "is_used": payload["is_used"],
                "attempts": payload["attempts"],
            },
        ]

        table_errors: dict[str, str] = {}

        for table_name in AccountsService.LINK_CODES_TABLES:
            for variant in payload_variants:
                try:
                    db.supabase.table(table_name).insert(variant, returning="minimal").execute()
                    return True
                except TypeError:
                    try:
                        db.supabase.table(table_name).insert(variant).execute()
                        return True
                    except Exception as legacy_error:
                        table_errors[table_name] = AccountsService._format_db_error(legacy_error)
                        logger.warning(
                            "link code insert failed (legacy) table=%s payload_keys=%s error=%s",
                            table_name,
                            sorted(variant.keys()),
                            table_errors[table_name],
                        )
                        continue
                except Exception as e:
                    table_errors[table_name] = AccountsService._format_db_error(e)
                    logger.warning(
                        "link code insert failed table=%s payload_keys=%s error=%s",
                        table_name,
                        sorted(variant.keys()),
                        table_errors[table_name],
                    )
                    continue

            if table_name in table_errors:
                logger.error(
                    "link code table rejected all payload variants table=%s source=%s:%s last_error=%s",
                    table_name,
                    source_provider,
                    source_provider_user_id,
                    table_errors[table_name],
                )

        logger.error(
            "failed to persist link code in all candidate tables=%s for source=%s:%s errors=%s",
            AccountsService.LINK_CODES_TABLES,
            source_provider,
            source_provider_user_id,
            table_errors,
        )
        return False

    @staticmethod
    def _find_link_code(code: str):
        for table_name in AccountsService.LINK_CODES_TABLES:
            try:
                lookup = db.supabase.table(table_name).select("*").eq("code", code).limit(1).execute()
                if lookup.data:
                    return table_name, lookup.data[0]
            except Exception as e:
                logger.warning(
                    "link code lookup failed table=%s code=%s error=%s",
                    table_name,
                    code,
                    AccountsService._format_db_error(e),
                )
                continue
        logger.warning("link code not found in candidate tables=%s code=%s", AccountsService.LINK_CODES_TABLES, code)
        return None, None

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

        reusable_code = AccountsService._find_reusable_link_code(
            str(account_id),
            source_provider,
            source_provider_user_id,
            target_provider,
        )
        if reusable_code:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("link_issue_success")
            return True, reusable_code

        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(minutes=AccountsService.LINK_TTL_MINUTES)

        for _ in range(AccountsService.LINK_CODE_GENERATION_ATTEMPTS):
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

            if AccountsService._insert_link_code_with_fallback(payload, source_provider, source_provider_user_id):
                if hasattr(db, "_inc_metric"):
                    db._inc_metric("link_issue_success")
                return True, code

        if hasattr(db, "_inc_metric"):
            db._inc_metric("link_issue_fail")
        return False, "Не удалось создать код привязки"

    @staticmethod
    def _safe_update_code(table_name: str, code: str, payloads: list[dict]) -> bool:
        for payload in payloads:
            try:
                update_response = db.supabase.table(table_name).update(payload).eq("code", code).execute()
                if update_response.data:
                    return True
                logger.warning(
                    "link code update affected zero rows table=%s code=%s payload_keys=%s",
                    table_name,
                    code,
                    sorted(payload.keys()),
                )
            except Exception as e:
                logger.warning(
                    "link code update failed table=%s code=%s payload_keys=%s error=%s",
                    table_name,
                    code,
                    sorted(payload.keys()),
                    AccountsService._format_db_error(e),
                )
                continue
        logger.warning("all link code updates failed for table=%s code=%s", table_name, code)
        return False

    @staticmethod
    def _find_reusable_link_code(
        account_id: str,
        source_provider: str,
        source_provider_user_id: str,
        target_provider: str,
    ) -> Optional[str]:
        now = datetime.now(timezone.utc)

        for table_name in AccountsService.LINK_CODES_TABLES:
            query_variants = [
                [
                    ("account_id", account_id),
                    ("source_provider", source_provider),
                    ("source_provider_user_id", source_provider_user_id),
                    ("target_provider", target_provider),
                    ("is_used", False),
                ],
                [("account_id", account_id), ("is_used", False)],
            ]

            for filters in query_variants:
                try:
                    query = db.supabase.table(table_name).select("*")
                    for key, value in filters:
                        query = query.eq(key, value)
                    lookup = query.limit(20).execute()
                except Exception:
                    continue

                for row in lookup.data or []:
                    code = str(row.get("code") or "").strip().upper()
                    expires_at_raw = row.get("expires_at")
                    if not code or not expires_at_raw:
                        continue
                    try:
                        expires_at = datetime.fromisoformat(str(expires_at_raw).replace("Z", "+00:00"))
                    except ValueError:
                        continue
                    if now <= expires_at and not row.get("is_used"):
                        return code

        return None

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
            logger.warning(
                "consume_link_code invalid params target_provider=%s target_provider_user_id=%s code=%s",
                target_provider,
                target_provider_user_id,
                code,
            )
            return False, "Некорректные параметры привязки"
        if not code:
            logger.warning(
                "consume_link_code empty code target_provider=%s target_provider_user_id=%s",
                target_provider,
                target_provider_user_id,
            )
            return False, "Пустой код"

        try:
            table_name, row = AccountsService._find_link_code(code)
            if not row:
                if hasattr(db, "_inc_metric"):
                    db._inc_metric("link_consume_fail")
                logger.warning(
                    "consume_link_code code not found target_provider=%s target_provider_user_id=%s code=%s",
                    target_provider,
                    target_provider_user_id,
                    code,
                )
                return False, "Код не найден"

            now = datetime.now(timezone.utc)
            expires_at_raw = row.get("expires_at")
            if not expires_at_raw:
                logger.warning("consume_link_code corrupted code without expires_at code=%s table=%s", code, table_name)
                return False, "Код повреждён"
            expires_at = datetime.fromisoformat(str(expires_at_raw).replace("Z", "+00:00"))
            attempts = int(row.get("attempts", 0) or 0)

            if row.get("is_used"):
                if hasattr(db, "_inc_metric"):
                    db._inc_metric("link_consume_fail")
                logger.warning("consume_link_code code already used code=%s table=%s", code, table_name)
                return False, "Код уже использован"
            if now > expires_at:
                if hasattr(db, "_inc_metric"):
                    db._inc_metric("link_consume_fail")
                logger.warning("consume_link_code code expired code=%s expires_at=%s now=%s", code, expires_at.isoformat(), now.isoformat())
                return False, "Срок действия кода истёк"
            if attempts >= AccountsService.MAX_ATTEMPTS:
                if hasattr(db, "_inc_metric"):
                    db._inc_metric("link_consume_fail")
                logger.warning("consume_link_code attempts exceeded code=%s attempts=%s", code, attempts)
                return False, "Превышено число попыток"

            expected_target = row.get("target_provider")
            if expected_target and expected_target != target_provider:
                logger.warning(
                    "consume_link_code wrong target expected=%s actual=%s code=%s",
                    expected_target,
                    target_provider,
                    code,
                )
                return False, f"Этот код предназначен для {expected_target}"

            AccountsService._safe_update_code(table_name, code, [{"attempts": attempts + 1}])

            account_id = row.get("account_id")
            if not account_id:
                logger.warning("consume_link_code missing account_id code=%s table=%s", code, table_name)
                return False, "Код не содержит account_id"

            existing_account_id = AccountsService.resolve_account_id(target_provider, target_provider_user_id)
            if existing_account_id and str(existing_account_id) != str(account_id):
                logger.info(
                    "consume_link_code merging duplicate accounts provider=%s provider_user_id=%s from_account_id=%s to_account_id=%s",
                    target_provider,
                    target_provider_user_id,
                    existing_account_id,
                    account_id,
                )
                AccountsService._rebind_account_id(str(existing_account_id), str(account_id))
                existing_account_id = str(account_id)

            if not existing_account_id:
                identity_payload = {
                    "account_id": account_id,
                    "provider": target_provider,
                    "provider_user_id": target_provider_user_id,
                }
                try:
                    db.supabase.table("account_identities").upsert(
                        identity_payload,
                        on_conflict="provider,provider_user_id",
                    ).execute()
                except TypeError:
                    db.supabase.table("account_identities").upsert(identity_payload).execute()
                except Exception as e:
                    if AccountsService._is_unique_violation(e):
                        existing_account_id = AccountsService.resolve_account_id(target_provider, target_provider_user_id)
                        if existing_account_id and str(existing_account_id) != str(account_id):
                            logger.info(
                                "consume_link_code merging duplicate accounts after retry provider=%s provider_user_id=%s from_account_id=%s to_account_id=%s",
                                target_provider,
                                target_provider_user_id,
                                existing_account_id,
                                account_id,
                            )
                            AccountsService._rebind_account_id(str(existing_account_id), str(account_id))
                    else:
                        raise

            is_marked_used = AccountsService._safe_update_code(
                table_name,
                code,
                [
                    {
                        "is_used": True,
                        "used_at": now.isoformat(),
                        "used_by_provider": target_provider,
                        "used_by_provider_user_id": target_provider_user_id,
                    },
                    {"is_used": True, "used_at": now.isoformat()},
                ],
            )
            if not is_marked_used:
                if hasattr(db, "_inc_metric"):
                    db._inc_metric("link_consume_fail")
                logger.error("consume_link_code failed to mark code as used code=%s table=%s", code, table_name)
                return False, "Не удалось завершить привязку, попробуйте позже"

            if hasattr(db, "_inc_metric"):
                db._inc_metric("link_consume_success")
            return True, "Аккаунт успешно привязан"
        except Exception as e:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("link_consume_fail")
            logger.exception(
                "consume_link_code failed target_provider=%s target_provider_user_id=%s code=%s error=%s",
                target_provider,
                target_provider_user_id,
                code,
                AccountsService._format_db_error(e),
            )
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

        has_discord = any(identity.get("provider") == "discord" for identity in identities)
        has_telegram = any(identity.get("provider") == "telegram" for identity in identities)

        custom_nick = display_name or "Пользователь"
        description = "Описание не заполнено"
        nulls_id = "—"
        nulls_status = "Не подтвержден (заглушка)"

        return {
            "account_id": account_id,
            "custom_nick": custom_nick,
            "description": description,
            "has_discord": has_discord,
            "has_telegram": has_telegram,
            "link_status": "Привязан" if has_discord and has_telegram else "Не привязан",
            "nulls_brawl_id": nulls_id,
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
