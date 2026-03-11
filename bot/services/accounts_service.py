import logging
import os
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
    PROFILE_FIELDS_CONFIG = {
        "custom_nick": {"default": "Игрок", "max_length": 32, "label": "Никнейм"},
        "description": {"default": "—", "max_length": 100, "label": "Описание"},
        "nulls_brawl_id": {"default": "—", "max_length": 32, "label": "Null's Brawl ID"},
    }
    _account_titles_cache: dict[str, list[str]] = {}
    _title_roles_cache: dict[int, str] | None = None

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

        try:
            identities_response = (
                db.supabase.table("account_identities")
                .select("provider")
                .eq("account_id", str(account_id))
                .execute()
            )
            identities = identities_response.data or []
            if any(identity.get("provider") == target_provider for identity in identities):
                if hasattr(db, "_inc_metric"):
                    db._inc_metric("link_issue_fail")
                return False, f"Аккаунт уже привязан к {target_provider}"
        except Exception as e:
            logger.warning(
                "issue_link_code identity lookup failed source=%s:%s target=%s account_id=%s error=%s",
                source_provider,
                source_provider_user_id,
                target_provider,
                account_id,
                AccountsService._format_db_error(e),
            )

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
                if hasattr(db, "_inc_metric"):
                    db._inc_metric("link_consume_fail")
                logger.error(
                    "consume_link_code refused cross-account merge target_provider=%s target_provider_user_id=%s code=%s existing_account_id=%s requested_account_id=%s",
                    target_provider,
                    target_provider_user_id,
                    code,
                    existing_account_id,
                    account_id,
                )
                return False, "Этот профиль уже привязан к другому общему аккаунту. Сначала отвяжите его через администратора"

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
                            logger.error(
                                "consume_link_code unique violation with cross-account binding target_provider=%s target_provider_user_id=%s code=%s existing_account_id=%s requested_account_id=%s",
                                target_provider,
                                target_provider_user_id,
                                existing_account_id,
                                account_id,
                            )
                            if hasattr(db, "_inc_metric"):
                                db._inc_metric("link_consume_fail")
                            return False, "Этот профиль уже привязан к другому общему аккаунту. Сначала отвяжите его через администратора"
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
    def _format_points(points_value: object) -> str:
        try:
            points_float = float(points_value)
        except (TypeError, ValueError):
            return "0"

        if points_float.is_integer():
            return str(int(points_float))
        return f"{points_float:.2f}".rstrip("0").rstrip(".")

    @staticmethod
    def _load_points_from_actions(account_id: str, discord_identity: Optional[dict]) -> Optional[float]:
        """Возвращает баланс по сумме действий, чтобы не зависеть от дублей в scores."""
        if not db.supabase:
            return None

        action_rows: list[dict] = []
        try:
            action_response = (
                db.supabase.table("actions")
                .select("points")
                .eq("account_id", str(account_id))
                .execute()
            )
            action_rows = action_response.data or []
        except Exception as e:
            logger.exception(
                "get_profile actions read failed account_id=%s error=%s",
                account_id,
                AccountsService._format_db_error(e),
            )

        if not action_rows and discord_identity:
            discord_user_id = discord_identity.get("provider_user_id")
            if discord_user_id:
                try:
                    action_response = (
                        db.supabase.table("actions")
                        .select("points")
                        .eq("user_id", str(discord_user_id))
                        .execute()
                    )
                    action_rows = action_response.data or []
                except Exception as e:
                    logger.exception(
                        "get_profile actions fallback failed account_id=%s discord_user_id=%s error=%s",
                        account_id,
                        discord_user_id,
                        AccountsService._format_db_error(e),
                    )

        if action_rows:
            return sum(float(row.get("points") or 0) for row in action_rows)
        return None

    @staticmethod
    def _normalize_profile_field_value(field_name: str, value: object) -> str:
        config = AccountsService.PROFILE_FIELDS_CONFIG.get(field_name, {})
        default_value = str(config.get("default", ""))
        max_length = int(config.get("max_length", 255))
        normalized = str(value or "").strip()
        if not normalized:
            return default_value
        return normalized[:max_length]

    @staticmethod
    def update_profile_field(provider: str, provider_user_id: str, field_name: str, value: str) -> Tuple[bool, str]:
        config = AccountsService.PROFILE_FIELDS_CONFIG.get(field_name)
        if not config:
            logger.warning(
                "update_profile_field unknown field provider=%s provider_user_id=%s field=%s",
                provider,
                provider_user_id,
                field_name,
            )
            return False, "Неизвестное поле профиля"

        account_id = AccountsService.resolve_account_id(provider, provider_user_id)
        if not account_id or not db.supabase:
            logger.warning(
                "update_profile_field failed to resolve account provider=%s provider_user_id=%s field=%s",
                provider,
                provider_user_id,
                field_name,
            )
            return False, "Профиль не найден. Сначала зарегистрируйтесь"

        normalized = AccountsService._normalize_profile_field_value(field_name, value)
        try:
            db.supabase.table("accounts").update({field_name: normalized}).eq("id", str(account_id)).execute()
            return True, f"{config['label']} обновлён"
        except Exception as e:
            logger.exception(
                "update_profile_field failed account_id=%s provider=%s provider_user_id=%s field=%s error=%s",
                account_id,
                provider,
                provider_user_id,
                field_name,
                AccountsService._format_db_error(e),
            )
            return False, "Не удалось обновить профиль"

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
        discord_identity = next((identity for identity in identities if identity.get("provider") == "discord"), None)

        custom_nick = display_name or str(AccountsService.PROFILE_FIELDS_CONFIG["custom_nick"]["default"])
        description = str(AccountsService.PROFILE_FIELDS_CONFIG["description"]["default"])
        nulls_id = str(AccountsService.PROFILE_FIELDS_CONFIG["nulls_brawl_id"]["default"])
        try:
            account_response = (
                db.supabase.table("accounts")
                .select("custom_nick,description,nulls_brawl_id")
                .eq("id", str(account_id))
                .limit(1)
                .execute()
            )
            account_rows = account_response.data or []
            if account_rows:
                account_row = account_rows[0]
                custom_nick = AccountsService._normalize_profile_field_value(
                    "custom_nick",
                    account_row.get("custom_nick") or display_name,
                )
                description = AccountsService._normalize_profile_field_value("description", account_row.get("description"))
                nulls_id = AccountsService._normalize_profile_field_value("nulls_brawl_id", account_row.get("nulls_brawl_id"))
        except Exception as e:
            logger.exception(
                "get_profile account fields read failed account_id=%s provider=%s provider_user_id=%s error=%s",
                account_id,
                provider,
                provider_user_id,
                AccountsService._format_db_error(e),
            )
        nulls_status = "Не подтвержден (заглушка)"
        points = "Привяжите Discord для получения информации (временно)."
        titles: list[str] = AccountsService.get_account_titles(account_id)
        titles_text = (
            ", ".join(titles)
            if titles
            else "Привяжите Discord и/или подтвердите скрином свое звание администрации клуба для получения звания (временно)"
        )
        if not titles:
            logger.info("get_profile no titles yet account_id=%s provider=%s", account_id, provider)

        points_from_actions = AccountsService._load_points_from_actions(str(account_id), discord_identity)
        if points_from_actions is not None:
            points = AccountsService._format_points(points_from_actions)
        else:
            logger.warning(
                "get_profile actions points unavailable account_id=%s; fallback to scores snapshot",
                account_id,
            )

        try:
            points_response = (
                db.supabase.table("scores")
                .select("points")
                .eq("account_id", str(account_id))
                .limit(1)
                .execute()
            )
            points_rows = points_response.data or []
            if not points_rows and discord_identity:
                discord_user_id = discord_identity.get("provider_user_id")
                points_response = (
                    db.supabase.table("scores")
                    .select("points")
                    .eq("user_id", str(discord_user_id))
                    .limit(1)
                    .execute()
                )
                points_rows = points_response.data or []
            if points_from_actions is None:
                points = AccountsService._format_points(points_rows[0].get("points", 0)) if points_rows else "0"
        except Exception as e:
            logger.exception(
                "get_profile points failed account_id=%s provider=%s provider_user_id=%s error=%s",
                account_id,
                provider,
                provider_user_id,
                AccountsService._format_db_error(e),
            )

        return {
            "account_id": account_id,
            "custom_nick": custom_nick,
            "description": description,
            "has_discord": has_discord,
            "has_telegram": has_telegram,
            "link_status": "Привязан" if has_discord and has_telegram else "Не привязан",
            "nulls_brawl_id": nulls_id,
            "nulls_status": nulls_status,
            "points": points,
            "titles": titles,
            "titles_text": titles_text,
        }

    @staticmethod
    def get_account_titles(account_id: str) -> list[str]:
        if not account_id:
            return []

        cached = AccountsService._account_titles_cache.get(str(account_id))
        if cached is not None:
            return list(cached)

        if not db.supabase:
            return []

        try:
            response = (
                db.supabase.table("accounts")
                .select("titles")
                .eq("id", str(account_id))
                .limit(1)
                .execute()
            )
            rows = response.data or []
            if not rows:
                return []

            value = rows[0].get("titles")
            if isinstance(value, list):
                titles = [str(item).strip() for item in value if str(item).strip()]
            elif isinstance(value, str):
                titles = [item.strip() for item in value.split(",") if item.strip()]
            else:
                titles = []

            AccountsService._account_titles_cache[str(account_id)] = list(titles)
            return titles
        except Exception as e:
            logger.warning("get_account_titles failed for %s: %s", account_id, e)
            return []

    @staticmethod
    def save_account_titles(account_id: str, titles: list[str], source: str = "discord") -> bool:
        if not account_id:
            return False

        normalized = [str(item).strip() for item in (titles or []) if str(item).strip()]
        normalized = list(dict.fromkeys(normalized))
        AccountsService._account_titles_cache[str(account_id)] = list(normalized)

        if not db.supabase:
            logger.warning("save_account_titles skipped for account_id=%s source=%s: supabase is unavailable", account_id, source)
            return False

        payload = {
            "titles": normalized,
            "titles_updated_at": datetime.now(timezone.utc).isoformat(),
            "titles_source": source,
        }
        try:
            db.supabase.table("accounts").update(payload).eq("id", str(account_id)).execute()
            return True
        except Exception as e:
            logger.warning("save_account_titles failed for account_id=%s source=%s error=%s", account_id, source, e)
            return False

    @staticmethod
    def get_configured_title_role_ids() -> set[int]:
        configured = AccountsService.get_configured_title_roles()
        if configured:
            return set(configured.keys())

        raw = os.getenv("PROFILE_DISCORD_TITLE_ROLE_IDS", "")
        result: set[int] = set()
        for item in raw.split(","):
            item = item.strip()
            if not item:
                continue
            try:
                result.add(int(item))
            except ValueError:
                logger.warning("PROFILE_DISCORD_TITLE_ROLE_IDS contains invalid role id: %s", item)
        return result

    @staticmethod
    def get_configured_title_role_names() -> set[str]:
        configured = AccountsService.get_configured_title_roles()
        if configured:
            return {title.strip().lower() for title in configured.values() if str(title).strip()}

        raw = os.getenv("PROFILE_DISCORD_TITLE_ROLE_NAMES", "")
        return {item.strip().lower() for item in raw.split(",") if item.strip()}

    @staticmethod
    def get_configured_title_roles() -> dict[int, str]:
        """Return Discord role_id -> profile title mapping from DB with env fallback support."""
        if AccountsService._title_roles_cache is not None:
            return dict(AccountsService._title_roles_cache)

        if not db.supabase:
            AccountsService._title_roles_cache = {}
            return {}

        try:
            response = (
                db.supabase.table("profile_title_roles")
                .select("discord_role_id,title_name,is_active")
                .eq("is_active", True)
                .execute()
            )
            rows = response.data or []
            mapping: dict[int, str] = {}
            for row in rows:
                role_id_value = row.get("discord_role_id")
                title_name = str(row.get("title_name") or "").strip()
                if not role_id_value or not title_name:
                    continue
                try:
                    mapping[int(role_id_value)] = title_name
                except (TypeError, ValueError):
                    logger.warning("profile_title_roles contains invalid discord_role_id=%s", role_id_value)

            AccountsService._title_roles_cache = mapping
            if mapping:
                logger.info("loaded profile title role mappings from db count=%s", len(mapping))
            return dict(mapping)
        except Exception as e:
            logger.warning("get_configured_title_roles failed, fallback to env mapping: %s", e)
            AccountsService._title_roles_cache = {}
            return {}

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
