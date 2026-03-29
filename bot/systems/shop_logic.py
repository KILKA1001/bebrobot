import logging
from dataclasses import dataclass

from bot.services import AccountsService

logger = logging.getLogger(__name__)

SHOP_PROFILE_REQUIRED_TEXT = (
    "🛒 Магазин доступен после создания профиля.\n"
    "Сделайте 2 шага:\n"
    "1) Откройте личные сообщения с ботом.\n"
    "2) Отправьте команду {register_command}.\n"
    "После этого снова выполните /shop."
)
SHOP_PROMPT_TEXT = (
    "🛒 <b>Магазин</b>\n"
    "Нажмите кнопку <b>«Открыть магазин»</b> ниже.\n"
    "Если кнопка не открывается, используйте подсказку из сообщения и повторите команду /shop."
)


@dataclass(frozen=True)
class ShopProfileCheckResult:
    ok: bool
    account_id: str | None = None
    user_message: str | None = None


def build_shop_profile_required_text(register_command: str) -> str:
    command = str(register_command or "/register").strip() or "/register"
    return SHOP_PROFILE_REQUIRED_TEXT.format(register_command=command)


def build_shop_prompt_text() -> str:
    return SHOP_PROMPT_TEXT


def check_shop_profile_access(provider: str, platform_user_id: str | int | None, *, register_command: str) -> ShopProfileCheckResult:
    normalized_provider = str(provider or "").strip().lower()
    normalized_platform_user_id = str(platform_user_id or "").strip()
    if not normalized_provider or not normalized_platform_user_id:
        logger.error(
            "shop_profile_check_fail provider=%s platform_user_id=%s reason=missing_identity",
            normalized_provider,
            normalized_platform_user_id,
        )
        return ShopProfileCheckResult(ok=False, user_message=build_shop_profile_required_text(register_command))

    try:
        account_id = AccountsService.resolve_account_id(normalized_provider, normalized_platform_user_id)
    except Exception as error:  # noqa: BLE001
        logger.exception(
            "shop profile check failed provider=%s platform_user_id=%s error=%s",
            normalized_provider,
            normalized_platform_user_id,
            error,
        )
        logger.error(
            "shop_profile_check_fail provider=%s platform_user_id=%s reason=resolve_error",
            normalized_provider,
            normalized_platform_user_id,
        )
        return ShopProfileCheckResult(ok=False, user_message=build_shop_profile_required_text(register_command))

    if account_id:
        logger.info(
            "shop_profile_check_pass provider=%s platform_user_id=%s account_id=%s",
            normalized_provider,
            normalized_platform_user_id,
            account_id,
        )
        return ShopProfileCheckResult(ok=True, account_id=account_id)

    logger.info(
        "shop_profile_check_fail provider=%s platform_user_id=%s reason=profile_missing",
        normalized_provider,
        normalized_platform_user_id,
    )
    return ShopProfileCheckResult(ok=False, user_message=build_shop_profile_required_text(register_command))
