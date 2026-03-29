import logging
from dataclasses import dataclass

from bot.services import AccountsService, RoleManagementService

logger = logging.getLogger(__name__)

SHOP_PROFILE_REQUIRED_TEXT = (
    "🛒 Магазин доступен после создания профиля.\n"
    "Сделайте 2 шага:\n"
    "1) Откройте личные сообщения с ботом.\n"
    "2) Отправьте команду {register_command}.\n"
    "После этого снова выполните /shop."
)
SHOP_RENDER_TITLE = "Магазин"
SHOP_RENDER_CATEGORY = "Роли"
SHOP_RENDER_INSTRUCTION = "Нажмите на товар, чтобы посмотреть описание и купить."
SHOP_RENDER_ERROR_TEXT = (
    "🛒 <b>Магазин</b>\n"
    "Категория: <b>Роли</b>\n"
    "Баланс: <b>0 баллов</b>\n"
    "Нажмите на товар, чтобы посмотреть описание и купить."
)


@dataclass(frozen=True)
class ShopProfileCheckResult:
    ok: bool
    account_id: str | None = None
    user_message: str | None = None


@dataclass(frozen=True)
class ShopRenderPayload:
    title: str
    category: str
    points: str
    instruction: str

    @property
    def telegram_text(self) -> str:
        return (
            f"🛒 <b>{self.title}</b>\n"
            f"Категория: <b>{self.category}</b>\n"
            f"Баланс: <b>{self.points} баллов</b>\n"
            f"{self.instruction}"
        )

    @property
    def discord_description(self) -> str:
        return (
            f"**{self.title}**\n"
            f"Категория: **{self.category}**\n"
            f"Баланс: **{self.points} баллов**\n"
            f"{self.instruction}"
        )


def build_shop_profile_required_text(register_command: str) -> str:
    command = str(register_command or "/register").strip() or "/register"
    return SHOP_PROFILE_REQUIRED_TEXT.format(register_command=command)


def build_shop_render_payload(account_id: str | None) -> ShopRenderPayload:
    try:
        points = "0"
        if account_id:
            profile = AccountsService.get_profile_by_account(str(account_id)) or {}
            points = str(profile.get("points") or "0").strip() or "0"
        catalog = RoleManagementService.list_public_roles_catalog(log_context="shop:/shop")
        if not catalog:
            logger.warning("shop_empty_catalog provider=shared account_id=%s category=%s", account_id, SHOP_RENDER_CATEGORY)
        return ShopRenderPayload(
            title=SHOP_RENDER_TITLE,
            category=SHOP_RENDER_CATEGORY,
            points=points,
            instruction=SHOP_RENDER_INSTRUCTION,
        )
    except Exception as error:  # noqa: BLE001
        logger.exception("shop_render_error provider=shared account_id=%s error=%s", account_id, error)
        return ShopRenderPayload(
            title=SHOP_RENDER_TITLE,
            category=SHOP_RENDER_CATEGORY,
            points="0",
            instruction=SHOP_RENDER_INSTRUCTION,
        )


def build_shop_prompt_text(account_id: str | None = None) -> str:
    try:
        return build_shop_render_payload(account_id).telegram_text
    except Exception as error:  # noqa: BLE001
        logger.exception("shop_render_error provider=telegram account_id=%s error=%s", account_id, error)
        return SHOP_RENDER_ERROR_TEXT


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
