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
SHOP_RENDER_INSTRUCTION = "Выберите товар ниже: короткая кнопка открывает карточку товара с описанием."
SHOP_RENDER_ERROR_TEXT = (
    "🛒 <b>Магазин</b>\n"
    "Категория: <b>Роли</b>\n"
    "Баланс: <b>0 баллов</b>\n"
    "Выберите товар ниже: короткая кнопка открывает карточку товара с описанием."
)
SHOP_PAGE_SIZE = 8


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


@dataclass(frozen=True)
class ShopItem:
    shop_item_id: str
    role_name: str
    short_name: str
    category: str
    position: int
    category_position: int
    description: str
    acquire_hint: str


@dataclass(frozen=True)
class ShopPageSlice:
    items: list[ShopItem]
    page: int
    total_pages: int


def build_shop_profile_required_text(register_command: str) -> str:
    command = str(register_command or "/register").strip() or "/register"
    return SHOP_PROFILE_REQUIRED_TEXT.format(register_command=command)


def _short_role_name(value: str, *, limit: int = 24) -> str:
    name = str(value or "").strip() or "Без названия"
    return name if len(name) <= limit else f"{name[: limit - 1]}…"


def _normalize_shop_page(requested_page: int, total_items: int, *, page_size: int = SHOP_PAGE_SIZE) -> int:
    max_page = max((int(total_items) - 1) // max(int(page_size), 1), 0)
    return min(max(int(requested_page), 0), max_page)


def get_shop_catalog_items(*, log_context: str = "shop") -> list[ShopItem]:
    grouped = RoleManagementService.list_public_roles_catalog(log_context=f"{log_context}:catalog")
    items: list[ShopItem] = []
    for category in grouped:
        category_name = str(category.get("category") or "Без категории").strip() or "Без категории"
        category_position = int(category.get("position") or 0)
        category_roles = list(category.get("roles") or [])
        ordered_roles = sorted(category_roles, key=lambda role: (int(role.get("position") or 0), str(role.get("name") or "").lower()))
        for role in ordered_roles:
            role_name = str(role.get("name") or "").strip()
            if not role_name:
                logger.error("shop_position_inconsistency reason=missing_role_name category=%s log_context=%s", category_name, log_context)
                continue
            role_position = int(role.get("position") or 0)
            if role_position < 0:
                logger.error(
                    "shop_position_inconsistency reason=negative_role_position category=%s role_name=%s position=%s log_context=%s",
                    category_name,
                    role_name,
                    role_position,
                    log_context,
                )
            shop_item_id = f"{category_name}:{role_name}".lower()
            items.append(
                ShopItem(
                    shop_item_id=shop_item_id,
                    role_name=role_name,
                    short_name=_short_role_name(role_name),
                    category=category_name,
                    position=role_position,
                    category_position=category_position,
                    description=str(role.get("description") or "").strip(),
                    acquire_hint=str(role.get("acquire_hint") or "").strip(),
                )
            )

    items.sort(key=lambda item: (item.category_position, item.position, item.role_name.lower()))
    indexed_ids: list[ShopItem] = []
    for index, item in enumerate(items):
        indexed_ids.append(
            ShopItem(
                shop_item_id=f"shop_{index + 1}",
                role_name=item.role_name,
                short_name=item.short_name,
                category=item.category,
                position=item.position,
                category_position=item.category_position,
                description=item.description,
                acquire_hint=item.acquire_hint,
            )
        )
    return indexed_ids


def get_shop_page_slice(items: list[ShopItem], requested_page: int, *, page_size: int = SHOP_PAGE_SIZE) -> ShopPageSlice:
    safe_page = _normalize_shop_page(requested_page, len(items), page_size=page_size)
    safe_page_size = max(int(page_size), 1)
    total_pages = max((len(items) - 1) // safe_page_size + 1, 1)
    start = safe_page * safe_page_size
    return ShopPageSlice(items=items[start : start + safe_page_size], page=safe_page, total_pages=total_pages)


def find_shop_item(items: list[ShopItem], shop_item_id: str) -> ShopItem | None:
    target = str(shop_item_id or "").strip()
    if not target:
        return None
    for item in items:
        if item.shop_item_id == target:
            return item
    logger.error("shop_pagination_error reason=item_not_found shop_item_id=%s items_count=%s", target, len(items))
    return None


def build_shop_render_payload(account_id: str | None) -> ShopRenderPayload:
    try:
        points = "0"
        if account_id:
            profile = AccountsService.get_profile_by_account(str(account_id)) or {}
            points = str(profile.get("points") or "0").strip() or "0"
        catalog = get_shop_catalog_items(log_context="shop:/shop")
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
