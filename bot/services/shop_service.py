import logging
from dataclasses import dataclass

from bot.services import AccountsService, PointsService, RoleManagementService
from bot.services.ux_texts import compose_three_block_plain
from bot.utils.roles_and_activities import ROLE_THRESHOLDS

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
SHOP_RENDER_INSTRUCTION = (
    "Выберите роль кнопкой ниже и откройте карточку с описанием.\n"
    "Проверьте цену и нажмите «Купить», если готовы."
)
SHOP_RENDER_ERROR_TEXT = (
    "🛒 <b>Магазин</b>\n"
    "Категория: <b>Роли</b>\n"
    "Баланс: <b>0 баллов</b>\n"
    "Выберите роль кнопкой ниже и откройте карточку с описанием.\n"
    "Проверьте цену и нажмите «Купить», если готовы."
)
SHOP_PAGE_SIZE = 8
SHOP_TEXT_ITEM_PLACEHOLDER = "Описание скоро добавим."
SHOP_TEXT_ACQUIRE_HINT_PLACEHOLDER = "Подсказка по получению скоро появится."
SHOP_TEXT_CATEGORIES_HINT = compose_three_block_plain(
    what="Это стартовый экран магазина.",
    now="Выберите категорию кнопкой ниже.",
    next_step="Откроется список ролей для покупки.",
)
SHOP_TEXT_LIST_HINT = compose_three_block_plain(
    what="Это список ролей в магазине.",
    now="Нажмите на роль, чтобы открыть карточку.",
    next_step="Вы увидите цену и кнопку покупки.",
)
SHOP_TEXT_CARD_HINT = compose_three_block_plain(
    what="Это карточка выбранной роли.",
    now="Проверьте цену и условия, затем нажмите «Купить».",
    next_step="Откроется подтверждение покупки.",
)
SHOP_TEXT_CONFIRM_PURCHASE = compose_three_block_plain(
    what="Это подтверждение покупки.",
    now="Нажмите «Подтвердить покупку».",
    next_step="Роль будет выдана, а баллы спишутся.",
    emoji="⚠️",
)
SHOP_TEXT_ITEM_UNAVAILABLE = "❌ Этот товар сейчас недоступен. Что дальше: вернитесь в список и выберите другой."
SHOP_TEXT_ROLE_NOT_SELLABLE = "❌ Этот товар сейчас недоступен. Что дальше: вернитесь в список и выберите другой."
SHOP_TEXT_ROLE_CHAIN_REQUIRED = "❌ Для покупки роли «{target}» сначала купите роль «{required}». Что дальше: вернитесь в список и купите предыдущую роль цепочки."
SHOP_TEXT_PRICE_CHANGED = "❌ Цена изменилась. Что дальше: вернитесь в список и откройте товар ещё раз."
SHOP_TEXT_ALREADY_OWNED = "ℹ️ Эта роль уже есть у вас."
SHOP_TEXT_INSUFFICIENT_POINTS = "❌ Не хватает баллов: нужно {required}, у вас {current}. Что дальше: выберите роль подешевле или накопите баллы."
SHOP_TEXT_DEBIT_FAILED = "❌ Пока не удалось завершить покупку. Что дальше: попробуйте ещё раз чуть позже."
SHOP_TEXT_GRANT_FAILED = "❌ Покупка не завершена, баллы уже возвращены. Что дальше: попробуйте ещё раз позже."
SHOP_TEXT_UNEXPECTED_ERROR = "❌ Пока не удалось завершить покупку. Что дальше: попробуйте ещё раз чуть позже."
SHOP_TEXT_PROTECTED_FAILURE = compose_three_block_plain(
    what="Экран магазина временно недоступен.",
    now="Нажмите «Обновить» или вернитесь к категориям.",
    next_step="После обновления вы снова увидите кнопки магазина.",
)
SHOP_TEXT_PURCHASE_SUCCESS = "✅ Готово! Роль «{role}» куплена за {points} баллов. Что дальше: можете вернуться в категории и выбрать ещё роль."
SHOP_TEXT_PAGINATION_ERROR = "Не получилось обновить список. Что дальше: нажмите «Обновить»."
SHOP_TEXT_ITEM_OPEN_ERROR = "Не получилось открыть карточку товара. Что дальше: нажмите «Обновить»."
SHOP_TEXT_ITEM_NOT_FOUND = compose_three_block_plain(
    what="Эта роль сейчас не найдена в каталоге.",
    now="Нажмите «Обновить» и выберите роль снова.",
    next_step="Откроется актуальная карточка роли.",
)


SHOP_FLOW_PARITY_MATRIX: tuple[dict[str, str], ...] = (
    {"scenario": "shop_entry", "description": "Вход в магазин из /shop", "platform_diff": "none"},
    {"scenario": "dm_transfer", "description": "Перенос открытия магазина в ЛС из группы/сервера", "platform_diff": "transport_only"},
    {"scenario": "profile_check", "description": "Проверка профиля перед доступом к магазину", "platform_diff": "none"},
    {"scenario": "category_selection", "description": "Выбор категории/товара через кнопки", "platform_diff": "button_layout_limits"},
    {"scenario": "pagination", "description": "Переход по страницам каталога", "platform_diff": "button_layout_limits"},
    {"scenario": "item_card", "description": "Открытие карточки товара с ценой и описанием", "platform_diff": "text_length_limits"},
    {"scenario": "purchase", "description": "Покупка роли с подтверждением и списанием баллов", "platform_diff": "none"},
    {"scenario": "back_to_shop", "description": "Возврат из карточки товара в общий список", "platform_diff": "none"},
    {"scenario": "admin_settings", "description": "Админ-настройки магазина через roles_admin/rolesadmin", "platform_diff": "none"},
)

SHOP_UX_CHECKLIST: tuple[str, ...] = (
    "Главный экран: пользователь видит следующий шаг — выбрать роль кнопкой.",
    "Карточка роли: пользователь видит цену, описание и кнопку «Купить».",
    "Подтверждение: пользователь понимает, что покупка произойдёт после подтверждения.",
    "Ошибки: каждое сообщение объясняет причину и что делать дальше.",
)

_VOLUNTEER_ROLE_NAMES_BY_DISCORD_ID: dict[int, str] = {
    1212624623548768287: "Бог среди волонтеров",
    1105906637824331788: "Легендарный среди волонтеров",
    1137775519589466203: "Мастер волонтер",
    1105906455233703989: "Хороший Помощник Бебр",
    1105906310131744868: "Новый волонтер",
}
_VOLUNTEER_ROLE_CHAIN_BY_DISCORD_ID: tuple[int, ...] = tuple(
    role_id for role_id, _ in sorted(ROLE_THRESHOLDS.items(), key=lambda item: item[1])
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
    price_points: int
    base_price_points: int
    sale_price_points: int | None
    is_sale_active: bool


@dataclass(frozen=True)
class ShopPageSlice:
    items: list[ShopItem]
    page: int
    total_pages: int


@dataclass(frozen=True)
class ShopPurchaseResult:
    ok: bool
    message: str
    reason: str | None = None
    spent_points: int = 0
    role_name: str | None = None


def build_shop_profile_required_text(register_command: str) -> str:
    command = str(register_command or "/register").strip() or "/register"
    return SHOP_PROFILE_REQUIRED_TEXT.format(register_command=command)


def _short_role_name(value: str, *, limit: int = 24) -> str:
    name = str(value or "").strip() or "Без названия"
    return name if len(name) <= limit else f"{name[: limit - 1]}…"


def _normalize_shop_page(requested_page: int, total_items: int, *, page_size: int = SHOP_PAGE_SIZE) -> int:
    max_page = max((int(total_items) - 1) // max(int(page_size), 1), 0)
    return min(max(int(requested_page), 0), max_page)


def _load_owned_role_names_for_account(account_id: str) -> set[str]:
    try:
        owned_roles = RoleManagementService.get_user_roles_by_account(account_id)
        return {str(role.get("name") or "").strip().lower() for role in owned_roles if str(role.get("name") or "").strip()}
    except Exception as error:  # noqa: BLE001
        logger.exception("shop_owned_roles_load_error account_id=%s error=%s", account_id, error)
        return set()


def _is_volunteer_chain_locked(*, role_name: str, role_meta: dict, owned_roles: set[str]) -> bool:
    required_role_name, required_role_discord_id = _required_previous_volunteer_role(role_name, role_meta)
    if not required_role_name:
        return False
    if required_role_name.lower() in owned_roles:
        return False
    for owned_role_name in owned_roles:
        try:
            owned_role_state = RoleManagementService.get_role(owned_role_name) or {}
            owned_discord_role_id = int(str(owned_role_state.get("discord_role_id") or "").strip())
            if required_role_discord_id is not None and owned_discord_role_id == required_role_discord_id:
                return False
        except (TypeError, ValueError):
            continue
        except Exception as error:  # noqa: BLE001
            logger.exception(
                "shop_chain_role_lookup_error role_name=%s owned_role=%s error=%s",
                role_name,
                owned_role_name,
                error,
            )
    return True


def get_shop_catalog_items(*, log_context: str = "shop", account_id: str | None = None) -> list[ShopItem]:
    items: list[ShopItem] = []
    shop_rows = RoleManagementService.list_active_shop_role_items(category_code="roles")
    if not shop_rows:
        logger.warning("shop_catalog_empty log_context=%s", log_context)
        return []
    grouped = RoleManagementService.list_public_roles_catalog(log_context=f"{log_context}:catalog", only_sellable=False)
    role_lookup: dict[str, dict] = {}
    category_pos: dict[str, int] = {}
    for category in grouped:
        category_name = str(category.get("category") or "Без категории").strip() or "Без категории"
        category_pos[category_name] = int(category.get("position") or 0)
        for role in list(category.get("roles") or []):
            role_name = str(role.get("name") or "").strip()
            if role_name:
                role_lookup[role_name.lower()] = {"role": role, "category": category_name}

    owned_roles: set[str] = set()
    account_key = str(account_id or "").strip()
    if account_key:
        owned_roles = _load_owned_role_names_for_account(account_key)

    hidden_locked_roles = 0
    for row in shop_rows:
        role_name = str(row.get("role_name") or "").strip()
        role_meta = role_lookup.get(role_name.lower())
        if not role_meta:
            logger.error("shop_item_skipped reason=missing_role_catalog role_name=%s log_context=%s", role_name, log_context)
            continue
        role = role_meta["role"]
        category_name = str(role_meta["category"])
        if account_key and _is_volunteer_chain_locked(role_name=role_name, role_meta=role, owned_roles=owned_roles):
            hidden_locked_roles += 1
            logger.info(
                "shop_catalog_hidden_locked_role log_context=%s account_id=%s role_name=%s reason=missing_chain_prerequisite",
                log_context,
                account_key,
                role_name,
            )
            continue
        effective_price = max(int(row.get("effective_price_points") or 0), 0)
        items.append(
            ShopItem(
                shop_item_id=f"{category_name}:{role_name}".lower(),
                role_name=role_name,
                short_name=_short_role_name(role_name),
                category=category_name,
                position=int(row.get("display_position") or 0),
                category_position=category_pos.get(category_name, 0),
                description=str(role.get("description") or "").strip(),
                acquire_hint=str(role.get("acquire_hint") or "").strip(),
                price_points=effective_price,
                base_price_points=max(int(row.get("base_price_points") or 0), 0),
                sale_price_points=None if row.get("sale_price_points") is None else max(int(row.get("sale_price_points") or 0), 0),
                is_sale_active=bool(row.get("is_sale_active")),
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
                price_points=item.price_points,
                base_price_points=item.base_price_points,
                sale_price_points=item.sale_price_points,
                is_sale_active=item.is_sale_active,
            )
        )
    if hidden_locked_roles:
        logger.info(
            "shop_catalog_chain_filter_applied log_context=%s account_id=%s hidden_locked_roles=%s visible_items=%s",
            log_context,
            account_key or None,
            hidden_locked_roles,
            len(indexed_ids),
        )
    return indexed_ids


def _parse_points(value: object) -> float:
    raw = str(value or "0").strip().replace(" ", "").replace(",", ".")
    try:
        return float(raw)
    except (TypeError, ValueError):
        logger.error("shop_points_parse_error value=%s", value)
        return 0.0


def _required_previous_volunteer_role(
    role_name: str,
    role_state: dict,
) -> tuple[str | None, int | None]:
    try:
        role_discord_id = int(str(role_state.get("discord_role_id") or "").strip())
    except (TypeError, ValueError):
        return None, None
    if role_discord_id not in _VOLUNTEER_ROLE_CHAIN_BY_DISCORD_ID:
        return None, None
    role_position = _VOLUNTEER_ROLE_CHAIN_BY_DISCORD_ID.index(role_discord_id)
    if role_position <= 0:
        return None, None
    required_discord_id = _VOLUNTEER_ROLE_CHAIN_BY_DISCORD_ID[role_position - 1]
    required_name = _VOLUNTEER_ROLE_NAMES_BY_DISCORD_ID.get(required_discord_id)
    if not required_name:
        logger.error(
            "shop_purchase_chain_config_error role_name=%s required_discord_role_id=%s reason=missing_role_name",
            role_name,
            required_discord_id,
        )
        return None, None
    return required_name, required_discord_id


def purchase_shop_item(
    *,
    account_id: str,
    shop_item_id: str,
    actor_provider: str,
    actor_user_id: str | int,
    expected_price_points: int | None = None,
) -> ShopPurchaseResult:
    account_key = str(account_id or "").strip()
    item_key = str(shop_item_id or "").strip()
    provider = str(actor_provider or "unknown").strip().lower() or "unknown"
    actor_id = str(actor_user_id or "").strip() or "unknown"

    logger.info(
        "shop_purchase_attempt provider=%s actor_user_id=%s account_id=%s shop_item_id=%s expected_price=%s",
        provider,
        actor_id,
        account_key,
        item_key,
        expected_price_points,
    )

    if not account_key or not item_key:
        logger.error(
            "shop_purchase_reject provider=%s actor_user_id=%s account_id=%s shop_item_id=%s reason=invalid_identity",
            provider,
            actor_id,
            account_key,
            item_key,
        )
        return ShopPurchaseResult(ok=False, message="❌ Не удалось определить профиль или товар.", reason="invalid_identity")

    try:
        current_items = get_shop_catalog_items(log_context=f"shop:purchase:{provider}", account_id=account_key)
        item = find_shop_item(current_items, item_key)
        if not item:
            logger.warning(
                "shop_purchase_reject provider=%s actor_user_id=%s account_id=%s shop_item_id=%s reason=item_unavailable",
                provider,
                actor_id,
                account_key,
                item_key,
            )
            return ShopPurchaseResult(ok=False, message=SHOP_TEXT_ITEM_UNAVAILABLE, reason="item_unavailable")

        logger.info(
            "shop_purchase_audit provider=%s actor_user_id=%s account_id=%s shop_item_id=%s role_name=%s price_points=%s is_sale=%s",
            provider,
            actor_id,
            account_key,
            item_key,
            item.role_name,
            item.price_points,
            item.is_sale_active,
        )

        role_state = RoleManagementService.get_role(item.role_name) or {}
        if not bool(role_state.get("is_sellable")):
            logger.error(
                "shop_purchase_filter_bypass_blocked provider=%s actor_user_id=%s account_id=%s shop_item_id=%s role_name=%s reason=role_not_sellable",
                provider,
                actor_id,
                account_key,
                item_key,
                item.role_name,
            )
            return ShopPurchaseResult(ok=False, message=SHOP_TEXT_ROLE_NOT_SELLABLE, reason="role_not_sellable")

        if expected_price_points is not None and int(expected_price_points) != int(item.price_points):
            logger.warning(
                "shop_purchase_reject provider=%s actor_user_id=%s account_id=%s shop_item_id=%s reason=price_changed expected_price=%s actual_price=%s",
                provider,
                actor_id,
                account_key,
                item_key,
                expected_price_points,
                item.price_points,
            )
            return ShopPurchaseResult(ok=False, message=SHOP_TEXT_PRICE_CHANGED, reason="price_changed")

        owned_roles = {str(role.get("name") or "").strip().lower() for role in RoleManagementService.get_user_roles_by_account(account_key)}
        if item.role_name.lower() in owned_roles:
            logger.info(
                "shop_purchase_reject provider=%s actor_user_id=%s account_id=%s shop_item_id=%s reason=already_owned role_name=%s",
                provider,
                actor_id,
                account_key,
                item_key,
                item.role_name,
            )
            return ShopPurchaseResult(ok=False, message="ℹ️ Эта роль уже есть у вас.", reason="already_owned", role_name=item.role_name)

        required_role_name, required_role_discord_id = _required_previous_volunteer_role(item.role_name, role_state)
        if required_role_name:
            owned_discord_role_ids: set[int] = set()
            for owned_role_name in owned_roles:
                try:
                    owned_role_state = RoleManagementService.get_role(owned_role_name) or {}
                    owned_discord_role_id = int(str(owned_role_state.get("discord_role_id") or "").strip())
                    owned_discord_role_ids.add(owned_discord_role_id)
                except (TypeError, ValueError):
                    continue
                except Exception as error:  # noqa: BLE001
                    logger.exception(
                        "shop_purchase_chain_lookup_error provider=%s actor_user_id=%s account_id=%s owned_role=%s error=%s",
                        provider,
                        actor_id,
                        account_key,
                        owned_role_name,
                        error,
                    )
            has_required_role = required_role_name.lower() in owned_roles or (
                required_role_discord_id is not None and required_role_discord_id in owned_discord_role_ids
            )
            if not has_required_role:
                logger.info(
                    "shop_purchase_reject provider=%s actor_user_id=%s account_id=%s shop_item_id=%s reason=missing_chain_prerequisite role_name=%s required_role=%s",
                    provider,
                    actor_id,
                    account_key,
                    item_key,
                    item.role_name,
                    required_role_name,
                )
                return ShopPurchaseResult(
                    ok=False,
                    message=SHOP_TEXT_ROLE_CHAIN_REQUIRED.format(target=item.role_name, required=required_role_name),
                    reason="missing_chain_prerequisite",
                    role_name=item.role_name,
                )

        profile = AccountsService.get_profile_by_account(account_key) or {}
        current_points = _parse_points(profile.get("points"))
        if current_points < float(item.price_points):
            logger.info(
                "shop_purchase_reject provider=%s actor_user_id=%s account_id=%s shop_item_id=%s reason=insufficient_points current_points=%s required_points=%s",
                provider,
                actor_id,
                account_key,
                item_key,
                current_points,
                item.price_points,
            )
            return ShopPurchaseResult(
                ok=False,
                message=SHOP_TEXT_INSUFFICIENT_POINTS.format(required=item.price_points, current=int(current_points)),
                reason="insufficient_points",
                role_name=item.role_name,
            )

        charged = True
        if item.price_points > 0:
            charged = PointsService.remove_points_by_account(
                account_key,
                float(item.price_points),
                f"Покупка роли в магазине: {item.role_name}",
                account_key,
            )
        if not charged:
            logger.error(
                "shop_purchase_reject provider=%s actor_user_id=%s account_id=%s shop_item_id=%s reason=debit_failed required_points=%s",
                provider,
                actor_id,
                account_key,
                item_key,
                item.price_points,
            )
            return ShopPurchaseResult(ok=False, message=SHOP_TEXT_DEBIT_FAILED, reason="debit_failed")

        grant_result = RoleManagementService.assign_user_role_by_account(
            account_key,
            item.role_name,
            category=item.category,
            actor_account_id=account_key,
            actor_provider=provider,
            actor_user_id=actor_id,
            target_provider=provider,
            target_user_id=actor_id,
            source=f"shop_purchase:{provider}",
        )
        if not bool(grant_result.get("ok")):
            logger.error(
                "shop_role_grant_error provider=%s actor_user_id=%s account_id=%s shop_item_id=%s role_name=%s grant_reason=%s",
                provider,
                actor_id,
                account_key,
                item_key,
                item.role_name,
                grant_result.get("reason"),
            )
            if item.price_points > 0:
                rollback_ok = PointsService.add_points_by_account(
                    account_key,
                    float(item.price_points),
                    f"Откат списания за роль {item.role_name}: не удалось выдать роль",
                    account_key,
                )
                logger.info(
                    "shop_purchase_refund provider=%s actor_user_id=%s account_id=%s shop_item_id=%s amount=%s rollback_ok=%s",
                    provider,
                    actor_id,
                    account_key,
                    item_key,
                    item.price_points,
                    rollback_ok,
                )
            return ShopPurchaseResult(ok=False, message=SHOP_TEXT_GRANT_FAILED, reason="grant_failed")

        logger.info(
            "shop_purchase_audit_result provider=%s actor_user_id=%s account_id=%s shop_item_id=%s role_name=%s price_points=%s is_sale=%s role_grant_ok=true",
            provider,
            actor_id,
            account_key,
            item_key,
            item.role_name,
            item.price_points,
            item.is_sale_active,
        )

        logger.info(
            "shop_purchase_success provider=%s actor_user_id=%s account_id=%s shop_item_id=%s role_name=%s spent_points=%s",
            provider,
            actor_id,
            account_key,
            item_key,
            item.role_name,
            item.price_points,
        )
        return ShopPurchaseResult(
            ok=True,
            message=SHOP_TEXT_PURCHASE_SUCCESS.format(role=item.role_name, points=item.price_points),
            spent_points=item.price_points,
            role_name=item.role_name,
        )
    except Exception as error:  # noqa: BLE001
        logger.exception(
            "shop_purchase_reject provider=%s actor_user_id=%s account_id=%s shop_item_id=%s reason=unexpected_error error=%s",
            provider,
            actor_id,
            account_key,
            item_key,
            error,
        )
        return ShopPurchaseResult(ok=False, message=SHOP_TEXT_PROTECTED_FAILURE, reason="unexpected_error")


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
        catalog = get_shop_catalog_items(log_context="shop:/shop", account_id=account_id)
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
            "shop_profile_check_exception provider=%s platform_user_id=%s error=%s",
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
