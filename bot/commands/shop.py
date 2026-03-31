"""
Назначение: модуль "shop" реализует продуктовый контур в зоне Discord.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Discord.
Пользовательский вход: команда /shop и связанный пользовательский сценарий.
"""

import logging

import discord

from bot.commands.base import bot
from bot.services import AuthorityService
from bot.services.shop_service import (
    SHOP_PAGE_SIZE,
    SHOP_TEXT_ACQUIRE_HINT_PLACEHOLDER,
    SHOP_TEXT_CARD_HINT,
    SHOP_TEXT_CATEGORIES_HINT,
    SHOP_TEXT_CONFIRM_PURCHASE,
    SHOP_TEXT_ITEM_NOT_FOUND,
    SHOP_TEXT_ITEM_PLACEHOLDER,
    SHOP_TEXT_LIST_HINT,
    SHOP_TEXT_PROTECTED_FAILURE,
    build_shop_render_payload,
    check_shop_profile_access,
    find_shop_item,
    get_shop_catalog_items,
    get_shop_page_slice,
    purchase_shop_item,
)
from bot.utils import send_temp

logger = logging.getLogger(__name__)

SHOP_OPEN_PROMPT_TEXT = "Откройте магазин в личных сообщениях, я уже отправил вам инструкцию."
DM_FALLBACK_TEXT = (
    "❌ Не удалось отправить инструкцию в личные сообщения.\n"
    "Откройте чат с ботом, включите личные сообщения для сервера и снова выполните /shop."
)


class ShopView(discord.ui.View):
    def __init__(self, *, author_id: int, account_id: str | None, page: int = 0):
        super().__init__(timeout=600)
        self.author_id = int(author_id)
        self.account_id = account_id
        self.page = max(int(page), 0)
        self.total_pages = 1
        self.mode = "categories"
        self.selected_item_id: str | None = None
        self.is_superadmin = AuthorityService.is_super_admin("discord", str(self.author_id))
        self._render()

    def _render(self) -> None:
        if self.mode == "categories":
            self._render_categories()
            return
        if self.mode == "list":
            self._render_grid()
            return
        if self.mode == "card":
            self._render_card()
            return
        self._render_confirm()

    def _render_categories(self) -> None:
        self.clear_items()
        logger.info(
            "ux_screen_open event=ux_screen_open screen=shop_categories provider=discord account_id=%s actor_user_id=%s",
            self.account_id,
            self.author_id,
        )
        logger.info(
            "ux_action_hint_shown event=ux_action_hint_shown screen=shop_categories provider=discord account_id=%s actor_user_id=%s",
            self.account_id,
            self.author_id,
        )
        roles_btn = discord.ui.Button(label="Роли", style=discord.ButtonStyle.primary)
        admin_btn = discord.ui.Button(label="⚙️ Настройка магазина", style=discord.ButtonStyle.secondary, row=1)

        async def roles_cb(interaction: discord.Interaction):
            logger.info(
                "shop_category_selected provider=discord actor_user_id=%s account_id=%s category=roles",
                self.author_id,
                self.account_id,
            )
            try:
                self.mode = "list"
                self.page = 0
                self._render()
                await interaction.response.edit_message(embed=self._list_embed(), view=self)
            except Exception as error:  # noqa: BLE001
                logger.exception(
                    "shop_list_screen_render_error provider=discord actor_user_id=%s action=category_select error=%s",
                    self.author_id,
                    error,
                )
                logger.exception(
                    "ux_render_error event=ux_render_error screen=shop_list provider=discord actor_user_id=%s error=%s",
                    self.author_id,
                    error,
                )
                await interaction.response.send_message(SHOP_TEXT_PROTECTED_FAILURE, ephemeral=True)

        roles_btn.callback = roles_cb
        self.add_item(roles_btn)
        if self.is_superadmin:
            async def admin_cb(interaction: discord.Interaction):
                logger.info("shop_admin_entry_open provider=discord actor_user_id=%s", self.author_id)
                await interaction.response.send_message(
                    "⚙️ Настройка магазина\n\nШаг 1/2: выберите категорию: Роли.\nШаг 2/2: выберите действие изменения витрины.",
                    ephemeral=True,
                )

            admin_btn.callback = admin_cb
            self.add_item(admin_btn)

    def _render_grid(self) -> None:
        self.clear_items()
        logger.info(
            "ux_action_hint_shown event=ux_action_hint_shown screen=shop_list provider=discord account_id=%s actor_user_id=%s",
            self.account_id,
            self.author_id,
        )
        items = get_shop_catalog_items(log_context="shop:discord:view", account_id=self.account_id)
        page_data = get_shop_page_slice(items, self.page, page_size=SHOP_PAGE_SIZE)
        self.page = page_data.page
        self.total_pages = page_data.total_pages

        for idx, item in enumerate(page_data.items):
            button = discord.ui.Button(label=item.short_name, style=discord.ButtonStyle.secondary, row=idx // 4)

            async def on_click(interaction: discord.Interaction, *, shop_item_id: str = item.shop_item_id, current_page: int = self.page):
                await self._on_item_click(interaction, shop_item_id=shop_item_id, page=current_page)

            button.callback = on_click
            self.add_item(button)

        back = discord.ui.Button(label="⬅️ Назад", style=discord.ButtonStyle.primary, row=2, disabled=self.page <= 0)
        page_info = discord.ui.Button(label=f"Стр. {self.page + 1}/{self.total_pages}", style=discord.ButtonStyle.secondary, row=2, disabled=True)
        next_btn = discord.ui.Button(
            label="➡️ Вперёд",
            style=discord.ButtonStyle.primary,
            row=2,
            disabled=self.page >= self.total_pages - 1,
        )
        refresh = discord.ui.Button(label="Обновить", style=discord.ButtonStyle.success, row=2)
        back_to_categories = discord.ui.Button(label="К категориям", style=discord.ButtonStyle.secondary, row=3)

        async def back_cb(interaction: discord.Interaction):
            await self._switch_page(interaction, requested_page=self.page - 1, action="back")

        async def next_cb(interaction: discord.Interaction):
            await self._switch_page(interaction, requested_page=self.page + 1, action="next")

        async def refresh_cb(interaction: discord.Interaction):
            await self._switch_page(interaction, requested_page=self.page, action="refresh")

        async def categories_cb(interaction: discord.Interaction):
            logger.info(
                "shop_back_to_categories provider=discord actor_user_id=%s account_id=%s source=grid",
                self.author_id,
                self.account_id,
            )
            try:
                self.mode = "categories"
                self.selected_item_id = None
                self._render()
                await interaction.response.edit_message(embed=self._category_embed(), view=self)
            except Exception as error:  # noqa: BLE001
                logger.exception(
                    "shop_category_screen_render_error provider=discord actor_user_id=%s source=grid error=%s",
                    self.author_id,
                    error,
                )
                await interaction.response.send_message(SHOP_TEXT_PROTECTED_FAILURE, ephemeral=True)

        back.callback = back_cb
        next_btn.callback = next_cb
        refresh.callback = refresh_cb
        back_to_categories.callback = categories_cb
        self.add_item(back)
        self.add_item(page_info)
        self.add_item(next_btn)
        self.add_item(refresh)
        self.add_item(back_to_categories)

    def _render_card(self) -> None:
        self.clear_items()
        item = self._selected_item()
        if not item:
            self.mode = "list"
            self._render_grid()
            return

        buy_btn = discord.ui.Button(label="Купить", style=discord.ButtonStyle.success)
        back_btn = discord.ui.Button(label="Назад в магазин", style=discord.ButtonStyle.secondary)

        async def buy_cb(interaction: discord.Interaction):
            try:
                self.mode = "confirm"
                self._render()
                await interaction.response.edit_message(embed=self._item_confirm_embed(item), view=self)
            except Exception as error:  # noqa: BLE001
                logger.exception(
                    "shop_confirm_screen_render_error provider=discord actor_user_id=%s shop_item_id=%s error=%s",
                    self.author_id,
                    item.shop_item_id,
                    error,
                )
                await interaction.response.send_message(SHOP_TEXT_PROTECTED_FAILURE, ephemeral=True)

        async def back_cb(interaction: discord.Interaction):
            try:
                self.mode = "list"
                self._render()
                await interaction.response.edit_message(embed=self._list_embed(), view=self)
            except Exception as error:  # noqa: BLE001
                logger.exception(
                    "shop_list_screen_render_error provider=discord actor_user_id=%s action=back_from_card error=%s",
                    self.author_id,
                    error,
                )
                await interaction.response.send_message(SHOP_TEXT_PROTECTED_FAILURE, ephemeral=True)

        buy_btn.callback = buy_cb
        back_btn.callback = back_cb
        self.add_item(buy_btn)
        self.add_item(back_btn)

    def _render_confirm(self) -> None:
        self.clear_items()
        item = self._selected_item()
        if not item:
            self.mode = "list"
            self._render_grid()
            return

        confirm_btn = discord.ui.Button(label="Подтвердить покупку", style=discord.ButtonStyle.danger)
        cancel_btn = discord.ui.Button(label="Отмена", style=discord.ButtonStyle.secondary)

        async def confirm_cb(interaction: discord.Interaction):
            result = purchase_shop_item(
                account_id=str(self.account_id or ""),
                shop_item_id=item.shop_item_id,
                actor_provider="discord",
                actor_user_id=self.author_id,
                expected_price_points=item.price_points,
            )
            if not result.ok:
                logger.warning(
                    "shop_purchase_reject provider=discord actor_user_id=%s account_id=%s shop_item_id=%s reason=%s",
                    self.author_id,
                    self.account_id,
                    item.shop_item_id,
                    result.reason,
                )
                self.mode = "card"
                self._render()
                await interaction.response.edit_message(embed=self._item_embed(item), view=self)
                await interaction.followup.send(result.message, ephemeral=True)
                return

            self.page = 0
            self.mode = "categories"
            self.selected_item_id = None
            self._render()
            logger.info(
                "shop_back_to_categories provider=discord actor_user_id=%s account_id=%s source=purchase_success",
                self.author_id,
                self.account_id,
            )
            embed = self._category_embed()
            embed.description = f"{embed.description}\n\n{result.message}"
            await interaction.response.edit_message(embed=embed, view=self)

        async def cancel_cb(interaction: discord.Interaction):
            try:
                self.mode = "card"
                self._render()
                await interaction.response.edit_message(embed=self._item_embed(item), view=self)
            except Exception as error:  # noqa: BLE001
                logger.exception(
                    "shop_card_screen_render_error provider=discord actor_user_id=%s action=cancel_confirm shop_item_id=%s error=%s",
                    self.author_id,
                    item.shop_item_id,
                    error,
                )
                await interaction.response.send_message(SHOP_TEXT_PROTECTED_FAILURE, ephemeral=True)

        confirm_btn.callback = confirm_cb
        cancel_btn.callback = cancel_cb
        self.add_item(confirm_btn)
        self.add_item(cancel_btn)

    def _selected_item(self):
        items = get_shop_catalog_items(log_context="shop:discord:selected", account_id=self.account_id)
        return find_shop_item(items, self.selected_item_id or "")

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        actor_id = getattr(getattr(interaction, "user", None), "id", None)
        if actor_id != self.author_id:
            await interaction.response.send_message("Эта панель доступна только автору команды /shop.", ephemeral=True)
            return False
        return True

    def _list_embed(self) -> discord.Embed:
        payload = build_shop_render_payload(self.account_id)
        return discord.Embed(
            title=f"{payload.title} — Роли",
            description=(
                f"Выберите роль из списка.\n"
                f"{SHOP_TEXT_LIST_HINT}\n\n"
                f"Страница: **{self.page + 1}/{self.total_pages}**"
            ),
            color=discord.Color.blurple(),
        )

    def _category_embed(self) -> discord.Embed:
        payload = build_shop_render_payload(self.account_id)
        return discord.Embed(
            title=payload.title,
            description=(
                f"Баланс: **{payload.points} баллов**\n"
                f"{SHOP_TEXT_CATEGORIES_HINT}"
            ),
            color=discord.Color.blurple(),
        )

    def _item_embed(self, item) -> discord.Embed:
        payload = build_shop_render_payload(self.account_id)
        description = item.description or SHOP_TEXT_ITEM_PLACEHOLDER
        acquire_hint = item.acquire_hint or SHOP_TEXT_ACQUIRE_HINT_PLACEHOLDER
        price_line = f"Цена: **{item.price_points} баллов**"
        if item.is_sale_active and item.sale_price_points is not None:
            price_line = f"Цена: **{item.price_points} баллов** (акция)\nБазовая цена: ~~{item.base_price_points} баллов~~"
        embed = discord.Embed(title=item.role_name, color=discord.Color.blurple())
        embed.description = (
            f"Баланс: **{payload.points} баллов**\n"
            f"Категория: **{item.category}**\n"
            f"{price_line}\n"
            f"Описание: {description}\n"
            f"Как получить: {acquire_hint}\n"
            f"{SHOP_TEXT_CARD_HINT}"
        )
        return embed

    def _item_confirm_embed(self, item) -> discord.Embed:
        embed = self._item_embed(item)
        embed.description = f"{embed.description}\n\n{SHOP_TEXT_CONFIRM_PURCHASE}"
        return embed

    async def _switch_page(self, interaction: discord.Interaction, *, requested_page: int, action: str) -> None:
        try:
            old_page = self.page
            items = get_shop_catalog_items(log_context="shop:discord:page_switch", account_id=self.account_id)
            page_data = get_shop_page_slice(items, requested_page, page_size=SHOP_PAGE_SIZE)
            self.page = page_data.page
            self.mode = "list"
            self._render()
            logger.info(
                "shop_page_switch provider=discord actor_user_id=%s account_id=%s from_page=%s to_page=%s total_pages=%s action=%s",
                self.author_id,
                self.account_id,
                old_page + 1,
                self.page + 1,
                self.total_pages,
                action,
            )
            await interaction.response.edit_message(embed=self._list_embed(), view=self)
        except Exception as error:  # noqa: BLE001
            logger.exception(
                "shop_list_screen_render_error provider=discord actor_user_id=%s requested_page=%s action=%s error=%s",
                self.author_id,
                requested_page,
                action,
                error,
            )
            await interaction.response.send_message(SHOP_TEXT_PROTECTED_FAILURE, ephemeral=True)

    async def _on_item_click(self, interaction: discord.Interaction, *, shop_item_id: str, page: int) -> None:
        try:
            items = get_shop_catalog_items(log_context="shop:discord:item", account_id=self.account_id)
            item = find_shop_item(items, shop_item_id)
            if not item:
                logger.error(
                    "shop_pagination_error provider=discord reason=item_not_found actor_user_id=%s shop_item_id=%s",
                    self.author_id,
                    shop_item_id,
                )
                await interaction.response.send_message(SHOP_TEXT_ITEM_NOT_FOUND, ephemeral=True)
                return
            self.page = max(int(page), 0)
            self.mode = "card"
            self.selected_item_id = shop_item_id
            self._render()
            logger.info(
                "shop_item_click provider=discord actor_user_id=%s account_id=%s shop_item_id=%s page=%s",
                self.author_id,
                self.account_id,
                shop_item_id,
                self.page + 1,
            )
            await interaction.response.edit_message(embed=self._item_embed(item), view=self)
        except Exception as error:  # noqa: BLE001
            logger.exception(
                "shop_card_screen_render_error provider=discord actor_user_id=%s shop_item_id=%s error=%s",
                self.author_id,
                shop_item_id,
                error,
            )
            await interaction.response.send_message(SHOP_TEXT_PROTECTED_FAILURE, ephemeral=True)


@bot.hybrid_command(name="shop", description="Открыть магазин (в личных сообщениях)")
async def shop(ctx):
    source = "dm" if getattr(ctx, "guild", None) is None else "group"
    actor_id = getattr(getattr(ctx, "author", None), "id", None)
    logger.info(
        "shop_flow_received provider=discord source=%s actor_user_id=%s guild_id=%s channel_id=%s",
        source,
        actor_id,
        getattr(getattr(ctx, "guild", None), "id", None),
        getattr(getattr(ctx, "channel", None), "id", None),
    )

    profile_check = check_shop_profile_access("discord", actor_id, register_command="/register_account")
    if not profile_check.ok:
        await send_temp(ctx, profile_check.user_message or "Сначала создайте профиль и повторите команду /shop.", delete_after=None)
        return

    dm_view = ShopView(author_id=actor_id or 0, account_id=profile_check.account_id, page=0)
    try:
        dm_embed = dm_view._category_embed()
    except Exception as error:  # noqa: BLE001
        logger.exception(
            "shop_category_screen_render_error provider=discord actor_user_id=%s source=shop_command error=%s",
            actor_id,
            error,
        )
        await send_temp(ctx, SHOP_TEXT_PROTECTED_FAILURE, delete_after=None)
        return
    logger.info(
        "shop_category_screen_open provider=discord actor_user_id=%s account_id=%s",
        actor_id,
        profile_check.account_id,
    )
    logger.info(
        "shop_page_open provider=discord actor_user_id=%s account_id=%s page=1 total_pages=%s page_size=%s",
        actor_id,
        profile_check.account_id,
        dm_view.total_pages,
        SHOP_PAGE_SIZE,
    )

    if source == "dm":
        await send_temp(ctx, embed=dm_embed, view=dm_view, delete_after=None)
        return

    await send_temp(ctx, SHOP_OPEN_PROMPT_TEXT, delete_after=None)
    try:
        await ctx.author.send(embed=dm_embed, view=dm_view)
    except Exception as error:  # noqa: BLE001
        logger.exception("shop_dm_transfer_error provider=discord actor_user_id=%s dm_sent=false error=%s", actor_id, error)
        logger.error(
            "ux_fallback_shown event=ux_fallback_shown screen=shop_dm_transfer provider=discord actor_user_id=%s",
            actor_id,
        )
        await send_temp(ctx, DM_FALLBACK_TEXT, delete_after=None)
