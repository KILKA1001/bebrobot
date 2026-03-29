import logging

import discord

from bot.commands.base import bot
from bot.services.shop_service import (
    SHOP_PAGE_SIZE,
    SHOP_TEXT_ACQUIRE_HINT_PLACEHOLDER,
    SHOP_TEXT_CONFIRM_PURCHASE,
    SHOP_TEXT_ITEM_NOT_FOUND,
    SHOP_TEXT_ITEM_PLACEHOLDER,
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
        self.mode = "list"
        self.selected_item_id: str | None = None
        self._render()

    def _render(self) -> None:
        if self.mode == "list":
            self._render_grid()
            return
        if self.mode == "card":
            self._render_card()
            return
        self._render_confirm()

    def _render_grid(self) -> None:
        self.clear_items()
        items = get_shop_catalog_items(log_context="shop:discord:view")
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

        async def back_cb(interaction: discord.Interaction):
            await self._switch_page(interaction, requested_page=self.page - 1, action="back")

        async def next_cb(interaction: discord.Interaction):
            await self._switch_page(interaction, requested_page=self.page + 1, action="next")

        async def refresh_cb(interaction: discord.Interaction):
            await self._switch_page(interaction, requested_page=self.page, action="refresh")

        back.callback = back_cb
        next_btn.callback = next_cb
        refresh.callback = refresh_cb
        self.add_item(back)
        self.add_item(page_info)
        self.add_item(next_btn)
        self.add_item(refresh)

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
            self.mode = "confirm"
            self._render()
            await interaction.response.edit_message(embed=self._item_confirm_embed(item), view=self)

        async def back_cb(interaction: discord.Interaction):
            self.mode = "list"
            self._render()
            await interaction.response.edit_message(embed=self._list_embed(), view=self)

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
            self.mode = "list"
            self.selected_item_id = None
            self._render()
            embed = self._list_embed()
            embed.description = f"{embed.description}\n\n{result.message}"
            await interaction.response.edit_message(embed=embed, view=self)

        async def cancel_cb(interaction: discord.Interaction):
            self.mode = "card"
            self._render()
            await interaction.response.edit_message(embed=self._item_embed(item), view=self)

        confirm_btn.callback = confirm_cb
        cancel_btn.callback = cancel_cb
        self.add_item(confirm_btn)
        self.add_item(cancel_btn)

    def _selected_item(self):
        items = get_shop_catalog_items(log_context="shop:discord:selected")
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
            title=payload.title,
            description=f"{payload.discord_description}\n\nСтраница: **{self.page + 1}/{self.total_pages}**",
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
            f"Как получить: {acquire_hint}"
        )
        return embed

    def _item_confirm_embed(self, item) -> discord.Embed:
        embed = self._item_embed(item)
        embed.description = f"{embed.description}\n\n{SHOP_TEXT_CONFIRM_PURCHASE}"
        return embed

    async def _switch_page(self, interaction: discord.Interaction, *, requested_page: int, action: str) -> None:
        try:
            old_page = self.page
            items = get_shop_catalog_items(log_context="shop:discord:page_switch")
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
                "shop_pagination_error provider=discord actor_user_id=%s requested_page=%s action=%s error=%s",
                self.author_id,
                requested_page,
                action,
                error,
            )
            await interaction.response.send_message(SHOP_TEXT_PROTECTED_FAILURE, ephemeral=True)

    async def _on_item_click(self, interaction: discord.Interaction, *, shop_item_id: str, page: int) -> None:
        try:
            items = get_shop_catalog_items(log_context="shop:discord:item")
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
                "shop_pagination_error provider=discord actor_user_id=%s shop_item_id=%s error=%s",
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
    dm_embed = dm_view._list_embed()
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
        await send_temp(ctx, DM_FALLBACK_TEXT, delete_after=None)
