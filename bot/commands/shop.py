import logging

import discord

from bot.commands.base import bot
from bot.systems.shop_logic import (
    SHOP_PAGE_SIZE,
    build_shop_render_payload,
    check_shop_profile_access,
    find_shop_item,
    get_shop_catalog_items,
    get_shop_page_slice,
)
from bot.utils import send_temp

logger = logging.getLogger(__name__)

SHOP_OPEN_PROMPT_TEXT = "Откройте магазин в личных сообщениях, я уже отправил вам инструкцию."
DM_FALLBACK_TEXT = (
    "❌ Не удалось отправить инструкцию в личные сообщения.\n"
    "Откройте ЛС с ботом: нажмите на профиль бота → Message, включите личные сообщения для сервера и снова выполните /shop."
)


class ShopView(discord.ui.View):
    def __init__(self, *, author_id: int, account_id: str | None, page: int = 0):
        super().__init__(timeout=600)
        self.author_id = int(author_id)
        self.account_id = account_id
        self.page = max(int(page), 0)
        self.total_pages = 1
        self._render_grid()

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
        description = item.description or "Описание пока не добавлено."
        acquire_hint = item.acquire_hint or "Способ получения пока не указан."
        embed = discord.Embed(title=item.role_name, color=discord.Color.blurple())
        embed.description = (
            f"Баланс: **{payload.points} баллов**\n"
            f"Категория: **{item.category}**\n"
            f"Описание: {description}\n"
            f"Как получить: {acquire_hint}"
        )
        return embed

    async def _switch_page(self, interaction: discord.Interaction, *, requested_page: int, action: str) -> None:
        try:
            old_page = self.page
            items = get_shop_catalog_items(log_context="shop:discord:page_switch")
            page_data = get_shop_page_slice(items, requested_page, page_size=SHOP_PAGE_SIZE)
            self.page = page_data.page
            self._render_grid()
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
            await interaction.response.send_message("Ошибка пагинации, нажмите «Обновить».", ephemeral=True)

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
                await interaction.response.send_message("Товар не найден. Нажмите «Обновить».", ephemeral=True)
                return
            self.page = max(int(page), 0)
            self._render_grid()
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
            await interaction.response.send_message("Ошибка открытия товара.", ephemeral=True)


@bot.hybrid_command(name="shop", description="Открыть магазин (в личных сообщениях)")
async def shop(ctx):
    source = "dm" if getattr(ctx, "guild", None) is None else "group"
    actor_id = getattr(getattr(ctx, "author", None), "id", None)
    logger.info(
        "shop flow step=received provider=discord source=%s actor_user_id=%s guild_id=%s channel_id=%s",
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
        logger.warning("shop flow step=dm_attempt provider=discord actor_user_id=%s dm_sent=false error=%s", actor_id, error)
        await send_temp(ctx, DM_FALLBACK_TEXT, delete_after=None)
