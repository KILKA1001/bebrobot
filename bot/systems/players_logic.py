import discord
from discord import ui, Embed
from bot.utils import SafeView
from discord.ext import commands
from typing import Optional, List, Tuple
from functools import partial

from bot.data.players_db import (
    create_player,
    get_player_by_id,
    get_player_by_tg,
    list_players,
    update_player_field,
    delete_player,
    add_player_to_tournament,
    remove_player_from_tournament,
    list_player_logs,
)
from bot.data.tournament_db import (
    add_player_participant,
    get_announcement_message_id,
    get_tournament_size,
)
from bot.systems.tournament_logic import RegistrationView, ANNOUNCE_CHANNEL_ID
from bot.utils import send_temp

async def register_player(
    ctx: commands.Context,
    nick: str,
    tg_username: str
) -> None:
    """
    Создаёт нового игрока в системе.
    """
    # проверяем формат TG-username
    if not tg_username.startswith("@"):
        await send_temp(ctx, "❌ Telegram-ник должен начинаться с `@`.")
        return

    # проверяем, что такого TG ещё нет
    if get_player_by_tg(tg_username):
        await send_temp(ctx, "❌ Пользователь с таким Telegram-ником уже зарегистрирован.")
        return

    pid = create_player(nick, tg_username)
    if pid is not None:
        await send_temp(ctx, f"✅ Игрок #{pid} добавлен: `{nick}`, {tg_username}")
    else:
        await send_temp(ctx, "❌ Ошибка при создании игрока.")

async def register_player_by_id(
    ctx: commands.Context,
    player_id: int,
    tournament_id: int,
) -> None:
    """
    Связывает существующего игрока с указанным турниром.
    """
    player = get_player_by_id(player_id)
    if not player:
        await send_temp(ctx, "❌ Игрок с таким ID не найден.")
        return

    # Привязываем игрока к указанному турниру
    ok = add_player_to_tournament(player_id, tournament_id)
    ok_part = add_player_participant(tournament_id, player_id)

    if ok and ok_part:
        await send_temp(
            f"✅ Игрок #{player_id} (`{player['nick']}`) зарегистрирован в турнире #{tournament_id}."
        )
        # Обновляем кнопку регистрации, если сообщение доступно
        msg_id = get_announcement_message_id(tournament_id)
        if msg_id and ctx.guild:
            channel = ctx.guild.get_channel(ANNOUNCE_CHANNEL_ID)
            if channel:
                try:
                    message = await channel.fetch_message(msg_id)
                    view = RegistrationView(tournament_id, get_tournament_size(tournament_id))
                    await message.edit(view=view)
                except Exception:
                    pass
    else:
        await send_temp(ctx, "❌ Не удалось зарегистрировать игрока в турнире.")

async def list_players_view(
    ctx: commands.Context,
    page: int = 1
) -> None:
    """
    Выводит Embed со списком игроков постранично и кнопками навигации.
    Каждому игроку соответствует кнопка 📋, которая шлёт его tg_username в личку.
    """
    per_page = 5
    rows, pages = list_players(page, per_page)

    embed = Embed(
        title=f"📋 Список игроков — страница {page}/{pages}",
        color=discord.Color.blue()
    )
    for p in rows:
        embed.add_field(
            name=f"#{p['id']} • {p['nick']}",
            value=p['tg_username'],
            inline=False
        )

    view = SafeView(timeout=120)

    # Кнопки для копирования Telegram-ника
    for p in rows:
        btn = ui.Button(
            label=f"📋 {p['id']}",
            style=discord.ButtonStyle.secondary,
            custom_id=f"copy_{p['id']}"
        )
        async def _copy(interaction: discord.Interaction, tg_username: str):
            await interaction.response.send_message(
                f"Telegram-ник игрока: `{tg_username}`", ephemeral=True
            )
        btn.callback = partial(_copy, tg_username=p['tg_username'])
        view.add_item(btn)

    # Навигационные кнопки
    prev_btn = ui.Button(label="◀️", style=discord.ButtonStyle.primary)
    next_btn = ui.Button(label="▶️", style=discord.ButtonStyle.primary)

    # Колбэк «назад»
    async def go_prev(interaction: discord.Interaction):
        new_page = max(1, page - 1)
        new_rows, new_pages = list_players(new_page, per_page)
        new_embed = Embed(
            title=f"📋 Список игроков — страница {new_page}/{new_pages}",
            color=discord.Color.blue()
        )
        for p2 in new_rows:
            new_embed.add_field(
                name=f"#{p2['id']} • {p2['nick']}",
                value=p2['tg_username'],
                inline=False
            )
        prev_btn.disabled = new_page <= 1
        next_btn.disabled = new_page >= new_pages
        await interaction.response.edit_message(embed=new_embed, view=view)

    # Колбэк «вперед»
    async def go_next(interaction: discord.Interaction):
        new_page = min(pages, page + 1)
        new_rows, new_pages = list_players(new_page, per_page)
        new_embed = Embed(
            title=f"📋 Список игроков — страница {new_page}/{new_pages}",
            color=discord.Color.blue()
        )
        for p2 in new_rows:
            new_embed.add_field(
                name=f"#{p2['id']} • {p2['nick']}",
                value=p2['tg_username'],
                inline=False
            )
        prev_btn.disabled = new_page <= 1
        next_btn.disabled = new_page >= new_pages
        await interaction.response.edit_message(embed=new_embed, view=view)

    prev_btn.callback = go_prev
    next_btn.callback = go_next
    view.add_item(prev_btn)
    view.add_item(next_btn)

    await send_temp(ctx, embed=embed, view=view)

async def edit_player(
    ctx: commands.Context,
    player_id: int,
    field: str,
    new_value: str
) -> None:
    """
    Редактирует nick или tg_username игрока.
    """
    if field not in ("nick", "tg_username"):
        await send_temp(ctx, "❌ Можно править только `nick` или `tg_username`.")
        return

    if field == "tg_username" and not new_value.startswith("@"):
        await send_temp(ctx, "❌ Telegram-ник должен начинаться с `@`.")
        return

    ok = update_player_field(player_id, field, new_value)
    if ok:
        await send_temp(ctx, f"✅ Игрок #{player_id} обновлён: {field} = `{new_value}`")
    else:
        await send_temp(ctx, "❌ Ошибка при обновлении или игрок не найден.")

async def delete_player_cmd(
    ctx: commands.Context,
    player_id: int
) -> None:
    """
    Удаляет игрока из системы.
    """
    ok = delete_player(player_id)
    if ok:
        await send_temp(ctx, f"✅ Игрок #{player_id} удалён из системы.")
    else:
        await send_temp(ctx, "❌ Игрок не найден или уже удалён.")

async def unregister_player(
    ctx: commands.Context,
    player_id: int,
    tournament_id: int
) -> None:
    """
    Убирает игрока из турнира.
    """
    ok = remove_player_from_tournament(player_id, tournament_id)
    if ok:
        await send_temp(ctx, f"✅ Игрок #{player_id} удалён из турнира #{tournament_id}.")
    else:
        await send_temp(ctx, "❌ Не удалось удалить привязку (возможно, её нет).")

async def list_player_logs_view(
    ctx: commands.Context,
    player_id: int,
    page: int = 1
) -> None:
    """
    Постранично выводит логи изменений конкретного игрока.
    """
    per_page = 5
    logs, pages = list_player_logs(player_id, page, per_page)
    if not logs:
        await send_temp(ctx, f"📭 Нет изменений для игрока #{player_id}.")
        return

    embed = Embed(
        title=f"📝 Логи игрока #{player_id} — страница {page}/{pages}",
        color=discord.Color.dark_gray()
    )
    for log in logs:
        at = log["changed_at"][:19].replace("T", " ")
        embed.add_field(
            name=f"{at} — {log['field_name']}",
            value=f"`{log['old_value']}` → `{log['new_value']}`",
            inline=False
        )

    view = SafeView(timeout=120)
    prev_btn = ui.Button(label="◀️", style=discord.ButtonStyle.secondary)
    async def go_prev(interaction: discord.Interaction):
        await interaction.response.edit_message(view=None)
        await list_player_logs_view(ctx, player_id, max(1, page - 1))
    prev_btn.callback = go_prev
    view.add_item(prev_btn)

    next_btn = ui.Button(label="▶️", style=discord.ButtonStyle.secondary)
    async def go_next(interaction: discord.Interaction):
        await interaction.response.edit_message(view=None)
        await list_player_logs_view(ctx, player_id, min(pages, page + 1))
    next_btn.callback = go_next
    view.add_item(next_btn)

    await send_temp(ctx, embed=embed, view=view)
