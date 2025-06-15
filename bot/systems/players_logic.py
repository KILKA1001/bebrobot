import discord
from discord import ui, Embed
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
        await ctx.send("❌ Telegram-ник должен начинаться с `@`.")
        return

    # проверяем, что такого TG ещё нет
    if get_player_by_tg(tg_username):
        await ctx.send("❌ Пользователь с таким Telegram-ником уже зарегистрирован.")
        return

    pid = create_player(nick, tg_username)
    if pid is not None:
        await ctx.send(f"✅ Игрок #{pid} добавлен: `{nick}`, {tg_username}")
    else:
        await ctx.send("❌ Ошибка при создании игрока.")

async def register_player_by_id(
    ctx: commands.Context,
    player_id: int
) -> None:
    """
    Берёт уже существующего игрока и связывает его с текущим турниром через add_player_to_tournament.
    """
    player = get_player_by_id(player_id)
    if not player:
        await ctx.send("❌ Игрок с таким ID не найден.")
        return

    # предположим, что текущий турнир указан в аргументах команды jointournament,
    # здесь просто демонстрация:
    # Например, берем tournament_id из первого аргумента после player_id
    args = ctx.message.content.split()
    if len(args) < 3:
        await ctx.send("❌ Укажите ID турнира: `?register <player_id> <tournament_id>`")
        return
    try:
        tournament_id = int(args[2])
    except ValueError:
        await ctx.send("❌ Неверный ID турнира.")
        return

    ok = add_player_to_tournament(player_id, tournament_id)
    if ok:
        await ctx.send(
            f"✅ Игрок #{player_id} (`{player['nick']}`) зарегистрирован в турнире #{tournament_id}."
        )
    else:
        await ctx.send("❌ Не удалось зарегистрировать игрока в турнире.")

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

    view = ui.View(timeout=120)

    # Кнопки для копирования Telegram-ника
    for p in rows:
        btn = ui.Button(
            label=f"📋 {p['id']}",
            style=discord.ButtonStyle.secondary,
            custom_id=f"copy_{p['id']}"
        )
        async def _copy(interaction: discord.Interaction, tg_username: str):
            await interaction.response.send_message(
                f"Telegram-ник игрока: {tg_username}", ephemeral=True
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

    await ctx.send(embed=embed, view=view)

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
        await ctx.send("❌ Можно править только `nick` или `tg_username`.")
        return

    if field == "tg_username" and not new_value.startswith("@"):
        await ctx.send("❌ Telegram-ник должен начинаться с `@`.")
        return

    ok = update_player_field(player_id, field, new_value)
    if ok:
        await ctx.send(f"✅ Игрок #{player_id} обновлён: {field} = `{new_value}`")
    else:
        await ctx.send("❌ Ошибка при обновлении или игрок не найден.")

async def delete_player_cmd(
    ctx: commands.Context,
    player_id: int
) -> None:
    """
    Удаляет игрока из системы.
    """
    ok = delete_player(player_id)
    if ok:
        await ctx.send(f"✅ Игрок #{player_id} удалён из системы.")
    else:
        await ctx.send("❌ Игрок не найден или уже удалён.")

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
        await ctx.send(f"✅ Игрок #{player_id} удалён из турнира #{tournament_id}.")
    else:
        await ctx.send("❌ Не удалось удалить привязку (возможно, её нет).")

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
        await ctx.send(f"📭 Нет изменений для игрока #{player_id}.")
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

    view = ui.View(timeout=120)
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

    await ctx.send(embed=embed, view=view)