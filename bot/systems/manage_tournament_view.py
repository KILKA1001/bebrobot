"""
Назначение: модуль "manage tournament view" реализует продуктовый контур в зоне общая логика.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: общая логика.
"""

import discord
from discord import ui, ButtonStyle, Interaction
from discord.ext import commands

from bot.utils import SafeView, safe_send, format_points
from bot.data.tournament_db import (
    get_tournament_status,
    get_tournament_size,
    list_participants_full,
    remove_player_from_tournament,
    count_matches,
    get_tournament_info,
)
from bot.systems.tournament_logic import (
    set_tournament_status,
    generate_first_round,
    build_tournament_status_embed,
    build_tournament_bracket_embed,
    send_announcement_embed,
    send_status_message,
    send_participation_confirmations,
    delete_tournament as send_delete_confirmation,
    _get_round_results,
    update_registration_message,
    rename_tournament,
    refresh_bracket_message,
    format_tournament_title,
)
import math
from bot.systems.interactive_rounds import RoundManagementView
from bot.systems.tournament_logic import (
    create_tournament_logic,
    load_tournament_logic_from_db,
    is_auto_team,
    assign_auto_team,
    rename_auto_team,
)
from bot.data.players_db import add_player_to_tournament


class PlayerIdModal(ui.Modal, title="ID игрока"):
    player_id = ui.TextInput(label="ID игрока", required=True)

    def __init__(self, callback, *, ask_team: bool = False):
        super().__init__()
        self._callback = callback
        self.ask_team = ask_team
        if ask_team:
            self.team_name = ui.TextInput(label="Название команды", required=True)
            self.add_item(self.team_name)

    async def on_submit(self, interaction: Interaction):
        try:
            pid = int(str(self.player_id))
        except ValueError:
            await interaction.response.send_message("Неверный ID", ephemeral=True)
            return
        team = str(self.team_name) if self.ask_team else None
        if self.ask_team:
            await self._callback(interaction, pid, team)
        else:
            await self._callback(interaction, pid)


class TeamNameModal(ui.Modal, title="Название команды"):
    team_name = ui.TextInput(label="Название команды", required=True)

    def __init__(self, callback, pid: int, *, is_discord: bool = False):
        super().__init__()
        self._callback = callback
        self.pid = pid
        self.is_discord = is_discord

    async def on_submit(self, interaction: Interaction):
        # Передаём дополнительный флаг, чтобы знать, ID это игрока или Discord
        await self._callback(
            interaction, self.pid, str(self.team_name), is_discord=self.is_discord
        )


class RegisterPlayerView(SafeView):
    def __init__(self, parent_view):
        super().__init__(timeout=60)
        self.parent_view = parent_view
        select = ui.UserSelect(placeholder="Выберите игрока")

        async def on_select(interaction: Interaction):
            user = select.values[0]
            pid = user.id
            if self.parent_view.is_team and not self.parent_view.team_auto:
                await interaction.response.send_modal(
                    TeamNameModal(
                        self.parent_view._register, pid, is_discord=True
                    )
                )
            else:
                await self.parent_view._register(
                    interaction, pid, is_discord=True
                )
            self.stop()

        select.callback = on_select
        self.add_item(select)

        id_btn = ui.Button(label="Ввести ID", style=ButtonStyle.secondary)

        async def on_id(interaction: Interaction):
            await interaction.response.send_modal(
                PlayerIdModal(
                    self.parent_view._register,
                    ask_team=self.parent_view.is_team and not self.parent_view.team_auto,
                )
            )
            self.stop()

        id_btn.callback = on_id
        self.add_item(id_btn)


class TeamRenameModal(ui.Modal, title="Переименовать команду"):
    team_id = ui.TextInput(label="ID команды", required=True)
    new_name = ui.TextInput(label="Новое название", required=True)

    def __init__(self, callback):
        super().__init__()
        self._callback = callback

    async def on_submit(self, interaction: Interaction):
        try:
            tid = int(str(self.team_id))
        except ValueError:
            await interaction.response.send_message("Неверный ID", ephemeral=True)
            return
        await self._callback(interaction, tid, str(self.new_name))


class TournamentRenameModal(ui.Modal, title="Новое название турнира"):
    new_name = ui.TextInput(label="Название", required=True)

    def __init__(self, callback):
        super().__init__()
        self._callback = callback

    async def on_submit(self, interaction: Interaction):
        await self._callback(interaction, str(self.new_name))


class SizeModal(ui.Modal, title="Новое количество"):
    new_size = ui.TextInput(label="Количество участников", required=True)

    def __init__(self, callback):
        super().__init__()
        self._callback = callback

    async def on_submit(self, interaction: Interaction):
        try:
            size = int(str(self.new_size))
        except ValueError:
            await interaction.response.send_message("Неверное число", ephemeral=True)
            return
        await self._callback(interaction, size)


class BetModal(ui.Modal, title="Сделать ставку"):
    round_no = ui.TextInput(label="Раунд", required=True)
    pair_index = ui.TextInput(label="Пара", required=True)
    bet_on = ui.TextInput(label="ID игрока/команды", required=True)
    amount = ui.TextInput(label="Баллы", required=True)

    def __init__(self, callback):
        super().__init__()
        self._callback = callback

    async def on_submit(self, interaction: Interaction):
        try:
            rnd = int(str(self.round_no))
            pair = int(str(self.pair_index))
            bet_on = int(str(self.bet_on))
            amount = float(str(self.amount))
        except ValueError:
            await interaction.response.send_message("Неверные данные", ephemeral=True)
            return
        await self._callback(interaction, rnd, pair, bet_on, amount)


class BetAmountModal(ui.Modal, title="Размер ставки"):
    amount = ui.TextInput(label="Баллы", required=True)

    def __init__(
        self, callback, round_no: int, pair_index: int, bet_on: int, name: str
    ):
        super().__init__()
        self._callback = callback
        self.round_no = round_no
        self.pair_index = pair_index
        self.bet_on = bet_on
        self.name = name

    async def on_submit(self, interaction: Interaction):
        try:
            amount = float(str(self.amount))
        except ValueError:
            await interaction.response.send_message("Неверные данные", ephemeral=True)
            return
        await self._callback(
            interaction,
            self.round_no,
            self.pair_index,
            self.bet_on,
            amount,
            self.name,
        )


class BetPlayerView(SafeView):
    def __init__(
        self,
        round_no: int,
        pair_index: int,
        player1: int,
        player2: int,
        callback,
        name_map: dict[int, str],
    ):
        super().__init__(timeout=60)
        self.round_no = round_no
        self.pair_index = pair_index
        self.player1 = player1
        self.player2 = player2
        self._callback = callback
        self.name_map = name_map

    @ui.button(label="Игрок 1", style=ButtonStyle.primary)
    async def bet_p1(self, interaction: Interaction, button: ui.Button):
        await interaction.response.send_modal(
            BetAmountModal(
                self._callback,
                self.round_no,
                self.pair_index,
                self.player1,
                self.name_map.get(self.player1, str(self.player1)),
            )
        )

    @ui.button(label="Игрок 2", style=ButtonStyle.secondary)
    async def bet_p2(self, interaction: Interaction, button: ui.Button):
        await interaction.response.send_modal(
            BetAmountModal(
                self._callback,
                self.round_no,
                self.pair_index,
                self.player2,
                self.name_map.get(self.player2, str(self.player2)),
            )
        )


class BetPairSelectView(SafeView):
    def __init__(
        self,
        round_no: int,
        pairs: dict[int, tuple[int, int]],
        name_map: dict[int, str],
        maps: dict[int, list[dict]],
        callback,
    ):
        super().__init__(timeout=60)
        self.round_no = round_no
        self.pairs = pairs
        self.name_map = name_map
        self.maps = maps
        self._callback = callback
        options = []
        for idx, (p1, p2) in pairs.items():
            n1 = name_map.get(p1, str(p1))
            n2 = name_map.get(p2, str(p2))
            options.append(
                discord.SelectOption(label=f"Пара {idx}: {n1} vs {n2}", value=str(idx))
            )
        self.select = ui.Select(placeholder="Выберите пару", options=options)

        async def _callback(interaction: Interaction):
            await self.on_select(interaction)

        self.select.callback = _callback

        self.add_item(self.select)

    async def on_select(self, interaction: Interaction):
        idx = int(self.select.values[0])
        p1, p2 = self.pairs.get(idx, (0, 0))
        n1 = self.name_map.get(p1, str(p1))
        n2 = self.name_map.get(p2, str(p2))
        desc = f"{n1} vs {n2}"
        embed = discord.Embed(
            title=f"Пара {idx}",
            description=desc,
            color=discord.Color.blue(),
        )
        maps = self.maps.get(idx)
        if maps:
            lines = [f"{m.get('name', m.get('id'))} (`{m.get('id')}`)" for m in maps]
            embed.add_field(name="Карты", value="\n".join(lines), inline=False)
            first = maps[0]
            if first.get("image_url"):
                embed.set_image(url=first["image_url"])
        view = BetPlayerView(self.round_no, idx, p1, p2, self._callback, self.name_map)
        await interaction.response.edit_message(embed=embed, view=view)


class ConfirmBetView(SafeView):
    def __init__(self, callback):
        super().__init__(timeout=60)
        self._callback = callback

    @ui.button(label="Подтвердить", style=ButtonStyle.success)
    async def confirm(self, interaction: Interaction, button: ui.Button):
        await self._callback(interaction)
        self.stop()


class BetEditAmountModal(ui.Modal, title="Изменить ставку"):
    """Modal for editing bet amount."""

    amount = ui.TextInput(label="Баллы", required=True)

    def __init__(self, callback, bet_id: int, bet_on: int):
        super().__init__()
        self._callback = callback
        self.bet_id = bet_id
        self.bet_on = bet_on

    async def on_submit(self, interaction: Interaction):
        try:
            amount = float(str(self.amount))
        except Exception:
            await interaction.response.send_message("Неверные данные", ephemeral=True)
            return
        await self._callback(interaction, self.bet_id, self.bet_on, amount)


class BetEditView(SafeView):
    """View to select new bet target before entering amount."""

    def __init__(
        self,
        bet_id: int,
        options: list[discord.SelectOption],
        callback,
        default: int | None = None,
    ):
        super().__init__(timeout=60)
        self.bet_id = bet_id
        self._callback = callback
        if default is not None:
            for opt in options:
                if opt.value == str(default):
                    opt.default = True

        self.select = ui.Select(placeholder="Игрок/команда", options=options)
        self.add_item(self.select)

        self.confirm_btn = ui.Button(label="Далее", style=ButtonStyle.primary)
        self.confirm_btn.callback = self.on_confirm
        self.add_item(self.confirm_btn)

    async def on_confirm(self, interaction: Interaction):
        if not self.select.values:
            await interaction.response.send_message(
                "Выберите игрока/команду", ephemeral=True
            )
            return
        bet_on = int(self.select.values[0])
        await interaction.response.send_modal(
            BetEditAmountModal(self._callback, self.bet_id, bet_on)
        )


class BetStatusView(SafeView):
    def __init__(
        self, bets: list[dict], edit_cb, delete_cb, locked: set[int] | None = None
    ):
        super().__init__(timeout=60)

        options = [
            discord.SelectOption(
                label=f"ID {b['id']} (пара {b['pair_index']})", value=str(b["id"])
            )
            for b in bets
        ]

        class _Select(ui.Select):
            def __init__(self):
                # используем встроенное свойство `view` вместо собственного поля `parent`
                super().__init__(placeholder="Выберите ставку", options=options)

            async def callback(self, interaction: Interaction):
                await self.view.on_select(interaction)

        class _EditBtn(ui.Button):
            def __init__(self):
                # кнопка редактирования по умолчанию отключена
                super().__init__(
                    label="Изменить", style=ButtonStyle.primary, disabled=True
                )

            async def callback(self, interaction: Interaction):
                await self.view.on_edit(interaction)

        class _DelBtn(ui.Button):
            def __init__(self):
                # кнопку удаления также блокируем до выбора ставки
                super().__init__(
                    label="Удалить", style=ButtonStyle.danger, disabled=True
                )

            async def callback(self, interaction: Interaction):
                await self.view.on_delete(interaction)

        # создаём элементы интерфейса и добавляем их в текущее View
        self.select = _Select()
        self.edit_btn = _EditBtn()
        self.del_btn = _DelBtn()

        self.add_item(self.select)
        self.add_item(self.edit_btn)
        self.add_item(self.del_btn)
        self.selected: int | None = None
        self._edit_cb = edit_cb
        self._delete_cb = delete_cb
        self.locked = locked or set()

    async def on_select(self, interaction: Interaction):
        if not self.select.values:
            return
        self.selected = int(self.select.values[0])
        locked = self.selected in self.locked
        self.edit_btn.disabled = locked
        self.del_btn.disabled = locked
        await interaction.response.edit_message(view=self)

    async def on_edit(self, interaction: Interaction):
        if self.selected is None:
            await interaction.response.send_message("Выберите ставку", ephemeral=True)
            return
        if self.selected in self.locked:
            await interaction.response.send_message(
                "Ставку нельзя изменить после начала пары", ephemeral=True
            )
            return
        await self._edit_cb(interaction, self.selected)

    async def on_delete(self, interaction: Interaction):
        if self.selected is None:
            await interaction.response.send_message("Выберите ставку", ephemeral=True)
            return
        if self.selected in self.locked:
            await interaction.response.send_message(
                "Ставку нельзя удалить после начала пары", ephemeral=True
            )
            return
        await self._delete_cb(interaction, self.selected)


class BetRootView(SafeView):
    """Корневое меню раздела ставок."""

    def __init__(self, parent):
        super().__init__(timeout=60)
        self.parent = parent

    @ui.button(label="Свои ставки", style=ButtonStyle.primary)
    async def mine(self, interaction: Interaction, button: ui.Button):
        """Переход к меню управления собственными ставками."""
        view = BetMenuView(self.parent)
        embed = discord.Embed(
            title="Ставки",
            description="Выберите действие",
            color=discord.Color.orange(),
        )
        await interaction.response.edit_message(embed=embed, view=view)

    @ui.button(label="Все ставки", style=ButtonStyle.secondary)
    async def all(self, interaction: Interaction, button: ui.Button):
        """Показать сводку по всем ставкам турнира."""
        await self.parent._show_all_bets(interaction)


class BetMenuView(SafeView):
    """Меню работы со своими ставками."""

    def __init__(self, parent):
        super().__init__(timeout=60)
        self.parent = parent

    @ui.button(label="Сделать ставку", style=ButtonStyle.primary)
    async def place(self, interaction: Interaction, button: ui.Button):
        await self.parent._show_pair_select(interaction)

    @ui.button(label="Статус ставок", style=ButtonStyle.secondary)
    async def status(self, interaction: Interaction, button: ui.Button):
        await self.parent._show_bet_status(interaction)

    @ui.button(label="Назад", style=ButtonStyle.gray)
    async def back(self, interaction: Interaction, button: ui.Button):
        """Возврат в главное меню ставок."""
        view = BetRootView(self.parent)
        embed = discord.Embed(
            title="Ставки",
            description="Выберите раздел",
            color=discord.Color.orange(),
        )
        await interaction.response.edit_message(embed=embed, view=view)


class FinishModal(ui.Modal):
    """Modal with dropdowns to select winners."""

    def __init__(
        self,
        tid: int,
        ctx: commands.Context,
        options: list[discord.SelectOption],
        title: str = "Завершить турнир",
        submit_callback=None,
    ):
        super().__init__(title=title)
        self.tid = tid
        self.ctx = ctx
        self._submit_callback = submit_callback

        self.first_select = ui.Select(
            placeholder="🥇 1 место",
            options=options,
        )
        self.second_select = ui.Select(
            placeholder="🥈 2 место",
            options=options,
        )
        self.third_select = ui.Select(
            placeholder="🥉 3 место (опционально)",
            options=[discord.SelectOption(label="—", value="0")] + options,
        )
        self.add_item(self.first_select)
        self.add_item(self.second_select)
        self.add_item(self.third_select)

    async def on_submit(self, interaction: Interaction):
        try:
            first = int(self.first_select.values[0])
            second = int(self.second_select.values[0])
            third_val = self.third_select.values[0]
            third = int(third_val) if third_val != "0" else None
        except Exception:
            await interaction.response.send_message("Неверные данные", ephemeral=True)
            return

        if first == second or (third is not None and third in {first, second}):
            await interaction.response.send_message(
                "Выберите разные места", ephemeral=True
            )
            return

        ctx = self.ctx
        try:
            ctx = await self.ctx.bot.get_context(interaction)
        except ValueError:
            ctx = self.ctx
        if self._submit_callback:
            await self._submit_callback(ctx, self.tid, first, second, third)
            await interaction.response.send_message("Данные отправлены", ephemeral=True)
        else:
            from bot.commands.tournament import endtournament

            await endtournament(ctx, self.tid, first, second, third)
            await interaction.response.send_message(
                "Попытка завершить турнир", ephemeral=True
            )


class FinishChoiceView(SafeView):
    """Offers automatic or manual finalization."""

    def __init__(
        self,
        tid: int,
        ctx: commands.Context,
        auto_first: int | None,
        auto_second: int | None,
        options: list[discord.SelectOption],
    ):
        super().__init__(timeout=60)
        self.tid = tid
        self.ctx = ctx
        self.auto_first = auto_first
        self.auto_second = auto_second
        self.options = options

    @ui.button(label="Автоматически", style=ButtonStyle.success)
    async def auto_finish(self, interaction: Interaction, button: ui.Button):
        if self.auto_first is None or self.auto_second is None:
            await interaction.response.send_message(
                "Недостаточно данных для автозавершения", ephemeral=True
            )
            return
        from bot.commands.tournament import endtournament

        ctx = self.ctx

        try:
            ctx = await self.ctx.bot.get_context(interaction)
        except ValueError:
            ctx = self.ctx

        await endtournament(ctx, self.tid, self.auto_first, self.auto_second)
        await interaction.response.edit_message(
            content="Попытка завершить турнир", view=None
        )
        self.stop()

    @ui.button(label="Выбрать вручную", style=ButtonStyle.secondary)
    async def manual_finish(self, interaction: Interaction, button: ui.Button):
        await interaction.response.send_modal(
            FinishModal(self.tid, self.ctx, self.options)
        )
        self.stop()


class ManageTournamentView(SafeView):
    persistent = True

    def __init__(self, tournament_id: int, ctx: commands.Context):
        super().__init__(timeout=None)
        self.tid = tournament_id
        self.ctx = ctx
        self.custom_id = f"manage_tour:{tournament_id}"
        self.paused = False
        from bot.data.tournament_db import get_tournament_info

        info = get_tournament_info(tournament_id) or {}
        self.is_team = info.get("type") == "team"
        self.team_auto = is_auto_team(tournament_id)
        self.refresh_buttons()

    def refresh_buttons(self):
        self.clear_items()
        status = get_tournament_status(self.tid)
        if status == "registration":
            self._add_pre_start_buttons()
        elif status == "active":
            self._add_active_buttons()
        else:
            self._add_finished_buttons()

    # ----- Stage 1 -----
    def _add_pre_start_buttons(self):
        join_btn = ui.Button(label="Рег. игрока", style=ButtonStyle.secondary)
        join_btn.callback = self.on_register_player
        self.add_item(join_btn)

        unreg_btn = ui.Button(label="Убрать игрока", style=ButtonStyle.secondary)
        unreg_btn.callback = self.on_unregister_player
        self.add_item(unreg_btn)

        list_btn = ui.Button(label="Участники", style=ButtonStyle.gray)
        list_btn.callback = self.on_list_players
        self.add_item(list_btn)

        announce_btn = ui.Button(label="Анонс", style=ButtonStyle.primary)
        announce_btn.callback = self.on_announce
        self.add_item(announce_btn)

        notify_btn = ui.Button(label="Напомнить", style=ButtonStyle.primary)
        notify_btn.callback = self.on_notify
        self.add_item(notify_btn)

        rename_tour_btn = ui.Button(
            label="Название турнира", style=ButtonStyle.secondary
        )
        rename_tour_btn.callback = self.on_rename_tournament
        self.add_item(rename_tour_btn)

        size_btn = ui.Button(label="Изм. размер", style=ButtonStyle.secondary)
        size_btn.callback = self.on_change_size
        self.add_item(size_btn)

        if self.is_team:
            rename_btn = ui.Button(
                label="Переименовать команду", style=ButtonStyle.secondary
            )
            rename_btn.callback = self.on_rename_team
            self.add_item(rename_btn)

        activate_btn = ui.Button(label="Активировать", style=ButtonStyle.success)
        activate_btn.callback = self.on_activate
        size = get_tournament_size(self.tid)
        current = len(list_participants_full(self.tid))
        activate_btn.disabled = current < size
        self.add_item(activate_btn)

        del_btn = ui.Button(label="Удалить", style=ButtonStyle.danger)
        del_btn.callback = self.on_delete
        self.add_item(del_btn)

    # ----- Stage 2 -----
    def _add_active_buttons(self):
        manage_btn = ui.Button(label="Матчи", style=ButtonStyle.primary)
        manage_btn.callback = self.on_manage_rounds
        self.add_item(manage_btn)

        status_btn = ui.Button(label="Отправить статус", style=ButtonStyle.secondary)
        status_btn.callback = self.on_send_status
        self.add_item(status_btn)

        bet_btn = ui.Button(label="Ставки", style=ButtonStyle.gray)
        bet_btn.callback = self.on_bets
        self.add_item(bet_btn)

        pause_label = "Возобновить" if self.paused else "Пауза"
        pause_btn = ui.Button(label=pause_label, style=ButtonStyle.secondary)
        pause_btn.callback = self.on_pause
        self.add_item(pause_btn)

        finish_btn = ui.Button(label="Завершить", style=ButtonStyle.danger)
        finish_btn.callback = self.on_finish
        self.add_item(finish_btn)

    def _add_finished_buttons(self):
        if count_matches(self.tid) > 0:
            bracket_btn = ui.Button(label="Сетка", style=ButtonStyle.secondary)
            bracket_btn.callback = self.on_bracket
            self.add_item(bracket_btn)

        announce_btn = ui.Button(label="Анонс результатов", style=ButtonStyle.primary)
        announce_btn.callback = self.on_announce_results
        self.add_item(announce_btn)

        edit_btn = ui.Button(label="Изменить победителей", style=ButtonStyle.secondary)
        edit_btn.callback = self.on_edit_winners
        self.add_item(edit_btn)

        clear_btn = ui.Button(label="Удалить бои", style=ButtonStyle.danger)
        clear_btn.callback = self.on_clear_matches
        self.add_item(clear_btn)

    # ----- Callbacks -----
    async def on_register_player(self, interaction: Interaction):
        if interaction.guild:
            view = RegisterPlayerView(self)
            await interaction.response.send_message(
                "Выберите игрока или введите ID:", view=view, ephemeral=True
            )
        else:
            await interaction.response.send_modal(
                PlayerIdModal(
                    self._register, ask_team=self.is_team and not self.team_auto
                )
            )

    async def _register(
        self,
        interaction: Interaction,
        pid: int,
        team: str | None = None,
        *,
        is_discord: bool = False,
    ):
        if self.is_team and not self.team_auto and team:
            from bot.data.tournament_db import (
                get_team_id_by_name,
                get_next_team_id,
            )

            tid = get_team_id_by_name(self.tid, team)
            if tid is None:
                tid = get_next_team_id(self.tid)
        elif not self.team_auto:
            tid = None
            if is_discord:
                ok_db = add_player_to_tournament(
                    None,
                    self.tid,
                    discord_user_id=pid,
                    team_id=tid,
                    team_name=team if tid else None,
                )
            else:
                ok_db = add_player_to_tournament(
                    pid,
                    self.tid,
                    team_id=tid,
                    team_name=team if tid else None,
                )
        else:
            # При автоматическом распределении команд нам нужен Discord ID
            ok_db = assign_auto_team(self.tid, pid) if is_discord else False
        if ok_db:
            await interaction.response.send_message("Игрок добавлен", ephemeral=True)
        else:
            await interaction.response.send_message(
                "Не удалось добавить", ephemeral=True
            )
        self.refresh_buttons()
        if interaction.message:
            try:
                # Обновляем исходное сообщение с кнопками, если оно ещё существует
                await interaction.message.edit(view=self)
            except discord.NotFound:
                # Сообщение могли удалить или оно было эфемерным — просто игнорируем
                pass

    async def on_unregister_player(self, interaction: Interaction):
        await interaction.response.send_modal(PlayerIdModal(self._unregister))

    async def _unregister(self, interaction: Interaction, pid: int):
        if remove_player_from_tournament(pid, self.tid):
            await interaction.response.send_message("Игрок убран", ephemeral=True)
        else:
            await interaction.response.send_message("Не удалось убрать", ephemeral=True)
        self.refresh_buttons()
        if interaction.message:
            await interaction.message.edit(view=self)

    async def on_list_players(self, interaction: Interaction):
        embed = await build_tournament_status_embed(self.tid, include_id=True)
        if embed:
            await interaction.response.send_message(embed=embed, ephemeral=True)
        else:
            await interaction.response.send_message("Нет данных", ephemeral=True)

    async def on_announce(self, interaction: Interaction):
        success = await send_announcement_embed(self.ctx, self.tid)
        if success:
            info = get_tournament_info(self.tid) or {}
            title = format_tournament_title(
                info.get("name"), info.get("start_time"), self.tid, include_id=True
            )
            await interaction.response.send_message(
                f"Анонс отправлен: {title}", ephemeral=True
            )
        else:
            await interaction.response.send_message(
                "Не удалось отправить анонс", ephemeral=True
            )

    async def on_notify(self, interaction: Interaction):
        admin_id = self.ctx.author.id
        await interaction.response.defer(ephemeral=True)
        await send_participation_confirmations(interaction.client, self.tid, admin_id)
        await interaction.followup.send("Уведомления отправлены", ephemeral=True)

    async def on_rename_tournament(self, interaction: Interaction):
        await interaction.response.send_modal(
            TournamentRenameModal(self._rename_tournament)
        )

    async def _rename_tournament(self, interaction: Interaction, new_name: str):
        ok = rename_tournament(self.tid, new_name)
        if ok:
            info = get_tournament_info(self.tid) or {}
            title = format_tournament_title(
                new_name, info.get("start_time"), self.tid, include_id=True
            )
            await interaction.response.send_message(
                f"Название обновлено: {title}", ephemeral=True
            )
            guild = interaction.guild or (
                self.ctx.guild if hasattr(self.ctx, "guild") else None
            )
            if guild:
                await send_announcement_embed(self.ctx, self.tid)
                await refresh_bracket_message(guild, self.tid)
        else:
            await interaction.response.send_message(
                "Не удалось обновить название", ephemeral=True
            )
        self.refresh_buttons()
        if interaction.message:
            await interaction.message.edit(view=self)

    async def on_change_size(self, interaction: Interaction):
        await interaction.response.send_modal(SizeModal(self._change_size))

    async def _change_size(self, interaction: Interaction, size: int):
        from bot.data.tournament_db import update_tournament_size

        ok = update_tournament_size(self.tid, size)
        if ok:
            await interaction.response.send_message(
                f"Размер обновлён: {size}", ephemeral=True
            )
            guild = interaction.guild or (
                self.ctx.guild if hasattr(self.ctx, "guild") else None
            )
            if guild:
                await update_registration_message(guild, self.tid)
        else:
            await interaction.response.send_message(
                "Не удалось обновить", ephemeral=True
            )
        self.refresh_buttons()
        if interaction.message:
            await interaction.message.edit(view=self)

    async def on_rename_team(self, interaction: Interaction):
        await interaction.response.send_modal(TeamRenameModal(self._rename_team))

    async def _rename_team(self, interaction: Interaction, team_id: int, name: str):
        from bot.data.tournament_db import update_team_name

        ok = update_team_name(self.tid, team_id, name)
        rename_auto_team(self.tid, team_id, name)
        if ok:
            await interaction.response.send_message(
                "Название обновлено", ephemeral=True
            )
        else:
            await interaction.response.send_message(
                "Не удалось обновить", ephemeral=True
            )
        embed = await build_tournament_status_embed(self.tid)
        if embed:
            await interaction.followup.send(embed=embed, ephemeral=True)
        self.refresh_buttons()
        if interaction.message:
            await interaction.message.edit(view=self)

    async def on_activate(self, interaction: Interaction):
        guild = interaction.guild or (
            self.ctx.guild if hasattr(self.ctx, "guild") else None
        )

        # Сначала откладываем ответ на взаимодействие, чтобы Discord не
        # "забыл" его, пока мы генерируем сетку и уведомляем игроков.
        await interaction.response.defer(ephemeral=True)

        if set_tournament_status(self.tid, "active"):
            if guild:
                await generate_first_round(interaction.client, guild, self.tid)
                from bot.systems.tournament_logic import update_bet_message

                await update_bet_message(guild, self.tid)
            await interaction.followup.send(
                "Турнир активирован", ephemeral=True
            )
            self.refresh_buttons()
            if interaction.message:
                await interaction.message.edit(view=self)
        else:
            await interaction.followup.send("Не удалось", ephemeral=True)

    async def on_delete(self, interaction: Interaction):
        await send_delete_confirmation(self.ctx, self.tid)
        await interaction.response.send_message(
            "Диалог удаления отправлен", ephemeral=True
        )

    async def on_manage_rounds(self, interaction: Interaction):
        logic = load_tournament_logic_from_db(self.tid)
        view = RoundManagementView(self.tid, logic, self.ctx)
        embed = await build_tournament_bracket_embed(self.tid, interaction.guild)
        if not embed:
            embed = await build_tournament_status_embed(self.tid, include_id=True)
        await interaction.response.edit_message(embed=embed, view=view)

    async def on_bracket(self, interaction: Interaction):
        embed = await build_tournament_bracket_embed(
            self.tid, interaction.guild, include_id=True
        )
        if not embed:
            embed = await build_tournament_status_embed(self.tid)

        if embed is None:
            await interaction.response.send_message("Турнир не найден", ephemeral=True)
            return

        msg = interaction.message
        if msg is None or (getattr(msg, "flags", None) and msg.flags.ephemeral):
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return
        try:
            await msg.edit(embed=embed, view=self)
        except Exception:
            if interaction.response.is_done():
                await interaction.followup.send(embed=embed, ephemeral=True)
            else:
                await interaction.response.send_message(embed=embed, ephemeral=True)

    async def on_send_status(self, interaction: Interaction):
        guild = interaction.guild or (
            self.ctx.guild if hasattr(self.ctx, "guild") else None
        )
        if not guild:
            await interaction.response.send_message(
                "Не удалось определить сервер", ephemeral=True
            )
            return


        bot = interaction.client
        ok = await send_status_message(guild, self.tid, bot=bot)
        if ok:
            await interaction.response.send_message("Статус отправлен", ephemeral=True)
        else:
            await interaction.response.send_message(
                "Не удалось отправить статус", ephemeral=True
            )

    async def on_bets(self, interaction: Interaction):
        """Открыть меню работы со ставками."""
        view = BetRootView(self)
        embed = discord.Embed(
            title="Ставки",
            description="Выберите раздел",
            color=discord.Color.orange(),
        )
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    async def _show_edit_modal(self, interaction: Interaction, bet_id: int):
        from bot.data.tournament_db import get_bet, get_matches, get_team_info
        from bot.data.players_db import get_player_by_id

        bet = get_bet(bet_id)
        if not bet:
            await interaction.response.send_message("Ставка не найдена", ephemeral=True)
            return
        round_no = int(bet["round"])
        pair_index = int(bet["pair_index"])
        matches = get_matches(self.tid, round_no)
        if not matches:
            await interaction.response.send_message("Матчи не найдены", ephemeral=True)
            return
        pairs: dict[int, tuple[int, int]] = {}
        idx_map: dict[tuple[int, int], int] = {}
        idx = 1
        for m in matches:
            key = (int(m["player1_id"]), int(m["player2_id"]))
            if key not in idx_map:
                idx_map[key] = idx
                idx += 1
            pid = idx_map[key]
            pairs[pid] = key
        pair = pairs.get(pair_index)
        if not pair:
            await interaction.response.send_message("Пара не найдена", ephemeral=True)
            return
        p1, p2 = pair
        name_map: dict[int, str] = {}
        if self.is_team:
            _, team_names = get_team_info(self.tid)
            name_map.update({int(k): v for k, v in team_names.items()})
        guild = interaction.guild or (
            self.ctx.guild if hasattr(self.ctx, "guild") else None
        )
        for pid in (p1, p2):
            if pid in name_map:
                continue
            name = None
            if guild:
                member = guild.get_member(pid)
                if member:
                    name = member.display_name
            if name is None:
                pl = get_player_by_id(pid)
                name = pl["nick"] if pl else f"ID:{pid}"
            name_map[pid] = name
        options = [
            discord.SelectOption(label=name_map.get(p1, str(p1)), value=str(p1)),
            discord.SelectOption(label=name_map.get(p2, str(p2)), value=str(p2)),
        ]
        view = BetEditView(bet_id, options, self._edit_bet, default=int(bet["bet_on"]))
        embed = discord.Embed(
            title="Изменить ставку",
            description="Выберите игрока/команду и нажмите Далее",
            color=discord.Color.orange(),
        )
        await interaction.response.edit_message(embed=embed, view=view)

    async def _edit_bet(
        self, interaction: Interaction, bet_id: int, bet_on: int, amount: float
    ):
        from bot.systems import bets_logic
        from bot.data.tournament_db import get_tournament_size, get_bet

        bet = get_bet(bet_id)
        if not bet:
            await interaction.response.send_message("Ставка не найдена", ephemeral=True)
            return
        if bets_logic.pair_started(self.tid, int(bet["round"]), int(bet["pair_index"])):
            await interaction.response.send_message(
                "Пара уже началась, ставку нельзя изменить", ephemeral=True
            )
            return
        size = get_tournament_size(self.tid)
        total_rounds = int(math.ceil(math.log2(size))) if size > 1 else 1
        ok, msg = bets_logic.modify_bet(
            bet_id, bet_on, amount, interaction.user.id, total_rounds
        )
        await interaction.response.send_message(msg, ephemeral=True)

    async def _delete_bet(self, interaction: Interaction, bet_id: int):
        from bot.systems import bets_logic
        from bot.data.tournament_db import get_bet

        bet = get_bet(bet_id)
        if bet and bets_logic.pair_started(
            self.tid, int(bet["round"]), int(bet["pair_index"])
        ):
            await interaction.response.send_message(
                "Пара уже началась, ставку нельзя удалить", ephemeral=True
            )
            return
        ok, msg = bets_logic.cancel_bet(bet_id)
        await interaction.response.send_message(msg, ephemeral=True)

    async def _show_pair_select(self, interaction: Interaction):
        from bot.data.tournament_db import get_matches, get_team_info
        from bot.data.players_db import get_player_by_id

        guild = interaction.guild or (
            self.ctx.guild if hasattr(self.ctx, "guild") else None
        )

        round_no = 1
        matches = []
        while True:
            m = get_matches(self.tid, round_no)
            if not m:
                round_no -= 1
                break
            matches = m
            if any(x.get("result") not in (1, 2) for x in m):
                break
            round_no += 1

        if not matches:
            await interaction.response.send_message(
                "Нет активных матчей", ephemeral=True
            )
            return

        pairs: dict[int, tuple[int, int]] = {}
        pair_maps: dict[int, list[dict]] = {}
        idx_map: dict[tuple[int, int], int] = {}
        idx = 1
        for m in matches:
            key = (int(m["player1_id"]), int(m["player2_id"]))
            if key not in idx_map:
                idx_map[key] = idx
                idx += 1
            pid = idx_map[key]
            pairs[pid] = key
            from bot.data.tournament_db import get_map_info

            info = get_map_info(str(m.get("map_id")))
            pair_maps.setdefault(pid, []).append(
                {
                    "id": str(m.get("map_id")),
                    "name": info.get("name") if info else str(m.get("map_id")),
                    "image_url": info.get("image_url") if info else None,
                }
            )

        name_map: dict[int, str] = {}
        if self.is_team:
            _, team_names = get_team_info(self.tid)
            name_map.update({int(k): v for k, v in team_names.items()})

        for pid in {p for pair in pairs.values() for p in pair}:
            if pid in name_map:
                continue
            name = None
            if guild:
                member = guild.get_member(pid)
                if member:
                    name = member.display_name
            if name is None:
                pl = get_player_by_id(pid)
                name = pl["nick"] if pl else f"ID:{pid}"
            name_map[pid] = name

        view = BetPairSelectView(round_no, pairs, name_map, pair_maps, self._place_bet)
        embed = discord.Embed(
            title=f"Ставки: раунд {round_no}",
            description="Выберите пару для ставки",
            color=discord.Color.orange(),
        )
        await interaction.response.edit_message(embed=embed, view=view)

    async def _show_bet_status(self, interaction: Interaction):
        from bot.systems import bets_logic

        bets = bets_logic.get_user_bets(self.tid, interaction.user.id)
        if not bets:
            await interaction.response.edit_message(
                content="Ставок нет", embed=None, view=None
            )
            return
        embed = discord.Embed(title="Ваши ставки", color=discord.Color.orange())
        locked: set[int] = set()
        for b in bets:
            if bets_logic.pair_started(self.tid, int(b["round"]), int(b["pair_index"])):
                locked.add(int(b["id"]))
            embed.add_field(
                name=f"ID {b['id']}",
                value=f"Раунд {b['round']} пара {b['pair_index']} на {b['bet_on']} — {b['amount']} баллов",
                inline=False,
            )
        view = BetStatusView(bets, self._show_edit_modal, self._delete_bet, locked)
        await interaction.response.edit_message(embed=embed, view=view)

    async def _show_all_bets(self, interaction: Interaction):
        """Выводит сводку по всем активным ставкам турнира."""
        from bot.data import tournament_db
        from bot.data.tournament_db import get_team_info
        from bot.data.players_db import get_player_by_id

        bets = [b for b in tournament_db.list_bets(self.tid) if b.get("won") is None]
        if not bets:
            embed = discord.Embed(
                title="Все ставки",
                description="Ставок нет",
                color=discord.Color.orange(),
            )
            await interaction.response.edit_message(embed=embed, view=BetRootView(self))
            return

        guild = interaction.guild or (
            self.ctx.guild if hasattr(self.ctx, "guild") else None
        )

        team_names: dict[int, str] = {}
        if self.is_team:
            _, team_names = get_team_info(self.tid)

        def resolve_name(uid: int) -> str:
            """Подбирает понятное имя по ID игрока/команды."""
            if uid in team_names:
                return team_names[uid]
            name = None
            if guild:
                member = guild.get_member(uid)
                if member:
                    name = member.display_name
            if name is None:
                pl = get_player_by_id(uid)
                if pl:
                    name = pl.get("nick")
            return name or f"ID:{uid}"

        users: dict[int, dict] = {}
        total_sum = 0.0
        for b in bets:
            uid = int(b.get("user_id"))
            amount = float(b.get("amount", 0))
            total_sum += amount
            entry = users.setdefault(
                uid, {"name": resolve_name(uid), "total": 0.0, "bets": []}
            )
            bet_on_name = resolve_name(int(b.get("bet_on")))
            entry["total"] += amount
            entry["bets"].append(
                f"Раунд {b['round']} пара {b['pair_index']} на {bet_on_name}: {format_points(amount)}"
            )

        embed = discord.Embed(title="Все ставки", color=discord.Color.orange())
        for data in sorted(users.values(), key=lambda x: x["name"].lower()):
            lines = "\n".join(data["bets"])
            value = f"{lines}\n**Итого:** {format_points(data['total'])}"
            embed.add_field(name=data["name"], value=value, inline=False)

        embed.set_footer(text=f"Общая сумма ставок: {format_points(total_sum)}")
        await interaction.response.edit_message(embed=embed, view=BetRootView(self))

    async def _place_bet(
        self,
        interaction: Interaction,
        round_no: int,
        pair_index: int,
        bet_on: int,
        amount: float,
        name: str,
    ):
        from bot.systems import bets_logic
        from bot.data.tournament_db import get_tournament_size

        size = get_tournament_size(self.tid)
        total_rounds = int(math.ceil(math.log2(size))) if size > 1 else 1
        payout = bets_logic.calculate_payout(round_no, total_rounds, amount)

        async def confirm(inter: Interaction):
            ok, msg = bets_logic.place_bet(
                self.tid,
                round_no,
                pair_index,
                inter.user.id,
                bet_on,
                amount,
                total_rounds,
            )
            if inter.response.is_done():
                await inter.followup.send(msg, ephemeral=True)
            else:
                await inter.response.send_message(msg, ephemeral=True)
            if ok:
                try:
                    await safe_send(
                        inter.user,
                        f"Вы поставили {amount} баллов на {name} в паре {pair_index} раунда {round_no}. Возможный выигрыш {payout}.\nОжидайте результата.",
                    )
                except Exception:
                    pass

        embed = discord.Embed(
            title="Подтверждение ставки",
            description=f"На {name} {amount} баллов. Возможный выигрыш {payout}",
            color=discord.Color.orange(),
        )
        view = ConfirmBetView(confirm)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    async def on_pause(self, interaction: Interaction):
        self.paused = not self.paused
        label = "Возобновить" if self.paused else "Пауза"
        for item in self.children:
            if isinstance(item, ui.Button) and item.label in ("Пауза", "Возобновить"):
                item.label = label
        await interaction.response.send_message(
            "Пауза" if self.paused else "Возобновлено", ephemeral=True
        )
        if interaction.message:
            await interaction.message.edit(view=self)

    async def on_finish(self, interaction: Interaction):
        from bot.data.tournament_db import get_tournament_info, get_team_info

        from bot.data.tournament_db import (
            get_matches,
        )

        from bot.data.players_db import get_player_by_id

        info = get_tournament_info(self.tid) or {}
        team_mode = info.get("type") == "team"

        if team_mode:
            team_map, team_names = get_team_info(self.tid)
            logic = create_tournament_logic(list(team_map.keys()), shuffle=False)
            logic.team_map = team_map
        else:
            participants = [
                p.get("discord_user_id") or p.get("player_id")
                for p in list_participants_full(self.tid)
            ]
            logic = create_tournament_logic(participants, shuffle=False)

        guild = interaction.guild or (
            self.ctx.guild if hasattr(self.ctx, "guild") else None
        )

        winners: list[int] | None = None
        losers: list[int] | None = None
        round_no = 1
        winners_found = False
        while True:
            data = get_matches(self.tid, round_no)
            if not data:
                break
            if any(m.get("result") not in (1, 2) for m in data):
                break
            res = _get_round_results(self.tid, round_no)
            if res is None:
                break
            winners, losers = res
            winners_found = True
            round_no += 1

        if not winners_found:
            set_tournament_status(self.tid, "finished")
            from bot.systems import bets_logic

            bets_logic.refund_all_bets(self.tid, interaction.user.id)
            await interaction.response.send_message(
                "🏁 Турнир завершён без наград.", ephemeral=True
            )
            self.refresh_buttons()
            if interaction.message:
                await interaction.message.edit(view=self)
            return

        auto_first = winners[0] if winners else None
        auto_second = losers[0] if losers else None

        options: list[discord.SelectOption] = []
        if team_mode:
            team_map, _ = get_team_info(self.tid)
            for tid in winners:
                members = team_map.get(int(tid), [])
                names: list[str] = []
                for m in members:
                    name = None
                    if guild:
                        member = guild.get_member(m)
                        if member:
                            name = member.display_name
                    if name is None:
                        pl = get_player_by_id(m)
                        name = pl["nick"] if pl else f"ID:{m}"
                    names.append(name)
                label = f"Команда {tid}: {', '.join(names)}"

                options.append(discord.SelectOption(label=label[:100], value=str(tid)))

            team_map, team_names = get_team_info(self.tid)
            for tid in winners:
                name = team_names.get(int(tid))
                if not name:
                    name = f"Команда {tid}"

                options.append(discord.SelectOption(label=name[:100], value=str(tid)))

        else:
            for pid in winners:
                name = None
                if guild:
                    member = guild.get_member(pid)
                    if member:
                        name = member.display_name
                if name is None:
                    pl = get_player_by_id(pid)
                    name = pl["nick"] if pl else f"ID:{pid}"
                options.append(discord.SelectOption(label=name[:100], value=str(pid)))

        view = FinishChoiceView(self.tid, self.ctx, auto_first, auto_second, options)
        await interaction.response.send_message(
            "Выберите способ завершения", ephemeral=True, view=view
        )

    async def on_announce_results(self, interaction: Interaction):
        from bot.systems import tournament_logic

        await tournament_logic.announce_results(self.ctx, self.tid)
        await interaction.response.send_message("Анонсирован результат", ephemeral=True)

    async def on_edit_winners(self, interaction: Interaction):
        from bot.data.tournament_db import get_team_info, get_matches
        from bot.data.players_db import get_player_by_id
        from bot.systems.tournament_logic import FinishModal

        guild = interaction.guild or (
            self.ctx.guild if hasattr(self.ctx, "guild") else None
        )

        team_mode = self.is_team
        ids = set()
        round_no = 1
        while True:
            data = get_matches(self.tid, round_no)
            if not data:
                break
            for m in data:
                ids.add(int(m.get("player1_id")))
                ids.add(int(m.get("player2_id")))
            round_no += 1

        options: list[discord.SelectOption] = []
        if team_mode:
            team_map, team_names = get_team_info(self.tid)
            for tid in sorted(ids):
                name = team_names.get(tid, f"Команда {tid}")
                options.append(discord.SelectOption(label=name[:100], value=str(tid)))
        else:
            for pid in sorted(ids):
                name = None
                if guild:
                    member = guild.get_member(pid)
                    if member:
                        name = member.display_name
                if name is None:
                    pl = get_player_by_id(pid)
                    name = pl["nick"] if pl else f"ID:{pid}"
                options.append(discord.SelectOption(label=name[:100], value=str(pid)))

        from bot.systems.tournament_logic import change_winners

        async def submit_cb(ctx, tid, first, second, third):
            await change_winners(ctx, tid, first, second, third)

        await interaction.response.send_modal(
            FinishModal(
                self.tid,
                self.ctx,
                options,
                title="Изменить победителей",
                submit_callback=submit_cb,
            )
        )

    async def on_clear_matches(self, interaction: Interaction):
        from bot.data.tournament_db import delete_match_records

        delete_match_records(self.tid)
        await interaction.response.send_message("Записи матчей удалены", ephemeral=True)
        self.refresh_buttons()
        if interaction.message:
            await interaction.message.edit(view=self)
