import discord
from discord import ui, ButtonStyle, Interaction
from discord.ext import commands

from bot.utils import SafeView
from bot.data.tournament_db import (
    get_tournament_status,
    get_tournament_size,
    list_participants_full,
    remove_player_from_tournament,
)
from bot.systems.tournament_logic import (
    set_tournament_status,
    generate_first_round,
    build_tournament_status_embed,
    build_tournament_bracket_embed,
    send_announcement_embed,
    send_participation_confirmations,
    delete_tournament as send_delete_confirmation,
    _get_round_results,
)
from bot.systems.interactive_rounds import RoundManagementView
from bot.systems.tournament_logic import (
    create_tournament_logic,
    load_tournament_logic_from_db,
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

        ctx = await self.ctx.bot.get_context(interaction)
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

        ctx = await self.ctx.bot.get_context(interaction)
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

        status_btn = ui.Button(label="Статус", style=ButtonStyle.secondary)
        status_btn.callback = self.on_status
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
        await interaction.response.send_modal(
            PlayerIdModal(self._register, ask_team=self.is_team)
        )

    async def _register(
        self, interaction: Interaction, pid: int, team: str | None = None
    ):
        if self.is_team and team:
            from bot.data.tournament_db import (
                get_team_id_by_name,
                get_next_team_id,
            )

            tid = get_team_id_by_name(self.tid, team)
            if tid is None:
                tid = get_next_team_id(self.tid)
        else:
            tid = None

        ok_db = add_player_to_tournament(
            pid, self.tid, team_id=tid, team_name=team if tid else None
        )
        if ok_db:
            await interaction.response.send_message("Игрок добавлен", ephemeral=True)
        else:
            await interaction.response.send_message(
                "Не удалось добавить", ephemeral=True
            )
        self.refresh_buttons()
        if interaction.message:
            await interaction.message.edit(view=self)

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
        embed = await build_tournament_status_embed(self.tid)
        if embed:
            await interaction.response.send_message(embed=embed, ephemeral=True)
        else:
            await interaction.response.send_message("Нет данных", ephemeral=True)

    async def on_announce(self, interaction: Interaction):
        await send_announcement_embed(self.ctx, self.tid)
        await interaction.response.send_message("Анонс отправлен", ephemeral=True)

    async def on_notify(self, interaction: Interaction):
        admin_id = self.ctx.author.id
        await send_participation_confirmations(interaction.client, self.tid, admin_id)
        await interaction.response.send_message(
            "Уведомления отправлены", ephemeral=True
        )

    async def on_rename_team(self, interaction: Interaction):
        await interaction.response.send_modal(TeamRenameModal(self._rename_team))

    async def _rename_team(self, interaction: Interaction, team_id: int, name: str):
        from bot.data.tournament_db import update_team_name

        ok = update_team_name(self.tid, team_id, name)
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
        if set_tournament_status(self.tid, "active"):
            if guild:
                await generate_first_round(interaction.client, guild, self.tid)
            await interaction.response.send_message(
                "Турнир активирован", ephemeral=True
            )
            self.refresh_buttons()
            if interaction.message:
                await interaction.message.edit(view=self)
        else:
            await interaction.response.send_message("Не удалось", ephemeral=True)

    async def on_delete(self, interaction: Interaction):
        await send_delete_confirmation(self.ctx, self.tid)
        await interaction.response.send_message(
            "Диалог удаления отправлен", ephemeral=True
        )

    async def on_manage_rounds(self, interaction: Interaction):
        logic = load_tournament_logic_from_db(self.tid)
        view = RoundManagementView(self.tid, logic)
        embed = await build_tournament_bracket_embed(self.tid, interaction.guild)
        if not embed:
            embed = await build_tournament_status_embed(self.tid)
        await interaction.response.edit_message(embed=embed, view=view)

    async def on_status(self, interaction: Interaction):
        embed = await build_tournament_bracket_embed(self.tid, interaction.guild)
        if not embed:
            embed = await build_tournament_status_embed(self.tid)

        if embed is None:
            await interaction.response.send_message(
                "Турнир не найден", ephemeral=True
            )
            return

        msg = interaction.message
        # Don't try to edit ephemeral or missing messages
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

    async def on_bets(self, interaction: Interaction):
        await interaction.response.send_message(
            "Система ставок в разработке", ephemeral=True
        )

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
            get_tournament_info,
            get_team_info,
            get_matches,
        )

        from bot.data.players_db import get_player_by_id

        info = get_tournament_info(self.tid) or {}
        team_mode = info.get("type") == "team"


        if team_mode:
            team_map, team_names = get_team_info(self.tid)
            logic = create_tournament_logic(list(team_map.keys()))
            logic.team_map = team_map
        else:
            participants = [
                p.get("discord_user_id") or p.get("player_id")
                for p in list_participants_full(self.tid)
            ]
            logic = create_tournament_logic(participants)

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

                options.append(
                    discord.SelectOption(label=name[:100], value=str(tid))
                )

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

        view = FinishChoiceView(
            self.tid, self.ctx, auto_first, auto_second, options
        )
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

        guild = interaction.guild or (self.ctx.guild if hasattr(self.ctx, "guild") else None)

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
