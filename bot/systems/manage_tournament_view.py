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
)
from bot.systems.interactive_rounds import RoundManagementView
from bot.systems.tournament_logic import create_tournament_logic
from bot.data.players_db import add_player_to_tournament


class PlayerIdModal(ui.Modal, title="ID игрока"):
    player_id = ui.TextInput(label="ID игрока", required=True)

    def __init__(self, callback):
        super().__init__()
        self._callback = callback

    async def on_submit(self, interaction: Interaction):
        try:
            pid = int(str(self.player_id))
        except ValueError:
            await interaction.response.send_message("Неверный ID", ephemeral=True)
            return
        await self._callback(interaction, pid)


class FinishModal(ui.Modal, title="Завершить турнир"):
    first = ui.TextInput(label="ID 1 места", required=True)
    second = ui.TextInput(label="ID 2 места", required=True)
    third = ui.TextInput(label="ID 3 места", required=False)

    def __init__(self, tid: int, ctx: commands.Context):
        super().__init__()
        self.tid = tid
        self.ctx = ctx

    async def on_submit(self, interaction: Interaction):
        from bot.commands.tournament import endtournament

        try:
            first = int(str(self.first))
            second = int(str(self.second))
            third = int(str(self.third)) if str(self.third) else None
        except ValueError:
            await interaction.response.send_message("Неверные ID", ephemeral=True)
            return
        ctx = await self.ctx.bot.get_context(interaction)
        await endtournament(ctx, self.tid, first, second, third)
        await interaction.response.send_message(
            "Попытка завершить турнир", ephemeral=True
        )


class ManageTournamentView(SafeView):
    persistent = True

    def __init__(self, tournament_id: int, ctx: commands.Context):
        super().__init__(timeout=None)
        self.tid = tournament_id
        self.ctx = ctx
        self.custom_id = f"manage_tour:{tournament_id}"
        self.paused = False
        self.refresh_buttons()

    def refresh_buttons(self):
        self.clear_items()
        status = get_tournament_status(self.tid)
        if status == "registration":
            self._add_pre_start_buttons()
        else:
            self._add_active_buttons()

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

    # ----- Callbacks -----
    async def on_register_player(self, interaction: Interaction):
        await interaction.response.send_modal(PlayerIdModal(self._register))

    async def _register(self, interaction: Interaction, pid: int):
        ok_db = add_player_to_tournament(pid, self.tid)
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
        from bot.data.tournament_db import get_tournament_info

        info = get_tournament_info(self.tid) or {}
        team_size = 3 if info.get("type") == "team" else 1
        participants = [
            p.get("discord_user_id") or p.get("player_id")
            for p in list_participants_full(self.tid)
        ]
        logic = create_tournament_logic(participants, team_size=team_size)
        view = RoundManagementView(self.tid, logic)
        embed = await build_tournament_bracket_embed(self.tid, interaction.guild)
        if not embed:
            embed = await build_tournament_status_embed(self.tid)
        await interaction.response.edit_message(embed=embed, view=view)

    async def on_status(self, interaction: Interaction):
        embed = await build_tournament_bracket_embed(self.tid, interaction.guild)
        if not embed:
            embed = await build_tournament_status_embed(self.tid)
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
        await interaction.response.send_modal(FinishModal(self.tid, self.ctx))
