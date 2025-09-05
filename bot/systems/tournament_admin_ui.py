from __future__ import annotations

import discord
from discord import ui, ButtonStyle, Interaction
from discord.ext import commands

from bot.utils.safe_view import SafeView
from bot.data import tournament_db
from bot.systems.tournament_logic import (
    TournamentSetupView,
    build_tournament_status_embed,
    build_tournament_bracket_embed,
    load_tournament_logic_from_db,
)
from bot.systems.manage_tournament_view import ManageTournamentView
from bot.systems.interactive_rounds import RoundManagementView


class TournamentAdminDashboard(SafeView):
    """Главная панель для администраторов турниров.

    Позволяет выбрать активный турнир или создать новый, а также перейти к
    регистрационному этапу, управлению боями и завершению турнира.
    """

    def __init__(self, ctx: commands.Context):
        super().__init__(timeout=300)
        self.ctx = ctx
        self.tournament_id: int | None = None

        # Список активных турниров для быстрого выбора
        active = tournament_db.get_active_tournaments()
        options: list[discord.SelectOption] = []
        for t in active[:25]:
            label = f"Турнир #{t['id']}"
            options.append(discord.SelectOption(label=label, value=str(t["id"])))

        if options:
            self.select = ui.Select(placeholder="Выберите турнир", options=options)
            self.select.callback = self.on_select
            self.add_item(self.select)
        else:
            self.select = None

        self.reg_btn = ui.Button(
            label="Регистрация", style=ButtonStyle.secondary, disabled=True
        )
        self.reg_btn.callback = self.on_registration
        self.add_item(self.reg_btn)

        self.match_btn = ui.Button(
            label="Матчи", style=ButtonStyle.primary, disabled=True
        )
        self.match_btn.callback = self.on_matches
        self.add_item(self.match_btn)

        self.finish_btn = ui.Button(
            label="Завершение", style=ButtonStyle.danger, disabled=True
        )
        self.finish_btn.callback = self.on_finish
        self.add_item(self.finish_btn)

        create_btn = ui.Button(label="Создать турнир", style=ButtonStyle.success)
        create_btn.callback = self.on_create
        self.add_item(create_btn)

    async def on_select(self, interaction: Interaction):
        self.tournament_id = int(self.select.values[0])
        self.reg_btn.disabled = False
        self.match_btn.disabled = False
        self.finish_btn.disabled = False
        await interaction.response.edit_message(view=self)

    async def on_create(self, interaction: Interaction):
        view = TournamentSetupView(interaction.user.id)
        embed = view.initial_embed()
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        view.message = await interaction.original_response()

    async def _send_manage_view(self, interaction: Interaction):
        if not self.tournament_id:
            await interaction.response.send_message(
                "Турнир не выбран", ephemeral=True
            )
            return
        embed = await build_tournament_status_embed(self.tournament_id, include_id=True)
        if not embed:
            embed = await build_tournament_bracket_embed(
                self.tournament_id, interaction.guild, include_id=True
            )
        view = ManageTournamentView(self.tournament_id, self.ctx)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    async def on_registration(self, interaction: Interaction):
        await self._send_manage_view(interaction)

    async def on_finish(self, interaction: Interaction):
        await self._send_manage_view(interaction)

    async def on_matches(self, interaction: Interaction):
        if not self.tournament_id:
            await interaction.response.send_message(
                "Турнир не выбран", ephemeral=True
            )
            return
        logic = load_tournament_logic_from_db(self.tournament_id)
        embed = await build_tournament_bracket_embed(
            self.tournament_id, interaction.guild
        )
        if not embed:
            embed = await build_tournament_status_embed(
                self.tournament_id, include_id=True
            )
        view = RoundManagementView(self.tournament_id, logic, self.ctx)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
