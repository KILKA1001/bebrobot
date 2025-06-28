import discord
from discord import Embed, Interaction, ButtonStyle, ui
from discord.ui import View, Button
from typing import Optional
from bot.systems.tournament_logic import (
    start_round as cmd_start_round,
    join_tournament,            # не обязательно, но для примера
    build_tournament_status_embed,
    MODE_NAMES
)
from bot.data.tournament_db import record_match_result as db_record_match_result

from bot.systems.tournament_logic import Tournament

class RoundManagementView(View):
    """UI для управления раундами одного турнира."""

    persistent = True

    def __init__(self, tournament_id: int, logic: Tournament):
        super().__init__(timeout=None)
        self.tournament_id = tournament_id
        self.logic = logic
        self.custom_id = f"manage_rounds:{tournament_id}"  # Добавляем custom_id

        # Получаем статус турнира
        from bot.data.tournament_db import get_tournament_status
        status = get_tournament_status(tournament_id)

        # Настройка кнопки "Начать раунд"
        start_disabled = status != "active"
        start_btn = Button(
            label="▶️ Начать раунд",
            style=ButtonStyle.green,
            custom_id=f"start_round:{tournament_id}",
            row=0,
            disabled=start_disabled
        )
        start_btn.callback = self.on_start_round
        self.add_item(start_btn)

        next_btn = Button(
            label="⏭ Перейти к следующему",
            style=ButtonStyle.blurple,
            custom_id=f"next_round:{tournament_id}",
            row=0,
        )
        next_btn.callback = self.on_next_round
        self.add_item(next_btn)

        stop_btn = Button(
            label="🛑 Остановить раунд",
            style=ButtonStyle.red,
            custom_id=f"stop_round:{tournament_id}",
            row=1,
        )
        stop_btn.callback = self.on_stop_round
        self.add_item(stop_btn)

        status_btn = Button(
            label="📊 Показать статус",
            style=ButtonStyle.gray,
            custom_id=f"status_round:{tournament_id}",
            row=1,
        )
        status_btn.callback = self.on_status_round
        self.add_item(status_btn)

        # Кнопка активации турнира (если статус "registration")
        if status == "registration":
            activate_btn = Button(
                label="✅ Активировать турнир",
                style=ButtonStyle.success,
                custom_id=f"activate_tournament:{tournament_id}",
                row=2,
            )
            activate_btn.callback = self.on_activate_tournament
            self.add_item(activate_btn)
        else:
            manage_btn = Button(
                label="⚙ Управление раундами",
                style=ButtonStyle.primary,
                custom_id=f"manage_rounds:{tournament_id}",
                row=2,
            )
            manage_btn.callback = self.on_manage_rounds
            self.add_item(manage_btn)

    async def on_activate_tournament(self, interaction: Interaction):
        """Переводит турнир в активный статус"""
        from bot.systems.tournament_logic import set_tournament_status
        if set_tournament_status(self.tournament_id, "active"):
            await interaction.response.send_message(
                f"✅ Турнир #{self.tournament_id} активирован!",
                ephemeral=True
            )
            # Обновляем View
            self.clear_items()
            await self.__init__(self.tournament_id, self.logic)
            await interaction.message.edit(view=self)
        else:
            await interaction.response.send_message(
                "❌ Не удалось активировать турнир",
                ephemeral=True
            )


    async def on_start_round(self, interaction: Interaction):
        await cmd_start_round(interaction, self.tournament_id)

    async def on_next_round(self, interaction: Interaction):
        await cmd_start_round(interaction, self.tournament_id)

    async def on_stop_round(self, interaction: Interaction):
        status = await build_tournament_status_embed(self.tournament_id)
        if status:
            await interaction.response.edit_message(embed=status, view=self)
        else:
            await interaction.response.send_message(
                "❌ Не удалось получить статус турнира.", ephemeral=True
            )

    async def on_status_round(self, interaction: Interaction):
        await self.on_stop_round(interaction)

    async def on_manage_rounds(self, interaction: Interaction):
        """Повторно открывает панель управления раундами."""
        embed = Embed(
            title=f"⚙️ Управление турниром #{self.tournament_id}",
            description=(
                "Используйте кнопки ниже для контроля раундов.\n"
                "Нажмите **▶️** для старта первого раунда."
            ),
            color=0xF39C12
        )
        view = RoundManagementView(self.tournament_id, self.logic)
        await interaction.response.edit_message(embed=embed, view=view)

class MatchResultView(View):
    """UI для ввода результата конкретного матча."""

    def __init__(self, match_id: int):
        super().__init__(timeout=60)
        self.match_id = match_id

    async def interaction_check(self, interaction: Interaction) -> bool:
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message(
                "❌ Эта команда работает только на сервере.",
                ephemeral=True,
            )
            return False

        member = guild.get_member(interaction.user.id)
        if member is None:
            await interaction.response.send_message(
                "❌ Не удалось определить вас на сервере.",
                ephemeral=True,
            )
            return False

        if not member.guild_permissions.administrator:
            await interaction.response.send_message(
                "❌ Только администратор может сообщить результат матча.",
                ephemeral=True,
            )
            return False

        return True

    @ui.button(label="🏆 Игрок 1", style=ButtonStyle.primary)
    async def win1(self, interaction: Interaction, button: Button):
        await self._report(interaction, 1)

    @ui.button(label="🏆 Игрок 2", style=ButtonStyle.secondary)
    async def win2(self, interaction: Interaction, button: Button):
        await self._report(interaction, 2)

    async def _report(self, interaction: Interaction, winner: int):
        ok = db_record_match_result(self.match_id, winner)
        if ok:
            await interaction.response.edit_message(
                embed=Embed(
                    title=f"Матч #{self.match_id}: победитель — игрок {winner}",
                    color=discord.Color.green(),
                ),
                view=None,
            )
        else:
            await interaction.response.send_message(
                "❌ Ошибка при сохранении результата.",
                ephemeral=True,
            )


class PairSelectionView(View):
    """Выбор пары для начала матчей."""

    def __init__(
        self,
        tournament_id: int,
        pairs: dict[int, list],
        guild: discord.Guild,
        round_no: int,
        team_display: Optional[dict[int, str]] = None,
    ):
        super().__init__(timeout=None)
        self.tournament_id = tournament_id
        self.pairs = pairs
        self.guild = guild
        self.round_no = round_no
        self.team_display = team_display or {}
        self._build_buttons()

    def _build_buttons(self):
        self.clear_items()
        row = 0
        for idx, matches in self.pairs.items():
            m = matches[0]
            if m.player1_id in self.team_display:
                name1 = self.team_display[m.player1_id]
            else:
                p1 = self.guild.get_member(m.player1_id)
                name1 = p1.display_name if p1 else str(m.player1_id)
            if m.player2_id in self.team_display:
                name2 = self.team_display[m.player2_id]
            else:
                p2 = self.guild.get_member(m.player2_id)
                name2 = p2.display_name if p2 else str(m.player2_id)
            label = f"{name1} vs {name2}"
            btn = ui.Button(label=label, style=ButtonStyle.primary, custom_id=f"pair_{idx}", row=row)
            btn.callback = self._make_callback(idx)
            self.add_item(btn)
            row = (row + 1) % 5

    def _make_callback(self, idx: int):
        async def callback(interaction: Interaction):
            await self.send_pair_matches(interaction, idx)
        return callback

    async def send_pair_matches(self, interaction: Interaction, idx: int):
        matches = self.pairs.get(idx)
        if not matches:
            await interaction.response.send_message("Эта пара уже была выбрана.", ephemeral=True)
            return
        self.pairs.pop(idx)
        # отключаем кнопку
        for item in self.children:
            if hasattr(item, "custom_id") and item.custom_id == f"pair_{idx}":
                item.disabled = True
        if interaction.message:
            await interaction.message.edit(view=self)

        await interaction.response.send_message(f"Запускаем пару {idx}", ephemeral=True)

        channel = interaction.channel
        for n, m in enumerate(matches, start=1):
            if m.player1_id in self.team_display:
                v1 = self.team_display[m.player1_id]
            else:
                p1 = self.guild.get_member(m.player1_id)
                v1 = p1.mention if p1 else f"<@{m.player1_id}>"
            if m.player2_id in self.team_display:
                v2 = self.team_display[m.player2_id]
            else:
                p2 = self.guild.get_member(m.player2_id)
                v2 = p2.mention if p2 else f"<@{m.player2_id}>"

            mode_name = MODE_NAMES.get(m.mode_id, str(m.mode_id))

            match_embed = discord.Embed(
                title=f"Матч {n} — Раунд {self.round_no}",
                description=f"{v1} vs {v2}",
                color=discord.Color.blue(),
            )
            match_embed.add_field(name="Режим", value=mode_name, inline=True)
            match_embed.add_field(name="Карта", value=f"`{m.map_id}`", inline=True)

            from bot.data.tournament_db import get_map_image_url
            map_url = get_map_image_url(str(m.map_id))
            if map_url:
                match_embed.set_image(url=map_url)

            view = MatchResultView(match_id=m.match_id)

            if channel:
                await channel.send(embed=match_embed, view=view)



# Функция-помощник для отправки стартового сообщения турнира
async def announce_round_management(channel, tournament_id: int, logic: Tournament):
    """
    Отправляет embed-подложку с кнопками управления раундами.
    """
    embed = Embed(
        title=f"⚙️ Управление турниром #{tournament_id}",
        description=(
            "Используйте кнопки ниже для контроля раундов.\n"
            "Нажмите **▶️** для старта первого раунда."
        ),
        color=0xF39C12
    )
    view = RoundManagementView(tournament_id, logic)
    await channel.send(embed=embed, view=view)

