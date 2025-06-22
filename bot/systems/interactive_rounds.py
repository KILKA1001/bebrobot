from discord import Embed, Interaction, ButtonStyle, SelectOption
from discord.ui import View, Button, Select


from bot.systems.tournament_logic import Tournament

class RoundManagementView(View):
    """UI для управления раундами одного турнира."""

    persistent = True

    def __init__(self, tournament_id: int, logic: Tournament):
        super().__init__(timeout=None)
        self.tournament_id = tournament_id
        self.logic = logic

        start_btn = Button(
            label="▶️ Начать раунд",
            style=ButtonStyle.green,
            custom_id=f"start_round:{tournament_id}",
            row=0,
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

        manage_btn = Button(
            label="⚙ Управление раундами",
            style=ButtonStyle.primary,
            custom_id=f"manage_rounds:{tournament_id}",
            row=2,
        )
        manage_btn.callback = self.on_manage_rounds
        self.add_item(manage_btn)


    async def on_start_round(self, interaction: Interaction):
        embed = self.logic.start_round(self.tournament_id)
        await interaction.response.edit_message(embed=embed, view=self)

    async def on_next_round(self, interaction: Interaction):
        embed = self.logic.generate_next_round(self.tournament_id)
        await interaction.response.edit_message(embed=embed, view=self)

    async def on_stop_round(self, interaction: Interaction):
        embed = self.logic.get_current_round_embed(self.tournament_id)
        await interaction.response.edit_message(embed=embed, view=self)

    async def on_status_round(self, interaction: Interaction):
        embed = self.logic.get_current_round_embed(self.tournament_id)
        await interaction.response.edit_message(embed=embed, view=self)

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
    """Представление для ввода результатов матчей."""
    def __init__(self, tournament_id: int, logic: Tournament, matches: list):
        super().__init__(timeout=None)
        self.tournament_id = tournament_id
        self.logic = logic

        # Для каждого матча добавляем select-меню
        for match in matches:
            options = [
                SelectOption(label=f"Победа {match.player_a}", value=f"{match.id}:A"),
                SelectOption(label=f"Победа {match.player_b}", value=f"{match.id}:B"),
                SelectOption(label="Ничья", value=f"{match.id}:D"),
            ]
            self.add_item(MatchResultSelect(tournament_id, logic, options))

class MatchResultSelect(Select):
    def __init__(self, tournament_id: int, logic: Tournament, options: list):
        super().__init__(placeholder="Выберите исход матча", options=options)
        self.tournament_id = tournament_id
        self.logic = logic

    async def callback(self, interaction: Interaction):
        # Разбираем выбор: match_id и результат
        raw = interaction.values[0]
        match_id_str, result_code = raw.split(':', 1)
        match_id = int(match_id_str)
        # record_result пересекается с турниром.recordResult — учесть, чтобы не дублировать записи
        embed = self.logic.record_result(self.tournament_id, match_id, result_code)
        await interaction.response.edit_message(embed=embed, view=self.view)

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

