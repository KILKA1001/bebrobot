from discord import Embed, Interaction, ButtonStyle, SelectOption
from discord.ui import View, Button, Select, button

from bot.systems.tournament_logic import Tournament

class RoundManagementView(View):
    """UI для управления раундами одного турнира."""

    persistent = True

    def __init__(self, tournament_id: int, logic: Tournament):
        super().__init__(timeout=None)
        self.tournament_id = tournament_id
        self.logic = logic

        self.start_round_button = Button(
            label="▶️ Начать раунд",
            style=ButtonStyle.green,
            custom_id=f"start_round:{tournament_id}",
            row=0,
        )
        self.start_round_button.callback = self.on_start_round
        self.add_item(self.start_round_button)

        self.next_round_button = Button(
            label="⏭ Перейти к следующему",
            style=ButtonStyle.blurple,
            custom_id=f"next_round:{tournament_id}",
            row=0,
        )
        self.next_round_button.callback = self.on_next_round
        self.add_item(self.next_round_button)

        self.stop_round_button = Button(
            label="🛑 Остановить раунд",
            style=ButtonStyle.red,
            custom_id=f"stop_round:{tournament_id}",
            row=1,
        )
        self.stop_round_button.callback = self.on_stop_round
        self.add_item(self.stop_round_button)

        self.status_round_button = Button(
            label="📊 Показать статус",
            style=ButtonStyle.gray,
            custom_id=f"status_round:{tournament_id}",
            row=1,
        )
        self.status_round_button.callback = self.on_status_round
        self.add_item(self.status_round_button)

        self.manage_rounds_button = Button(
            label="⚙ Управление раундами",
            style=ButtonStyle.primary,
            custom_id=f"manage_rounds:{tournament_id}",
            row=2,
        )
        self.manage_rounds_button.callback = self.on_manage_rounds
        self.add_item(self.manage_rounds_button)

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
                SelectOption(label=f"Победа {match.player_a}", value=f"{match.id}:A"),
                SelectOption(label=f"Победа {match.player_b}", value=f"{match.id}:B"),
                SelectOption(label="Ничья", value=f"{match.id}:D"),
        self.next_round_button.custom_id = f"next_round:{tournament_id}"
        self.stop_round_button.custom_id = f"stop_round:{tournament_id}"
        self.status_round_button.custom_id = f"status_round:{tournament_id}"
        self.manage_rounds_button.custom_id = f"manage_rounds:{tournament_id}"

    @button(label="▶️ Начать раунд", style=ButtonStyle.green, row=0)
    async def start_round_button(self, interaction: Interaction, button: Button):
        # Пересекается с командой ?startround – при наличии данной функции команду лучше отключить или перенаправить здесь
        embed = self.logic.start_round(self.tournament_id)
        await interaction.response.edit_message(embed=embed, view=self)

    @button(label="⏭ Перейти к следующему", style=ButtonStyle.blurple, row=0)
    async def next_round_button(self, interaction: Interaction, button: Button):
        # Проверяем, что все результаты есть: логика генерации нового раунда уже есть в generate_next_round
        embed = self.logic.generate_next_round(self.tournament_id)
        await interaction.response.edit_message(embed=embed, view=self)

    @button(label="🛑 Остановить раунд", style=ButtonStyle.red, row=1)
    async def stop_round_button(self, interaction: Interaction, button: Button):
        # Позволяет приостановить текущий раунд и вернуться к редактированию
        embed = self.logic.get_current_round_embed(self.tournament_id)
        await interaction.response.edit_message(embed=embed, view=self)

    @button(label="📊 Показать статус", style=ButtonStyle.gray, row=1)
    async def status_round_button(self, interaction: Interaction, button: Button):
        # Пересекается с командой ?tournamentstatus — можно либо отключить команду, либо внутри команды отправлять этот же вид

        embed = self.logic.get_current_round_embed(self.tournament_id)
        current_matches = self.logic.get_current_matches(self.tournament_id)
        view = MatchResultView(self.tournament_id, self.logic, current_matches)
        await interaction.response.edit_message(embed=embed, view=view)


    async def on_manage_rounds(self, interaction: Interaction):
=======
    @button(label="⚙ Управление раундами", style=ButtonStyle.primary, row=2)
    async def manage_rounds_button(self, interaction: Interaction, button: Button):
        """
        Обработчик клика по кнопке ⚙ — просто заново открывает панель управления раундами.
        """

        await announce_round_management(
            interaction.channel,
            self.tournament_id,
            self.logic,
        )


class MatchResultView(View):
    """Представление для ввода результатов матчей."""

    def __init__(self, tournament_id: int, logic: Tournament, matches: list):
        super().__init__(timeout=None)
        self.tournament_id = tournament_id
        self.logic = logic

        # Для каждого матча добавляем select-меню
        for match in matches:
            options = [
                Select.Option(label=f"Победа {match.player_a}", value=f"{match.id}:A"),
                Select.Option(label=f"Победа {match.player_b}", value=f"{match.id}:B"),
                Select.Option(label="Ничья", value=f"{match.id}:D")
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
        match_id_str, result_code = raw.split(':')
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

