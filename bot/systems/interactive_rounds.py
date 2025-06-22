from discord import Embed, Interaction
from discord import Embed, Interaction, SelectOption, ButtonStyle
from discord.ui import View, Button, Select, button

from bot.systems.tournament_logic import create_tournament_logic, Tournament

class RoundManagementView(View):
    persistent = True
    def __init__(self, tournament_id: int, logic: Tournament):
        super().__init__(timeout=None)
        self.tournament_id = tournament_id
        self.logic = logic
    """
    View для интерактивного управления раундами турнира через кнопки и меню.
    """
    def __init__(self, tournament_id: int, logic: Tournament):
        super().__init__(timeout=None)
        self.tournament_id = tournament_id
        self.logic = logic

        # Кнопка начала раунда
        self.add_item(Button(
            label="▶️ Начать раунд",
            style=ButtonStyle.green,
            custom_id=f"start_round:{tournament_id}"
        ))
        # Кнопка перехода к следующему раунду
        self.add_item(Button(
            label="⏭ Перейти к следующему",
            style=ButtonStyle.blurple,
            custom_id=f"next_round:{tournament_id}"
        ))
        # Кнопка остановки/возврата
        self.add_item(Button(
            label="🛑 Остановить раунд",
            style=ButtonStyle.red,
            custom_id=f"stop_round:{tournament_id}"
        ))
        # Кнопка показа статуса
        self.add_item(Button(
            label="📊 Показать статус",
            style=ButtonStyle.gray,
            custom_id=f"status_round:{tournament_id}"
        ))
        self.add_item(Button(
            label="⚙ Управление раундами",
            style=ButtonStyle.primary,
            custom_id=f"manage_rounds:{tournament_id}"
        ))

    @button(custom_id=lambda self: f"start_round:{self.tournament_id}")
    async def start_round_button(self, button: Button, interaction: Interaction):
        # Пересекается с командой ?startround – при наличии данной функции команду лучше отключить или перенаправить здесь
        embed = self.logic.start_round(self.tournament_id)
        await interaction.response.edit_message(embed=embed, view=self)

    @button(custom_id=lambda self: f"next_round:{self.tournament_id}")
    async def next_round_button(self, button: Button, interaction: Interaction):
        # Проверяем, что все результаты есть: логика генерации нового раунда уже есть в generate_next_round
        embed = self.logic.generate_next_round(self.tournament_id)
        await interaction.response.edit_message(embed=embed, view=self)

    @button(custom_id=lambda self: f"stop_round:{self.tournament_id}")
    async def stop_round_button(self, button: Button, interaction: Interaction):
        # Позволяет приостановить текущий раунд и вернуться к редактированию
        embed = self.logic.get_current_round_embed(self.tournament_id)
        await interaction.response.edit_message(embed=embed, view=self)

    @button(custom_id=lambda self: f"status_round:{self.tournament_id}")
    async def status_round_button(self, button: Button, interaction: Interaction):
        # Пересекается с командой ?tournamentstatus — можно либо отключить команду, либо внутри команды отправлять этот же вид
        embed = self.logic.get_current_round_embed(self.tournament_id)
        # Добавляем Select меню для каждого матча
        current_matches = self.logic.get_current_matches(self.tournament_id)
        view = MatchResultView(self.tournament_id, self.logic, current_matches)
        await interaction.response.edit_message(embed=embed, view=view)

    @button(custom_id=lambda self: f"manage_rounds:{self.tournament_id}")
    async def manage_rounds_button(self, button: Button, interaction: Interaction):
        """
        Обработчик клика по кнопке ⚙ — просто заново открывает панель управления раундами.
        """
        await announce_round_management(
            interaction.channel,
            self.tournament_id,
            self.logic
        )


class MatchResultView(View):
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
        super().__init__(placeholder="Выберите результат", options=options)
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
    embed = Embed(title=f"Управление Турниром #{tournament_id}")
    embed.description = "Нажмите ▶️, чтобы начать первый раунд."
    view = RoundManagementView(tournament_id, logic)
    await channel.send(embed=embed, view=view)

