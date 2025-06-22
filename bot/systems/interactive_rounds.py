from discord import Embed, Interaction, ButtonStyle
from discord.ui import View, Button, Select
from dataclasses import dataclass

from bot.systems.tournament_logic import Tournament


@dataclass
class MatchInfo:
    id: int
    player_a: str
    player_b: str


class InteractiveTournamentLogic:
    """Wrapper providing high level helpers expected by the UI."""

    def __init__(self, tournament: Tournament):
        self.tournament = tournament

    # ─── Internal helpers ──────────────────────────────────────────────────
    def _build_round_embed(self, round_no: int, matches: list) -> Embed:
        embed = Embed(
            title=f"Раунд {round_no}",
            description=f"Всего матчей: {len(matches)}",
            color=0x3498DB,
        )
        for idx, m in enumerate(matches, start=1):
            embed.add_field(
                name=f"Матч {idx}",
                value=(
                    f"{m.player1_id} vs {m.player2_id}\n"
                    f"Режим {m.mode_id}\n"
                    f"Карта {m.map_id}"
                ),
                inline=False,
            )
        return embed

    # ─── Public API used by RoundManagementView ────────────────────────────
    def start_round(self, tournament_id: int) -> Embed:
        matches = self.tournament.generate_round()
        return self._build_round_embed(self.tournament.current_round - 1, matches)

    def generate_next_round(self, tournament_id: int) -> Embed:
        prev = self.tournament.current_round - 1
        winners = self.tournament.get_winners(prev)
        if len(winners) < 2 or len(winners) % 2 != 0:
            return Embed(title="Невозможно начать следующий раунд",
                         description="Недостаточно победителей.")
        self.tournament.participants = winners
        matches = self.tournament.generate_round()
        return self._build_round_embed(self.tournament.current_round - 1, matches)

    def get_current_round_embed(self, tournament_id: int) -> Embed:
        round_no = self.tournament.current_round - 1
        matches = self.tournament.matches.get(round_no, [])
        return self._build_round_embed(round_no, matches)

    def get_current_matches(self, tournament_id: int) -> list[MatchInfo]:
        round_no = self.tournament.current_round - 1
        matches = self.tournament.matches.get(round_no, [])
        info: list[MatchInfo] = []
        for idx, m in enumerate(matches, start=1):
            info.append(MatchInfo(idx, str(m.player1_id), str(m.player2_id)))
        return info

    def record_result(self, tournament_id: int, match_id: int, result_code: str) -> Embed:
        round_no = self.tournament.current_round - 1
        winner_map = {"A": 1, "B": 2, "D": 0}
        winner = winner_map.get(result_code, 0)
        self.tournament.record_result(round_no, match_id - 1, winner)
        return self.get_current_round_embed(tournament_id)

class RoundManagementView(View):
    """UI для управления раундами одного турнира."""

    persistent = True

    def __init__(self, tournament_id: int, logic: InteractiveTournamentLogic):
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
        embed = self.logic.get_current_round_embed(self.tournament_id)
        current_matches = self.logic.get_current_matches(self.tournament_id)
        view = MatchResultView(self.tournament_id, self.logic, current_matches)
        await interaction.response.edit_message(embed=embed, view=view)


    async def on_manage_rounds(self, interaction: Interaction):
        await announce_round_management(
            interaction.channel,
            self.tournament_id,
            self.logic,
        )


class MatchResultView(View):
    """Представление для ввода результатов матчей."""

    def __init__(self, tournament_id: int, logic: InteractiveTournamentLogic, matches: list[MatchInfo]):
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
    def __init__(self, tournament_id: int, logic: InteractiveTournamentLogic, options: list):
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
async def announce_round_management(channel, tournament_id: int, logic: InteractiveTournamentLogic):
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

