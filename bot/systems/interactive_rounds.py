from discord import Embed, Interaction, ButtonStyle, SelectOption
from discord.ui import View, Button, Select
from bot.systems.tournament_logic import (
    start_round as cmd_start_round,
    join_tournament,            # не обязательно, но для примера
    report_result as cmd_report_result,
    build_tournament_status_embed,
)

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
    """Представление для ввода результатов матчей."""
    def __init__(self, tournament_id: int, logic: Tournament, matches: list):
        super().__init__(timeout=None)
        self.tournament_id = tournament_id
        self.logic = logic

        # Для каждого матча добавляем select-меню
        for match in matches:
            opts = [
                SelectOption(label=f"Победа {match.player_a}", value=f"{match.id}:A"),
                SelectOption(label=f"Победа {match.player_b}", value=f"{match.id}:B"),
                SelectOption(label="Ничья", value=f"{match.id}:D"),
            ]
            sel = MatchResultSelect(tournament_id, logic, opts)
            self.add_item(sel)

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
        await cmd_report_result(interaction, match_id, int(result_code))


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

