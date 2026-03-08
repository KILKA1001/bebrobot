import discord
from discord import Embed, Interaction, ButtonStyle, ui
from discord.ui import Button
from bot.utils import SafeView, safe_send
from typing import Optional
from discord.ext import commands
from bot.data.players_db import get_player_by_id
from bot.data.tournament_db import get_tournament_info
import math
from bot.systems.tournament_logic import (
    start_round as cmd_start_round,
    next_round as cmd_next_round,
    build_tournament_status_embed,
    build_participants_embed,
    build_tournament_bracket_embed,
    MODE_NAMES,
    refresh_bracket_message,
)
from bot.data.tournament_db import record_match_result as db_record_match_result

from bot.systems.tournament_logic import Tournament


def get_stage_name(participants: int) -> str:
    mapping = {
        2: "Финал",
        4: "1/2 финала",
        8: "1/4 финала",
        16: "1/8 финала",
        32: "1/16 финала",
    }
    return mapping.get(participants, f"Топ-{participants}")


class RoundManagementView(SafeView):
    """UI для управления раундами одного турнира."""

    persistent = True

    def __init__(
        self,
        tournament_id: int,
        logic: Tournament,
        ctx: commands.Context | None = None,
    ) -> None:
        super().__init__(timeout=None)
        self.tournament_id = tournament_id
        self.logic = logic
        self.ctx = ctx
        self.custom_id = f"manage_rounds:{tournament_id}"  # Добавляем custom_id

        # Первоначальная настройка кнопок
        self._setup_view()

    def _setup_view(self):
        """Создаёт кнопки управления в соответствии со статусом турнира."""
        self.clear_items()

        from bot.data.tournament_db import get_tournament_status

        status = get_tournament_status(self.tournament_id)

        start_disabled = status != "active"
        start_btn = Button(
            label="▶️ Начать раунд",
            style=ButtonStyle.green,
            custom_id=f"start_round:{self.tournament_id}",
            row=0,
            disabled=start_disabled,
        )
        start_btn.callback = self.on_start_round
        self.add_item(start_btn)

        next_btn = Button(
            label="⏭ Перейти к следующему",
            style=ButtonStyle.blurple,
            custom_id=f"next_round:{self.tournament_id}",
            row=0,
        )
        next_btn.callback = self.on_next_round
        self.add_item(next_btn)

        stop_btn = Button(
            label="🛑 Остановить раунд",
            style=ButtonStyle.red,
            custom_id=f"stop_round:{self.tournament_id}",
            row=1,
        )
        stop_btn.callback = self.on_stop_round
        self.add_item(stop_btn)

        status_btn = Button(
            label="📊 Показать статус",
            style=ButtonStyle.gray,
            custom_id=f"status_round:{self.tournament_id}",
            row=1,
        )
        status_btn.callback = self.on_status_round
        self.add_item(status_btn)

        participants_btn = Button(
            label="👥 Участники",
            style=ButtonStyle.gray,
            custom_id=f"list_participants:{self.tournament_id}",
            row=2,
        )
        participants_btn.callback = self.on_list_participants
        self.add_item(participants_btn)

        if status == "registration":
            activate_btn = Button(
                label="✅ Активировать турнир",
                style=ButtonStyle.success,
                custom_id=f"activate_tournament:{self.tournament_id}",
                row=2,
            )
            activate_btn.callback = self.on_activate_tournament
            self.add_item(activate_btn)
        else:
            back_btn = Button(
                label="🔙 Назад",
                style=ButtonStyle.secondary,
                custom_id=f"back_to_main:{self.tournament_id}",
                row=2,
            )
            back_btn.callback = self.on_back_to_main
            self.add_item(back_btn)

    async def on_activate_tournament(self, interaction: Interaction):
        """Переводит турнир в активный статус"""
        from bot.systems.tournament_logic import (
            set_tournament_status,
            generate_first_round,
            update_bet_message,
        )

        if set_tournament_status(self.tournament_id, "active"):
            await interaction.response.send_message(
                f"✅ Турнир #{self.tournament_id} активирован!",
                ephemeral=True,
            )
            guild = interaction.guild
            if guild:
                tour = await generate_first_round(
                    interaction.client, guild, self.tournament_id
                )
                if tour:
                    self.logic = tour
                await update_bet_message(guild, self.tournament_id)
            # Обновляем View
            self._setup_view()
            if interaction.message:
                await interaction.message.edit(view=self)
        else:
            await interaction.response.send_message(
                "❌ Не удалось активировать турнир", ephemeral=True
            )

    async def on_start_round(self, interaction: Interaction):
        await cmd_start_round(interaction, self.tournament_id)

    async def on_next_round(self, interaction: Interaction):
        await cmd_next_round(interaction, self.tournament_id)

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
            color=0xF39C12,
        )
        view = RoundManagementView(self.tournament_id, self.logic, self.ctx)
        await interaction.response.edit_message(embed=embed, view=view)

    async def on_list_participants(self, interaction: Interaction):
        embed = await build_participants_embed(
            self.tournament_id, interaction.guild
        )
        if embed:
            await interaction.response.send_message(embed=embed, ephemeral=True)
        else:
            await interaction.response.send_message(
                "❌ Не удалось получить список участников.", ephemeral=True
            )

    async def on_back_to_main(self, interaction: Interaction):
        """Возвращает на главное меню управления турниром."""
        from .manage_tournament_view import ManageTournamentView

        from types import SimpleNamespace

        if self.ctx is not None:
            ctx = self.ctx
            try:
                ctx = await self.ctx.bot.get_context(interaction)
            except ValueError:
                ctx = self.ctx
        else:
            try:
                ctx = await interaction.client.get_context(interaction)
            except ValueError:
                # Fallback to a minimal context with only bot, guild and author
                ctx = SimpleNamespace(
                    bot=interaction.client,
                    guild=interaction.guild,
                    author=interaction.user,
                )

        view = ManageTournamentView(self.tournament_id, ctx)
        embed = await build_tournament_bracket_embed(
            self.tournament_id, interaction.guild, include_id=True
        )
        if not embed:
            embed = await build_tournament_status_embed(
                self.tournament_id, include_id=True
            )

        if embed:
            await interaction.response.edit_message(embed=embed, view=view)
        else:
            await interaction.response.send_message(
                "❌ Не удалось загрузить данные турнира.", ephemeral=True
            )


class MatchResultView(SafeView):
    """UI для ввода результата конкретного матча."""

    def __init__(
        self,
        match_id: int,
        tournament_id: int,
        guild: discord.Guild,
        team_display: Optional[dict[int, str]] = None,
        *,
        round_no: int = 1,
        pair_index: int = 0,
    ):
        # timeout=None чтобы ожидать регистрации результата без ограничения
        super().__init__(timeout=None)
        self.match_id = match_id
        self.tournament_id = tournament_id
        self.guild = guild
        self.winner: Optional[int] = None
        self.round_no = round_no
        self.pair_index = pair_index
        info = get_tournament_info(tournament_id) or {}
        self.is_team = info.get("type") == "team"
        self.team_display = team_display or {}
        if self.is_team:
            self.win1.label = "\U0001F3C6 Команда 1"
            self.win2.label = "\U0001F3C6 Команда 2"

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

    @ui.button(label="🤝 Ничья", style=ButtonStyle.gray)
    async def draw(self, interaction: Interaction, button: Button):
        await self._report(interaction, 0)

    async def _report(self, interaction: Interaction, winner: int):
        ok = db_record_match_result(self.match_id, winner)
        if ok:
            self.winner = winner
            if winner == 0:
                text = "ничья"
            else:
                if self.is_team:
                    name = self.team_display.get(winner, f"Команда {winner}")
                    text = f"победитель — {name}"
                else:
                    text = f"победитель — игрок {winner}"

            await interaction.response.edit_message(
                embed=Embed(
                    title=f"Матч #{self.match_id}: {text}",
                    color=discord.Color.green(),
                ),
                view=None,
            )
            try:
                if self.guild:
                    await refresh_bracket_message(self.guild, self.tournament_id)
            except Exception:
                pass
            self.stop()
        else:
            await interaction.response.send_message(
                "❌ Ошибка при сохранении результата.",
                ephemeral=True,
            )


class PairSelectionView(SafeView):
    """Выбор пары для начала матчей."""

    def __init__(
        self,
        tournament_id: int,
        pairs: dict[int, list],
        guild: discord.Guild,
        round_no: int,
        stage_name: str,
        team_display: Optional[dict[int, str]] = None,
    ):
        super().__init__(timeout=None)
        self.tournament_id = tournament_id
        self.pairs = pairs
        self.guild = guild
        self.round_no = round_no
        self.stage_name = stage_name
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
                if p1:
                    name1 = p1.display_name
                else:
                    pl = get_player_by_id(m.player1_id)
                    name1 = pl["nick"] if pl else f"Игрок#{m.player1_id}"
            if m.player2_id in self.team_display:
                name2 = self.team_display[m.player2_id]
            else:
                p2 = self.guild.get_member(m.player2_id)
                if p2:
                    name2 = p2.display_name
                else:
                    pl = get_player_by_id(m.player2_id)
                    name2 = pl["nick"] if pl else f"Игрок#{m.player2_id}"
            label = f"{name1} vs {name2}"
            btn = ui.Button(
                label=label, style=ButtonStyle.primary, custom_id=f"pair_{idx}", row=row
            )
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
            await interaction.response.send_message(
                "Эта пара уже была выбрана.", ephemeral=True
            )
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
        wins = {1: 0, 2: 0}
        # запоминаем, какие игроки участвуют в паре,
        # чтобы позже определить победителя по их ID
        p1_id = matches[0].player1_id
        p2_id = matches[0].player2_id
        for n, m in enumerate(matches, start=1):
            if m.player1_id in self.team_display:
                v1 = self.team_display[m.player1_id]
            else:
                p1 = self.guild.get_member(m.player1_id)
                if p1:
                    v1 = p1.mention
                else:
                    pl = get_player_by_id(m.player1_id)
                    v1 = pl["nick"] if pl else f"Игрок#{m.player1_id}"
            if m.player2_id in self.team_display:
                v2 = self.team_display[m.player2_id]
            else:
                p2 = self.guild.get_member(m.player2_id)
                if p2:
                    v2 = p2.mention
                else:
                    pl = get_player_by_id(m.player2_id)
                    v2 = pl["nick"] if pl else f"Игрок#{m.player2_id}"

            mode_name = MODE_NAMES.get(m.mode_id, str(m.mode_id))

            match_embed = discord.Embed(
                title=f"Матч {n} — {self.stage_name}",
                description=f"{v1} vs {v2}",
                color=discord.Color.blue(),
            )
            match_embed.add_field(name="Режим", value=mode_name, inline=True)

            from bot.data.tournament_db import get_map_info

            info = get_map_info(str(m.map_id))
            if info:
                match_embed.add_field(
                    name="Карта",
                    value=f"{info.get('name', '')} (`{m.map_id}`)",
                    inline=True,
                )
                if info.get("image_url"):
                    match_embed.set_image(url=info["image_url"])
            else:
                match_embed.add_field(name="Карта", value=f"`{m.map_id}`", inline=True)

            view = MatchResultView(
                match_id=m.match_id,
                tournament_id=self.tournament_id,
                guild=self.guild,
                team_display=self.team_display,
                round_no=self.round_no,
                pair_index=idx,
            )

            if channel:
                await safe_send(channel, embed=match_embed, view=view)
                await view.wait()
                if view.winner == 1:
                    wins[1] += 1
                elif view.winner == 2:
                    wins[2] += 1

                if wins[1] == 2 or wins[2] == 2:
                    break

        if channel:
            if wins[1] > wins[2]:
                result_text = "Победила команда 1"
            elif wins[2] > wins[1]:
                result_text = "Победила команда 2"
            else:
                result_text = "Ничья"
            await safe_send(channel, f"Результат пары {idx}: {result_text}")

            try:
                await refresh_bracket_message(self.guild, self.tournament_id)
            except Exception:
                pass

            try:
                from bot.systems.bets_logic import (
                    payout_bets,
                    get_pair_summary,
                )
                from bot.data.tournament_db import get_tournament_size

                size = get_tournament_size(self.tournament_id)
                total_rounds = int(math.ceil(math.log2(size))) if size > 1 else 1
                # определяем ID победителя пары
                winner_id = (
                    p1_id if wins[1] > wins[2] else p2_id if wins[2] > wins[1] else 0
                )
                summary = get_pair_summary(
                    self.tournament_id,
                    self.round_no,
                    idx,
                    winner_id,
                    total_rounds,
                )

                class PayoutView(SafeView):
                    def __init__(self,
                                 tournament_id: int,
                                 round_no: int,
                                 pair_index: int,
                                 winner_id: int,
                                 total_rounds: int) -> None:
                        super().__init__(timeout=60)
                        self.tournament_id = tournament_id
                        self.round_no = round_no
                        self.pair_index = pair_index
                        self.winner_id = winner_id
                        self.total_rounds = total_rounds

                    @ui.button(label="Выплатить", style=ButtonStyle.success)
                    async def confirm(self, inter: Interaction, button: ui.Button):
                        payout_bets(
                            self.tournament_id,
                            self.round_no,
                            self.pair_index,
                            self.winner_id,
                            self.total_rounds,
                        )
                        await inter.response.edit_message(
                            content="Ставки выплачены",
                            view=None,
                        )
                        self.stop()

                emb = discord.Embed(
                    title="Ставки на пару",
                    description=(
                        f"Всего: {summary['total']}\n"
                        f"Выиграли: {summary['won']}\n"
                        f"Проиграли: {summary['lost']}\n"
                        f"Профит банка: {summary['profit']:.1f}"
                    ),
                    color=discord.Color.orange(),
                )
                view = PayoutView(
                    self.tournament_id,
                    self.round_no,
                    idx,
                    winner_id,
                    total_rounds,
                )
                await safe_send(channel, embed=emb, view=view)
            except Exception:
                pass


# Функция-помощник для отправки стартового сообщения турнира
async def announce_round_management(
    channel,
    tournament_id: int,
    logic: Tournament,
    ctx: commands.Context | None = None,
):
    """
    Отправляет embed-подложку с кнопками управления раундами.
    """
    embed = Embed(
        title=f"⚙️ Управление турниром #{tournament_id}",
        description=(
            "Используйте кнопки ниже для контроля раундов.\n"
            "Нажмите **▶️** для старта первого раунда."
        ),
        color=0xF39C12,
    )
    view = RoundManagementView(tournament_id, logic, ctx)
    await safe_send(channel, embed=embed, view=view)
