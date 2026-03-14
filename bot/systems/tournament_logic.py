import random
import logging
from typing import List, Dict, Optional
import asyncio
import math
import discord
from discord import ui, Embed, ButtonStyle
from bot.utils import SafeView, safe_send, format_moscow_time
import os
from bot.data import db
from discord.ext import commands
from discord.abc import Messageable
from discord import TextChannel, Thread, Interaction
import bot.data.tournament_db as tournament_db
from bot.data.players_db import get_player_by_id, add_player_to_tournament
from bot.utils import send_temp
from bot.data.tournament_db import count_matches
from bot.data.tournament_db import (
    add_discord_participant as db_add_participant,
    list_participants as db_list_participants,
    create_matches as db_create_matches,
    get_matches as db_get_matches,
    record_match_result as db_record_match_result,
    save_tournament_result as db_save_tournament_result,
    update_tournament_status as db_update_tournament_status,
    list_participants_full as db_list_participants_full,
    remove_discord_participant as db_remove_discord_participant,
    remove_player_from_tournament,
    create_tournament_record as db_create_tournament_record,
    get_tournament_info,
    get_announcement_message_id,
    get_tournament_size,
    get_tournament_author,
    set_tournament_author,
    confirm_participant,
    list_recent_results,
    get_expired_registrations,
    update_start_time,
    delete_tournament as db_delete_tournament,
    list_maps_by_mode,
)
from bot.systems import tournament_rewards_logic as rewards
from bot.systems.tournament_bank_logic import validate_and_save_bank

logger = logging.getLogger(__name__)

# Уже уведомлённые о завершении регистрации турниры
expired_notified: set[int] = set()

# Настройки автоматических команд турниров
# {tournament_id: {"auto": bool, "team_names": {team_id: name}}}
AUTO_TEAM_DATA: Dict[int, dict] = {}


def create_auto_teams(tournament_id: int, team_count: int) -> None:
    """Создаёт записи о командах по умолчанию."""
    AUTO_TEAM_DATA[tournament_id] = {
        "auto": True,
        "team_names": {i: f"Новая команда {i}" for i in range(1, team_count + 1)},
    }


def is_auto_team(tournament_id: int) -> bool:
    return AUTO_TEAM_DATA.get(tournament_id, {}).get("auto", False)


def get_auto_team_names(tournament_id: int) -> Dict[int, str]:
    return AUTO_TEAM_DATA.get(tournament_id, {}).get("team_names", {})


def rename_auto_team(tournament_id: int, team_id: int, new_name: str) -> None:
    if tournament_id in AUTO_TEAM_DATA:
        AUTO_TEAM_DATA[tournament_id].setdefault("team_names", {})[team_id] = new_name


def assign_auto_team(tournament_id: int, user_id: int) -> bool:
    """Регистрирует игрока в первую неполную команду."""
    teams = get_auto_team_names(tournament_id)
    if not teams:
        return False
    participants = db_list_participants_full(tournament_id)
    counts: Dict[int, int] = {tid: 0 for tid in teams}
    for p in participants:
        tid = p.get("team_id")
        if tid is not None and tid in counts:
            counts[tid] += 1
    for tid in sorted(teams):
        if counts.get(tid, 0) < 3:
            return db_add_participant(
                tournament_id, user_id, team_id=tid, team_name=teams[tid]
            )
    return False


MODE_NAMES: Dict[int, str] = {
    1: "Нокаут",
    2: "Награда за поимку",
    3: "Захват кристаллов",
    4: "Броулбол",
}
ANNOUNCE_CHANNEL_ID = int(os.getenv("TOURNAMENT_ANNOUNCE_CHANNEL_ID", 0))
MODE_IDS = list(MODE_NAMES.keys())

# Карты сгруппированы по режиму; по возможности берём из базы
MAPS_BY_MODE: Dict[int, List[str]] = list_maps_by_mode()
if not MAPS_BY_MODE:
    MAPS_BY_MODE = {
        1: ["1.1 1", "1.2 2", "1.3 3"],
        2: ["2.1 4", "2.2 5", "2.3 6"],
        3: ["3.1 7", "3.2 8", "3.3 9"],
        4: ["4.1 10", "4.2 11", "4.3 12"],
    }


def format_reward_details(bank_type: int, manual_amount: float, team_mode: bool) -> str:
    """Возвращает текст с наградами за 1 и 2 место."""
    bank_total, _u, _b = rewards.calculate_bank(bank_type, manual_amount=manual_amount)
    if bank_total <= 0:
        return "Награды не выдаются"

    members = 3 if team_mode else 1
    reward_first_each = bank_total * 0.5 / members
    reward_second_each = bank_total * 0.25 / members

    return (
        f"🥇 1 место — {reward_first_each:.1f} баллов и золотой билет каждому\n"
        f"🥈 2 место — {reward_second_each:.1f} баллов и обычный билет каждому"
    )


def format_tournament_title(
    name: str | None,
    start_time: Optional[str],
    tournament_id: int,
    include_id: bool = False,
) -> str:
    """Формирует строку вида 'Название Месяц, Год [#id]'."""
    title = name or "Турнир"
    date_part = ""
    if start_time:
        from datetime import datetime

        try:
            dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
            months = [
                "января",
                "февраля",
                "марта",
                "апреля",
                "мая",
                "июня",
                "июля",
                "августа",
                "сентября",
                "октября",
                "ноября",
                "декабря",
            ]
            date_part = f" {months[dt.month - 1]} {dt.year}"
        except Exception:
            pass
    full = f"{title}{date_part}".strip()
    if include_id:
        full += f" #{tournament_id}"
    return full


# ───── База данных ─────


def create_tournament_record(
    t_type: str,
    size: int,
    start_time: Optional[str] = None,
    author_id: Optional[int] = None,
    team_auto: bool | None = None,
    name: Optional[str] = None,
) -> int:
    """Создаёт запись о турнире и возвращает его ID."""
    return db_create_tournament_record(
        t_type, size, start_time, author_id, team_auto, name
    )


def set_tournament_status(tournament_id: int, status: str) -> bool:
    """
    Изменяет статус турнира (registration/active/finished).
    Возвращает True при успехе.
    """
    return db_update_tournament_status(tournament_id, status)


def delete_tournament_record(tournament_id: int) -> bool:
    """
    Удаляет турнир и все связанные с ним записи (ON DELETE CASCADE).
    """
    try:
        db_delete_tournament(tournament_id)
        return True
    except Exception:
        return False


def rename_tournament(tournament_id: int, new_name: str) -> bool:
    """Изменяет название турнира."""
    from bot.data.tournament_db import update_tournament_name

    return update_tournament_name(tournament_id, new_name)


# ───── Доменные классы ─────


class Match:
    def __init__(self, player1_id: int, player2_id: int, mode_id: int, map_id: str):
        self.player1_id = player1_id
        self.player2_id = player2_id
        self.mode_id = mode_id  # сохраняем числовой ID
        self.map_id = map_id
        self.result: Optional[int] = None
        self.match_id: Optional[int] = None
        self.bank_type: Optional[int] = None
        self.manual_amount: Optional[float] = None


class Tournament:
    """Управление сеткой турнира в оперативной памяти."""

    def __init__(
        self,
        participants: List[int],
        modes: List[int],
        maps_by_mode: Dict[int, List[str]],
        team_size: int = 1,
        *,
        shuffle: bool = True,
    ) -> None:
        self.team_size = max(1, team_size)
        self.modes = modes
        self.maps_by_mode = maps_by_mode
        self.current_round = 1
        self.matches: Dict[int, List[Match]] = {}

        if self.team_size > 1:
            self.team_map: Dict[int, List[int]] = {}
            team_ids: List[int] = []
            tid = 1
            for i in range(0, len(participants), self.team_size):
                members = participants[i : i + self.team_size]
                if len(members) < self.team_size:
                    break
                self.team_map[tid] = members
                team_ids.append(tid)
                tid += 1
            self.participants = team_ids
            if shuffle:
                random.shuffle(self.participants)
        else:
            self.participants = participants.copy()
            if shuffle:
                random.shuffle(self.participants)
            self.team_map = {}

    def generate_round(self) -> List[Match]:
        round_matches: List[Match] = []
        for i in range(0, len(self.participants), 2):
            p1, p2 = self.participants[i], self.participants[i + 1]
            picked = self.modes[:3]
            for mode_id in picked:
                map_list = self.maps_by_mode.get(mode_id, [])
                if map_list:
                    raw_choice = random.choice(map_list)
                    map_choice = str(raw_choice).split()[-1]
                else:
                    map_choice = ""
                round_matches.append(Match(p1, p2, mode_id, map_choice))
        self.matches[self.current_round] = round_matches
        self.current_round += 1
        return round_matches

    def record_result(self, round_number: int, match_index: int, winner: int):
        try:
            match = self.matches[round_number][match_index]
            match.result = winner
        except Exception:
            raise IndexError("Матч не найден в указанном раунде")

    def get_winners(self, round_number: int) -> List[int]:
        winners: List[int] = []
        pairs: Dict[tuple[int, int], list[int]] = {}
        for m in self.matches.get(round_number, []):
            res = m.result
            if res not in (1, 2):
                continue
            key = (m.player1_id, m.player2_id)
            if key not in pairs:
                pairs[key] = [0, 0]
            if res == 1:
                pairs[key][0] += 1
            else:
                pairs[key][1] += 1

        for (p1, p2), (w1, w2) in pairs.items():
            winners.append(p1 if w1 >= w2 else p2)

        return winners


# Предопределённые режимы и карты
MODES = ["режим1", "режим2", "режим3", "режим4"]
MAPS = {
    "режим1": ["1.1 1", "1.2 2", "1.3 3"],
    "режим2": ["2.1 4", "2.2 5", "2.3 6"],
    "режим3": ["3.1 7", "3.2 8", "3.3 9"],
    "режим4": ["4.1 10", "4.2 11", "4.3 12"],
}


def create_tournament_object(participants: List[int]) -> Tournament:
    """
    Возвращает в оперативке (без БД) новый объект Tournament с заданными участниками.
    """
    return Tournament(participants, MODE_IDS, MAPS_BY_MODE)


# ───── UI для создания турнира ─────


class TournamentSetupView(SafeView):
    """
    Многошаговый UI: выбор типа, размера, подтверждение, а затем запись в БД.
    """

    def __init__(self, author_id: int):
        super().__init__(timeout=120)
        self.author_id = author_id
        self.manual_amount = 0.0
        self.bets_bank = 0.0
        self.t_type: Optional[str] = None
        self.size: Optional[int] = None
        self.bank_type: Optional[int] = None
        self.start_time: Optional[str] = None
        self.title: Optional[str] = None
        self.team_auto: bool = False
        self.message: Optional[discord.Message] = None
        self._build_type_buttons()

    @staticmethod
    def initial_embed() -> discord.Embed:
        return discord.Embed(
            title="Создание нового турнира",
            description="Выберите **тип** турнира:",
            color=discord.Color.gold(),
        )

    def disable_all_items(self) -> None:
        """
        Отключает все кнопки (делает их disabled=True),
        чтобы избежать дальнейших нажатий.
        """
        for item in self.children:
            if isinstance(item, ui.Button):
                item.disabled = True

    def _build_type_buttons(self):
        self.clear_items()
        # создаём кнопку Дуэль
        btn1 = ui.Button(
            label="Дуэльный 1×1",
            style=discord.ButtonStyle.primary,
            custom_id="type_duel",
        )
        # привязываем её колбэк
        btn1.callback = self.on_type_duel
        self.add_item(btn1)

        # создаём кнопку Командный
        btn2 = ui.Button(
            label="Командный 3×3",
            style=discord.ButtonStyle.primary,
            custom_id="type_team",
        )
        btn2.callback = self.on_type_team
        self.add_item(btn2)

    def _build_distribution_buttons(self):
        self.clear_items()
        auto_btn = ui.Button(
            label="Авто-команды",
            style=discord.ButtonStyle.primary,
            custom_id="dist_auto",
        )
        manual_btn = ui.Button(
            label="Вручную",
            style=discord.ButtonStyle.secondary,
            custom_id="dist_manual",
        )
        auto_btn.callback = self.on_dist_auto
        manual_btn.callback = self.on_dist_manual
        self.add_item(auto_btn)
        self.add_item(manual_btn)

    def _build_size_buttons(self):
        self.clear_items()
        # Варианты размера в зависимости от типа
        choices = [4, 8, 16] if self.t_type == "duel" else [6, 12, 24]
        for n in choices:
            btn = ui.Button(
                label=str(n), style=discord.ButtonStyle.secondary, custom_id=f"size_{n}"
            )
            # вешаем callback, который будет получать только interaction
            btn.callback = self.on_size
            self.add_item(btn)

    def _build_bank_type_selector(self):
        self.clear_items()

        select = ui.Select(
            placeholder="Выберите источник банка наград",
            options=[
                discord.SelectOption(
                    label="Тип 1 — Пользователь",
                    value="1",
                    description="Пользователь платит 50% (мин. 15 баллов)",
                ),
                discord.SelectOption(
                    label="Тип 2 — Смешанный",
                    value="2",
                    description="25% платит пользователь, 75% — банк Бебр",
                ),
                discord.SelectOption(
                    label="Тип 3 — Клуб", value="3", description="100% из банка Бебр"
                ),
                discord.SelectOption(
                    label="🧪 TEST — Без наград (тест)",
                    value="4",
                    description="Никаких выплат и списаний, только для проверки",
                ),
            ],
            custom_id="bank_type",
        )
        select.callback = self.on_select_bank_type
        self.add_item(select)

    def _build_confirm_buttons(self):
        self.clear_items()
        name_btn = ui.Button(
            label="Название", style=discord.ButtonStyle.secondary, custom_id="set_name"
        )
        name_btn.callback = self.on_set_name
        self.add_item(name_btn)
        date_btn = ui.Button(
            label="📅 Дата старта",
            style=discord.ButtonStyle.secondary,
            custom_id="set_date",
        )
        date_btn.callback = self.on_set_date
        self.add_item(date_btn)

        bet_bank_btn = ui.Button(
            label="Банк ставок",
            style=discord.ButtonStyle.secondary,
            custom_id="bet_bank",
        )
        bet_bank_btn.callback = self.on_set_bet_bank
        self.add_item(bet_bank_btn)
        # Кнопка «Подтвердить»
        btn_confirm = ui.Button(
            label="✅ Подтвердить",
            style=discord.ButtonStyle.success,
            custom_id="confirm",
        )
        btn_confirm.callback = self.on_confirm
        self.add_item(btn_confirm)

        # Кнопка «Отменить»
        btn_cancel = ui.Button(
            label="❌ Отменить", style=discord.ButtonStyle.danger, custom_id="cancel"
        )
        btn_cancel.callback = self.on_cancel
        self.add_item(btn_cancel)

    async def on_set_date(self, interaction: discord.Interaction):
        await interaction.response.send_modal(StartDateModal(self))

    async def on_set_bet_bank(self, interaction: discord.Interaction):
        await interaction.response.send_modal(BetBankModal(self))

    async def on_set_name(self, interaction: discord.Interaction):
        await interaction.response.send_modal(TournamentNameModal(self))

    async def interaction_check(self, inter: discord.Interaction) -> bool:
        # Только автор команды может управлять этим View
        return inter.user.id == self.author_id

    async def on_type_duel(self, interaction: discord.Interaction):
        self.t_type = "duel"
        embed = discord.Embed(
            title="Создание турнира",
            description="🏆 **Дуэльный 1×1**\n\nТеперь выберите **количество участников**:",
            color=discord.Color.gold(),
        )
        self._build_size_buttons()
        await interaction.response.edit_message(embed=embed, view=self)

    async def on_type_team(self, interaction: discord.Interaction):
        self.t_type = "team"
        embed = discord.Embed(
            title="Создание турнира",
            description="🤝 **Командный 3×3**\n\nВыберите способ распределения команд:",
            color=discord.Color.gold(),
        )
        self._build_distribution_buttons()
        await interaction.response.edit_message(embed=embed, view=self)

    async def on_dist_auto(self, interaction: discord.Interaction):
        self.team_auto = True
        embed = discord.Embed(
            title="Создание турнира",
            description="🤖 Автоматическое распределение\n\nВыберите **количество участников**:",
            color=discord.Color.gold(),
        )
        self._build_size_buttons()
        await interaction.response.edit_message(embed=embed, view=self)

    async def on_dist_manual(self, interaction: discord.Interaction):
        self.team_auto = False
        embed = discord.Embed(
            title="Создание турнира",
            description="📝 Ручное распределение\n\nВыберите **количество участников**:",
            color=discord.Color.gold(),
        )
        self._build_size_buttons()
        await interaction.response.edit_message(embed=embed, view=self)

    async def on_select_bank_type(self, interaction: discord.Interaction):
        data = interaction.data or {}
        selected = data.get("values", ["1"])[0]
        self.bank_type = int(selected)

        embed = discord.Embed(
            title="Источник банка наград выбран",
            description=f"Вы выбрали тип: **{self.bank_type}**",
            color=discord.Color.blue(),
        )

        # Тип 1 требует сумму
        if self.bank_type == 1:
            embed.add_field(
                name="⚠️ Нужно ввести сумму", value="Мин. 15 баллов", inline=False
            )
            await interaction.response.send_modal(BankAmountModal(self))
        else:
            embed = discord.Embed(
                title="Источник банка наград выбран",
                description=f"Вы выбрали тип: **{self.bank_type}**",
                color=discord.Color.blue(),
            )
            self._build_confirm_buttons()
            await interaction.response.edit_message(embed=embed, view=self)

    async def on_size(self, interaction: discord.Interaction):
        # достаём custom_id из payload и парсим число
        data = interaction.data or {}
        cid = data.get("custom_id", "")
        try:
            self.size = int(cid.split("_", 1)[1])
        except (IndexError, ValueError):
            # если вдруг не удалось, просто игнорируем
            return
        type_name = "Дуэльный 1×1" if self.t_type == "duel" else "Командный 3×3"
        if self.t_type == "team":
            count_text = f"👥 **Команд:** {self.size // 3} ({self.size} игроков)"
        else:
            count_text = f"👥 **Участников:** {self.size}"
        embed = discord.Embed(
            title="Создание турнира",
            description=(
                f"🏆 **Тип:** {type_name}\n"
                f"{count_text}\n\n"
                "Нажмите **✅ Подтвердить** или **❌ Отменить**"
            ),
            color=discord.Color.gold(),
        )
        self._build_confirm_buttons()
        self._build_bank_type_selector()
        await interaction.response.edit_message(embed=embed, view=self)

    async def on_confirm(self, interaction: discord.Interaction):
        try:
            # Убедимся, что пользователь действительно выбрал и тип, и размер
            if self.t_type is None or self.size is None:
                # На случай, если кто-то умудрился нажать «Подтвердить» раньше времени
                await interaction.response.send_message(
                    "❌ Ошибка: сначала выберите тип и количество участников.",
                    ephemeral=True,
                )
                return

            if self.start_time is None:
                await interaction.response.send_message(
                    '❌ Сначала укажите дату начала турнира через кнопку "📅 Дата старта".',
                    ephemeral=True,
                )
                return

            # Теперь тип и размер — точно str и int
            tour_id = create_tournament_record(
                self.t_type,
                self.size,
                self.start_time,
                author_id=self.author_id,
                team_auto=self.team_auto if self.t_type == "team" else None,
                name=self.title,
            )
            ok, msg = validate_and_save_bank(
                tour_id, self.bank_type or 1, self.manual_amount
            )
            if not ok:
                await interaction.response.send_message(msg, ephemeral=True)
                return
            if self.t_type == "team" and self.team_auto:
                create_auto_teams(tour_id, self.size // 3)
            if self.bets_bank > 0:
                from bot.data import db as _db
                from bot.data import tournament_db as tdb

                if self.bank_type != 4:
                    if not _db.spend_from_bank(
                        self.bets_bank,
                        self.author_id,
                        f"Банк ставок турнира #{tour_id}",
                    ):
                        await interaction.response.send_message(
                            "❌ Недостаточно средств в банке для банка ставок",
                            ephemeral=True,
                        )
                        return
                tdb.create_bet_bank(tour_id, self.bets_bank)
            typetxt = "Дуэльный 1×1" if self.t_type == "duel" else "Командный 3×3"
            display_public = format_tournament_title(
                self.title, self.start_time, tour_id
            )
            display_admin = format_tournament_title(
                self.title, self.start_time, tour_id, include_id=True
            )
            prize_text = {
                1: f"🏅 Тип 1 — {self.manual_amount:.2f} баллов от пользователя",
                2: "🥈 Тип 2 — 30 баллов (25% платит игрок)",
                3: "🥇 Тип 3 — 30 баллов (из банка Бебр)",
                4: "🛠️ TEST — тестовый режим, награды не выдаются",
            }.get(self.bank_type or 1, "❓ Неизвестно")
            count_line = (
                f"👥 Команд: {self.size // 3} ({self.size} игроков)"
                if self.t_type == "team"
                else f"👥 Участников: {self.size}"
            )
            embed = discord.Embed(
                title=f"✅ {display_admin} создан!",
                description=(
                    f"🏆 Тип: {'Дуэльный 1×1' if self.t_type=='duel' else 'Командный 3×3'}\n"
                    f"{count_line}\n"
                    f"🎁 Приз: {prize_text}\n"
                    f"ID турнира: **{tour_id}**"
                ),
                color=discord.Color.green(),
            )
            self.disable_all_items()
            await interaction.response.edit_message(embed=embed, view=self)
            announcement = discord.Embed(
                title=f"📣 Открыта регистрация — {display_public}",
                color=discord.Color.gold(),
            )
            # тип турнира
            announcement.add_field(name="Тип", value=typetxt, inline=True)
            if self.t_type == "team":
                announcement.add_field(
                    name="Команд",
                    value=str(self.size // 3),
                    inline=True,
                )
            else:
                announcement.add_field(
                    name="Участников",
                    value=str(self.size),
                    inline=True,
                )
            announcement.add_field(name="Приз", value=prize_text, inline=False)
            reward_info = format_reward_details(
                self.bank_type or 1, self.manual_amount, self.t_type == "team"
            )
            announcement.add_field(name="Награды", value=reward_info, inline=False)
            if self.start_time:
                announcement.add_field(
                    name="Начало", value=self.start_time, inline=False
                )
            announcement.set_footer(text="Нажмите, чтобы зарегистрироваться")
            # если есть награда
            # (можно добавить параметр reward в конструктор, либо оставить пустым)

            # прикрепляем нашу RegistrationView и запоминаем автора
            set_tournament_author(tour_id, self.author_id)

            # прикрепляем нашу RegistrationView
            from bot.commands.tournament import tournament_admins

            tournament_admins[tour_id] = self.author_id

            reg_view = RegistrationView(
                tournament_id=tour_id,
                max_participants=self.size,
                tour_type=typetxt,
                author_id=self.author_id,
            )
            # отправляем в тот же канал, где был setup
            guild = interaction.guild
            if guild:
                chan = guild.get_channel(ANNOUNCE_CHANNEL_ID)
                if isinstance(chan, (TextChannel, Thread)):
                    sent = await safe_send(chan, embed=announcement, view=reg_view)
                    # сохраняем sent.id вместе с tour_id в БД
                    tournament_db.save_announcement_message(
                        tournament_id=tour_id, message_id=sent.id
                    )
                    return

            # fallback на текущий канал
            msg = interaction.message
            if msg and isinstance(msg.channel, (TextChannel, Thread, Messageable)):
                await safe_send(msg.channel, embed=announcement, view=reg_view)
            else:
                # в самом крайнем случае используем interaction.response
                await interaction.response.send_message(
                    embed=announcement, view=reg_view
                )
        except Exception as e:
            await interaction.response.send_message(
                f"❌ Произошла ошибка при подтверждении: `{e}`", ephemeral=True
            )
            import traceback

            logger.error("Ошибка в on_confirm:\n%s", traceback.format_exc())

    async def on_cancel(self, interaction: discord.Interaction):
        embed = discord.Embed(
            title="❌ Создание турнира отменено", color=discord.Color.red()
        )
        self.disable_all_items()
        await interaction.response.edit_message(embed=embed, view=self)


def create_tournament_logic(
    participants: List[int],
    team_size: int = 1,
    *,
    shuffle: bool = True,
) -> Tournament:
    return Tournament(
        participants, MODE_IDS, MAPS_BY_MODE, team_size=team_size, shuffle=shuffle
    )


def load_tournament_logic_from_db(tournament_id: int) -> Tournament:
    """Восстанавливает объект ``Tournament`` из сохранённых матчей."""
    info = get_tournament_info(tournament_id) or {}
    if info.get("type") == "team":
        team_map, _ = tournament_db.get_team_info(tournament_id)
        participants = list(team_map.keys())
        tour = create_tournament_logic(participants, shuffle=False)
        tour.team_map = team_map
    else:
        participants = [
            p.get("discord_user_id") or p.get("player_id")
            for p in tournament_db.list_participants_full(tournament_id)
        ]
        tour = create_tournament_logic(participants, shuffle=False)

    round_no = 1
    incomplete_round = None
    while True:
        rows = tournament_db.get_matches(tournament_id, round_no)
        if not rows:
            break
        matches: list[Match] = []
        all_done = True
        for r in rows:
            m = Match(r["player1_id"], r["player2_id"], r["mode"], r["map_id"])
            m.match_id = r.get("id")
            m.result = r.get("result")
            if m.result not in (1, 2):
                all_done = False
            matches.append(m)
        tour.matches[round_no] = matches
        if not all_done and incomplete_round is None:
            incomplete_round = round_no
        round_no += 1
    if incomplete_round is not None:
        tour.current_round = incomplete_round
    else:
        tour.current_round = round_no
    return tour


# ───── Вспомогательные функции ─────


def _get_round_results(
    tournament_id: int, round_no: int
) -> Optional[tuple[list[int], list[int]]]:
    """Возвращает списки победителей и проигравших указанного раунда.

    Возвращает ``None`` только если для какой-либо пары недостаточно
    результатов, чтобы определить победителя.
    """
    matches = tournament_db.get_matches(tournament_id, round_no)
    if not matches:
        return None

    totals: Dict[tuple[int, int], int] = {}
    results: Dict[tuple[int, int], list[int]] = {}
    for m in matches:
        pair = (m["player1_id"], m["player2_id"])
        totals[pair] = totals.get(pair, 0) + 1
        res = m.get("result")
        if res not in (1, 2):
            continue  # матч не сыгран
        if pair not in results:
            results[pair] = [0, 0]
        if res == 1:
            results[pair][0] += 1
        else:
            results[pair][1] += 1

    winners: list[int] = []
    losers: list[int] = []
    for pair, total in totals.items():
        p1, p2 = pair
        w1, w2 = results.get(pair, [0, 0])
        if w1 == w2:
            if (w1 + w2) < total:
                # Ещё есть несыгранные матчи и нет явного победителя
                return None
            # Ничья после всех матчей
            return None
        if w1 > w2:
            winners.append(p1)
            losers.append(p2)
        else:
            winners.append(p2)
            losers.append(p1)

    if len(winners) < len(totals):
        # Есть пара без сыгранных матчей
        return None

    return winners, losers


def _sync_participants_after_round(
    tournament_id: int,
    winners: list[int],
    team_map: Optional[Dict[int, List[int]]] = None,
) -> None:
    """Удаляет из таблицы участников всех, кто не прошёл далее."""

    keep: set[int] = set()
    if team_map:
        for tid in winners:
            keep.update(team_map.get(tid, []))
    else:
        keep.update(winners)

    current = db_list_participants_full(tournament_id)
    for entry in current:
        disc_id = entry.get("discord_user_id")
        player_id = entry.get("player_id")
        pid = disc_id or player_id
        if pid not in keep:
            if disc_id is not None:
                db_remove_discord_participant(tournament_id, disc_id)
            if player_id is not None:
                remove_player_from_tournament(player_id, tournament_id)


async def join_tournament(ctx: commands.Context, tournament_id: int) -> None:
    """
    Регистрирует автора команды в турнире через запись в БД
    и отправляет ответ в канал.
    """
    ok = db_add_participant(tournament_id, ctx.author.id)
    if ok:
        await send_temp(
            ctx,
            f"✅ {ctx.author.mention}, вы зарегистрированы в турнире #{tournament_id}",
        )
    else:
        await send_temp(
            "❌ Не удалось зарегистрироваться "
            "(возможно, вы уже в списке или турнир не существует)."
        )


async def start_round(interaction: Interaction, tournament_id: int) -> None:
    """Открывает меню выбора пары для текущего раунда без генерации нового."""
    from bot.systems.interactive_rounds import PairSelectionView, get_stage_name

    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message(
            "❌ Эту команду можно использовать только на сервере.",
            ephemeral=True,
        )
        return

    tour = load_tournament_logic_from_db(tournament_id)
    round_no = tour.current_round
    matches = tournament_db.get_matches(tournament_id, round_no)
    if not matches:
        await interaction.response.send_message(
            "⚠️ Раунд ещё не создан. Перейдите к следующему раунду.",
            ephemeral=True,
        )
        return

    info = get_tournament_info(tournament_id) or {}
    team_display = {}
    if info.get("type") == "team":
        _map, team_display = tournament_db.get_team_info(tournament_id)

    pairs: dict[int, list[Match]] = {}
    step = len(tour.modes[:3])
    pid = 1
    for i in range(0, len(matches), step):
        pairs[pid] = [
            Match(m["player1_id"], m["player2_id"], m["mode"], m["map_id"])
            if isinstance(m, dict)
            else m
            for m in matches[i : i + step]
        ]
        for m, row in zip(pairs[pid], matches[i : i + step]):
            if isinstance(row, dict):
                m.match_id = row.get("id")
                m.result = row.get("result")
        pid += 1

    participants = {m.player1_id for ms in pairs.values() for m in ms}
    participants.update({m.player2_id for ms in pairs.values() for m in ms})
    stage_name = get_stage_name(len(participants))

    embed = discord.Embed(
        title=f"Раунд {round_no} — выбор пары",
        description="Нажмите кнопку ниже, чтобы начать матчи для выбранной пары.",
        color=discord.Color.orange(),
    )

    view_pairs = PairSelectionView(
        tournament_id,
        pairs,
        guild,
        round_no,
        stage_name,
        team_display,
    )
    await interaction.response.send_message(embed=embed, view=view_pairs)


async def next_round(interaction: Interaction, tournament_id: int) -> None:
    """Генерирует следующий раунд и отправляет меню выбора пары."""
    from bot.systems.interactive_rounds import (
        PairSelectionView,
        get_stage_name,
    )

    # 1) Участники
    raw_participants = db_list_participants(tournament_id)
    if len(raw_participants) < 2:
        await interaction.response.send_message(
            "❌ Недостаточно участников для начала раунда."
        )
        return

    if len(raw_participants) % 2 != 0:
        await interaction.response.send_message(
            "⚠️ Нечётное число участников — нужно чётное для пар."
        )
        return

    full_participants = db_list_participants_full(tournament_id)
    if any(not p.get("confirmed") for p in full_participants):
        await interaction.response.send_message(
            "❌ Не все участники подтвердили участие.",
            ephemeral=True,
        )
        return

    info = get_tournament_info(tournament_id) or {}
    is_team = info.get("type") == "team"

    if is_team:
        from bot.data.tournament_db import get_team_info

        team_map, team_display = get_team_info(tournament_id)
        participants = list(team_map.keys())
    else:
        team_map, team_display = {}, {}
        participants = [
            p.get("discord_user_id") or p.get("player_id") for p in raw_participants
        ]

    # 2) Только на сервере
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message(
            "❌ Эту команду можно использовать только на сервере."
        )
        return

    # 3) Объект турнира
    # Ищем существующий View или создаем новый
    view = None
    for v in interaction.client.persistent_views:
        if hasattr(v, "custom_id") and v.custom_id == f"manage_rounds:{tournament_id}":
            view = v
            break

    if view and hasattr(view, "logic"):
        tour = view.logic
    else:
        tour = create_tournament_logic(participants)
        if is_team:
            tour.team_map = team_map

    # 3a) Обработка результатов предыдущего раунда
    if tour.current_round > 1:
        res = _get_round_results(tournament_id, tour.current_round - 1)
        if res is None:
            await interaction.response.send_message(
                "⚠️ Сначала внесите результаты предыдущего раунда.", ephemeral=True
            )
            return

        winners, _losers = res
        _sync_participants_after_round(
            tournament_id, winners, getattr(tour, "team_map", None)
        )
        if is_team:
            tour.team_map = {tid: tour.team_map[tid] for tid in winners}
        tour.participants = winners
        participants = winners
        if len(participants) < 2:
            champ = (
                winners[0] if winners else (participants[0] if participants else None)
            )
            runner = _losers[0] if _losers else None
            await request_finish_confirmation(
                interaction.client,
                guild,
                tournament_id,
                champ,
                runner,
                tour,
            )
            return

        if tour.team_size > 1:
            tour = create_tournament_logic(
                participants, team_size=tour.team_size, shuffle=False
            )

    # 4) Генерация и запись
    existing = db_get_matches(tournament_id, tour.current_round)
    if existing:
        matches = []
        for r in existing:
            m = Match(r["player1_id"], r["player2_id"], r["mode"], r["map_id"])
            m.match_id = r.get("id")
            m.result = r.get("result")
            matches.append(m)
        tour.matches[tour.current_round] = matches
        round_no = tour.current_round
        tour.current_round += 1
    else:
        matches = tour.generate_round()
        round_no = tour.current_round - 1
        db_create_matches(tournament_id, round_no, matches)

        try:
            await refresh_bracket_message(guild, tournament_id)
        except Exception:
            pass

        if round_no == 1:
            await notify_first_round_participants(
                interaction.client, guild, tour, matches, tournament_id
            )

    pairs: dict[int, list[Match]] = {}
    step = len(tour.modes[:3])
    pid = 1
    for i in range(0, len(matches), step):
        pairs[pid] = matches[i : i + step]
        pid += 1

    stage_name = get_stage_name(len(participants))

    embed = discord.Embed(
        title=f"Раунд {round_no} — выбор пары",
        description="Нажмите кнопку ниже, чтобы начать матчи для выбранной пары.",
        color=discord.Color.orange(),
    )

    view_pairs = PairSelectionView(
        tournament_id,
        pairs,
        guild,
        round_no,
        stage_name,
        team_display,
    )
    await interaction.response.send_message(embed=embed, view=view_pairs)


async def report_result(ctx: commands.Context, match_id: int, winner: int) -> None:
    """
    Обрабатывает команду /reportresult:
     1) Проверяет, что winner == 1 или 2
     2) Записывает в БД через db_record_match_result
     3) Отправляет уведомление об успехе/ошибке
    """
    if winner not in (1, 2):
        await send_temp(ctx, "❌ Укажите победителя: 1 (player1) или 2 (player2).")
        return

    match = tournament_db.get_match(match_id)
    if not match:
        await send_temp(ctx, "❌ Матч не найден.")
        return

    all_matches = tournament_db.get_matches(
        match["tournament_id"], match["round_number"]
    )
    pairs: dict[int, list[dict]] = {}
    idx_map: dict[tuple[int, int], int] = {}
    idx = 1
    for m in all_matches:
        key = (int(m["player1_id"]), int(m["player2_id"]))
        if key not in idx_map:
            idx_map[key] = idx
            idx += 1
        pid = idx_map[key]
        pairs.setdefault(pid, []).append(m)

    pair_idx = idx_map.get((int(match["player1_id"]), int(match["player2_id"])))
    pair_matches = pairs.get(pair_idx, [])
    if not all(x.get("result") is not None for x in pair_matches):
        await send_temp(ctx, "❌ Изменять результат можно только после оглашения пары.")
        return

    ok = db_record_match_result(match_id, winner)
    if ok:
        await send_temp(
            ctx,
            f"✅ Результат матча #{match_id} сохранён: победитель — игрок {winner}.",
        )
    else:
        await send_temp(ctx, "❌ Не удалось сохранить результат. Проверьте ID матча.")


async def show_status(
    ctx: commands.Context, tournament_id: int, round_number: Optional[int] = None
) -> None:
    """
    Показывает общее состояние турнира или детально раунд.
    """
    # общий статус
    if round_number is None:
        participants = db_list_participants_full(tournament_id)
        tour = ctx.bot.get_cog("TournamentCog").active_tournaments.get(tournament_id)
        last_round = (tour.current_round - 1) if tour else 0
        await send_temp(
            f"🏟 Турнир #{tournament_id}: участников {len(participants)}, "
            f"последний раунд {last_round}"
        )
        return

    # детально по раунду
    data = tournament_db.get_matches(tournament_id, round_number)
    matches = []
    for r in data:
        m = Match(r["player1_id"], r["player2_id"], r["mode"], r["map_id"])
        m.result = r.get("result")
        matches.append(m)
    if not matches:
        await send_temp(ctx, f"❌ Раунд {round_number} не найден.")
        return

    embed = Embed(
        title=f"📋 Турнир #{tournament_id} — Раунд {round_number}",
        color=discord.Color.green(),
    )
    guild = ctx.guild
    for idx, m in enumerate(matches, start=1):
        status = "⏳" if m.result is None else ("🏆 1" if m.result == 1 else "🏆 2")
        mode_name = MODE_NAMES.get(m.mode_id, str(m.mode_id))
        # упоминания игроков
        if guild:
            p1 = guild.get_member(m.player1_id)
            p2 = guild.get_member(m.player2_id)
            v1 = p1.mention if p1 else f"<@{m.player1_id}>"
            v2 = p2.mention if p2 else f"<@{m.player2_id}>"
        else:
            v1 = f"<@{m.player1_id}>"
            v2 = f"<@{m.player2_id}>"

        embed.add_field(
            name=f"Матч {idx} {status}",
            value=(
                f"{v1} vs {v2}\n" f"**Режим:** {mode_name}\n" f"**Карта:** `{m.map_id}`"
            ),
            inline=False,
        )

    await send_temp(ctx, embed=embed)


async def end_tournament(
    ctx: commands.Context,
    tournament_id: int,
    first: int,
    second: int,
    third: Optional[int] = None,
) -> None:
    """
    Завершает турнир:
     1) Формирует банк турнира (тип 1 — временно)
     2) Списывает баллы с игрока/банка
     3) Начисляет награды
     4) Сохраняет в базу
    """

    # Получаем тип банка и сумму
    info = get_tournament_info(tournament_id) or {}

    bank_type = info.get("bank_type", 1)
    manual_amount = info.get("manual_amount") or 20.0

    user_balance = db.scores.get(ctx.author.id, 0.0)

    try:
        bank_total, user_part, bank_part = rewards.calculate_bank(
            bank_type, user_balance, manual_amount
        )
    except ValueError as e:
        await send_temp(ctx, f"❌ Ошибка: {e}")
        return

    # 🔹 Списание с баланса / банка
    success = rewards.charge_bank_contribution(
        user_id=ctx.author.id,
        user_amount=user_part,
        bank_amount=bank_part,
        reason=f"Формирование банка турнира #{tournament_id}",
    )
    if not success:
        await send_temp(ctx, "❌ Недостаточно баллов у пользователя или ошибка банка.")
        return

    # 🔹 Получаем участников турнира
    all_participants = db_list_participants(tournament_id)

    def resolve_team(place_id: int):
        return [
            p["discord_user_id"] or p["player_id"]
            for p in all_participants
            if (p["discord_user_id"] == place_id or p["player_id"] == place_id)
        ]

    first_team = resolve_team(first)
    second_team = resolve_team(second)

    # Удаляем из таблицы участников всех, кто не занял первое место
    _sync_participants_after_round(tournament_id, [first])

    # 🔹 Начисление наград
    rewards.distribute_rewards(
        tournament_id=tournament_id,
        bank_total=bank_total,
        first_team_ids=first_team,
        second_team_ids=second_team,
        author_id=ctx.author.id,
    )

    reward_first_each = bank_total * 0.5 / max(1, len(first_team))
    reward_second_each = (
        bank_total * 0.25 / max(1, len(second_team)) if second_team else 0
    )

    # 🔹 Обновляем статус и сохраняем результат
    ok1 = db_save_tournament_result(tournament_id, first, second, third)
    ok2 = db_update_tournament_status(tournament_id, "finished")

    if ok1 and ok2:
        await send_temp(
            f"🏁 Турнир #{tournament_id} завершён и награды выданы:\n"
            f"🥇 {first} (x{len(first_team)})\n"
            f"🥈 {second} (x{len(second_team)})"
            + (f"\n🥉 {third}" if third is not None else "")
        )
        if ctx.guild:
            await update_result_message(
                ctx.guild,
                tournament_id,
                first_team,
                second_team,
                reward_first_each,
                reward_second_each,
            )
    else:
        await send_temp(ctx, "❌ Не удалось завершить турнир. Проверьте ID и повторите.")


class ConfirmDeleteView(SafeView):
    def __init__(self, tournament_id: int):
        super().__init__(timeout=60)
        self.tid = tournament_id

    @ui.button(label="❌ Удалить турнир", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: ui.Button):
        from bot.systems import bets_logic

        bets_logic.refund_all_bets(self.tid, interaction.user.id)
        ok = delete_tournament_record(self.tid)
        if ok:
            await interaction.response.edit_message(
                embed=Embed(
                    title=f"✅ Турнир #{self.tid} успешно удалён",
                    color=discord.Color.green(),
                ),
                view=None,
            )
        else:
            await interaction.response.edit_message(
                embed=Embed(
                    title="❌ Не удалось удалить турнир. Проверьте ID.",
                    color=discord.Color.red(),
                ),
                view=None,
            )


async def delete_tournament(ctx: commands.Context, tournament_id: int) -> None:
    """
    Шлёт embed с просьбой подтвердить удаление турнира.
    Само удаление выполняется по клику кнопки.
    """
    embed = Embed(
        title=f"❗ Подтвердите удаление турнира #{tournament_id}",
        description="Это действие **безвозвратно**.",
        color=discord.Color.red(),
    )
    view = ConfirmDeleteView(tournament_id)
    await send_temp(ctx, embed=embed, view=view)


class FinishConfirmView(SafeView):
    """Запрос подтверждения финала турнира."""

    def __init__(
        self,
        tid: int,
        first_id: int | None,
        second_id: int | None,
        tour: Tournament,
        admin_id: int,
    ):
        super().__init__(timeout=86400)
        self.tid = tid
        self.first_id = first_id
        self.second_id = second_id
        self.tour = tour
        self.admin_id = admin_id

    async def interaction_check(self, inter: Interaction) -> bool:
        return inter.user.id == self.admin_id

    @ui.button(label="✅ Подтвердить", style=ButtonStyle.success)
    async def confirm(self, interaction: Interaction, button: ui.Button):
        ok, msg = await finalize_tournament_logic(
            interaction.client,
            interaction.client.get_guild(db.guild_id),
            self.tid,
            self.first_id,
            self.second_id,
            self.tour,
            self.admin_id,
        )
        if ok:
            await interaction.response.edit_message(
                content="🏁 Турнир завершён и награды выданы.", view=None
            )
        else:
            await interaction.response.edit_message(content=msg or "Ошибка", view=None)
        self.stop()

    @ui.button(label="Отмена", style=ButtonStyle.danger)
    async def cancel(self, interaction: Interaction, button: ui.Button):
        await interaction.response.edit_message(content="Отменено", view=None)
        self.stop()


async def request_finish_confirmation(
    bot: commands.Bot,
    guild: discord.Guild,
    tid: int,
    first_id: int | None,
    second_id: int | None,
    tour: Tournament,
) -> None:
    """Отправляет админу запрос на подтверждение финала."""

    admin_id = get_tournament_author(tid)
    from bot.commands.tournament import tournament_admins

    admin_id = tournament_admins.get(tid, admin_id)
    admin = bot.get_user(admin_id) if admin_id else None
    if not admin:
        return

    def _mention(pid: int | None) -> str:
        if pid is None:
            return "—"
        if getattr(tour, "team_map", None) and pid in tour.team_map:
            parts = [guild.get_member(m) for m in tour.team_map[pid]]
            return ", ".join(
                p.mention if p else f"<@{m}>" for p, m in zip(parts, tour.team_map[pid])
            )
        member = guild.get_member(pid)
        if member:
            return member.mention
        pl = get_player_by_id(pid)
        return pl["nick"] if pl else f"ID:{pid}"

    embed = discord.Embed(
        title=f"Финал турнира #{tid}",
        description="Подтвердите распределение наград",
        color=discord.Color.green(),
    )
    embed.add_field(name="🥇 1 место", value=_mention(first_id), inline=False)
    if second_id is not None:
        embed.add_field(name="🥈 2 место", value=_mention(second_id), inline=False)

    view = FinishConfirmView(tid, first_id, second_id, tour, admin_id)
    try:
        await safe_send(admin, embed=embed, view=view)
    except Exception:
        pass


async def finalize_tournament_logic(
    bot: commands.Bot,
    guild: discord.Guild | None,
    tournament_id: int,
    first_id: int | None,
    second_id: int | None,
    tour: Tournament,
    admin_id: int,
) -> tuple[bool, str]:
    info = get_tournament_info(tournament_id) or {}
    bank_type = info.get("bank_type", 1)
    manual = info.get("manual_amount") or 20.0
    user_balance = db.scores.get(admin_id, 0.0)

    try:
        bank_total, user_part, bank_part = rewards.calculate_bank(
            bank_type, user_balance, manual
        )
    except Exception as e:
        return False, f"Ошибка: {e}"

    if not rewards.charge_bank_contribution(
        admin_id, user_part, bank_part, f"Формирование банка турнира #{tournament_id}"
    ):
        return False, "Недостаточно средств для формирования банка"

    def _resolve(pid: int | None) -> list[int]:
        if pid is None:
            return []
        if getattr(tour, "team_map", None) and pid in tour.team_map:
            return tour.team_map[pid]
        return [pid]

    first_team = _resolve(first_id)
    second_team = _resolve(second_id)

    # Удаляем из таблицы участников всех, кто не занял первое место
    if first_id is not None:
        _sync_participants_after_round(
            tournament_id,
            [first_id],
            getattr(tour, "team_map", None),
        )

    rewards.distribute_rewards(
        tournament_id, bank_total, first_team, second_team, admin_id
    )

    reward_first_each = bank_total * 0.5 / max(1, len(first_team))
    reward_second_each = (
        bank_total * 0.25 / max(1, len(second_team)) if second_team else 0
    )

    db_save_tournament_result(tournament_id, first_id or 0, second_id or 0, None)
    db_update_tournament_status(tournament_id, "finished")

    channel = guild.get_channel(ANNOUNCE_CHANNEL_ID) if guild else None
    if channel:

        def mlist(ids: list[int]) -> str:
            return (
                ", ".join(
                    guild.get_member(i).mention if guild.get_member(i) else f"<@{i}>"
                    for i in ids
                )
                if ids
                else "—"
            )

        emb = discord.Embed(
            title=f"🏁 Турнир #{tournament_id} завершён!",
            color=discord.Color.gold(),
        )
        emb.add_field(
            name="🥇 1 место",
            value=f"{mlist(first_team)} — {reward_first_each:.1f} баллов каждому",
            inline=False,
        )
        if second_team:
            emb.add_field(
                name="🥈 2 место",
                value=f"{mlist(second_team)} — {reward_second_each:.1f} баллов каждому",
                inline=False,
            )
        await safe_send(channel, embed=emb)

    class RewardConfirmView(SafeView):
        def __init__(self, tid: int):
            super().__init__(timeout=86400)
            self.tid = tid

        # Кнопка для подтверждения получения награды
        @ui.button(label="Подтвердить", style=ButtonStyle.success)
        async def confirm(self, interaction: Interaction, button: ui.Button):
            await interaction.response.send_message(
                "Награда подтверждена!", ephemeral=True
            )
            self.stop()

    for uid in first_team + second_team:
        user = bot.get_user(uid)
        if user:
            try:
                # Сообщаем пользователю о начисленной награде и просим подтвердить её получение
                await safe_send(
                    user,
                    f"Вам начислена награда за турнир #{tournament_id}! Подтвердите её получение.",
                    view=RewardConfirmView(tournament_id),
                )
            except Exception:
                pass

    if guild:
        await update_result_message(
            guild,
            tournament_id,
            first_team,
            second_team,
            reward_first_each,
            reward_second_each,
        )

    from bot.data.tournament_db import close_bet_bank
    from bot.data import db as _db

    remaining = close_bet_bank(tournament_id)
    if remaining > 0:
        _db.add_to_bank(remaining)
        _db.log_bank_income(
            admin_id,
            remaining,
            f"Возврат банка ставок турнира #{tournament_id}",
        )

    return True, ""


async def show_history(ctx: commands.Context, limit: int = 10) -> None:
    """
    Выводит последние `limit` завершённых турниров
    вместе с базовой статистикой и ссылкой на детальную страницу.
    """
    rows = list_recent_results(limit)
    if not rows:
        await send_temp(ctx, "📭 Нет истории завершённых турниров.")
        return

    embed = Embed(title="📜 История турниров", color=discord.Color.teal())

    for r in rows:
        tid = r["tournament_id"]
        first = r["first_place_id"]
        second = r["second_place_id"]
        third = r.get("third_place_id")

        # --- НОВАЯ СТАТИСТИКА ---
        participants = db_list_participants(tid)  # возвращает List[int]
        total_participants = len(participants)

        total_matches = count_matches(tid)  # возвращает int
        t_info = get_tournament_info(tid) or {}
        if t_info.get("type") == "team":
            team_total = total_participants // 3
            participant_line = f"👥 Команд: {team_total} ({total_participants} игроков)"
        else:
            participant_line = f"👥 Участников: {total_participants}"

        places_line = f"🥇 {first}  🥈 {second}" + (f"  🥉 {third}" if third else "")
        stats_line = (
            f"{participant_line}\n"
            f"🎲 Матчей сыграно: {total_matches}\n"
            f"ℹ️ Подробно: `/tournamentstatus {tid}`"
        )

        # объединяем всё в одно поле
        embed.add_field(
            name=f"Турнир #{tid}", value=f"{places_line}\n\n{stats_line}", inline=False
        )

    await send_temp(ctx, embed=embed)


class RegistrationView(SafeView):
    persistent = True

    def __init__(
        self,
        tournament_id: int,
        max_participants: int,
        tour_type: Optional[str] = None,
        author_id: Optional[int] = None,
    ):
        super().__init__(timeout=None)
        self.tid = tournament_id
        self.max = max_participants
        self.tour_type = tour_type
        self.author_id = author_id
        self._build_button()

    def _build_button(self):
        self.clear_items()
        raw = db_list_participants_full(self.tid)
        current = len(raw)
        btn = ui.Button(
            label=f"📝 Зарегистрироваться ({current}/{self.max})",
            style=discord.ButtonStyle.primary,
            custom_id=f"register_{self.tid}",
        )
        btn.callback = self.register
        btn.disabled = current >= self.max
        self.add_item(btn)

        time_btn = ui.Button(
            label="⏰ Время начала",
            style=discord.ButtonStyle.secondary,
            custom_id=f"start_{self.tid}",
        )
        time_btn.callback = self.show_start_time
        self.add_item(time_btn)

    async def show_start_time(self, interaction: discord.Interaction):
        from datetime import datetime

        info = get_tournament_info(self.tid) or {}
        start_raw = info.get("start_time")
        if start_raw:
            try:
                dt = datetime.fromisoformat(start_raw)
                start_text = format_moscow_time(dt)
            except Exception:
                start_text = start_raw
        else:
            start_text = "Не указано"
        await interaction.response.send_message(
            f"🕒 Турнир начнётся: {start_text}", ephemeral=True
        )

    async def register(self, interaction: discord.Interaction):
        if is_auto_team(self.tid):
            ok = assign_auto_team(self.tid, interaction.user.id)
        else:
            ok = db_add_participant(self.tid, interaction.user.id)
        if not ok:
            return await interaction.response.send_message(
                "⚠️ Вы уже зарегистрированы или турнир не существует.", ephemeral=True
            )
        # приватный ответ
        await interaction.response.send_message(
            f"✅ {interaction.user.mention}, вы зарегистрированы в турнире #{self.tid}.",
            ephemeral=True,
        )
        # обновляем кнопку
        self._build_button()
        assert interaction.message is not None, "interaction.message не может быть None"
        await interaction.message.edit(view=self)

        # Если достигнуто максимальное число участников — уведомляем автора
        raw = db_list_participants_full(self.tid)
        if len(raw) >= self.max:
            admin_id = get_tournament_author(self.tid)

            from bot.commands.tournament import (
                tournament_admins,
                confirmed_participants,
            )

            admin_id = tournament_admins.get(self.tid)
            confirmed_participants[self.tid] = set()

            if admin_id:
                admin_user = interaction.client.get_user(admin_id)
                if admin_user:
                    try:
                        await safe_send(
                            admin_user,
                            f"Турнир #{self.tid} собрал максимум участников. Подтвердите начало.",
                        )
                    except Exception:
                        pass

            # Рассылаем подтверждения участникам
            for p in raw:
                uid = p.get("discord_user_id")
                if not uid:
                    continue
                user = interaction.client.get_user(uid)
                if not user:
                    continue
                try:
                    await safe_send(
                        user,
                        f"Вы зарегистрированы в турнире #{self.tid}. Подтвердите участие:",
                        view=ParticipationConfirmView(self.tid, uid, admin_id),
                    )
                except Exception:
                    continue


class ParticipationConfirmView(SafeView):
    def __init__(self, tournament_id: int, user_id: int, admin_id: Optional[int]):
        super().__init__(timeout=86400)
        self.tournament_id = tournament_id
        self.user_id = user_id
        self.admin_id = admin_id

    @ui.button(label="Да, буду участвовать", style=ButtonStyle.success)
    async def confirm(self, interaction: Interaction, button: ui.Button):
        confirm_participant(self.tournament_id, self.user_id)

        from bot.commands.tournament import confirmed_participants

        confirmed_participants.setdefault(self.tournament_id, set()).add(self.user_id)

        await interaction.response.send_message("Участие подтверждено!", ephemeral=True)
        self.stop()

    @ui.button(label="Нет, передумал", style=ButtonStyle.danger)
    async def decline(self, interaction: Interaction, button: ui.Button):

        tournament_db.remove_discord_participant(self.tournament_id, self.user_id)
        await interaction.response.send_message(
            "Вы отказались от участия.", ephemeral=True
        )
        admin = interaction.client.get_user(self.admin_id) if self.admin_id else None
        if admin:
            try:
                await safe_send(
                    admin,
                    f"Игрок <@{self.user_id}> отказался от участия в турнире #{self.tournament_id}.",
                )
            except Exception:
                pass
        self.stop()


class BettingView(SafeView):
    """Simple view with a button to place bets after the tournament starts."""

    persistent = True

    def __init__(self, tournament_id: int):
        super().__init__(timeout=None)
        self.tid = tournament_id
        info = get_tournament_info(tournament_id) or {}
        self.is_team = info.get("type") == "team"

        btn = ui.Button(label="Поставить ставку", style=ButtonStyle.primary)
        btn.callback = self.on_bets
        self.add_item(btn)

    async def on_bets(self, interaction: Interaction):
        from .manage_tournament_view import BetMenuView

        view = BetMenuView(self)
        embed = discord.Embed(
            title="Ставки",
            description="Выберите действие",
            color=discord.Color.orange(),
        )
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    async def _edit_bet(
        self, interaction: Interaction, bet_id: int, bet_on: int, amount: float
    ):
        from bot.systems import bets_logic
        from bot.data.tournament_db import get_tournament_size, get_bet

        bet = get_bet(bet_id)
        if not bet:
            await interaction.response.send_message("Ставка не найдена", ephemeral=True)
            return
        if bets_logic.pair_started(self.tid, int(bet["round"]), int(bet["pair_index"])):
            await interaction.response.send_message(
                "Пара уже началась, ставку нельзя изменить",
                ephemeral=True,
            )
            return
        size = get_tournament_size(self.tid)
        total_rounds = int(math.ceil(math.log2(size))) if size > 1 else 1
        ok, msg = bets_logic.modify_bet(
            bet_id, bet_on, amount, interaction.user.id, total_rounds
        )
        await interaction.response.send_message(msg, ephemeral=True)

    async def _delete_bet(self, interaction: Interaction, bet_id: int):
        from bot.systems import bets_logic
        from bot.data.tournament_db import get_bet

        bet = get_bet(bet_id)
        if bet and bets_logic.pair_started(
            self.tid, int(bet["round"]), int(bet["pair_index"])
        ):
            await interaction.response.send_message(
                "Пара уже началась, ставку нельзя удалить",
                ephemeral=True,
            )
            return
        ok, msg = bets_logic.cancel_bet(bet_id)
        await interaction.response.send_message(msg, ephemeral=True)

    async def _show_pair_select(self, interaction: Interaction):
        from bot.data.tournament_db import get_matches, get_team_info, get_map_info
        from bot.data.players_db import get_player_by_id
        from .manage_tournament_view import BetPairSelectView

        guild = interaction.guild

        round_no = 1
        matches = []
        while True:
            m = get_matches(self.tid, round_no)
            if not m:
                round_no -= 1
                break
            matches = m
            if any(x.get("result") not in (1, 2) for x in m):
                break
            round_no += 1

        if not matches:
            await interaction.response.send_message(
                "Нет активных матчей", ephemeral=True
            )
            return

        pairs: dict[int, tuple[int, int]] = {}
        pair_maps: dict[int, list[dict]] = {}
        idx_map: dict[tuple[int, int], int] = {}
        idx = 1
        for m in matches:
            key = (int(m["player1_id"]), int(m["player2_id"]))
            if key not in idx_map:
                idx_map[key] = idx
                idx += 1
            pid = idx_map[key]
            pairs[pid] = key
            info = get_map_info(str(m.get("map_id")))
            pair_maps.setdefault(pid, []).append(
                {
                    "id": str(m.get("map_id")),
                    "name": info.get("name") if info else str(m.get("map_id")),
                    "image_url": info.get("image_url") if info else None,
                }
            )

        name_map: dict[int, str] = {}
        if self.is_team:
            _, team_names = get_team_info(self.tid)
            name_map.update({int(k): v for k, v in team_names.items()})

        for pid in {p for pair in pairs.values() for p in pair}:
            if pid in name_map:
                continue
            name = None
            if guild:
                member = guild.get_member(pid)
                if member:
                    name = member.display_name
            if name is None:
                pl = get_player_by_id(pid)
                name = pl["nick"] if pl else f"ID:{pid}"
            name_map[pid] = name

        view = BetPairSelectView(round_no, pairs, name_map, pair_maps, self._place_bet)
        embed = discord.Embed(
            title=f"Ставки: раунд {round_no}",
            description="Выберите пару для ставки",
            color=discord.Color.orange(),
        )
        await interaction.response.edit_message(embed=embed, view=view)

    async def _show_bet_status(self, interaction: Interaction):
        from bot.systems import bets_logic
        from .manage_tournament_view import BetStatusView

        bets = bets_logic.get_user_bets(self.tid, interaction.user.id)
        if not bets:
            await interaction.response.edit_message(
                content="Ставок нет", embed=None, view=None
            )
            return
        embed = discord.Embed(title="Ваши ставки", color=discord.Color.orange())
        locked: set[int] = set()
        for b in bets:
            if bets_logic.pair_started(self.tid, int(b["round"]), int(b["pair_index"])):
                locked.add(int(b["id"]))
            embed.add_field(
                name=f"ID {b['id']}",
                value=f"Раунд {b['round']} пара {b['pair_index']} на {b['bet_on']} — {b['amount']} баллов",
                inline=False,
            )
        view = BetStatusView(bets, self._edit_bet, self._delete_bet, locked)
        await interaction.response.edit_message(embed=embed, view=view)

    async def _place_bet(
        self,
        interaction: Interaction,
        round_no: int,
        pair_index: int,
        bet_on: int,
        amount: float,
        name: str,
    ):
        from bot.systems import bets_logic
        from bot.data.tournament_db import get_tournament_size
        from .manage_tournament_view import ConfirmBetView

        size = get_tournament_size(self.tid)
        total_rounds = int(math.ceil(math.log2(size))) if size > 1 else 1
        payout = bets_logic.calculate_payout(round_no, total_rounds, amount)

        async def confirm(inter: Interaction):
            ok, msg = bets_logic.place_bet(
                self.tid,
                round_no,
                pair_index,
                inter.user.id,
                bet_on,
                amount,
                total_rounds,
            )
            if inter.response.is_done():
                await inter.followup.send(msg, ephemeral=True)
            else:
                await inter.response.send_message(msg, ephemeral=True)
            if ok:
                try:
                    await safe_send(
                        inter.user,
                        f"Вы поставили {amount} баллов на {name} в паре {pair_index} раунда {round_no}. Возможный выигрыш {payout}.\nОжидайте результата.",
                    )
                except Exception:
                    pass

        embed = discord.Embed(
            title="Подтверждение ставки",
            description=f"На {name} {amount} баллов. Возможный выигрыш {payout}",
            color=discord.Color.orange(),
        )
        view = ConfirmBetView(confirm)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


async def announce_tournament(
    ctx: commands.Context,
    tournament_id: int,
    tour_type: str,
    max_participants: int,
    reward: Optional[str] = None,
    author_id: Optional[int] = None,
) -> None:
    """
    Отправляет в канал Embed с информацией о турнире и кнопкой регистрации.
    """
    embed = Embed(
        title=f"📣 Открыта регистрация — Турнир #{tournament_id}",
        color=discord.Color.gold(),
    )
    embed.add_field(name="Тип турнира", value=tour_type, inline=True)
    embed.add_field(
        name="Максимум участников", value=str(max_participants), inline=True
    )
    if reward:
        embed.add_field(name="Приз", value=reward, inline=False)
    embed.set_footer(text="Нажмите на кнопку ниже, чтобы зарегистрироваться")

    view = RegistrationView(
        tournament_id,
        max_participants,
        tour_type,
        author_id=author_id,
    )
    await send_temp(ctx, embed=embed, view=view)


async def handle_jointournament(ctx: commands.Context, tournament_id: int):
    ok = db_add_participant(tournament_id, ctx.author.id)
    if not ok:
        return await send_temp(
            ctx, "❌ Не удалось зарегистрироваться (возможно, вы уже в списке)."
        )
    await send_temp(
        ctx, f"✅ <@{ctx.author.id}> зарегистрирован в турнире #{tournament_id}."
    )
    # тут можно ещё обновить RegistrationView, если нужно


async def handle_regplayer(ctx: commands.Context, player_id: int, tournament_id: int):
    ok_db = add_player_to_tournament(player_id, tournament_id)
    if not ok_db:
        return await send_temp(ctx, "❌ Игрок уже зарегистрирован или произошла ошибка.")
    pl = get_player_by_id(player_id)
    name = pl["nick"] if pl else f"Игрок#{player_id}"
    await send_temp(ctx, f"✅ {name} зарегистрирован в турнире #{tournament_id}.")
    # Обновляем кнопку регистрации
    if ctx.guild:
        msg_id = get_announcement_message_id(tournament_id)
        if msg_id:
            channel = ctx.guild.get_channel(ANNOUNCE_CHANNEL_ID)
            if channel:
                try:
                    message = await channel.fetch_message(msg_id)
                    info = get_tournament_info(tournament_id) or {}
                    t_type = info.get("type", "duel")
                    type_text = "Дуэльный 1×1" if t_type == "duel" else "Командный 3×3"

                    admin_id = get_tournament_author(tournament_id)

                    from bot.commands.tournament import tournament_admins

                    admin_id = tournament_admins.get(tournament_id)

                    view = RegistrationView(
                        tournament_id,
                        get_tournament_size(tournament_id),
                        type_text,
                        author_id=admin_id,
                    )
                    await message.edit(view=view)
                except Exception:
                    pass


async def handle_unregister(ctx: commands.Context, identifier: str, tournament_id: int):
    # определяем тип идентификатора
    if identifier.startswith("<@") and identifier.endswith(">"):
        uid = int(identifier.strip("<@!>"))
        ok = db_remove_discord_participant(tournament_id, uid)
        name = f"<@{uid}>"
    else:
        pid = int(identifier)
        ok = remove_player_from_tournament(pid, tournament_id)
        pl = get_player_by_id(pid)
        name = pl["nick"] if pl else f"Игрок#{pid}"

    if not ok:
        return await send_temp(
            ctx, "❌ Не удалось снять с турнира (возможно, нет в списке)."
        )
    await send_temp(ctx, f"✅ {name} удалён из турнира #{tournament_id}.")


class TournamentNameModal(ui.Modal, title="Название турнира"):
    name = ui.TextInput(label="Название", required=True)

    def __init__(self, view: TournamentSetupView):
        super().__init__()
        self.view = view

    async def on_submit(self, interaction: discord.Interaction):
        self.view.title = str(self.name)
        await interaction.response.send_message(
            f"✅ Название установлено: **{self.view.title}**",
            ephemeral=True,
        )
        if self.view.message:
            self.view._build_confirm_buttons()
            await self.view.message.edit(view=self.view)


class StartDateModal(ui.Modal, title="Дата начала турнира"):
    start = ui.TextInput(
        label="ДД.ММ.ГГГГ ЧЧ:ММ", placeholder="01.12.2023 18:00", required=True
    )

    def __init__(self, view: TournamentSetupView):
        super().__init__()
        self.view = view

    async def on_submit(self, interaction: discord.Interaction):
        from datetime import datetime

        try:
            dt = datetime.strptime(str(self.start), "%d.%m.%Y %H:%M")
            self.view.start_time = dt.isoformat()
            await interaction.response.send_message(
                f"✅ Дата начала установлена: {format_moscow_time(dt)}",
                ephemeral=True,
            )
            if self.view.message:
                self.view._build_confirm_buttons()
                await self.view.message.edit(view=self.view)
        except Exception:
            await interaction.response.send_message(
                "❌ Неверный формат. Используйте ДД.ММ.ГГГГ ЧЧ:ММ", ephemeral=True
            )


class BankAmountModal(ui.Modal, title="Введите сумму банка"):
    amount = ui.TextInput(label="Сумма (минимум 15)", placeholder="20", required=True)

    def __init__(self, view: TournamentSetupView):
        super().__init__()
        self.view = view

    async def on_submit(self, interaction: discord.Interaction):
        try:
            value = float(self.amount.value.replace(",", "."))
            if value < 15:
                raise ValueError("Слишком мало")
            self.view.manual_amount = value
            await interaction.response.send_message(
                f"✅ Сумма банка установлена: **{value:.2f}**", ephemeral=True
            )
        except Exception:
            await interaction.response.send_message(
                "❌ Ошибка: введите корректное число (мин. 15)", ephemeral=True
            )


class BetBankModal(ui.Modal, title="Банк ставок"):
    amount = ui.TextInput(label="Сумма (0-20)", placeholder="10", required=True)

    def __init__(self, view: TournamentSetupView):
        super().__init__()
        self.view = view

    async def on_submit(self, interaction: discord.Interaction):
        try:
            value = float(self.amount.value.replace(",", "."))
            if value < 0 or value > 20:
                raise ValueError
            self.view.bets_bank = value
            await interaction.response.send_message(
                f"✅ Банк ставок: **{value:.1f}** баллов", ephemeral=True
            )
        except Exception:
            await interaction.response.send_message(
                "❌ Введите число от 0 до 20", ephemeral=True
            )


class ExtendDateModal(ui.Modal, title="Новая дата"):
    new_date = ui.TextInput(
        label="ДД.ММ.ГГГГ ЧЧ:ММ", placeholder="02.12.2023 18:00", required=True
    )

    def __init__(self, view: ui.View, tournament_id: int):
        super().__init__()
        self.view = view
        self.tid = tournament_id

    async def on_submit(self, interaction: discord.Interaction):
        from datetime import datetime

        try:
            dt = datetime.strptime(str(self.new_date), "%d.%m.%Y %H:%M")
            if update_start_time(self.tid, dt.isoformat()):
                expired_notified.discard(self.tid)
                await interaction.response.send_message(
                    f"✅ Регистрация продлена до {format_moscow_time(dt)}",
                    ephemeral=True,
                )
            else:
                await interaction.response.send_message(
                    "❌ Не удалось сохранить дату", ephemeral=True
                )
        except Exception:
            await interaction.response.send_message(
                "❌ Неверный формат даты", ephemeral=True
            )
        finally:
            self.stop()


class ExtendRegistrationView(SafeView):
    def __init__(self, tournament_id: int):
        super().__init__(timeout=86400)
        self.tid = tournament_id

    @ui.button(label="+1 день", style=ButtonStyle.primary)
    async def plus_day(self, interaction: Interaction, button: ui.Button):
        from datetime import datetime, timedelta

        info = get_tournament_info(self.tid) or {}
        start = info.get("start_time")
        try:
            dt = datetime.fromisoformat(start) + timedelta(days=1)
            ok = update_start_time(self.tid, dt.isoformat())
            if ok:
                expired_notified.discard(self.tid)
                await interaction.response.send_message(
                    f"✅ Новое время: {format_moscow_time(dt)}",
                    ephemeral=True,
                )
            else:
                await interaction.response.send_message(
                    "❌ Не удалось обновить время", ephemeral=True
                )
        except Exception:
            await interaction.response.send_message("❌ Ошибка даты", ephemeral=True)
        self.stop()

    @ui.button(label="Указать дату", style=ButtonStyle.secondary)
    async def custom(self, interaction: Interaction, button: ui.Button):
        await interaction.response.send_modal(ExtendDateModal(self, self.tid))


async def send_participation_confirmations(
    bot: commands.Bot, tournament_id: int, admin_id: Optional[int]
) -> None:
    """Отправляет участникам запрос на подтверждение участия."""
    raw = db_list_participants_full(tournament_id)
    for p in raw:
        uid = p.get("discord_user_id")
        if not uid or p.get("confirmed"):
            continue
        user = bot.get_user(uid)
        if not user:
            try:
                user = await bot.fetch_user(uid)
            except Exception:
                user = None
        if not user:
            continue
        try:
            await safe_send(
                user,
                f"Вы зарегистрированы в турнире #{tournament_id}. Подтвердите участие:",
                view=ParticipationConfirmView(tournament_id, uid, admin_id),
            )
        except Exception:
            continue


async def notify_first_round_participants(
    bot: commands.Bot,
    guild: discord.Guild,
    tour: Tournament,
    matches: List[Match],
    tournament_id: int,
) -> None:
    """Отправляет участникам информацию о первом раунде."""
    step = len(tour.modes[:3])
    pairs: dict[tuple[int, int], list[Match]] = {}
    for i in range(0, len(matches), step):
        ms = matches[i : i + step]
        if ms:
            pairs[(ms[0].player1_id, ms[0].player2_id)] = ms

    for (p1, p2), ms in pairs.items():
        disp = {}
        for pid in (p1, p2):
            if getattr(tour, "team_map", None) and pid in tour.team_map:
                members = tour.team_map[pid]
                names = [
                    guild.get_member(m).mention if guild.get_member(m) else f"<@{m}>"
                    for m in members
                ]
                disp[pid] = ", ".join(names)
            else:
                member = guild.get_member(pid)
                disp[pid] = member.mention if member else f"<@{pid}>"

        map_lines = [
            f"{MODE_NAMES.get(m.mode_id, m.mode_id)} — `{m.map_id}`" for m in ms
        ]

        for pid, opp in ((p1, disp[p2]), (p2, disp[p1])):
            targets = (
                tour.team_map.get(pid, [pid])
                if getattr(tour, "team_map", None)
                else [pid]
            )
            for uid in targets:
                user = bot.get_user(uid)
                if not user:
                    continue
                embed = discord.Embed(
                    title=f"Турнир #{tournament_id} — Раунд 1",
                    description=f"Твой соперник: {opp}",
                    color=discord.Color.blue(),
                )
                embed.add_field(name="Карты", value="\n".join(map_lines), inline=False)
                try:
                    await safe_send(user, embed=embed)
                except Exception:
                    continue


async def generate_first_round(
    bot: commands.Bot,
    guild: discord.Guild,
    tournament_id: int,
) -> Tournament | None:
    """Генерирует первый раунд и обновляет сообщение с сеткой."""
    from bot.data.tournament_db import (
        list_participants as db_list_participants,
        list_participants_full as db_list_participants_full,
        create_matches as db_create_matches,
    )

    raw_participants = db_list_participants(tournament_id)
    if len(raw_participants) < 2 or len(raw_participants) % 2 != 0:
        return None

    if any(not p.get("confirmed") for p in db_list_participants_full(tournament_id)):
        return None

    info = get_tournament_info(tournament_id) or {}
    if info.get("type") == "team":
        team_map, _ = tournament_db.get_team_info(tournament_id)
        participants = list(team_map.keys())
        tour = create_tournament_logic(participants)
        tour.team_map = team_map
    else:
        participants = [
            p.get("discord_user_id") or p.get("player_id") for p in raw_participants
        ]
        tour = create_tournament_logic(participants)
    matches = tour.generate_round()
    round_no = tour.current_round - 1
    db_create_matches(tournament_id, round_no, matches)

    try:
        await refresh_bracket_message(guild, tournament_id)
    except Exception:
        pass

    await notify_first_round_participants(bot, guild, tour, matches, tournament_id)
    return tour


async def send_announcement_embed(ctx, tournament_id: int) -> bool:
    data = get_tournament_info(tournament_id)
    if not data:
        return False

    from bot.data.tournament_db import (
        list_participants_full as db_list_participants_full,
    )

    t_type = data["type"]
    size = data["size"]
    bank_type = data.get("bank_type", 1)
    manual = data.get("manual_amount") or 20.0
    current = len(db_list_participants_full(tournament_id))

    type_text = "Дуэльный 1×1" if t_type == "duel" else "Командный 3×3"
    if bank_type == 1:
        prize_text = f"🏅 Тип 1 — {manual:.2f} баллов от пользователя"
    elif bank_type == 2:
        prize_text = "🥈 Тип 2 — 30 баллов (25% платит игрок)"
    elif bank_type == 3:
        prize_text = "🥇 Тип 3 — 30 баллов (из банка Бебр)"
    else:
        prize_text = "❓"

    display_public = format_tournament_title(
        data.get("name"), data.get("start_time"), tournament_id
    )
    embed = discord.Embed(
        title=f"📣 Открыта регистрация — {display_public}",
        color=discord.Color.gold(),
    )
    embed.add_field(name="Тип турнира", value=type_text, inline=True)
    if t_type == "team":
        team_map, _ = tournament_db.get_team_info(tournament_id)
        current_teams = len(team_map)
        embed.add_field(
            name="Команд",
            value=f"{current_teams}/{size // 3}",
            inline=True,
        )
    else:
        embed.add_field(
            name="Участников",
            value=f"{current}/{size}",
            inline=True,
        )
    embed.add_field(name="Приз", value=prize_text, inline=False)
    reward_info = format_reward_details(bank_type, manual, t_type == "team")
    embed.add_field(name="Награды", value=reward_info, inline=False)
    embed.set_footer(text="Нажмите на кнопку ниже, чтобы зарегистрироваться")

    admin_id = get_tournament_author(tournament_id)

    from bot.commands.tournament import tournament_admins

    admin_id = tournament_admins.get(tournament_id, admin_id)

    view = RegistrationView(
        tournament_id,
        size,
        type_text,
        author_id=admin_id,
    )

    channel = None
    if getattr(ctx, "guild", None):
        channel = ctx.guild.get_channel(ANNOUNCE_CHANNEL_ID)

    target = channel or ctx

    from bot.data.tournament_db import (
        get_announcement_message_id,
        save_announcement_message,
    )

    sent_message = None
    if channel:
        msg_id = get_announcement_message_id(tournament_id)
        if msg_id:
            try:
                existing = await channel.fetch_message(msg_id)
                await existing.edit(embed=embed, view=view)
                sent_message = existing
            except Exception:
                pass

    if sent_message is None:
        sent_message = await send_temp(
            target, embed=embed, view=view, delete_after=None
        )

    if not sent_message:
        return False

    save_announcement_message(tournament_id, sent_message.id)
    try:
        ctx.bot.add_view(view, message_id=sent_message.id)
    except Exception:
        pass

    return True


async def build_tournament_status_embed(
    tournament_id: int, include_id: bool = False
) -> discord.Embed | None:
    t = get_tournament_info(tournament_id)
    if not t:
        return None

    from bot.data.tournament_db import list_participants_full

    participants = list_participants_full(tournament_id)
    current = len(participants)
    t_type = t["type"]
    size = t["size"]
    bank_type = t.get("bank_type", 1)
    manual = t.get("manual_amount") or 20.0
    status = t.get("status", "unknown")
    start = t.get("start_time")

    type_text = "Дуэльный 1×1" if t_type == "duel" else "Командный 3×3"
    if bank_type == 1:
        prize_text = f"🏅 Тип 1 — {manual:.2f} баллов от пользователя"
    elif bank_type == 2:
        prize_text = "🥈 Тип 2 — 30 баллов (25% платит игрок)"
    elif bank_type == 3:
        prize_text = "🥇 Тип 3 — 30 баллов (из банка Бебр)"
    else:
        prize_text = "❓"

    # Этап (только по статусу)
    stage = "❔ Не начат"
    if status == "active":
        stage = "🔁 Активен"
    elif status == "finished":
        stage = "✅ Завершён"

    title_str = format_tournament_title(
        t.get("name"), t.get("start_time"), tournament_id, include_id
    )
    embed = discord.Embed(
        title=f"📋 {title_str} — Статус", color=discord.Color.blue()
    )
    embed.add_field(name="Тип", value=type_text, inline=True)
    if t_type == "team":
        team_map, _ = tournament_db.get_team_info(tournament_id)
        current_teams = len(team_map)
        embed.add_field(
            name="Команд",
            value=f"{current_teams}/{size // 3}",
            inline=True,
        )
    else:
        embed.add_field(
            name="Участников",
            value=f"{current}/{size}",
            inline=True,
        )
    embed.add_field(name="Банк", value=prize_text, inline=False)
    embed.add_field(name="Статус", value=status.capitalize(), inline=True)
    embed.add_field(name="Этап", value=stage, inline=True)
    if start:
        embed.add_field(name="Начало", value=start, inline=False)

    # Участники (ID)
    names = [
        (
            f"<@{p['discord_user_id']}>"
            if p.get("discord_user_id")
            else f"ID: {p['player_id']}"
        )
        for p in participants[:10]
    ]
    name_list = "\n".join(f"• {n}" for n in names) if names else "—"
    title = "📌 Команды (первые 10)" if t_type == "team" else "📌 Участники (первые 10)"
    embed.add_field(name=title, value=name_list, inline=False)

    return embed


async def build_tournament_bracket_embed(
    tournament_id: int,
    guild: discord.Guild | None = None,
    include_id: bool = False,
) -> discord.Embed | None:
    """Строит embed-сетку турнира по сыгранным матчам."""

    round_no = 1
    team_map, team_names = tournament_db.get_team_info(tournament_id)
    info = get_tournament_info(tournament_id) or {}
    title_str = format_tournament_title(
        info.get("name"), info.get("start_time"), tournament_id, include_id
    )
    embed = discord.Embed(
        title=f"🏟️ Сетка турнира {title_str}",
        color=discord.Color.purple(),
    )

    any_matches = False
    while True:
        matches = tournament_db.get_matches(tournament_id, round_no)
        if not matches:
            break

        any_matches = True
        pairs: dict[tuple[int, int], list[dict]] = {}
        for m in matches:
            key = (m["player1_id"], m["player2_id"])
            pairs.setdefault(key, []).append(m)

        lines: list[str] = []
        for (p1_id, p2_id), ms in pairs.items():
            if p1_id in team_names:
                name1 = team_names[p1_id]
            elif guild:
                p1m = guild.get_member(p1_id)
                name1 = (
                    p1m.mention
                    if p1m
                    else (get_player_by_id(p1_id) or {}).get("nick", f"ID:{p1_id}")
                )
            else:
                pl1 = get_player_by_id(p1_id)
                name1 = pl1["nick"] if pl1 else f"ID:{p1_id}"

            if p2_id in team_names:
                name2 = team_names[p2_id]
            elif guild:
                p2m = guild.get_member(p2_id)
                name2 = (
                    p2m.mention
                    if p2m
                    else (get_player_by_id(p2_id) or {}).get("nick", f"ID:{p2_id}")
                )
            else:
                pl2 = get_player_by_id(p2_id)
                name2 = pl2["nick"] if pl2 else f"ID:{p2_id}"

            wins1 = sum(1 for m in ms if m.get("result") == 1)
            wins2 = sum(1 for m in ms if m.get("result") == 2)

            required_wins = len(ms) // 2 + 1
            finished = (
                wins1 >= required_wins
                or wins2 >= required_wins
                or all(m.get("result") in (1, 2) for m in ms)
            )

            if finished:
                if wins1 > wins2:
                    line = f"✅ {name1} [{wins1}] ─┐\n{name2} [{wins2}] ─┘"
                elif wins2 > wins1:
                    line = f"{name1} [{wins1}] ─┐\n✅ {name2} [{wins2}] ─┘"
                else:
                    line = f"{name1} [{wins1}] ─┐\n{name2} [{wins2}] ─┘ ✅"
            else:
                line = f"{name1} [{wins1}] ─┐\n{name2} [{wins2}] ─┘ ❌"

            lines.append(line)

        embed.add_field(name=f"Раунд {round_no}", value="\n".join(lines), inline=False)
        round_no += 1

    if not any_matches:
        embed.description = "Матчи ещё не созданы"

    return embed


async def build_participants_embed(
    tournament_id: int, guild: discord.Guild | None = None
) -> discord.Embed | None:
    """Строит embed со списком участников турнира."""
    participants = tournament_db.list_participants_full(tournament_id)
    if not participants:
        return None

    t_info = get_tournament_info(tournament_id) or {}
    title = (
        f"👥 Команды турнира #{tournament_id}"
        if t_info.get("type") == "team"
        else f"👥 Участники турнира #{tournament_id}"
    )
    embed = discord.Embed(
        title=title,
        color=discord.Color.dark_teal(),
    )

    lines: list[str] = []
    for idx, p in enumerate(participants, start=1):
        prefix = f"[{p['team_name']}] " if p.get("team_name") else ""
        if p.get("discord_user_id"):
            uid = p["discord_user_id"]
            if guild:
                member = guild.get_member(uid)
                name = member.mention if member else f"<@{uid}>"
            else:
                name = f"<@{uid}>"
        else:
            pid = p.get("player_id")
            pl = get_player_by_id(pid)
            name = pl["nick"] if pl else f"Игрок#{pid}"

        mark = "✅" if p.get("confirmed") else "❔"
        lines.append(f"{idx}. {mark} {prefix}{name}")

    embed.description = "\n".join(lines) if lines else "—"
    return embed


async def refresh_bracket_message(guild: discord.Guild, tournament_id: int) -> bool:
    """Обновляет сообщение с сеткой турнира."""
    msg_id = get_announcement_message_id(tournament_id)
    if not msg_id:
        return False
    channel = guild.get_channel(ANNOUNCE_CHANNEL_ID)
    if not channel:
        return False
    try:
        message = await channel.fetch_message(msg_id)
    except Exception:
        return False

    embed = await build_tournament_bracket_embed(tournament_id, guild)
    if not embed:
        return False
    try:
        await message.edit(embed=embed)
        return True
    except Exception:
        return False


async def update_registration_message(guild: discord.Guild, tournament_id: int) -> bool:
    """Обновляет кнопку регистрации в сообщении анонса."""
    msg_id = get_announcement_message_id(tournament_id)
    if not msg_id:
        return False
    channel = guild.get_channel(ANNOUNCE_CHANNEL_ID)
    if not channel:
        return False
    try:
        message = await channel.fetch_message(msg_id)
    except Exception:
        return False

    info = get_tournament_info(tournament_id) or {}
    t_type = info.get("type", "duel")
    type_text = "Дуэльный 1×1" if t_type == "duel" else "Командный 3×3"
    admin_id = get_tournament_author(tournament_id)
    from bot.commands.tournament import tournament_admins

    admin_id = tournament_admins.get(tournament_id, admin_id)

    view = RegistrationView(
        tournament_id,
        get_tournament_size(tournament_id),
        type_text,
        author_id=admin_id,
    )
    try:
        await message.edit(view=view)
        return True
    except Exception:
        return False


async def update_bet_message(guild: discord.Guild, tournament_id: int) -> bool:
    """Replaces registration controls with a betting button."""
    msg_id = get_announcement_message_id(tournament_id)
    if not msg_id:
        return False
    channel = guild.get_channel(ANNOUNCE_CHANNEL_ID)
    if not channel:
        return False
    try:
        message = await channel.fetch_message(msg_id)
    except Exception:
        return False

    view = BettingView(tournament_id)
    try:
        await message.edit(view=view)
        # сохраняем в памяти ссылку на кнопку, чтобы она не "умирала" спустя время
        try:
            from bot.commands.base import bot

            bot.add_view(view, message_id=message.id)
        except Exception:
            pass
        return True
    except Exception:
        return False


async def send_status_message(
    guild: discord.Guild, tournament_id: int, bot: commands.Bot | None = None
) -> bool:
    """Отправляет новое сообщение со статусом турнира."""
    embed = await build_tournament_bracket_embed(tournament_id, guild)
    if not embed:
        embed = await build_tournament_status_embed(tournament_id)
    if not embed:
        return False

    channel = guild.get_channel(ANNOUNCE_CHANNEL_ID)
    if not channel:
        return False

    view = BettingView(tournament_id)
    sent = await safe_send(channel, embed=embed, view=view, delete_after=None)
    if not sent:
        return False

    from bot.data.tournament_db import save_status_message

    save_status_message(tournament_id, sent.id)
    if bot:
        try:
            bot.add_view(view, message_id=sent.id)
        except Exception:
            pass
    return True


async def update_result_message(
    guild: discord.Guild,
    tournament_id: int,
    first_team: list[int],
    second_team: list[int],
    reward_first_each: float,
    reward_second_each: float,
) -> bool:
    """Обновляет сообщение регистрации, показывая финальные награды."""

    msg_id = get_announcement_message_id(tournament_id)
    if not msg_id:
        return False
    channel = guild.get_channel(ANNOUNCE_CHANNEL_ID)
    if not channel:
        return False
    try:
        message = await channel.fetch_message(msg_id)
    except Exception:
        return False

    def mlist(ids: list[int]) -> str:
        return (
            ", ".join(
                guild.get_member(i).mention if guild.get_member(i) else f"<@{i}>"
                for i in ids
            )
            if ids
            else "—"
        )

    info = get_tournament_info(tournament_id) or {}
    title_str = format_tournament_title(
        info.get("name"), info.get("start_time"), tournament_id
    )
    embed = discord.Embed(
        title=f"🏁 {title_str} завершён!",
        color=discord.Color.gold(),
    )
    embed.add_field(
        name="🥇 1 место",
        value=f"{mlist(first_team)} — {reward_first_each:.1f} баллов каждому",
        inline=False,
    )
    if second_team:
        embed.add_field(
            name="🥈 2 место",
            value=f"{mlist(second_team)} — {reward_second_each:.1f} баллов каждому",
            inline=False,
        )

    try:
        await message.edit(embed=embed, view=None)
        return True
    except Exception:
        return False


async def build_tournament_result_embed(
    tournament_id: int, guild: discord.Guild | None = None
) -> discord.Embed | None:
    """Возвращает embed с итогами турнира и расчётом наград."""

    info = get_tournament_info(tournament_id) or {}
    result = tournament_db.get_tournament_result(tournament_id)
    if not result:
        return None

    first_id = result.get("first_place_id")
    second_id = result.get("second_place_id")

    team_mode = info.get("type") == "team"
    bank_type = info.get("bank_type", 1)
    manual = info.get("manual_amount") or 20.0

    if team_mode:
        team_map, _ = tournament_db.get_team_info(tournament_id)
        first_team = team_map.get(int(first_id), [])
        second_team = team_map.get(int(second_id), [])
    else:
        first_team = [int(first_id)] if first_id else []
        second_team = [int(second_id)] if second_id else []

    def mention(pid: int) -> str:
        if guild:
            member = guild.get_member(pid)
            if member:
                return member.mention
        pl = get_player_by_id(pid)
        return pl["nick"] if pl else f"<@{pid}>"

    bank_total, _u, _b = rewards.calculate_bank(bank_type, manual_amount=manual)
    reward_first_each = bank_total * 0.5 / max(1, len(first_team))
    reward_second_each = (
        bank_total * 0.25 / max(1, len(second_team)) if second_team else 0
    )

    embed = discord.Embed(
        title=f"🏁 Турнир #{tournament_id} завершён!",
        color=discord.Color.gold(),
    )
    embed.add_field(
        name="🥇 1 место",
        value=f"{', '.join(mention(i) for i in first_team)} — {reward_first_each:.1f} баллов каждому",
        inline=False,
    )
    if second_team:
        embed.add_field(
            name="🥈 2 место",
            value=f"{', '.join(mention(i) for i in second_team)} — {reward_second_each:.1f} баллов каждому",
            inline=False,
        )

    start_iso = info.get("start_time")
    finish_iso = result.get("finished_at")
    if start_iso and finish_iso:
        from datetime import datetime

        try:
            start_dt = datetime.fromisoformat(start_iso)
            end_dt = datetime.fromisoformat(finish_iso)
            duration = end_dt - start_dt
            minutes = int(duration.total_seconds() // 60)
            hours, minutes = divmod(minutes, 60)
            dur_text = f"{hours}ч {minutes}м" if hours else f"{minutes}м"
            embed.add_field(name="Длительность", value=dur_text, inline=False)
        except Exception:
            pass

    return embed


async def announce_results(ctx: commands.Context, tournament_id: int) -> bool:
    """Отправляет или обновляет сообщение с итогами турнира."""
    guild = ctx.guild
    info = get_tournament_info(tournament_id) or {}
    result = tournament_db.get_tournament_result(tournament_id)
    if not result:
        return False

    first_id = result.get("first_place_id")
    second_id = result.get("second_place_id")

    team_mode = info.get("type") == "team"
    bank_type = info.get("bank_type", 1)
    manual = info.get("manual_amount") or 20.0

    if team_mode:
        team_map, _ = tournament_db.get_team_info(tournament_id)
        first_team = team_map.get(int(first_id), [])
        second_team = team_map.get(int(second_id), [])
    else:
        first_team = [int(first_id)] if first_id else []
        second_team = [int(second_id)] if second_id else []

    bank_total, _u, _b = rewards.calculate_bank(bank_type, manual_amount=manual)
    reward_first_each = bank_total * 0.5 / max(1, len(first_team))
    reward_second_each = (
        bank_total * 0.25 / max(1, len(second_team)) if second_team else 0
    )

    if guild and await update_result_message(
        guild,
        tournament_id,
        first_team,
        second_team,
        reward_first_each,
        reward_second_each,
    ):
        return True

    if not guild:
        return False

    channel = guild.get_channel(ANNOUNCE_CHANNEL_ID)
    if not channel:
        return False

    def mlist(ids: list[int]) -> str:
        return (
            ", ".join(
                guild.get_member(i).mention if guild.get_member(i) else f"<@{i}>"
                for i in ids
            )
            if ids
            else "—"
        )

    embed = discord.Embed(
        title=f"🏁 Турнир #{tournament_id} завершён!",
        color=discord.Color.gold(),
    )
    embed.add_field(
        name="🥇 1 место",
        value=f"{mlist(first_team)} — {reward_first_each:.1f} баллов каждому",
        inline=False,
    )
    if second_team:
        embed.add_field(
            name="🥈 2 место",
            value=f"{mlist(second_team)} — {reward_second_each:.1f} баллов каждому",
            inline=False,
        )

    msg = await channel.send(embed=embed)
    tournament_db.save_announcement_message(tournament_id, msg.id)
    return True


async def change_winners(
    ctx: commands.Context,
    tournament_id: int,
    first_id: int,
    second_id: int,
    third_id: int | None = None,
) -> bool:
    """Переназначает победителей турнира и перераспределяет награды."""
    info = get_tournament_info(tournament_id) or {}
    prev = tournament_db.get_tournament_result(tournament_id)
    if not prev:
        return False

    team_mode = info.get("type") == "team"
    bank_type = info.get("bank_type", 1)
    manual = info.get("manual_amount") or 20.0

    if team_mode:
        team_map, _ = tournament_db.get_team_info(tournament_id)
        old_first_team = team_map.get(int(prev.get("first_place_id") or 0), [])
        old_second_team = team_map.get(int(prev.get("second_place_id") or 0), [])
        new_first_team = team_map.get(int(first_id), [])
        new_second_team = team_map.get(int(second_id), [])
    else:
        old_first_team = (
            [int(prev.get("first_place_id"))] if prev.get("first_place_id") else []
        )
        old_second_team = (
            [int(prev.get("second_place_id"))] if prev.get("second_place_id") else []
        )
        new_first_team = [int(first_id)]
        new_second_team = [int(second_id)] if second_id else []

    bank_total, _u, _b = rewards.calculate_bank(bank_type, manual_amount=manual)
    reward_first_each = bank_total * 0.5 / max(1, len(new_first_team))
    reward_second_each = (
        bank_total * 0.25 / max(1, len(new_second_team)) if new_second_team else 0
    )

    # Снимаем старые награды
    for uid in old_first_team:
        db.add_action(
            uid,
            -reward_first_each,
            f"Коррекция награды за турнир #{tournament_id}",
            ctx.author.id,
        )
        db.remove_ticket(
            uid,
            "gold",
            1,
            f"Коррекция билета за турнир #{tournament_id}",
            ctx.author.id,
        )

    for uid in old_second_team:
        db.add_action(
            uid,
            -reward_second_each,
            f"Коррекция награды за турнир #{tournament_id}",
            ctx.author.id,
        )
        db.remove_ticket(
            uid,
            "normal",
            1,
            f"Коррекция билета за турнир #{tournament_id}",
            ctx.author.id,
        )

    # Выдаём новые
    rewards.distribute_rewards(
        tournament_id,
        bank_total,
        new_first_team,
        new_second_team,
        ctx.author.id,
    )

    tournament_db.save_tournament_result(tournament_id, first_id, second_id, third_id)

    if ctx.guild:
        await update_result_message(
            ctx.guild,
            tournament_id,
            new_first_team,
            new_second_team,
            reward_first_each,
            reward_second_each,
        )

    return True


async def send_tournament_reminders(bot: commands.Bot, hours: int = 24) -> None:
    """Отправляет участникам напоминания о ближайших турнирах."""
    from datetime import datetime

    upcoming = tournament_db.get_upcoming_tournaments(hours)
    for t in upcoming:
        start_iso = t.get("start_time")
        if not start_iso:
            continue
        try:
            dt = datetime.fromisoformat(start_iso)
            start_text = format_moscow_time(dt)
        except Exception:
            start_text = start_iso
        participants = tournament_db.list_participants_full(t["id"])
        user_ids = [
            p.get("discord_user_id") for p in participants if p.get("discord_user_id")
        ]
        teams = []
        if t.get("type") == "team" and user_ids:
            for i in range(0, len(user_ids), 3):
                teams.append(user_ids[i : i + 3])

        matches = tournament_db.get_matches(t["id"], 1)
        for uid in user_ids:
            user = bot.get_user(uid)
            if not user:
                continue
            mate_list = []
            if teams:
                for tm in teams:
                    if uid in tm:
                        mate_list = [f"<@{m}>" for m in tm if m != uid]
                        break
            maps = [
                m["map_id"]
                for m in matches
                if uid in (m["player1_id"], m["player2_id"])
            ]
            text_lines = [f"Скоро начнётся турнир #{t['id']} ({start_text})"]
            if mate_list:
                text_lines.append("Твои тиммейты: " + ", ".join(mate_list))
            if maps:
                text_lines.append("Карты: " + ", ".join(maps))
            msg = "\n".join(text_lines)
            try:
                await safe_send(user, msg)
            except Exception:
                continue

        tournament_db.mark_reminder_sent(t["id"])


async def tournament_reminder_loop(bot: commands.Bot) -> None:
    await bot.wait_until_ready()
    while not bot.is_closed():
        await send_tournament_reminders(bot)
        # Run less frequently to avoid spamming users
        await asyncio.sleep(21600)


async def registration_deadline_loop(bot: commands.Bot) -> None:
    """Проверяет окончание регистрации турниров и уведомляет админа."""
    await bot.wait_until_ready()
    while not bot.is_closed():
        expired = get_expired_registrations()
        for t in expired:
            tid = t.get("id")
            if tid in expired_notified:
                continue
            admin_id = t.get("author_id") or get_tournament_author(tid)
            admin = bot.get_user(admin_id) if admin_id else None
            if admin:
                try:
                    await safe_send(
                        admin,
                        f"Регистрация на турнир #{tid} завершилась. Продлить?",
                        view=ExtendRegistrationView(tid),
                    )
                    await send_participation_confirmations(bot, tid, admin_id)
                    expired_notified.add(tid)
                except Exception:
                    logger.exception(
                        "Failed to notify admin about expired registration tournament_id=%s admin_id=%s",
                        tid,
                        admin_id,
                    )
        await asyncio.sleep(3600)
