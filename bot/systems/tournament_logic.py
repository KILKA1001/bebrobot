import random
import logging
from typing import List, Dict, Optional
import asyncio
import discord
from discord import ui, Embed, ButtonStyle, Color
import os
from bot.data import db
from discord.ext import commands
from discord.abc import Messageable
from discord import TextChannel, Thread, Interaction
import bot.data.tournament_db as tournament_db
from bot.data.players_db import get_player_by_id
from bot.utils import send_temp
from bot.data.tournament_db import count_matches 
from bot.data.tournament_db import (
    add_discord_participant as db_add_participant,
    list_participants as db_list_participants,
    create_matches as db_create_matches,
    record_match_result as db_record_match_result,
    save_tournament_result as db_save_tournament_result,
    update_tournament_status as db_update_tournament_status,
    list_participants_full as db_list_participants_full,
    remove_discord_participant as db_remove_discord_participant,
    remove_player_from_tournament,
    create_tournament_record as db_create_tournament_record,
    get_tournament_info,
    list_recent_results,
    delete_tournament as db_delete_tournament,
)
from bot.systems import tournament_rewards_logic as rewards
from bot.systems.tournament_bank_logic import validate_and_save_bank

logger = logging.getLogger(__name__)





MODE_NAMES: Dict[int, str] = {
    1: "Нокаут",
    2: "Награда за поимку",
    3: "Захват кристаллов",
    4: "Броулбол",
}
ANNOUNCE_CHANNEL_ID = int(os.getenv("MONTHLY_TOP_CHANNEL_ID", 0))
MODE_IDS = list(MODE_NAMES.keys())

# Карты, теперь сгруппированы по числовому режиму
MAPS_BY_MODE: Dict[int, List[str]] = {
    1: ["1.1 1", "1.2 2", "1.3 3"],
    2: ["2.1 4", "2.2 5", "2.3 6"],
    3: ["3.1 7", "3.2 8", "3.3 9"],
    4: ["4.1 10", "4.2 11", "4.3 12"],
}

# ───── База данных ─────

def create_tournament_record(t_type: str, size: int, start_time: Optional[str] = None) -> int:
    """Создаёт запись о турнире и возвращает его ID."""
    return db_create_tournament_record(t_type, size, start_time)

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


# ───── Доменные классы ─────

class Match:
    def __init__(self, player1_id: int, player2_id: int, mode_id: int, map_id: str):
        self.player1_id = player1_id
        self.player2_id = player2_id
        self.mode_id = mode_id      # сохраняем числовой ID
        self.map_id = map_id
        self.result: Optional[int] = None
        self.match_id: Optional[int] = None
        self.bank_type: Optional[int] = None
        self.manual_amount: Optional[float] = None

class Tournament:
    """
    Управление сеткой турнира в оперативке (не в БД).
    """
    def __init__(self,
         participants: List[int],
         modes: List[int],                  # теперь это MODE_IDS
         maps_by_mode: Dict[int, List[str]] # ключи — те же ID
    ):
        self.participants = participants.copy()
        self.modes = modes
        self.maps_by_mode = maps_by_mode
        self.current_round = 1
        self.matches: Dict[int, List[Match]] = {}

    def generate_round(self) -> List[Match]:
        random.shuffle(self.participants)
        round_matches: List[Match] = []
        for i in range(0, len(self.participants), 2):
            p1, p2 = self.participants[i], self.participants[i + 1]
            # используем последовательность режимов без перемешивания
            picked = self.modes[:3]
            for mode_id in picked:
                map_list = self.maps_by_mode.get(mode_id, [])
                map_choice = random.choice(map_list) if map_list else ""
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
        for m in self.matches.get(round_number, []):
            if m.result == 1:
                winners.append(m.player1_id)
            elif m.result == 2:
                winners.append(m.player2_id)
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

class TournamentSetupView(ui.View):
    """
    Многошаговый UI: выбор типа, размера, подтверждение, а затем запись в БД.
    """
    def __init__(self, author_id: int):
        super().__init__(timeout=120)
        self.author_id = author_id
        self.manual_amount = 0.0
        self.t_type: Optional[str] = None
        self.size:   Optional[int] = None
        self.bank_type: Optional[int] = None
        self.start_time: Optional[str] = None
        self.message: Optional[discord.Message] = None
        self._build_type_buttons()
        
        

    @staticmethod
    def initial_embed() -> discord.Embed:
        return discord.Embed(
            title="Создание нового турнира",
            description="Выберите **тип** турнира:",
            color=discord.Color.gold()
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
            custom_id="type_duel"
        )
        # привязываем её колбэк
        btn1.callback = self.on_type_duel
        self.add_item(btn1)

        # создаём кнопку Командный
        btn2 = ui.Button(
            label="Командный 3×3",
            style=discord.ButtonStyle.primary,
            custom_id="type_team"
        )
        btn2.callback = self.on_type_team
        self.add_item(btn2)

    def _build_size_buttons(self):
        self.clear_items()
        # Варианты размера в зависимости от типа
        choices = [4, 8, 16] if self.t_type == "duel" else [6, 12, 24]
        for n in choices:
            btn = ui.Button(label=str(n),
style=discord.ButtonStyle.secondary,
            custom_id=f"size_{n}")
            # вешаем callback, который будет получать только interaction
            btn.callback = self.on_size
            self.add_item(btn)

    def _build_bank_type_selector(self):
        self.clear_items()

        select = ui.Select(
            placeholder="Выберите источник банка наград",
            options=[
                discord.SelectOption(label="Тип 1 — Пользователь", value="1", description="Пользователь платит 50% (мин. 15 баллов)"),
                discord.SelectOption(label="Тип 2 — Смешанный", value="2", description="25% платит пользователь, 75% — банк Бебр"),
                discord.SelectOption(label="Тип 3 — Клуб", value="3", description="100% из банка Бебр"),
                discord.SelectOption(label="🧪 TEST — Без наград (тест)", value="4", description="Никаких выплат и списаний, только для проверки")
            ],
            custom_id="bank_type"
        )
        select.callback = self.on_select_bank_type
        self.add_item(select)

    def _build_confirm_buttons(self):
        self.clear_items()
        date_btn = ui.Button(
            label="📅 Дата старта",
            style=discord.ButtonStyle.secondary,
            custom_id="set_date"
        )
        date_btn.callback = self.on_set_date
        self.add_item(date_btn)
        # Кнопка «Подтвердить»
        btn_confirm = ui.Button(
            label="✅ Подтвердить",
            style=discord.ButtonStyle.success,
            custom_id="confirm"
        )
        btn_confirm.callback = self.on_confirm
        self.add_item(btn_confirm)

        # Кнопка «Отменить»
        btn_cancel = ui.Button(
            label="❌ Отменить",
            style=discord.ButtonStyle.danger,
            custom_id="cancel"
        )
        btn_cancel.callback = self.on_cancel
        self.add_item(btn_cancel)

    async def on_set_date(self, interaction: discord.Interaction):
        await interaction.response.send_modal(StartDateModal(self))

    async def interaction_check(self, inter: discord.Interaction) -> bool:
        # Только автор команды может управлять этим View
        return inter.user.id == self.author_id

    async def on_type_duel(self, interaction: discord.Interaction):
        self.t_type = "duel"
        embed = discord.Embed(
            title="Создание турнира",
            description="🏆 **Дуэльный 1×1**\n\nТеперь выберите **количество участников**:",
            color=discord.Color.gold()
        )
        self._build_size_buttons()
        await interaction.response.edit_message(embed=embed, view=self)

    async def on_type_team(self, interaction: discord.Interaction):
        self.t_type = "team"
        embed = discord.Embed(
            title="Создание турнира",
            description="🤝 **Командный 3×3**\n\nТеперь выберите **количество участников**:",
            color=discord.Color.gold()
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
            color=discord.Color.blue()
        )

        # Тип 1 требует сумму
        if self.bank_type == 1:
            embed.add_field(name="⚠️ Нужно ввести сумму", value="Мин. 15 баллов", inline=False)
            await interaction.response.send_modal(BankAmountModal(self))
        else:
            embed = discord.Embed(
                title="Источник банка наград выбран",
                description=f"Вы выбрали тип: **{self.bank_type}**",
                color=discord.Color.blue()
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
        embed = discord.Embed(
            title="Создание турнира",
            description=(
                f"🏆 **Тип:** {type_name}\n"
                f"👥 **Участников:** {self.size}\n\n"
                "Нажмите **✅ Подтвердить** или **❌ Отменить**"
            ),
            color=discord.Color.gold()
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
                        ephemeral=True
                    )
                    return

            if self.start_time is None:
                await interaction.response.send_message(
                    "❌ Сначала укажите дату начала турнира через кнопку \"📅 Дата старта\".",
                    ephemeral=True,
                )
                return

            # Теперь тип и размер — точно str и int
            tour_id = create_tournament_record(self.t_type, self.size, self.start_time)
            ok, msg = validate_and_save_bank(tour_id, self.bank_type or 1, self.manual_amount)
            if not ok:
                await interaction.response.send_message(msg, ephemeral=True)
                return
            typetxt = "Дуэльный 1×1" if self.t_type == "duel" else "Командный 3×3"
            prize_text = {
                1: f"🏅 Тип 1 — {self.manual_amount:.2f} баллов от пользователя",
                2: "🥈 Тип 2 — 30 баллов (25% платит игрок)",
                3: "🥇 Тип 3 — 30 баллов (из банка Бебр)",
                4: "🛠️ TEST — тестовый режим, награды не выдаются"
            }.get(self.bank_type or 1, "❓ Неизвестно")
            embed = discord.Embed(
                title=f"✅ Турнир #{tour_id} создан!",
                description=(
                    f"🏆 Тип: {'Дуэльный 1×1' if self.t_type=='duel' else 'Командный 3×3'}\n"
                    f"👥 Участников: {self.size}\n"
                    f"🎁 Приз: {prize_text}\n"
                    f"ID турнира: **{tour_id}**"
                ),
                color=discord.Color.green()
            )
            self.disable_all_items()
            await interaction.response.edit_message(embed=embed, view=self)
            announcement = discord.Embed(
                title=f"📣 Открыта регистрация — Турнир #{tour_id}",
                color=discord.Color.gold()
            )
            # тип турнира
            announcement.add_field(name="Тип", value=typetxt, inline=True)
            announcement.add_field(name="Участников", value=str(self.size), inline=True)
            announcement.add_field(name="Приз", value=prize_text, inline=False)
            if self.start_time:
                announcement.add_field(name="Начало", value=self.start_time, inline=False)
            announcement.set_footer(text="Нажмите, чтобы зарегистрироваться")
            # если есть награда
            # (можно добавить параметр reward в конструктор, либо оставить пустым)

            # прикрепляем нашу RegistrationView
            reg_view = RegistrationView(tournament_id=tour_id, max_participants=self.size, tour_type=typetxt)

            # добавляем к нему кнопку управления раундами
            reg_view.add_item(
                discord.ui.Button(
                    label="⚙ Управление раундами",
                    style=ButtonStyle.primary,
                    custom_id=f"manage_rounds:{tour_id}"
                )
            )
            # отправляем в тот же канал, где был setup
            guild = interaction.guild
            if guild:
                chan = guild.get_channel(ANNOUNCE_CHANNEL_ID)
                if isinstance(chan, (TextChannel, Thread)):
                    sent = await chan.send(embed=announcement, view=reg_view)
                        # сохраняем sent.id вместе с tour_id в БД
                    tournament_db.save_announcement_message(tournament_id=tour_id, message_id=sent.id)
                    return

            # fallback на текущий канал
            msg = interaction.message
            if msg and isinstance(msg.channel, (TextChannel, Thread, Messageable)):
                await msg.channel.send(embed=announcement, view=reg_view)
            else:
            # в самом крайнем случае используем interaction.response
                await interaction.response.send_message(embed=announcement, view=reg_view)
        except Exception as e:
            await interaction.response.send_message(
                f"❌ Произошла ошибка при подтверждении: `{e}`",
                ephemeral=True
            )
            import traceback
            logger.error("Ошибка в on_confirm:\n%s", traceback.format_exc())

        
    async def on_cancel(self, interaction: discord.Interaction):
        embed = discord.Embed(
            title="❌ Создание турнира отменено",
            color=discord.Color.red()
        )
        self.disable_all_items()
        await interaction.response.edit_message(embed=embed, view=self)


async def start_round_logic(ctx: commands.Context, tournament_id: int) -> None:
    # 0) Получаем «сырые» записи участников
    raw = db_list_participants_full(tournament_id)
    if not raw:
        await send_temp(ctx, f"❌ Турнир #{tournament_id} не найден или в нём нет участников.")
        return

    # ─── Формируем participants и display_map ────────────────────────────────
    participants: list[int] = []
    display_map: dict[int, str] = {}

    for entry in raw:
        d = entry.get("discord_user_id")
        p = entry.get("player_id")
        if d is not None:
            participants.append(d)
            display_map[d] = f"<@{d}>"
        elif p is not None:
            participants.append(p)
            pl = get_player_by_id(p)
            display_map[p] = pl["nick"] if pl else f"Игрок#{p}"
        else:
        # Ни того ни другого — пропускаем запись
            continue
    # ──────────────────────────────────────────────────────────────────────────
    # 1) Недостаточно участников
    if len(participants) < 2:
        await send_temp(ctx, "❌ Недостаточно участников для начала раунда.")
        return
    # Новая проверка на чётность участников
    if len(participants) % 2 != 0:
        await send_temp(ctx, "⚠️ Нечётное число участников — нужно чётное для пар.")
        return

    from bot.commands.tournament import active_tournaments
    tour = active_tournaments.get(tournament_id)
    if tour is None:
        tour = create_tournament_logic(participants)
        active_tournaments[tournament_id] = tour
    else:
        # Обработка результатов предыдущего раунда
        if tour.current_round > 1:
            res = _get_round_results(tournament_id, tour.current_round - 1)
            if res is None:
                await send_temp(ctx, "⚠️ Сначала введите результаты предыдущего раунда.")
                return
            winners, _losers = res
            _sync_participants_after_round(tournament_id, winners)
            tour.participants = winners
            participants = winners
            if len(participants) < 2:
                await send_temp(ctx, f"🏆 Турнир завершён! Победитель — <@{participants[0]}>.")
                db_update_tournament_status(tournament_id, "finished")
                return
    
    # 1) Проверяем, что команда в гильдии
    guild = ctx.guild
    if guild is None:
        await send_temp(ctx, "❌ Эту команду можно использовать только на сервере.")
        return

    matches = tour.generate_round()
    round_number = tour.current_round - 1

    # 3) Сохраняем в БД
    db_create_matches(tournament_id, round_number, matches)

    # 4) Формируем и отправляем Embed
    embed = Embed(
        title=f"Раунд {round_number} — Турнир #{tournament_id}",
        description=f"Сгенерировано {len(matches)} матчей:",
        color=discord.Color.blurple()
    )
    for idx, m in enumerate(matches, start=1):
        v1 = display_map.get(m.player1_id, f"<@{m.player1_id}>")
        v2 = display_map.get(m.player2_id, f"<@{m.player2_id}>")
        mode_name = MODE_NAMES.get(m.mode_id, str(m.mode_id))
        embed.add_field(
            name=f"Матч {idx}",
            value=(
                f"{v1} vs {v2}\n"
                f"**Режим:** {mode_name}\n"
                f"**Карта:** {m.map_id}"
            ),
            inline=False
        )

    await send_temp(ctx, embed=embed)


def create_tournament_logic(participants: List[int]) -> Tournament:
    return Tournament(participants, MODE_IDS, MAPS_BY_MODE)

# ───── Вспомогательные функции ─────

def _get_round_results(tournament_id: int, round_no: int) -> Optional[tuple[list[int], list[int]]]:
    """Возвращает списки победителей и проигравших указанного раунда.

    Если хотя бы один матч не имеет результата, возвращается ``None``.
    """
    matches = tournament_db.get_matches(tournament_id, round_no)
    winners: list[int] = []
    losers: list[int] = []
    for m in matches:
        res = m.get("result")
        if res not in (1, 2):
            return None
        if res == 1:
            winners.append(m["player1_id"])
            losers.append(m["player2_id"])
        else:
            winners.append(m["player2_id"])
            losers.append(m["player1_id"])
    return winners, losers


def _sync_participants_after_round(tournament_id: int, winners: list[int]) -> None:
    """Удаляет из таблицы участников всех, кто не вошёл в список ``winners``."""
    current = db_list_participants_full(tournament_id)
    for entry in current:
        disc_id = entry.get("discord_user_id")
        player_id = entry.get("player_id")
        pid = disc_id or player_id
        if pid not in winners:
            if disc_id is not None:
                remove_discord_participant(tournament_id, disc_id)
            if player_id is not None:
                remove_player_from_tournament(player_id, tournament_id)

async def join_tournament(ctx: commands.Context, tournament_id: int) -> None:
    """
    Регистрирует автора команды в турнире через запись в БД
    и отправляет ответ в канал.
    """
    ok = db_add_participant(tournament_id, ctx.author.id)
    if ok:
        await send_temp(ctx, f"✅ {ctx.author.mention}, вы зарегистрированы в турнире #{tournament_id}")
    else:
        await send_temp(
            "❌ Не удалось зарегистрироваться "
            "(возможно, вы уже в списке или турнир не существует)."
        )

async def start_round(interaction: Interaction, tournament_id: int) -> None:
    """
    1) Берёт участников
    2) Проверяет, что их >=2 и команда в гильдии
    3) Создаёт/достаёт объект Tournament
    4) Генерирует раунд, сохраняет в БД
    5) Строит Embed и шлёт в канал
    """
    from bot.systems.interactive_rounds import MatchResultView, PairSelectionView
    # 1) Участники
    raw_participants = db_list_participants(tournament_id)
    if len(raw_participants) < 2:
        await interaction.response.send_message("❌ Недостаточно участников для начала раунда.")
        return

    if len(raw_participants) % 2 != 0:
        await interaction.response.send_message("⚠️ Нечётное число участников — нужно чётное для пар.")
        return

    participants = [p.get("discord_user_id") or p.get("player_id") for p in raw_participants]
    
    # 2) Только на сервере
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message("❌ Эту команду можно использовать только на сервере.")
        return

    # 3) Объект турнира
    # Ищем существующий View или создаем новый
    view = None
    for v in interaction.client.persistent_views:
        if hasattr(v, 'custom_id') and v.custom_id == f"manage_rounds:{tournament_id}":
            view = v
            break

    if view and hasattr(view, 'logic'):
        tour = view.logic
    else:
        tour = create_tournament_logic(participants)

    # 3a) Обработка результатов предыдущего раунда
    if tour.current_round > 1:
        res = _get_round_results(tournament_id, tour.current_round - 1)
        if res is None:
            await interaction.response.send_message(
                "⚠️ Сначала внесите результаты предыдущего раунда.", ephemeral=True
            )
            return
        winners, _losers = res
        _sync_participants_after_round(tournament_id, winners)
        tour.participants = winners
        participants = winners
        if len(participants) < 2:
            await interaction.response.send_message(
                f"🏆 Турнир завершён! Победитель — <@{participants[0]}>."
                if participants else "Турнир завершён."
            )
            db_update_tournament_status(tournament_id, "finished")
            return

    # 4) Генерация и запись
    matches = tour.generate_round()
    round_no = tour.current_round - 1
    db_create_matches(tournament_id, round_no, matches)

    pairs: dict[int, list[Match]] = {}
    step = len(tour.modes[:3])
    pid = 1
    for i in range(0, len(matches), step):
        pairs[pid] = matches[i:i + step]
        pid += 1

    embed = discord.Embed(
        title=f"Раунд {round_no} — выбор пары",
        description="Нажмите кнопку ниже, чтобы начать матчи для выбранной пары.",
        color=discord.Color.orange(),
    )
    view_pairs = PairSelectionView(tournament_id, pairs, guild, round_no)
    await interaction.response.send_message(embed=embed, view=view_pairs)

async def report_result(ctx: commands.Context, match_id: int, winner: int) -> None:
    """
    Обрабатывает команду ?reportresult:
     1) Проверяет, что winner == 1 или 2
     2) Записывает в БД через db_record_match_result
     3) Отправляет уведомление об успехе/ошибке
    """
    if winner not in (1, 2):
        await send_temp(ctx, "❌ Укажите победителя: 1 (player1) или 2 (player2).")
        return

    ok = db_record_match_result(match_id, winner)
    if ok:
        await send_temp(ctx, f"✅ Результат матча #{match_id} сохранён: победитель — игрок {winner}.")
    else:
        await send_temp(ctx, "❌ Не удалось сохранить результат. Проверьте ID матча.")

async def show_status(
    ctx: commands.Context,
    tournament_id: int,
    round_number: Optional[int] = None
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
        color=discord.Color.green()
    )
    guild = ctx.guild
    for idx, m in enumerate(matches, start=1):
        status = (
            "⏳" if m.result is None
            else ("🏆 1" if m.result == 1 else "🏆 2")
        )
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
                f"{v1} vs {v2}\n"
                f"**Режим:** {mode_name}\n"
                f"**Карта:** `{m.map_id}`"
            ),
            inline=False
        )

    await send_temp(ctx, embed=embed)

async def end_tournament(
    ctx: commands.Context,
    tournament_id: int,
    first: int,
    second: int,
    third: Optional[int] = None
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
    manual_amount = info.get("manual_amount", 20.0)

    user_balance = db.scores.get(ctx.author.id, 0.0)

    try:
        bank_total, user_part, bank_part = rewards.calculate_bank(bank_type, user_balance, manual_amount)
    except ValueError as e:
        await send_temp(ctx, f"❌ Ошибка: {e}")
        return

    # 🔹 Списание с баланса / банка
    success = rewards.charge_bank_contribution(
        user_id=ctx.author.id,
        user_amount=user_part,
        bank_amount=bank_part,
        reason=f"Формирование банка турнира #{tournament_id}"
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

    # 🔹 Начисление наград
    rewards.distribute_rewards(
        tournament_id=tournament_id,
        bank_total=bank_total,
        first_team_ids=first_team,
        second_team_ids=second_team,
        author_id=ctx.author.id
    )

    # 🔹 Обновляем статус и сохраняем результат
    ok1 = db_save_tournament_result(tournament_id, first, second, third)
    ok2 = db_update_tournament_status(tournament_id, "finished")

    if ok1 and ok2:
        await send_temp(
            f"🏁 Турнир #{tournament_id} завершён и награды выданы:\n"
            f"🥇 {first} (x{len(first_team)})\n"
            f"🥈 {second} (x{len(second_team)})" +
            (f"\n🥉 {third}" if third is not None else "")
        )
    else:
        await send_temp(ctx, "❌ Не удалось завершить турнир. Проверьте ID и повторите.")

class ConfirmDeleteView(ui.View):
    def __init__(self, tournament_id: int):
        super().__init__(timeout=60)
        self.tid = tournament_id

    @ui.button(label="❌ Удалить турнир", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: ui.Button):
        ok = delete_tournament_record(self.tid)
        if ok:
            await interaction.response.edit_message(
                embed=Embed(
                    title=f"✅ Турнир #{self.tid} успешно удалён",
                    color=discord.Color.green()
                ),
                view=None
            )
        else:
            await interaction.response.edit_message(
                embed=Embed(
                    title="❌ Не удалось удалить турнир. Проверьте ID.",
                    color=discord.Color.red()
                ),
                view=None
            )

async def delete_tournament(
    ctx: commands.Context,
    tournament_id: int
) -> None:
    """
    Шлёт embed с просьбой подтвердить удаление турнира.
    Само удаление выполняется по клику кнопки.
    """
    embed = Embed(
        title=f"❗ Подтвердите удаление турнира #{tournament_id}",
        description="Это действие **безвозвратно**.",
        color=discord.Color.red()
    )
    view = ConfirmDeleteView(tournament_id)
    await send_temp(ctx, embed=embed, view=view)


async def show_history(ctx: commands.Context, limit: int = 10) -> None:
    """
    Выводит последние `limit` завершённых турниров
    вместе с базовой статистикой и ссылкой на детальную страницу.
    """
    rows = list_recent_results(limit)
    if not rows:
        await send_temp(ctx, "📭 Нет истории завершённых турниров.")
        return

    embed = Embed(
        title="📜 История турниров",
        color=discord.Color.teal()
    )

    for r in rows:
        tid = r["tournament_id"]
        first = r["first_place_id"]
        second = r["second_place_id"]
        third = r.get("third_place_id")

        # --- НОВАЯ СТАТИСТИКА ---
        participants = db_list_participants(tid)       # возвращает List[int]
        total_participants = len(participants)

        total_matches = count_matches(tid)          # возвращает int

        places_line = f"🥇 {first}  🥈 {second}" + (f"  🥉 {third}" if third else "")
        stats_line = (
            f"👥 Участников: {total_participants}\n"
            f"🎲 Матчей сыграно: {total_matches}\n"
            f"ℹ️ Подробно: `?tournamentstatus {tid}`"
        )

        # объединяем всё в одно поле
        embed.add_field(
            name=f"Турнир #{tid}",
            value=f"{places_line}\n\n{stats_line}",
            inline=False
        )

    await send_temp(ctx, embed=embed)

class RegistrationView(ui.View):
    persistent = True
    def __init__(self, tournament_id: int, max_participants: int, tour_type: Optional[str] = None):
        super().__init__(timeout=None)
        self.tid = tournament_id
        self.max = max_participants
        self.tour_type = tour_type
        self._build_button()

    def _build_button(self):
        self.clear_items()
        raw = db_list_participants_full(self.tid)
        current = len(raw)
        btn = ui.Button(
            label=f"📝 Зарегистрироваться ({current}/{self.max})",
            style=discord.ButtonStyle.primary,
            custom_id=f"register_{self.tid}"
        )
        btn.callback = self.register
        btn.disabled = current >= self.max
        self.add_item(btn)

    async def register(self, interaction: discord.Interaction):
        ok = db_add_participant(self.tid, interaction.user.id)
        if not ok:
            return await interaction.response.send_message(
                "⚠️ Вы уже зарегистрированы или турнир не существует.", ephemeral=True
            )
        # приватный ответ
        await interaction.response.send_message(
            f"✅ {interaction.user.mention}, вы зарегистрированы в турнире #{self.tid}.", ephemeral=True
        )
        # обновляем кнопку
        self._build_button()
        assert interaction.message is not None, "interaction.message не может быть None"
        await interaction.message.edit(view=self)

        # Если достигнуто максимальное число участников — генерируем 1-й раунд
        raw = db_list_participants_full(self.tid)
        if len(raw) >= self.max:
            from bot.systems.interactive_rounds import RoundManagementView
            from bot.data.tournament_db import create_matches as db_create_matches

            # Создаём объект логики и генерируем первый раунд
            ids = [p.get("discord_user_id") or p.get("player_id") for p in raw]
            logic = create_tournament_logic(ids)
            matches = logic.generate_round()
            db_create_matches(self.tid, 1, matches)

            # Показываем актуальную сетку
            bracket = await build_tournament_bracket_embed(self.tid, interaction.guild)
            view = RoundManagementView(self.tid, logic)
            await interaction.message.edit(embed=bracket, view=view)
        
async def announce_tournament(
    ctx: commands.Context,
    tournament_id: int,
    tour_type: str,
    max_participants: int,
    reward: Optional[str] = None
) -> None:
    """
    Отправляет в канал Embed с информацией о турнире и кнопкой регистрации.
    """
    embed = Embed(
        title=f"📣 Открыта регистрация — Турнир #{tournament_id}",
        color=discord.Color.gold()
    )
    embed.add_field(name="Тип турнира", value=tour_type, inline=True)
    embed.add_field(name="Максимум участников", value=str(max_participants), inline=True)
    if reward:
        embed.add_field(name="Приз", value=reward, inline=False)
    embed.set_footer(text="Нажмите на кнопку ниже, чтобы зарегистрироваться")

    view = RegistrationView(tournament_id, max_participants)
    await send_temp(ctx, embed=embed, view=view)

async def handle_jointournament(ctx: commands.Context, tournament_id: int):
    ok = db_add_participant(tournament_id, ctx.author.id)
    if not ok:
        return await send_temp(ctx, "❌ Не удалось зарегистрироваться (возможно, вы уже в списке).")
    await send_temp(ctx, f"✅ <@{ctx.author.id}> зарегистрирован в турнире #{tournament_id}.")
    # тут можно ещё обновить RegistrationView, если нужно

async def handle_regplayer(ctx: commands.Context, player_id: int, tournament_id: int):
    ok = db_add_participant(tournament_id, player_id)
    if not ok:
        return await send_temp(ctx, "❌ Не удалось зарегистрировать игрока.")
    pl = get_player_by_id(player_id)
    name = pl["nick"] if pl else f"Игрок#{player_id}"
    await send_temp(ctx, f"✅ {name} зарегистрирован в турнире #{tournament_id}.")

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
        return await send_temp(ctx, "❌ Не удалось снять с турнира (возможно, нет в списке).")
    await send_temp(ctx, f"✅ {name} удалён из турнира #{tournament_id}.")

class StartDateModal(ui.Modal, title="Дата начала турнира"):
    start = ui.TextInput(label="ДД.ММ.ГГГГ ЧЧ:ММ", placeholder="01.12.2023 18:00", required=True)

    def __init__(self, view: TournamentSetupView):
        super().__init__()
        self.view = view

    async def on_submit(self, interaction: discord.Interaction):
        from datetime import datetime
        try:
            dt = datetime.strptime(str(self.start), "%d.%m.%Y %H:%M")
            self.view.start_time = dt.isoformat()
            await interaction.response.send_message(
                f"✅ Дата начала установлена: {dt.strftime('%d.%m.%Y %H:%M')}",
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
            await interaction.response.send_message(f"✅ Сумма банка установлена: **{value:.2f}**", ephemeral=True)
        except Exception:
            await interaction.response.send_message("❌ Ошибка: введите корректное число (мин. 15)", ephemeral=True)

async def send_announcement_embed(ctx, tournament_id: int) -> bool:
    data = get_tournament_info(tournament_id)
    if not data:
        return False

    from bot.data.tournament_db import list_participants_full as db_list_participants_full

    t_type = data["type"]
    size = data["size"]
    bank_type = data.get("bank_type", 1)
    manual = data.get("manual_amount", 20.0)
    current = len(db_list_participants_full(tournament_id))

    type_text = "Дуэльный 1×1" if t_type == "duel" else "Командный 3×3"
    prize_text = {
        1: f"🏅 Тип 1 — {manual:.2f} баллов от пользователя",
        2: "🥈 Тип 2 — 30 баллов (25% платит игрок)",
        3: "🥇 Тип 3 — 30 баллов (из банка Бебр)"
    }.get(bank_type, "❓")

    embed = discord.Embed(
        title=f"📣 Открыта регистрация — Турнир #{tournament_id}",
        color=discord.Color.gold()
    )
    embed.add_field(name="Тип турнира", value=type_text, inline=True)
    embed.add_field(name="Участников", value=f"{current}/{size}", inline=True)
    embed.add_field(name="Приз", value=prize_text, inline=False)
    embed.set_footer(text="Нажмите на кнопку ниже, чтобы зарегистрироваться")

    view = RegistrationView(tournament_id, size, type_text)
    await send_temp(ctx, embed=embed, view=view)
    return True

async def build_tournament_status_embed(tournament_id: int) -> discord.Embed | None:
    t = get_tournament_info(tournament_id)
    if not t:
        return None

    from bot.data.tournament_db import list_participants_full

    participants = list_participants_full(tournament_id)
    current = len(participants)
    t_type = t["type"]
    size = t["size"]
    bank_type = t.get("bank_type", 1)
    manual = t.get("manual_amount", 20.0)
    status = t.get("status", "unknown")
    start = t.get("start_time")

    type_text = "Дуэльный 1×1" if t_type == "duel" else "Командный 3×3"
    prize_text = {
        1: f"🏅 Тип 1 — {manual:.2f} баллов от пользователя",
        2: "🥈 Тип 2 — 30 баллов (25% платит игрок)",
        3: "🥇 Тип 3 — 30 баллов (из банка Бебр)"
    }.get(bank_type, "❓")

    # Этап (только по статусу)
    stage = "❔ Не начат"
    if status == "active":
        stage = "🔁 Активен"
    elif status == "finished":
        stage = "✅ Завершён"

    embed = discord.Embed(
        title=f"📋 Турнир #{tournament_id} — Статус",
        color=discord.Color.blue()
    )
    embed.add_field(name="Тип", value=type_text, inline=True)
    embed.add_field(name="Участники", value=f"{current}/{size}", inline=True)
    embed.add_field(name="Банк", value=prize_text, inline=False)
    embed.add_field(name="Статус", value=status.capitalize(), inline=True)
    embed.add_field(name="Этап", value=stage, inline=True)
    if start:
        embed.add_field(name="Начало", value=start, inline=False)

    # Участники (ID)
    names = [
        f"<@{p['discord_user_id']}>" if p.get("discord_user_id") else f"ID: {p['player_id']}"
        for p in participants[:10]
    ]
    name_list = "\n".join(f"• {n}" for n in names) if names else "—"
    embed.add_field(name="📌 Участники (первые 10)", value=name_list, inline=False)

    return embed


async def build_tournament_bracket_embed(tournament_id: int, guild: discord.Guild | None = None) -> discord.Embed | None:
    """Строит embed-сетку турнира по имеющимся матчам."""
    round_no = 1
    embed = discord.Embed(
        title=f"🏟️ Сетка турнира #{tournament_id}",
        color=discord.Color.purple(),
    )

    any_matches = False
    while True:
        matches = tournament_db.get_matches(tournament_id, round_no)
        if not matches:
            break

        any_matches = True
        lines: list[str] = []
        for idx, m in enumerate(matches, start=1):
            if guild:
                p1m = guild.get_member(m["player1_id"])
                p2m = guild.get_member(m["player2_id"])
                p1 = p1m.mention if p1m else f"<@{m['player1_id']}>"
                p2 = p2m.mention if p2m else f"<@{m['player2_id']}>"
            else:
                p1 = f"<@{m['player1_id']}>"
                p2 = f"<@{m['player2_id']}>"

            mode_name = MODE_NAMES.get(m["mode"], str(m["mode"]))
            status = "⏳" if m.get("result") is None else ("🏆 1" if m["result"] == 1 else "🏆 2")
            lines.append(f"Матч {idx}: {p1} vs {p2} — `{m['map_id']}` ({mode_name}) {status}")

        embed.add_field(name=f"Раунд {round_no}", value="\n".join(lines), inline=False)
        round_no += 1

    if not any_matches:
        embed.description = "Матчи ещё не созданы"

    return embed


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
            start_text = dt.strftime("%d.%m.%Y %H:%M")
        except Exception:
            start_text = start_iso
        participants = tournament_db.list_participants_full(t["id"])
        user_ids = [p.get("discord_user_id") for p in participants if p.get("discord_user_id")]
        teams = []
        if t.get("type") == "team" and user_ids:
            for i in range(0, len(user_ids), 3):
                teams.append(user_ids[i:i+3])

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
            maps = [m["map_id"] for m in matches if uid in (m["player1_id"], m["player2_id"])]
            text_lines = [f"Скоро начнётся турнир #{t['id']} ({start_text})"]
            if mate_list:
                text_lines.append("Твои тиммейты: " + ", ".join(mate_list))
            if maps:
                text_lines.append("Карты: " + ", ".join(maps))
            msg = "\n".join(text_lines)
            try:
                await user.send(msg)
            except Exception:
                continue


async def tournament_reminder_loop(bot: commands.Bot) -> None:
    await bot.wait_until_ready()
    while not bot.is_closed():
        await send_tournament_reminders(bot)
        await asyncio.sleep(3600)
