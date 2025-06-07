import discord
from discord.ext import commands
from typing import Optional

from bot.systems.tournament_logic import create_tournament as create_tournament_logic, Tournament
from bot.systems.tournament_db import (
    create_tournament as db_create_tournament,
    add_participant as db_add_participant,
    list_participants as db_list_participants,
    create_matches as db_create_matches,
    get_matches as db_get_matches,
    record_match_result as db_record_match_result,
    save_tournament_result as db_save_tournament_result
)

# В памяти храним экземпляры турниров
active_tournaments: dict[int, Tournament] = {}

bot = commands.Bot(command_prefix='?', intents=commands.Intents.all())

@bot.command(name='createtournament')
@commands.has_permissions(administrator=True)
async def createtournament(ctx):
    """Создать новый турнир и начать регистрацию"""
    t_id = db_create_tournament()
    # Создаём пустой объект турнира, участники будут добавляться при регистрации
    active_tournaments[t_id] = create_tournament_logic([])
    await ctx.send(f"🏁 Турнир создан (ID: {t_id}). Регистрация открыта. Используйте `?jointournament {t_id}` чтобы присоединиться.")

@bot.command(name='jointournament')
async def jointournament(ctx, tournament_id: int):
    """Присоединиться к открытому турниру"""
    # Проверяем, не зарегистрирован ли уже
    participants = db_list_participants(tournament_id)
    if ctx.author.id in participants:
        await ctx.send("❌ Вы уже зарегистрированы в этом турнире.")
        return
    db_add_participant(tournament_id, ctx.author.id)
    # Обновляем в память
    if tournament_id in active_tournaments:
        active_tournaments[tournament_id].participants.append(ctx.author.id)
    await ctx.send(f"✅ {ctx.author.mention}, вы зарегистрированы в турнире {tournament_id}.")

@bot.command(name='startround')
@commands.has_permissions(administrator=True)
async def startround(ctx, tournament_id: int):
    """Начать новый раунд турнира"""
    # Получаем участников из БД
    participants = db_list_participants(tournament_id)
    if len(participants) < 2:
        await ctx.send("❌ Недостаточно участников для начала раунда.")
        return
    # Загружаем или создаём объект турнира
    tour = active_tournaments.get(tournament_id)
    if not tour:
        tour = create_tournament_logic(participants)
        active_tournaments[tournament_id] = tour
    # Генерируем пары и записываем в БД
    matches = tour.generate_round()
    round_no = tour.current_round - 1
    db_create_matches(tournament_id, round_no, matches)
    # Выводим embed с парами
    embed = discord.Embed(title=f"🎮 Турнир {tournament_id} — Раунд {round_no}", color=discord.Color.blue())
    for idx, m in enumerate(matches, start=1):
        embed.add_field(
            name=f"Матч {idx}",
            value=(f"<@{m.player1_id}> vs <@{m.player2_id}>\n"
                   f"Режим: **{m.mode}**\n"
                   f"Карта: `{m.map_id}`"),
            inline=False
        )
    await ctx.send(embed=embed)

@bot.command(name='reportresult')
async def reportresult(ctx, tournament_id: int, round_number: int, match_index: int, winner: int):
    """Записать результат матча (winner: 1 или 2)"""
    tour = active_tournaments.get(tournament_id)
    if not tour:
        await ctx.send("❌ Турнир не найден или не инициализирован.")
        return
    try:
        # Запись результата в память
        tour.record_result(round_number, match_index - 1, winner)
        # Обновление в БД
        db_matches = db_get_matches(tournament_id, round_number)
        match_rec = db_matches[match_index - 1]
        db_record_match_result(match_rec['id'], winner)
        await ctx.send("✅ Результат сохранён.")
    except Exception as e:
        await ctx.send(f"❌ Ошибка: {e}")

@bot.command(name='tournamentstatus')
async def tournamentstatus(ctx, tournament_id: int, round_number: Optional[int] = None):
    """Показать статус турнира или конкретного раунда"""
    if round_number is None:
        # Показать общее состояние: кол-во участников и последний раунд
        participants = db_list_participants(tournament_id)
        tour = active_tournaments.get(tournament_id)
        last_round = (tour.current_round - 1) if tour else 0
        await ctx.send(
            f"🏟 Турнир {tournament_id}: участников {len(participants)}, последний раунд {last_round}"
        )
    else:
        # Показать матчи раунда и результаты
        db_matches = db_get_matches(tournament_id, round_number)
        if not db_matches:
            await ctx.send(f"❌ Раунд {round_number} не найден.")
            return
        embed = discord.Embed(title=f"📋 Турнир {tournament_id} — Раунд {round_number}", color=discord.Color.green())
        for idx, m in enumerate(db_matches, start=1):
            res = m['result']
            status = '⏳' if res is None else ('🏆 1' if res == 1 else '🏆 2')
            embed.add_field(
                name=f"Матч {idx} {status}",
                value=f"<@{m['player1_id']}> vs <@{m['player2_id']}> — режим {m['mode']}, карта `{m['map_id']}`",
                inline=False
            )
        await ctx.send(embed=embed)

# Регистрация этой команды в основном боте происходит при импорте файла в main.py или командах_bot
