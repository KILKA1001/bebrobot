from typing import List, Optional
from bot.data.db import db
from supabase import Client
import logging
assert db.supabase
# Обёртки для работы с таблицами турниров в Supabase
# Гарантируем, что клиент Supabase инициализирован
supabase = db.supabase

if supabase is None:
    raise RuntimeError("Supabase client is not initialized")

logger = logging.getLogger(__name__)



def create_tournament_record(t_type: str, size: int, start_time: Optional[str] = None) -> int:
    """Создаёт запись о новом турнире и возвращает его ID."""
    payload = {"type": t_type, "size": size, "status": "registration"}
    if start_time:
        payload["start_time"] = start_time
    res = (
        supabase.table("tournaments")
        .insert(payload)
        .execute()
    )
    return res.data[0]["id"]


def add_discord_participant(tournament_id: int, discord_user_id: int) -> bool:
    """Для саморегистрации участника (по Discord ID)."""
    res = supabase.table("tournament_participants")\
        .insert({
            "tournament_id": tournament_id,
            "discord_user_id": discord_user_id,
            "player_id": None
        })\
        .execute()
    return bool(res.data)

def add_player_participant(tournament_id: int, player_id: int) -> bool:
    """Для админской регистрации (по player_id)."""
    res = supabase.table("tournament_participants")\
        .insert({
            "tournament_id": tournament_id,
            "discord_user_id": None,
            "player_id": player_id
        })\
        .execute()
    return bool(res.data)

def list_participants(tournament_id: int) -> List[dict]:
    """
    Возвращает список участников как словари с полями
    {discord_user_id, player_id}.
    """
    res = supabase.table("tournament_participants")\
        .select("discord_user_id, player_id")\
        .eq("tournament_id", tournament_id)\
        .execute()
    return res.data or []

def create_matches(tournament_id: int, round_number: int, matches: List) -> None:
    """
    Сохраняет все матчи раунда в таблицу tournament_matches.
    Универсальная обработка объектов Match разных типов.
    """
    records = []
    for m in matches:
        # Определяем тип объекта (имеет атрибуты или является словарем)
        is_dict_like = hasattr(m, 'items') or hasattr(m, 'get') or isinstance(m, dict)
        
        record = {
            "tournament_id": tournament_id,
            "round_number": round_number,
            "player1_id": m["player1_id"] if is_dict_like else m.player1_id,
            "player2_id": m["player2_id"] if is_dict_like else m.player2_id,
            "mode": m.get("mode", "default") if is_dict_like else getattr(m, "mode", "default"),
            "map_id": m.get("map_id", 0) if is_dict_like else getattr(m, "map_id", 0)
        }
        records.append(record)
    
    res = (
        supabase.table("tournament_matches")
        .insert(records, returning="representation")
        .execute()
    )

    rows = res.data or []
    for m, row in zip(matches, rows):
        if hasattr(m, "match_id"):
            m.match_id = row.get("id")


def get_matches(tournament_id: int, round_number: int) -> List[dict]:
    """
    Возвращает список матчей с полями id, player1_id, player2_id, mode, map_id, result.
    """
    res = supabase.table("tournament_matches") \
        .select("id, player1_id, player2_id, mode, map_id, result") \
        .eq("tournament_id", tournament_id) \
        .eq("round_number", round_number) \
        .order("id") \
        .execute()
    return res.data or []


def record_match_result(match_id: int, result: int) -> None:
    """
    Обновляет поле result у конкретного матча.
    result: 1 или 2
    """
    supabase.table("tournament_matches") \
        .update({"result": result}) \
        .eq("id", match_id) \
        .execute()

def delete_tournament(tournament_id: int) -> None:
    """
    Удаляет турнир и все связанные с ним записи.
    """
    supabase.table("tournaments")\
      .delete()\
      .eq("id", tournament_id)\
      .execute()

def save_tournament_result(
    tournament_id: int,
    first_place_id: int,
    second_place_id: int,
    third_place_id: Optional[int] = None
) -> bool:
    """
    Сохраняет итоговые места турнира в таблицу tournament_results.
    """
    try:
        payload = {
            "tournament_id": tournament_id,
            "first_place_id": first_place_id,
            "second_place_id": second_place_id,
            "third_place_id": third_place_id,
        }
        res = supabase.table("tournament_results") \
            .upsert(payload) \
            .execute()
        return bool(res.data)
    except Exception:
        return False

def update_tournament_status(tournament_id: int, status: str) -> bool:
    """
    Обновляет поле status в записи tournaments.
    """
    try:
        res = supabase.table("tournaments") \
            .update({"status": status}) \
            .eq("id", tournament_id) \
            .execute()
        return bool(res.data)
    except Exception:
        return False

def count_matches(tournament_id: int) -> int:
    """
    Возвращает общее число матчей для данного турнира.
    """
    res = supabase.table("tournament_matches") \
        .select("id") \
        .eq("tournament_id", tournament_id) \
        .execute()
    return len(res.data or [])

def list_participants_full(tournament_id: int) -> List[dict]:
    """
    Возвращает список записей участников турнира:
    [{"discord_user_id": int|None, "player_id": int|None}, ...]
    """
    res = supabase.table("tournament_participants") \
        .select("discord_user_id, player_id") \
        .eq("tournament_id", tournament_id) \
        .execute()
    return res.data or []

def remove_player_from_tournament(player_id: int, tournament_id: int) -> bool:
    """
    Удаляет связь игрока (по player_id) с турниром.
    """
    res = supabase.table("tournament_participants") \
        .delete() \
        .eq("player_id", player_id) \
        .eq("tournament_id", tournament_id) \
        .execute()
    return bool(res.data)

def remove_discord_participant(tournament_id: int, discord_user_id: int) -> bool:
    """
    Удаляет запись участника по его Discord-ID из турнира.
    """
    res = supabase.table("tournament_participants") \
        .delete() \
        .eq("tournament_id", tournament_id) \
        .eq("discord_user_id", discord_user_id) \
        .execute()
    # res.data — это список удалённых строк, пустой если ничего не удалено
    return bool(res.data)

def set_bank_type(tournament_id: int, bank_type: int, manual_amount: Optional[float] = None) -> bool:
    """Устанавливает тип банка и сумму (если задана)"""
    data = {"bank_type": bank_type}
    if manual_amount is not None:
        data["manual_amount"] = manual_amount

    res = supabase.table("tournaments") \
        .update(data) \
        .eq("id", tournament_id) \
        .execute()
    return bool(res.data)

def get_tournament_status(tournament_id: int) -> str:
    """Возвращает текущий статус турнира."""
    res = supabase.table("tournaments") \
        .select("status") \
        .eq("id", tournament_id) \
        .execute()
    return res.data[0]["status"] if res.data else "registration"

def get_tournament_size(tournament_id: int) -> int:
    """Возвращает максимальное количество участников турнира."""
    res = supabase.table("tournaments") \
        .select("size") \
        .eq("id", tournament_id) \
        .execute()
    return res.data[0]["size"] if res.data else 0

def get_active_tournaments() -> list[dict]:
    """Возвращает список активных турниров с полями id, size, type и announcement_message_id."""
    res = supabase.table("tournaments") \
        .select("id, size, type, announcement_message_id") \
        .eq("status", "active") \
        .execute()
    return res.data or []


def get_upcoming_tournaments(hours: int) -> list[dict]:
    """Возвращает турниры, которые стартуют в течение указанного числа часов."""
    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    later = now + timedelta(hours=hours)
    try:
        res = (
            supabase.table("tournaments")
            .select("id, type, start_time")
            .eq("status", "registration")
            .gte("start_time", now.isoformat())
            .lte("start_time", later.isoformat())
            .execute()
        )
        return res.data or []
    except Exception as e:
        if getattr(e, "code", None) == "42703":
            logger.warning("start_time column not found in tournaments table")
            return []
        raise


def save_announcement_message(tournament_id: int, message_id: int) -> bool:
    """Сохраняет ID сообщения с объявлением турнира."""
    res = supabase.table("tournaments") \
        .update({"announcement_message_id": message_id}) \
        .eq("id", tournament_id) \
        .execute()
    return bool(res.data)


def get_tournament_info(tournament_id: int) -> Optional[dict]:
    """Возвращает основные поля турнира или None."""
    try:
        res = (
            supabase.table("tournaments")
            .select("type, size, bank_type, manual_amount, status, start_time")
            .eq("id", tournament_id)
            .single()
            .execute()
        )
        return res.data or None
    except Exception:
        return None


def list_recent_results(limit: int) -> List[dict]:
    """Возвращает последние завершённые турниры."""
    res = (
        supabase.table("tournament_results")
        .select("*")
        .order("finished_at", desc=True)
        .limit(limit)
        .execute()
    )
    return res.data or []
