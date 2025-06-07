from typing import List, Optional
from bot.data.db import db

# Обёртки для работы с таблицами турниров в Supabase
# Гарантируем, что клиент Supabase инициализирован
supabase = db.supabase  # type: ignore

if supabase is None:
    raise RuntimeError("Supabase client is not initialized")


def create_tournament() -> int:
    """
    Создаёт запись о новом турнире и возвращает его ID.
    """
    res = supabase.table("tournaments").insert({}).execute()
    record = res.data[0]
    return record["id"]


def add_participant(tournament_id: int, user_id: int) -> None:
    """
    Добавляет пользователя в список участников турнира.
    """
    supabase.table("tournament_participants").insert({
        "tournament_id": tournament_id,
        "user_id": user_id
    }).execute()


def list_participants(tournament_id: int) -> List[int]:
    """
    Возвращает список Discord-ID участников турнира.
    """
    res = supabase.table("tournament_participants") \
        .select("user_id") \
        .eq("tournament_id", tournament_id) \
        .execute()
    return [r["user_id"] for r in res.data or []]


def create_matches(tournament_id: int, round_number: int, matches: List) -> None:
    """
    Сохраняет все матчи раунда в таблицу tournament_matches.
    """
    records = [
        {
            "tournament_id": tournament_id,
            "round_number": round_number,
            "player1_id": m.player1_id,
            "player2_id": m.player2_id,
            "mode": m.mode,
            "map_id": m.map_id
        }
        for m in matches
    ]
    supabase.table("tournament_matches").insert(records).execute()


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


def save_tournament_result(tournament_id: int,
                           first_place: int,
                           second_place: Optional[int] = None,
                           third_place: Optional[int] = None) -> None:
    """
    Сохраняет итоговые места турнира в таблицу tournament_results.
    """
    supabase.table("tournament_results").upsert({
        "tournament_id": tournament_id,
        "first_place_id": first_place,
        "second_place_id": second_place,
        "third_place_id": third_place
    }).execute()
