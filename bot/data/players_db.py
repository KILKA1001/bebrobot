from typing import List, Optional, Tuple
from bot.data import db
from datetime import datetime, timezone

assert db.supabase, "Supabase client not initialized"
supabase = db.supabase

def create_player(nick: str, tg_username: str) -> Optional[int]:
    """Добавляет нового игрока и возвращает его ID."""
    res = supabase.table("players") \
        .insert({
            "nick": nick,
            "tg_username": tg_username
        }) \
        .execute()
    if res.data:
        return res.data[0].get("id")
    return None

def get_player_by_id(player_id: int) -> Optional[dict]:
    """Возвращает запись игрока по ID."""
    res = supabase.table("players") \
        .select("*") \
        .eq("id", player_id) \
        .single() \
        .execute()
    return res.data

def get_player_by_tg(tg_username: str) -> Optional[dict]:
    """Возвращает запись игрока по Telegram-нику."""
    res = supabase.table("players") \
        .select("*") \
        .eq("tg_username", tg_username) \
        .single() \
        .execute()
    return res.data

def list_players(page: int = 1, per_page: int = 5) -> Tuple[List[dict], int]:
    """
    Возвращает кортеж (список игроков на странице, общее число страниц).
    """
    offset = (page - 1) * per_page
    # сначала общее количество
    all_res = supabase.table("players") \
        .select("id") \
        .execute()
    total = len(all_res.data or [])
    pages = max(1, (total + per_page - 1) // per_page)

    res = supabase.table("players") \
        .select("id, nick, tg_username") \
        .order("id", desc=False) \
        .range(offset, offset + per_page - 1) \
        .execute()
    return res.data or [], pages

def update_player_field(
    player_id: int,
    field_name: str,
    new_value: str
) -> bool:
    """
    Обновляет single-поле nick или tg_username и пишет лог изменения.
    """
    # 1) прочитать старое значение
    existing = get_player_by_id(player_id)
    if not existing or field_name not in existing:
        return False

    old = existing[field_name]
    # 2) обновить
    supabase.table("players") \
        .update({field_name: new_value, "updated_at": datetime.now(timezone.utc).isoformat()}) \
        .eq("id", player_id) \
        .execute()

    # 3) записать лог
    supabase.table("player_logs") \
        .insert({
            "player_id": player_id,
            "field_name": field_name,
            "old_value": old,
            "new_value": new_value
        }) \
        .execute()
    return True

def add_player_to_tournament(player_id: int, tournament_id: int) -> bool:
    """
    Связывает игрока с турниром.
    """
    res = supabase.table("tournament_players") \
        .insert({
            "player_id": player_id,
            "tournament_id": tournament_id
        }) \
        .execute()
    return bool(res.data)

def delete_player(player_id: int) -> bool:
    """
    Удаляет игрока из таблицы players.
    Благодаря ON DELETE CASCADE удалятся и связанные записи в tournament_players и player_logs.
    """
    res = supabase.table("players") \
        .delete() \
        .eq("id", player_id) \
        .execute()
    return bool(res.data)

def remove_player_from_tournament(player_id: int, tournament_id: int) -> bool:
    """
    Удаляет связь игрока с турниром.
    """
    res = supabase.table("tournament_players") \
        .delete() \
        .eq("player_id", player_id) \
        .eq("tournament_id", tournament_id) \
        .execute()
    return bool(res.data)

def list_player_logs(player_id: int, page: int = 1, per_page: int = 5) -> Tuple[List[dict], int]:
    """
    Возвращает (логи изменений игрока, число страниц).
    """
    offset = (page - 1) * per_page
    # читаем все логи, чтобы посчитать страницы
    all_res = supabase.table("player_logs") \
        .select("log_id") \
        .eq("player_id", player_id) \
        .execute()
    total = len(all_res.data or [])
    pages = max(1, (total + per_page - 1) // per_page)

    res = supabase.table("player_logs") \
        .select("changed_at, field_name, old_value, new_value") \
        .eq("player_id", player_id) \
        .order("changed_at", desc=True) \
        .range(offset, offset + per_page - 1) \
        .execute()
    return res.data or [], pages
