from typing import List, Optional, Tuple
from bot.data import db
from datetime import datetime, timezone
import logging
from postgrest.exceptions import APIError

logger = logging.getLogger(__name__)

assert db.supabase, "Supabase client not initialized"
supabase = db.supabase


def create_player(nick: str, tg_username: str) -> Optional[int]:
    """Добавляет нового игрока и возвращает его ID."""
    res = (
        supabase.table("players")
        .insert({"nick": nick, "tg_username": tg_username})
        .execute()
    )
    if res.data:
        return res.data[0].get("id")
    return None


def get_player_by_id(player_id: int) -> Optional[dict]:
    """Возвращает запись игрока по ID."""
    res = supabase.table("players").select("*").eq("id", player_id).single().execute()
    return res.data


def get_player_by_tg(tg_username: str) -> Optional[dict]:
    res = (
        supabase.table("players")
        .select("*")
        .eq("tg_username", tg_username)
        .limit(1)
        .execute()
    )
    return res.data[0] if res.data else None


def list_players(page: int = 1, per_page: int = 5) -> Tuple[List[dict], int]:
    """
    Возвращает кортеж (список игроков на странице, общее число страниц).
    """
    offset = (page - 1) * per_page
    # сначала общее количество
    all_res = supabase.table("players").select("id").execute()
    total = len(all_res.data or [])
    pages = max(1, (total + per_page - 1) // per_page)

    res = (
        supabase.table("players")
        .select("id, nick, tg_username")
        .order("id", desc=False)
        .range(offset, offset + per_page - 1)
        .execute()
    )
    return res.data or [], pages


def update_player_field(player_id: int, field_name: str, new_value: str) -> bool:
    """
    Обновляет single-поле nick или tg_username и пишет лог изменения.
    """
    # 1) прочитать старое значение
    existing = get_player_by_id(player_id)
    if not existing or field_name not in existing:
        return False

    old = existing[field_name]
    # 2) обновить
    supabase.table("players").update(
        {field_name: new_value, "updated_at": datetime.now(timezone.utc).isoformat()}
    ).eq("id", player_id).execute()

    # 3) записать лог
    supabase.table("player_logs").insert(
        {
            "player_id": player_id,
            "field_name": field_name,
            "old_value": old,
            "new_value": new_value,
        }
    ).execute()
    return True


def add_player_to_tournament(
    player_id: Optional[int],
    tournament_id: int,
    *,
    discord_user_id: Optional[int] = None,
    team_id: Optional[int] = None,
    team_name: Optional[str] = None,
) -> bool:
    """Регистрирует игрока в турнире.

    Можно передать либо ``player_id`` (ID в таблице ``players``),
    либо ``discord_user_id``. Если нам известен только Discord ID,
    ``player_id`` можно оставить ``None``.
    """

    # хотя бы одно из полей должно быть заполнено
    if player_id is None and discord_user_id is None:
        logger.error("add_player_to_tournament called without player identifiers")
        return False

    payload = {"tournament_id": tournament_id, "confirmed": True}
    if player_id is not None:
        payload["player_id"] = player_id
        if discord_user_id is None:
            # В базе столбец discord_user_id обязательный, поэтому,
            # если у игрока нет Discord, используем его player_id как заглушку
            payload["discord_user_id"] = player_id
    if discord_user_id is not None:
        payload["discord_user_id"] = discord_user_id
    if team_id is not None:
        payload["team_id"] = team_id
        payload["team_name"] = team_name
    try:
        res = supabase.table("tournament_participants").insert(payload).execute()
        return bool(res.data)
    except APIError as e:
        if e.code == "23505":
            return False
        logger.error("add_player_to_tournament failed: %s", e)
        return False
    except Exception as e:
        logger.error("Unexpected error in add_player_to_tournament: %s", e)
        return False


def delete_player(player_id: int) -> bool:
    """
    Удаляет игрока из таблицы players.
    Благодаря ON DELETE CASCADE удалятся и связанные записи в tournament_participants и player_logs.
    """
    res = supabase.table("players").delete().eq("id", player_id).execute()
    return bool(res.data)


def remove_player_from_tournament(player_id: int, tournament_id: int) -> bool:
    """
    Удаляет связь игрока с турниром.
    """
    res = (
        supabase.table("tournament_participants")
        .delete()
        .eq("player_id", player_id)
        .eq("tournament_id", tournament_id)
        .execute()
    )
    return bool(res.data)


def list_player_logs(
    player_id: int, page: int = 1, per_page: int = 5
) -> Tuple[List[dict], int]:
    """
    Возвращает (логи изменений игрока, число страниц).
    """
    offset = (page - 1) * per_page
    # читаем все логи, чтобы посчитать страницы
    all_res = (
        supabase.table("player_logs")
        .select("log_id")
        .eq("player_id", player_id)
        .execute()
    )
    total = len(all_res.data or [])
    pages = max(1, (total + per_page - 1) // per_page)

    res = (
        supabase.table("player_logs")
        .select("changed_at, field_name, old_value, new_value")
        .eq("player_id", player_id)
        .order("changed_at", desc=True)
        .range(offset, offset + per_page - 1)
        .execute()
    )
    return res.data or [], pages
