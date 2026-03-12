from typing import Optional
from bot.data import db
import logging
from postgrest.exceptions import APIError

logger = logging.getLogger(__name__)

assert db.supabase, "Supabase client not initialized"
supabase = db.supabase


def get_player_by_id(player_id: int) -> Optional[dict]:
    """Возвращает запись игрока по ID."""
    try:
        res = supabase.table("players").select("*").eq("id", player_id).single().execute()
        return res.data
    except Exception as e:
        logger.error("get_player_by_id failed for player_id=%s: %s", player_id, e)
        return None


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

def remove_player_from_tournament(player_id: int, tournament_id: int) -> bool:
    """
    Удаляет связь игрока с турниром.
    """
    try:
        res = (
            supabase.table("tournament_participants")
            .delete()
            .eq("player_id", player_id)
            .eq("tournament_id", tournament_id)
            .execute()
        )
        return bool(res.data)
    except Exception as e:
        logger.error(
            "remove_player_from_tournament failed for player_id=%s tournament_id=%s: %s",
            player_id,
            tournament_id,
            e,
        )
        return False
