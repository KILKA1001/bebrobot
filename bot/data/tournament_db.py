from typing import List, Optional, Dict
from bot.data.db import db
from supabase import Client
import logging
from postgrest.exceptions import APIError

assert db.supabase
# Обёртки для работы с таблицами турниров в Supabase
# Гарантируем, что клиент Supabase инициализирован
supabase = db.supabase

if supabase is None:
    raise RuntimeError("Supabase client is not initialized")

logger = logging.getLogger(__name__)


def create_tournament_record(
    t_type: str,
    size: int,
    start_time: Optional[str] = None,
    author_id: Optional[int] = None,
) -> int:
    """Создаёт запись о новом турнире и возвращает его ID."""
    payload = {"type": t_type, "size": size, "status": "registration"}
    if start_time:
        payload["start_time"] = start_time
    if author_id is not None:
        payload["author_id"] = author_id
    res = supabase.table("tournaments").insert(payload).execute()
    return res.data[0]["id"]


def add_discord_participant(
    tournament_id: int,
    discord_user_id: int,
    team_id: Optional[int] = None,
    team_name: Optional[str] = None,
) -> bool:
    """Для саморегистрации участника (по Discord ID)."""
    payload = {
        "tournament_id": tournament_id,
        "discord_user_id": discord_user_id,
        "player_id": None,
        "confirmed": False,
    }
    if team_id is not None:
        payload["team_id"] = team_id
        payload["team_name"] = team_name
    try:
        res = supabase.table("tournament_participants").insert(payload).execute()
        return bool(res.data)
    except APIError as e:
        if e.code == "23505":
            return False
        logger.error("add_discord_participant failed: %s", e)
        return False
    except Exception as e:
        logger.error("Unexpected error in add_discord_participant: %s", e)
        return False


def add_player_participant(
    tournament_id: int,
    player_id: int,
    team_id: Optional[int] = None,
    team_name: Optional[str] = None,
) -> bool:
    """Для админской регистрации (по player_id)."""
    payload = {
        "tournament_id": tournament_id,
        "discord_user_id": player_id,
        "player_id": player_id,
        "confirmed": True,
    }
    if team_id is not None:
        payload["team_id"] = team_id
        payload["team_name"] = team_name
    try:
        res = supabase.table("tournament_participants").insert(payload).execute()
        return bool(res.data)
    except APIError as e:
        if e.code == "23505":
            return False
        logger.error("add_player_participant failed: %s", e)
        return False
    except Exception as e:
        logger.error("Unexpected error in add_player_participant: %s", e)
        return False


def list_participants(tournament_id: int) -> List[dict]:
    """
    Возвращает список участников как словари с полями
    {discord_user_id, player_id}.
    """
    res = (
        supabase.table("tournament_participants")
        .select("discord_user_id, player_id, confirmed, team_id, team_name")
        .eq("tournament_id", tournament_id)
        .execute()
    )
    return res.data or []


def create_matches(tournament_id: int, round_number: int, matches: List) -> None:
    """
    Сохраняет все матчи раунда в таблицу tournament_matches.
    Универсальная обработка объектов Match разных типов.
    """
    records = []
    for m in matches:
        # Определяем тип объекта (имеет атрибуты или является словарем)
        is_dict_like = hasattr(m, "items") or hasattr(m, "get") or isinstance(m, dict)

        record = {
            "tournament_id": tournament_id,
            "round_number": round_number,
            "player1_id": m["player1_id"] if is_dict_like else m.player1_id,
            "player2_id": m["player2_id"] if is_dict_like else m.player2_id,
            "mode": (
                m.get("mode", "default")
                if is_dict_like
                else getattr(m, "mode", "default")
            ),
            "map_id": m.get("map_id", 0) if is_dict_like else getattr(m, "map_id", 0),
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
    res = (
        supabase.table("tournament_matches")
        .select("id, player1_id, player2_id, mode, map_id, result")
        .eq("tournament_id", tournament_id)
        .eq("round_number", round_number)
        .order("id")
        .execute()
    )
    return res.data or []


def get_map_image_url(map_id: str) -> Optional[str]:
    """Возвращает ссылку на изображение карты по её ID."""
    try:
        res = (
            supabase.table("maps")
            .select("image_url")
            .eq("id", map_id)
            .single()
            .execute()
        )
        return res.data.get("image_url") if res and res.data else None
    except Exception:
        return None


def get_map_info(map_id: str) -> Optional[dict]:
    """Возвращает полную информацию о карте по её ID."""
    try:
        res = (
            supabase.table("maps")
            .select("name, image_url, mode_id")
            .eq("id", map_id)
            .single()
            .execute()
        )
        return res.data if res and res.data else None
    except Exception:
        return None


def list_maps_by_mode() -> Dict[int, List[str]]:
    """Возвращает карты, сгруппированные по mode_id."""
    try:
        res = supabase.table("maps").select("mode_id, name").execute()
        data = res.data or []
        result: Dict[int, List[str]] = {}
        for entry in data:
            mode = entry.get("mode_id")
            name = entry.get("name")
            if mode is None or name is None:
                continue
            result.setdefault(int(mode), []).append(name)
        return result
    except Exception as e:
        logger.error(f"Failed to load maps: {e}")
        return {}


def record_match_result(match_id: int, result: int) -> bool:
    """Обновляет результат матча.

    Parameters
    ----------
    match_id : int
        ID матча, который нужно обновить.
    result : int
        Победитель (1 или 2). ``0`` обозначает ничью.

    Returns
    -------
    bool
        ``True`` при успешном обновлении, ``False`` при ошибке.
    """
    try:
        supabase.table("tournament_matches").update({"result": result}).eq(
            "id", match_id
        ).execute()
        return True
    except Exception as e:
        logger.error(f"Failed to record match result: {e}")
        return False


def delete_tournament(tournament_id: int) -> None:
    """Удаляет турнир и все связанные с ним записи."""
    # Удаляем результаты
    supabase.table("tournament_results").delete().eq(
        "tournament_id", tournament_id
    ).execute()
    # Удаляем матчи
    supabase.table("tournament_matches").delete().eq(
        "tournament_id", tournament_id
    ).execute()
    # Удаляем участников (discord и player)
    supabase.table("tournament_participants").delete().eq(
        "tournament_id", tournament_id
    ).execute()
    # Удаляем связи игроков с турниром, если таблица существует
    try:
        supabase.table("tournament_players").delete().eq(
            "tournament_id", tournament_id
        ).execute()
    except Exception:
        pass
    # Наконец удаляем сам турнир
    supabase.table("tournaments").delete().eq("id", tournament_id).execute()


def save_tournament_result(
    tournament_id: int,
    first_place_id: int,
    second_place_id: int,
    third_place_id: Optional[int] = None,
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
        res = supabase.table("tournament_results").upsert(payload).execute()
        return bool(res.data)
    except Exception:
        return False


def get_tournament_result(tournament_id: int) -> Optional[dict]:
    """Возвращает результат турнира или None."""
    try:
        res = (
            supabase.table("tournament_results")
            .select("first_place_id, second_place_id, third_place_id")
            .eq("tournament_id", tournament_id)
            .single()
            .execute()
        )
        return res.data or None
    except Exception:
        return None


def delete_match_records(tournament_id: int) -> bool:
    """Удаляет все записи матчей указанного турнира."""
    try:
        supabase.table("tournament_matches").delete().eq(
            "tournament_id", tournament_id
        ).execute()
        return True
    except Exception:
        return False


def update_tournament_status(tournament_id: int, status: str) -> bool:
    """
    Обновляет поле status в записи tournaments.
    """
    try:
        res = (
            supabase.table("tournaments")
            .update({"status": status})
            .eq("id", tournament_id)
            .execute()
        )
        return bool(res.data)
    except Exception:
        return False


def count_matches(tournament_id: int) -> int:
    """
    Возвращает общее число матчей для данного турнира.
    """
    res = (
        supabase.table("tournament_matches")
        .select("id")
        .eq("tournament_id", tournament_id)
        .execute()
    )
    return len(res.data or [])


def list_participants_full(tournament_id: int) -> List[dict]:
    """
    Возвращает список записей участников турнира:
    [{"discord_user_id": int|None, "player_id": int|None}, ...]
    """
    res = (
        supabase.table("tournament_participants")
        .select(
            "discord_user_id, player_id, confirmed, team_id, team_name"
        )
        .eq("tournament_id", tournament_id)
        .execute()
    )
    return res.data or []


def get_team_info(tournament_id: int) -> tuple[Dict[int, List[int]], Dict[int, str]]:
    """Возвращает отображение team_id->участники и их названия."""
    res = (
        supabase.table("tournament_participants")
        .select("discord_user_id, player_id, team_id, team_name")
        .eq("tournament_id", tournament_id)
        .not_.is_("team_id", "null")
        .execute()
    )
    mapping: Dict[int, List[int]] = {}
    names: Dict[int, str] = {}
    for row in res.data or []:
        tid = row.get("team_id")
        if tid is None:
            continue
        pid = row.get("discord_user_id") or row.get("player_id")
        if pid is None:
            continue
        mapping.setdefault(int(tid), []).append(pid)
        name = row.get("team_name")
        if name:
            names[int(tid)] = name
    return mapping, names


def get_team_id_by_name(tournament_id: int, team_name: str) -> Optional[int]:
    """Возвращает team_id по названию, если существует."""
    res = (
        supabase.table("tournament_participants")
        .select("team_id")
        .eq("tournament_id", tournament_id)
        .eq("team_name", team_name)
        .limit(1)
        .execute()
    )
    if res.data:
        return res.data[0].get("team_id")
    return None


def get_next_team_id(tournament_id: int) -> int:
    """Возвращает следующий свободный team_id для турнира."""
    res = (
        supabase.table("tournament_participants")
        .select("team_id")
        .eq("tournament_id", tournament_id)
        .execute()
    )
    ids = [r.get("team_id") for r in res.data or [] if r.get("team_id") is not None]
    return max(ids or [0]) + 1


def update_team_name(tournament_id: int, team_id: int, new_name: str) -> bool:
    """Обновляет название команды во всех связанных записях."""
    try:
        res = (
            supabase.table("tournament_participants")
            .update({"team_name": new_name})
            .eq("tournament_id", tournament_id)
            .eq("team_id", team_id)
            .execute()
        )
        return bool(res.data)
    except Exception:
        return False


def remove_player_from_tournament(player_id: int, tournament_id: int) -> bool:
    """
    Удаляет связь игрока (по player_id) с турниром.
    """
    res = (
        supabase.table("tournament_participants")
        .delete()
        .eq("player_id", player_id)
        .eq("tournament_id", tournament_id)
        .execute()
    )
    return bool(res.data)


def remove_discord_participant(tournament_id: int, discord_user_id: int) -> bool:
    """
    Удаляет запись участника по его Discord-ID из турнира.
    """
    res = (
        supabase.table("tournament_participants")
        .delete()
        .eq("tournament_id", tournament_id)
        .eq("discord_user_id", discord_user_id)
        .execute()
    )
    # res.data — это список удалённых строк, пустой если ничего не удалено
    return bool(res.data)


def confirm_participant(tournament_id: int, discord_user_id: int) -> bool:
    """Помечает участника как подтвердившего участие."""
    try:
        res = (
            supabase.table("tournament_participants")
            .update({"confirmed": True})
            .eq("tournament_id", tournament_id)
            .eq("discord_user_id", discord_user_id)
            .execute()
        )
        return bool(res.data)
    except Exception:
        return False


def set_bank_type(
    tournament_id: int, bank_type: int, manual_amount: Optional[float] = None
) -> bool:
    """Устанавливает тип банка и сумму (если задана)"""
    data = {"bank_type": bank_type}
    if manual_amount is not None:
        data["manual_amount"] = manual_amount

    res = supabase.table("tournaments").update(data).eq("id", tournament_id).execute()
    return bool(res.data)


def get_tournament_status(tournament_id: int) -> str:
    """Возвращает текущий статус турнира."""
    res = (
        supabase.table("tournaments").select("status").eq("id", tournament_id).execute()
    )
    return res.data[0]["status"] if res.data else "registration"


def get_tournament_size(tournament_id: int) -> int:
    """Возвращает максимальное количество участников турнира."""
    res = supabase.table("tournaments").select("size").eq("id", tournament_id).execute()
    return res.data[0]["size"] if res.data else 0


def get_announcement_message_id(tournament_id: int) -> Optional[int]:
    """Возвращает ID сообщения-объявления турнира."""
    res = (
        supabase.table("tournaments")
        .select("announcement_message_id")
        .eq("id", tournament_id)
        .single()
        .execute()
    )
    if res and res.data:
        return res.data.get("announcement_message_id")
    return None


def get_active_tournaments() -> list[dict]:
    """Возвращает список активных турниров с полями id, size, type и announcement_message_id."""
    res = (
        supabase.table("tournaments")
        .select("id, size, type, announcement_message_id")
        .eq("status", "active")
        .execute()
    )
    return res.data or []


def get_upcoming_tournaments(hours: int) -> list[dict]:
    """Возвращает турниры, которые стартуют в течение указанного числа часов."""
    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    later = now + timedelta(hours=hours)
    try:
        res = (
            supabase.table("tournaments")
            .select("id, type, start_time, reminder_sent")
            .eq("status", "registration")
            .gte("start_time", now.isoformat())
            .lte("start_time", later.isoformat())
            .execute()
        )
        tournaments = res.data or []
    except Exception as e:
        if getattr(e, "code", None) == "42703":
            logger.warning("start_time or reminder_sent column not found in tournaments table")
            res = (
                supabase.table("tournaments")
                .select("id, type, start_time")
                .eq("status", "registration")
                .gte("start_time", now.isoformat())
                .lte("start_time", later.isoformat())
                .execute()
            )
            tournaments = res.data or []
        else:
            raise

    return [t for t in tournaments if not t.get("reminder_sent")]


def save_announcement_message(tournament_id: int, message_id: int) -> bool:
    """Сохраняет ID сообщения с объявлением турнира."""
    res = (
        supabase.table("tournaments")
        .update({"announcement_message_id": message_id})
        .eq("id", tournament_id)
        .execute()
    )
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


def get_tournament_author(tournament_id: int) -> Optional[int]:
    """Возвращает ID автора турнира или None."""
    try:
        res = (
            supabase.table("tournaments")
            .select("author_id")
            .eq("id", tournament_id)
            .single()
            .execute()
        )
        if res and res.data:
            return res.data.get("author_id")
        return None
    except Exception:
        return None


def set_tournament_author(tournament_id: int, author_id: int) -> bool:
    """Сохраняет автора для указанного турнира."""
    try:
        res = (
            supabase.table("tournaments")
            .update({"author_id": author_id})
            .eq("id", tournament_id)
            .execute()
        )
        return bool(res.data)
    except Exception:
        return False


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


def get_expired_registrations() -> List[dict]:
    """Возвращает турниры, где истекла дата начала и статус всё ещё registration."""
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    res = (
        supabase.table("tournaments")
        .select("id, author_id, start_time")
        .eq("status", "registration")
        .lte("start_time", now.isoformat())
        .execute()
    )
    return res.data or []


def update_start_time(tournament_id: int, new_iso: str) -> bool:
    """Обновляет время начала турнира."""
    try:
        res = (
            supabase.table("tournaments")
            .update({"start_time": new_iso})
            .eq("id", tournament_id)
            .execute()
        )
        return bool(res.data)
    except Exception:
        return False


def mark_reminder_sent(tournament_id: int) -> bool:
    """Помечает турнир как отправивший напоминание."""
    try:
        res = (
            supabase.table("tournaments")
            .update({"reminder_sent": True})
            .eq("id", tournament_id)
            .execute()
        )
        return bool(res.data)
    except Exception as e:
        if getattr(e, "code", None) == "42703":
            logger.warning("reminder_sent column not found in tournaments table")
            return False
        logger.error("Failed to mark reminder sent: %s", e)
        return False
