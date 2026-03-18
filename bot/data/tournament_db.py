from typing import List, Optional, Dict
from bot.data.db import db
import logging
from postgrest.exceptions import APIError
from bot.legacy_identity_logging import (
    log_identity_resolve_error,
    log_legacy_schema_fallback,
    log_runtime_dependency_missing,
)

logger = logging.getLogger(__name__)


class _LazySupabaseProxy:
    """Ленивая прокси-обёртка для безопасного импорта tournament_db."""

    def _require_client(self, handler: str):
        client = getattr(db, "supabase", None)
        if client is not None:
            return client
        log_runtime_dependency_missing(
            logger,
            module=__name__,
            handler=handler,
            field="db.supabase",
            action="initialize_supabase_client_before_tournament_db_call",
            continue_execution=False,
            developer_hint="module import is intentionally allowed without Supabase; initialize db.supabase before calling tournament_db functions",
        )
        raise RuntimeError("Supabase client is not initialized")

    def table(self, *args, **kwargs):
        return self._require_client("supabase.table").table(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._require_client(f"supabase.{name}"), name)


# Обёртки для работы с таблицами турниров в Supabase
supabase = _LazySupabaseProxy()

# Флаг наличия столбца team_auto в таблице tournaments
_has_team_auto = True
# Флаг наличия столбца status_message_id
_has_status_msg = True
# Флаг наличия legacy-столбца player_id в tournament_participants
_has_tp_player_id = True
_has_tp_account_id = True
_has_tp_discord_user_id = True
_has_tb_account_id = True
_has_tb_user_id = True
_account_to_discord_cache: Dict[str, int] = {}


def _participants_select_fields() -> str:
    return (
        "discord_user_id, player_id, confirmed, team_id, team_name"
        if _has_tp_player_id
        else "discord_user_id, confirmed, team_id, team_name"
    )


def _participants_team_select_fields() -> str:
    return (
        "discord_user_id, player_id, team_id, team_name"
        if _has_tp_player_id
        else "discord_user_id, team_id, team_name"
    )


def _normalize_participant_rows(rows: List[dict]) -> List[dict]:
    normalized: List[dict] = []
    for row in rows or []:
        if "player_id" not in row:
            row = {**row, "player_id": None}
        normalized.append(row)
    return normalized


def _get_account_id_for_discord_user(discord_user_id: int) -> Optional[str]:
    if not discord_user_id:
        return None
    try:
        response = (
            supabase.table("account_identities")
            .select("account_id")
            .eq("provider", "discord")
            .eq("provider_user_id", str(discord_user_id))
            .limit(1)
            .execute()
        )
        if response.data:
            return response.data[0].get("account_id")
    except Exception as e:
        logger.exception("resolve account_id for discord failed discord_user_id=%s error=%s", discord_user_id, e)
    return None


def _get_discord_user_for_account(account_id: str) -> Optional[int]:
    if not account_id:
        return None
    if account_id in _account_to_discord_cache:
        return _account_to_discord_cache[account_id]
    try:
        response = (
            supabase.table("account_identities")
            .select("provider_user_id")
            .eq("provider", "discord")
            .eq("account_id", str(account_id))
            .limit(1)
            .execute()
        )
        if response.data:
            resolved = int(response.data[0]["provider_user_id"])
            _account_to_discord_cache[account_id] = resolved
            return resolved
    except Exception as e:
        logger.exception("resolve discord id for account failed account_id=%s error=%s", account_id, e)
    return None


def _normalize_bet_row(row: dict) -> dict:
    if not isinstance(row, dict):
        return row
    account_id = row.get("account_id")
    if account_id and row.get("user_id") is None:
        resolved = _get_discord_user_for_account(account_id)
        if resolved is not None:
            log_legacy_schema_fallback(
                logger,
                module=__name__,
                table="tournament_bets",
                field="user_id",
                action="replace_with_account_id_column",
                continue_execution=True,
                account_id=account_id,
                fallback_field="user_id",
            )
            return {**row, "user_id": resolved}
    return row


def _normalize_bet_rows(rows: List[dict]) -> List[dict]:
    return [_normalize_bet_row(r) for r in (rows or [])]


def create_tournament_record(
    t_type: str,
    size: int,
    start_time: Optional[str] = None,
    author_id: Optional[int] = None,
    team_auto: bool | None = None,
    name: Optional[str] = None,
) -> int:
    """Создаёт запись о новом турнире и возвращает его ID."""
    payload = {"type": t_type, "size": size, "status": "registration"}
    if start_time:
        payload["start_time"] = start_time
    if author_id is not None:
        payload["author_id"] = author_id
    if name:
        payload["name"] = name
    global _has_team_auto
    if team_auto is not None and _has_team_auto:
        payload["team_auto"] = team_auto
    try:
        res = supabase.table("tournaments").insert(payload).execute()
    except APIError as e:
        if (
            _has_team_auto
            and "team_auto" in str(e)
            and getattr(e, "code", "") == "PGRST204"
        ):
            logger.warning("'team_auto' column missing in tournaments table")
            payload.pop("team_auto", None)
            _has_team_auto = False
            res = supabase.table("tournaments").insert(payload).execute()
        else:
            raise
    return res.data[0]["id"]


def add_discord_participant(
    tournament_id: int,
    discord_user_id: int,
    team_id: Optional[int] = None,
    team_name: Optional[str] = None,
) -> bool:
    """Для саморегистрации участника (по Discord ID)."""
    global _has_tp_player_id, _has_tp_account_id, _has_tp_discord_user_id
    payload = {
        "tournament_id": tournament_id,
        "confirmed": False,
    }
    account_id = _get_account_id_for_discord_user(discord_user_id)
    if _has_tp_account_id and account_id:
        payload["account_id"] = account_id
    elif _has_tp_discord_user_id:
        log_legacy_schema_fallback(
            logger,
            module=__name__,
            table="tournament_participants",
            field="discord_user_id",
            action="replace_with_account_id_column",
            continue_execution=True,
            tournament_id=tournament_id,
        )
        payload["discord_user_id"] = discord_user_id

    if _has_tp_player_id:
        payload["player_id"] = None
    if team_id is not None:
        payload["team_id"] = team_id
        payload["team_name"] = team_name
    try:
        res = supabase.table("tournament_participants").insert(payload).execute()
        return bool(res.data)
    except APIError as e:
        if getattr(e, "code", "") == "PGRST204":
            if _has_tp_player_id and "player_id" in str(e):
                logger.warning("'player_id' column missing in tournament_participants table")
                _has_tp_player_id = False
                payload.pop("player_id", None)
                retry = supabase.table("tournament_participants").insert(payload).execute()
                return bool(retry.data)
            if _has_tp_account_id and "account_id" in str(e):
                log_legacy_schema_fallback(
                    logger,
                    module=__name__,
                    table="tournament_participants",
                    field="discord_user_id",
                    action="replace_with_account_id_column",
                    continue_execution=True,
                    tournament_id=tournament_id,
                )
                _has_tp_account_id = False
                payload.pop("account_id", None)
                if _has_tp_discord_user_id:
                    payload["discord_user_id"] = discord_user_id
                retry = supabase.table("tournament_participants").insert(payload).execute()
                return bool(retry.data)
            if _has_tp_discord_user_id and "discord_user_id" in str(e):
                logger.warning("'discord_user_id' column missing in tournament_participants table")
                _has_tp_discord_user_id = False
                payload.pop("discord_user_id", None)
                retry = supabase.table("tournament_participants").insert(payload).execute()
                return bool(retry.data)
        if e.code == "23505":
            return False
        logger.error("add_discord_participant failed: %s", e)
        return False
    except Exception as e:
        logger.exception("Unexpected error in add_discord_participant: %s", e)
        return False


def add_player_participant(
    tournament_id: int,
    player_id: int,
    team_id: Optional[int] = None,
    team_name: Optional[str] = None,
) -> bool:
    """Для админской регистрации (по player_id)."""
    global _has_tp_player_id, _has_tp_account_id, _has_tp_discord_user_id
    payload = {
        "tournament_id": tournament_id,
        "confirmed": True,
    }
    account_id = _get_account_id_for_discord_user(player_id)
    if _has_tp_account_id and account_id:
        payload["account_id"] = account_id
    elif _has_tp_discord_user_id:
        log_legacy_schema_fallback(
            logger,
            module=__name__,
            table="tournament_participants",
            field="player_id",
            action="replace_with_account_id_column",
            continue_execution=True,
            tournament_id=tournament_id,
        )
        payload["discord_user_id"] = player_id

    if _has_tp_player_id:
        payload["player_id"] = player_id
    if team_id is not None:
        payload["team_id"] = team_id
        payload["team_name"] = team_name
    try:
        res = supabase.table("tournament_participants").insert(payload).execute()
        return bool(res.data)
    except APIError as e:
        if getattr(e, "code", "") == "PGRST204":
            if _has_tp_player_id and "player_id" in str(e):
                logger.warning("'player_id' column missing in tournament_participants table")
                _has_tp_player_id = False
                payload.pop("player_id", None)
                retry = supabase.table("tournament_participants").insert(payload).execute()
                return bool(retry.data)
            if _has_tp_account_id and "account_id" in str(e):
                log_legacy_schema_fallback(
                    logger,
                    module=__name__,
                    table="tournament_participants",
                    field="discord_user_id",
                    action="replace_with_account_id_column",
                    continue_execution=True,
                    tournament_id=tournament_id,
                )
                _has_tp_account_id = False
                payload.pop("account_id", None)
                if _has_tp_discord_user_id:
                    payload["discord_user_id"] = player_id
                retry = supabase.table("tournament_participants").insert(payload).execute()
                return bool(retry.data)
            if _has_tp_discord_user_id and "discord_user_id" in str(e):
                logger.warning("'discord_user_id' column missing in tournament_participants table")
                _has_tp_discord_user_id = False
                payload.pop("discord_user_id", None)
                retry = supabase.table("tournament_participants").insert(payload).execute()
                return bool(retry.data)
        if e.code == "23505":
            return False
        logger.error("add_player_participant failed: %s", e)
        return False
    except Exception as e:
        logger.exception("Unexpected error in add_player_participant: %s", e)
        return False


def list_participants(tournament_id: int) -> List[dict]:
    """
    Возвращает список участников как словари с полями
    {account_id, discord_user_id, player_id}.
    """
    global _has_tp_player_id, _has_tp_account_id, _has_tp_discord_user_id

    select_fields = _participants_select_fields()
    if _has_tp_account_id:
        select_fields = f"account_id,{select_fields}"

    try:
        res = (
            supabase.table("tournament_participants")
            .select(select_fields)
            .eq("tournament_id", tournament_id)
            .execute()
        )
    except APIError as e:
        if getattr(e, "code", "") == "PGRST204":
            if _has_tp_player_id and "player_id" in str(e):
                logger.warning("'player_id' column missing in tournament_participants table")
                _has_tp_player_id = False
                return list_participants(tournament_id)
            if _has_tp_account_id and "account_id" in str(e):
                logger.warning("'account_id' column missing in tournament_participants table")
                _has_tp_account_id = False
                return list_participants(tournament_id)
            if _has_tp_discord_user_id and "discord_user_id" in str(e):
                logger.warning("'discord_user_id' column missing in tournament_participants table")
                _has_tp_discord_user_id = False
                return list_participants(tournament_id)
        logger.error("list_participants failed: %s", e)
        return []
    except Exception as e:
        logger.exception("Unexpected error in list_participants: %s", e)
        return []

    rows = _normalize_participant_rows(res.data or [])
    normalized = []
    for row in rows:
        if row.get("discord_user_id") is None and row.get("account_id"):
            resolved = _get_discord_user_for_account(row.get("account_id"))
            if resolved is not None:
                log_legacy_schema_fallback(
                    logger,
                    module=__name__,
                    table="tournament_participants",
                    field="discord_user_id",
                    action="replace_with_account_id_column",
                    continue_execution=True,
                    account_id=row.get("account_id"),
                )
                row = {**row, "discord_user_id": resolved}
        normalized.append(row)
    return normalized


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


def get_match(match_id: int) -> Optional[dict]:
    """Возвращает запись матча по ID или ``None``."""
    try:
        res = (
            supabase.table("tournament_matches")
            .select("id, tournament_id, round_number, player1_id, player2_id, result")
            .eq("id", match_id)
            .single()
            .execute()
        )
        return res.data if res and res.data else None
    except Exception as e:
        logger.error(f"Failed to get match {match_id}: {e}")
        return None


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
    except Exception as e:
        logger.error("Failed to get map image url: %s", e)
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
    except Exception as e:
        logger.error("Failed to get map info: %s", e)
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
    """Удаляет турнир и все связанные с ним записи и логи."""
    # Удаляем результаты
    supabase.table("tournament_results").delete().eq(
        "tournament_id", tournament_id
    ).execute()
    # Удаляем матчи
    supabase.table("tournament_matches").delete().eq(
        "tournament_id", tournament_id
    ).execute()
    # Удаляем ставки
    supabase.table("tournament_bets").delete().eq(
        "tournament_id", tournament_id
    ).execute()
    # Очищаем банк ставок
    supabase.table("tournament_bet_bank").delete().eq(
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
    except Exception as e:
        logger.error("Failed to delete tournament_players links: %s", e)
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
    except Exception as e:
        logger.error("Failed to save tournament result: %s", e)
        return False


def get_tournament_result(tournament_id: int) -> Optional[dict]:
    """Возвращает результат турнира или None."""
    try:
        res = (
            supabase.table("tournament_results")
            .select("first_place_id, second_place_id, third_place_id, finished_at")
            .eq("tournament_id", tournament_id)
            .single()
            .execute()
        )
        return res.data or None
    except Exception as e:
        logger.error("Failed to get tournament result: %s", e)
        return None


def delete_match_records(tournament_id: int) -> bool:
    """Удаляет все записи матчей указанного турнира."""
    try:
        supabase.table("tournament_matches").delete().eq(
            "tournament_id", tournament_id
        ).execute()
        return True
    except Exception as e:
        logger.error("Failed to delete match records: %s", e)
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
    except Exception as e:
        logger.error("Failed to update tournament status: %s", e)
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
    return list_participants(tournament_id)


def get_team_info(tournament_id: int) -> tuple[Dict[int, List[int]], Dict[int, str]]:
    """Возвращает отображение team_id->участники и их названия."""
    global _has_tp_player_id
    try:
        res = (
            supabase.table("tournament_participants")
            .select(_participants_team_select_fields())
            .eq("tournament_id", tournament_id)
            .not_.is_("team_id", "null")
            .execute()
        )
        rows = _normalize_participant_rows(res.data or [])
    except APIError as e:
        if _has_tp_player_id and "player_id" in str(e) and getattr(e, "code", "") == "PGRST204":
            logger.warning("'player_id' column missing in tournament_participants table")
            _has_tp_player_id = False
            res = (
                supabase.table("tournament_participants")
                .select(_participants_team_select_fields())
                .eq("tournament_id", tournament_id)
                .not_.is_("team_id", "null")
                .execute()
            )
            rows = _normalize_participant_rows(res.data or [])
        else:
            logger.error("get_team_info failed: %s", e)
            return {}, {}
    mapping: Dict[int, List[int]] = {}
    names: Dict[int, str] = {}
    for row in rows:
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
    except Exception as e:
        logger.error("Failed to update team name: %s", e)
        return False


def remove_player_from_tournament(player_id: int, tournament_id: int) -> bool:
    """
    Удаляет связь игрока (по player_id) с турниром.
    """
    global _has_tp_player_id
    if not _has_tp_player_id:
        return remove_discord_participant(tournament_id, player_id)
    try:
        res = (
            supabase.table("tournament_participants")
            .delete()
            .eq("player_id", player_id)
            .eq("tournament_id", tournament_id)
            .execute()
        )
        return bool(res.data)
    except APIError as e:
        if _has_tp_player_id and "player_id" in str(e) and getattr(e, "code", "") == "PGRST204":
            logger.warning("'player_id' column missing in tournament_participants table")
            _has_tp_player_id = False
            return remove_discord_participant(tournament_id, player_id)
        logger.error("remove_player_from_tournament failed: %s", e)
        return False
    except Exception as e:
        logger.error("Unexpected error in remove_player_from_tournament: %s", e)
        return False


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
    except Exception as e:
        logger.error("Failed to confirm participant: %s", e)
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


def get_team_auto(tournament_id: int) -> bool:
    """Возвращает True, если турнир использует авто-команды."""
    global _has_team_auto
    if not _has_team_auto:
        return False
    try:
        res = (
            supabase.table("tournaments")
            .select("team_auto")
            .eq("id", tournament_id)
            .single()
            .execute()
        )
        return bool(res.data.get("team_auto")) if res and res.data else False
    except APIError as e:
        if "team_auto" in str(e) and getattr(e, "code", "") == "PGRST204":
            logger.warning("'team_auto' column missing when reading tournament")
            _has_team_auto = False
            return False
        return False
    except Exception as e:
        logger.error("Failed to get team auto flag: %s", e)
        return False


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
            logger.warning(
                "start_time or reminder_sent column not found in tournaments table"
            )
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


def get_status_message_id(tournament_id: int) -> Optional[int]:
    """Возвращает ID сообщения со статусом турнира."""
    global _has_status_msg
    if not _has_status_msg:
        return None
    try:
        res = (
            supabase.table("tournaments")
            .select("status_message_id")
            .eq("id", tournament_id)
            .single()
            .execute()
        )
        return res.data.get("status_message_id") if res and res.data else None
    except APIError as e:
        if "status_message_id" in str(e) and getattr(e, "code", "") == "PGRST204":
            logger.warning("'status_message_id' column missing when reading tournament")
            _has_status_msg = False
            return None
        return None
    except Exception as e:
        logger.error("Failed to get status message id: %s", e)
        return None


def save_status_message(tournament_id: int, message_id: int) -> bool:
    """Сохраняет ID сообщения со статусом турнира."""
    global _has_status_msg
    if not _has_status_msg:
        return False
    try:
        res = (
            supabase.table("tournaments")
            .update({"status_message_id": message_id})
            .eq("id", tournament_id)
            .execute()
        )
        return bool(res.data)
    except APIError as e:
        if "status_message_id" in str(e) and getattr(e, "code", "") == "PGRST204":
            logger.warning("'status_message_id' column missing when saving tournament")
            _has_status_msg = False
            return False
        return False
    except Exception as e:
        logger.error("Failed to save status message: %s", e)
        return False


def get_tournament_info(tournament_id: int) -> Optional[dict]:
    """Возвращает основные поля турнира или None."""
    global _has_team_auto
    fields = "type, size, bank_type, manual_amount, status, start_time, name"
    if _has_team_auto:
        fields += ", team_auto"
    try:
        res = (
            supabase.table("tournaments")
            .select(fields)
            .eq("id", tournament_id)
            .single()
            .execute()
        )
        return res.data or None
    except APIError as e:
        if _has_team_auto and "team_auto" in str(e):
            logger.warning("'team_auto' column missing when fetching tournament info")
            _has_team_auto = False
            try:
                res = (
                    supabase.table("tournaments")
                    .select("type, size, bank_type, manual_amount, status, start_time, name")
                    .eq("id", tournament_id)
                    .single()
                    .execute()
                )
                return res.data or None
            except Exception as e:
                logger.error("Failed to fetch fallback tournament info: %s", e)
                return None
        return None
    except Exception as e:
        logger.error("Failed to get tournament info: %s", e)
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
    except Exception as e:
        logger.error("Failed to get tournament author: %s", e)
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
    except Exception as e:
        logger.error("Failed to set tournament author: %s", e)
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
    try:
        res = (
            supabase.table("tournaments")
            .select("id, author_id, start_time")
            .eq("status", "registration")
            .lte("start_time", now.isoformat())
            .execute()
        )
        return res.data or []
    except APIError as e:
        if getattr(e, "code", None) == "42703" and "author_id" in str(e):
            logger.warning(
                "author_id column missing in tournaments table; fallback to id,start_time only"
            )
            try:
                res = (
                    supabase.table("tournaments")
                    .select("id, start_time")
                    .eq("status", "registration")
                    .lte("start_time", now.isoformat())
                    .execute()
                )
                return res.data or []
            except Exception:
                logger.exception("Failed to fetch expired registrations in fallback mode")
                return []
        logger.exception("Failed to fetch expired registrations")
        return []
    except Exception:
        logger.exception("Unexpected error while fetching expired registrations")
        return []


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
    except Exception as e:
        logger.error("Failed to update start time: %s", e)
        return False


def update_tournament_name(tournament_id: int, new_name: str) -> bool:
    """Обновляет название турнира."""
    try:
        res = (
            supabase.table("tournaments")
            .update({"name": new_name})
            .eq("id", tournament_id)
            .execute()
        )
        return bool(res.data)
    except Exception as e:
        logger.error("Failed to update tournament name: %s", e)
        return False


def update_tournament_size(tournament_id: int, new_size: int) -> bool:
    """Обновляет максимальное число участников."""
    try:
        res = (
            supabase.table("tournaments")
            .update({"size": new_size})
            .eq("id", tournament_id)
            .execute()
        )
        return bool(res.data)
    except Exception as e:
        logger.error("Failed to update tournament size: %s", e)
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


# ---------------------------------------------------------------------------
# Betting helpers
# ---------------------------------------------------------------------------


def create_bet_by_account(
    tournament_id: int,
    round_no: int,
    pair_index: int,
    account_id: str,
    bet_on: int,
    amount: float,
    discord_user_id: int | None = None,
) -> int | None:
    """Creates a bet record and returns its ID."""
    global _has_tb_account_id, _has_tb_user_id
    payload = {
        "tournament_id": tournament_id,
        "round": round_no,
        "pair_index": pair_index,
        "bet_on": bet_on,
        "amount": amount,
    }
    if _has_tb_account_id and account_id:
        payload["account_id"] = account_id
    elif _has_tb_user_id and discord_user_id is not None:
        log_legacy_schema_fallback(
            logger,
            module=__name__,
            table="tournament_bets",
            field="user_id",
            action="replace_with_account_id_column",
            continue_execution=True,
            tournament_id=tournament_id,
            round=round_no,
            recommended_field="account_id",
        )
        payload["user_id"] = discord_user_id

    try:
        res = (
            supabase.table("tournament_bets")
            .insert(payload, returning="representation")
            .execute()
        )
        return res.data[0]["id"] if res.data else None
    except APIError as e:
        if getattr(e, "code", "") == "PGRST204":
            if _has_tb_account_id and "account_id" in str(e):
                log_legacy_schema_fallback(
                    logger,
                    module=__name__,
                    table="tournament_bets",
                    field="user_id",
                    action="replace_with_account_id_column",
                    continue_execution=True,
                    tournament_id=tournament_id,
                    round=round_no,
                    recommended_field="account_id",
                )
                _has_tb_account_id = False
                payload.pop("account_id", None)
                if _has_tb_user_id and discord_user_id is not None:
                    payload["user_id"] = discord_user_id
                retry = supabase.table("tournament_bets").insert(payload, returning="representation").execute()
                return retry.data[0]["id"] if retry.data else None
            if _has_tb_user_id and "user_id" in str(e):
                logger.warning("'user_id' column missing in tournament_bets table")
                _has_tb_user_id = False
                payload.pop("user_id", None)
                retry = supabase.table("tournament_bets").insert(payload, returning="representation").execute()
                return retry.data[0]["id"] if retry.data else None
        logger.error("Failed to create bet: %s", e)
        return None
    except Exception as e:
        logger.exception("Failed to create bet: %s", e)
        return None


def create_bet(
    tournament_id: int,
    round_no: int,
    pair_index: int,
    user_id: int,
    bet_on: int,
    amount: float,
) -> int | None:
    account_id = _get_account_id_for_discord_user(user_id)
    return create_bet_by_account(
        tournament_id,
        round_no,
        pair_index,
        account_id or "",
        bet_on,
        amount,
        discord_user_id=user_id,
    )


def list_bets(tournament_id: int, round_no: int | None = None) -> list[dict]:
    """Returns bets for a tournament (optionally filtered by round)."""
    query = (
        supabase.table("tournament_bets").select("*").eq("tournament_id", tournament_id)
    )
    if round_no is not None:
        query = query.eq("round", round_no)
    try:
        res = query.execute()
        return _normalize_bet_rows(res.data or [])
    except Exception as e:
        logger.error("Failed to list bets: %s", e)
        return []


def close_bet(bet_id: int, won: bool, payout: float) -> bool:
    """Updates bet result and payout."""
    try:
        res = (
            supabase.table("tournament_bets")
            .update({"won": won, "payout": payout})
            .eq("id", bet_id)
            .execute()
        )
        return bool(res.data)
    except Exception as e:
        logger.error("Failed to close bet: %s", e)
        return False


def get_bet(bet_id: int) -> dict | None:
    """Возвращает ставку по ID или None."""
    try:
        res = (
            supabase.table("tournament_bets")
            .select("*")
            .eq("id", bet_id)
            .single()
            .execute()
        )
        return _normalize_bet_row(res.data) if res and res.data else None
    except Exception as e:
        logger.error("Failed to get bet: %s", e)
        return None


def list_user_bets_by_account(
    tournament_id: int, account_id: str, open_only: bool = True
) -> list[dict]:
    """Возвращает ставки пользователя на турнир по account_id."""
    query = (
        supabase.table("tournament_bets")
        .select("*")
        .eq("tournament_id", tournament_id)
        .eq("account_id", str(account_id))
    )
    if open_only:
        query = query.is_("won", None)
    try:
        res = query.execute()
        return _normalize_bet_rows(res.data or [])
    except Exception as e:
        logger.error("Failed to list user bets by account: %s", e)
        return []


def list_user_bets(
    tournament_id: int, user_id: int, open_only: bool = True
) -> list[dict]:
    """Возвращает ставки пользователя на турнир."""
    account_id = _get_account_id_for_discord_user(user_id)
    if account_id:
        return list_user_bets_by_account(tournament_id, account_id, open_only=open_only)
    log_identity_resolve_error(
        logger,
        module=__name__,
        handler="list_user_bets",
        field="user_id",
        action="resolve_account_id",
        continue_execution=True,
        tournament_id=tournament_id,
        user_id=user_id,
    )
    log_legacy_schema_fallback(
        logger,
        module=__name__,
        table="tournament_bets",
        field="user_id",
        action="replace_with_account_id_column",
        continue_execution=True,
        tournament_id=tournament_id,
        user_id=user_id,
        recommended_field="account_id",
    )
    query = (
        supabase.table("tournament_bets")
        .select("*")
        .eq("tournament_id", tournament_id)
        .eq("user_id", user_id)
    )
    if open_only:
        query = query.is_("won", None)
    try:
        res = query.execute()
        return _normalize_bet_rows(res.data or [])
    except Exception as e:
        logger.error("Failed to list user bets: %s", e)
        return []


def update_bet(bet_id: int, bet_on: int, amount: float) -> bool:
    """Обновляет ставку."""
    try:
        res = (
            supabase.table("tournament_bets")
            .update({"bet_on": bet_on, "amount": amount})
            .eq("id", bet_id)
            .execute()
        )
        return bool(res.data)
    except Exception as e:
        logger.error("Failed to update bet: %s", e)
        return False


def delete_bet(bet_id: int) -> bool:
    """Удаляет ставку."""
    try:
        supabase.table("tournament_bets").delete().eq("id", bet_id).execute()
        return True
    except Exception as e:
        logger.error("Failed to delete bet: %s", e)
        return False


# ---------------------------------------------------------------------------
# Bet bank helpers
# ---------------------------------------------------------------------------


def create_bet_bank(tournament_id: int, amount: float) -> bool:
    """Creates or resets bet bank for a tournament."""
    try:
        supabase.table("tournament_bet_bank").upsert(
            {"tournament_id": tournament_id, "balance": amount},
            on_conflict="tournament_id",
        ).execute()
        return True
    except Exception as e:
        logger.error("Failed to create bet bank: %s", e)
        return False


def get_bet_bank(tournament_id: int) -> float:
    """Returns current bet bank balance."""
    try:
        # .single() вызывает ошибку, если строки нет.
        # Берём первую запись вручную, чтобы просто вернуть 0 при отсутствии данных.
        res = (
            supabase.table("tournament_bet_bank")
            .select("balance")
            .eq("tournament_id", tournament_id)
            .limit(1)
            .execute()
        )
        if res and res.data:
            return float(res.data[0].get("balance", 0))
    except Exception as e:
        logger.error("Failed to get bet bank: %s", e)
    return 0.0


def update_bet_bank(tournament_id: int, delta: float) -> bool:
    """Adds delta to bet bank balance."""
    current = get_bet_bank(tournament_id)
    new_balance = current + delta
    try:
        supabase.table("tournament_bet_bank").upsert(
            {"tournament_id": tournament_id, "balance": new_balance},
            on_conflict="tournament_id",
        ).execute()
        return True
    except Exception as e:
        logger.error("Failed to update bet bank: %s", e)
        return False


def close_bet_bank(tournament_id: int) -> float:
    """Deletes bet bank entry and returns remaining balance."""
    balance = get_bet_bank(tournament_id)
    try:
        supabase.table("tournament_bet_bank").delete().eq(
            "tournament_id", tournament_id
        ).execute()
    except Exception as e:
        logger.error("Failed to close bet bank: %s", e)
    return balance
