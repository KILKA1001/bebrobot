"""
Назначение: модуль "points service" реализует продуктовый контур в зоне общая логика.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: общая логика.
Доменные операции: операции баллов, начислений и списаний.
"""

import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from bot.data import db
from bot.legacy_identity_logging import (
    log_identity_resolve_error,
    log_legacy_identity_path_detected,
)
from .accounts_service import AccountsService


logger = logging.getLogger(__name__)


class PointsService:
    LEADERBOARD_PERIOD_ALL = "all"
    LEADERBOARD_PERIOD_MONTH = "month"
    LEADERBOARD_PERIOD_WEEK = "week"
    LEADERBOARD_PERIOD_DAYS = {
        LEADERBOARD_PERIOD_WEEK: 7,
        LEADERBOARD_PERIOD_MONTH: 30,
    }

    @staticmethod
    def _resolve_anchor_user_id(account_id: str) -> int | None:
        if not account_id:
            return None
        discord_user_id = db._get_discord_user_for_account_id(account_id)
        if discord_user_id is not None:
            return int(discord_user_id)
        if not db.supabase:
            return None
        try:
            response = (
                db.supabase.table("account_identities")
                .select("provider_user_id")
                .eq("account_id", str(account_id))
                .order("provider")
                .limit(1)
                .execute()
            )
            if response.data:
                return int(response.data[0]["provider_user_id"])
        except Exception:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("identity_resolve_errors")
        return None

    @staticmethod
    def add_points_by_identity(provider: str, provider_user_id: str, points: float, reason: str, author_id: int) -> bool:
        log_legacy_identity_path_detected(
            logger,
            module=__name__,
            handler="PointsService.add_points_by_identity",
            field=f"{provider}_user_id" if provider in {"telegram", "discord"} else "provider_user_id",
            action="resolve_account_id",
            continue_execution=True,
            provider=provider,
        )
        account_id = AccountsService.resolve_account_id(provider, str(provider_user_id))
        if not account_id:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("identity_resolve_errors")
            log_identity_resolve_error(
                logger,
                module=__name__,
                handler="PointsService.add_points_by_identity",
                field=f"{provider}_user_id" if provider in {"telegram", "discord"} else "provider_user_id",
                action="resolve_account_id",
                continue_execution=False,
                provider=provider,
                provider_user_id=provider_user_id,
            )
            return False
        author_account_id = AccountsService.resolve_account_id(provider, str(author_id))
        if not author_account_id:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("identity_resolve_errors")
            log_identity_resolve_error(
                logger,
                module=__name__,
                handler="PointsService.add_points_by_identity",
                field=f"{provider}_user_id" if provider in {"telegram", "discord"} else "provider_user_id",
                action="resolve_account_id",
                continue_execution=False,
                provider=provider,
                author_id=author_id,
                target="author_account_id",
            )
            return False
        return PointsService.add_points_by_account(account_id, points, reason, author_account_id)

    @staticmethod
    def remove_points_by_identity(provider: str, provider_user_id: str, points: float, reason: str, author_id: int) -> bool:
        log_legacy_identity_path_detected(
            logger,
            module=__name__,
            handler="PointsService.remove_points_by_identity",
            field=f"{provider}_user_id" if provider in {"telegram", "discord"} else "provider_user_id",
            action="resolve_account_id",
            continue_execution=True,
            provider=provider,
        )
        account_id = AccountsService.resolve_account_id(provider, str(provider_user_id))
        if not account_id:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("identity_resolve_errors")
            log_identity_resolve_error(
                logger,
                module=__name__,
                handler="PointsService.remove_points_by_identity",
                field=f"{provider}_user_id" if provider in {"telegram", "discord"} else "provider_user_id",
                action="resolve_account_id",
                continue_execution=False,
                provider=provider,
                provider_user_id=provider_user_id,
            )
            return False
        author_account_id = AccountsService.resolve_account_id(provider, str(author_id))
        if not author_account_id:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("identity_resolve_errors")
            log_identity_resolve_error(
                logger,
                module=__name__,
                handler="PointsService.remove_points_by_identity",
                field=f"{provider}_user_id" if provider in {"telegram", "discord"} else "provider_user_id",
                action="resolve_account_id",
                continue_execution=False,
                provider=provider,
                author_id=author_id,
                target="author_account_id",
            )
            return False
        return PointsService.remove_points_by_account(account_id, points, reason, author_account_id)

    @staticmethod
    def add_points(discord_user_id: int, points: float, reason: str, author_id: int) -> bool:
        return PointsService.add_points_by_identity("discord", str(discord_user_id), points, reason, author_id)

    @staticmethod
    def remove_points(discord_user_id: int, points: float, reason: str, author_id: int) -> bool:
        return PointsService.remove_points_by_identity("discord", str(discord_user_id), points, reason, author_id)

    @staticmethod
    def get_leaderboard_entries(period: str = LEADERBOARD_PERIOD_ALL) -> list[tuple[int, float]]:
        normalized_period = str(period or PointsService.LEADERBOARD_PERIOD_ALL).strip().lower()
        if normalized_period in PointsService.LEADERBOARD_PERIOD_DAYS:
            days = int(PointsService.LEADERBOARD_PERIOD_DAYS[normalized_period])
            return PointsService._get_scores_by_range(days)
        return PointsService._get_all_time_scores()

    @staticmethod
    def _get_all_time_scores() -> list[tuple[int, float]]:
        return sorted(((int(user_id), float(points)) for user_id, points in db.scores.items()), key=lambda item: item[1], reverse=True)

    @staticmethod
    def _get_scores_by_range(days: int) -> list[tuple[int, float]]:
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=int(days))
        temp_scores = defaultdict(float)
        for entry in db.actions:
            if entry.get("is_undo"):
                continue
            ts = entry.get("timestamp")
            if not ts:
                continue
            if isinstance(ts, str):
                try:
                    ts = datetime.fromisoformat(ts)
                except Exception:
                    continue
            if not isinstance(ts, datetime):
                continue
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            if ts >= cutoff:
                try:
                    user_id = int(entry["user_id"])
                    temp_scores[user_id] += float(entry.get("points") or 0)
                except (TypeError, ValueError, KeyError):
                    continue
        return sorted(temp_scores.items(), key=lambda item: item[1], reverse=True)

    @staticmethod
    def add_points_by_account(account_id: str, points: float, reason: str, author_account_id: str) -> bool:
        return db.add_action_by_account(account_id, points, reason, author_account_id)

    @staticmethod
    def remove_points_by_account(account_id: str, points: float, reason: str, author_account_id: str) -> bool:
        return db.add_action_by_account(account_id, -points, reason, author_account_id)
