from datetime import datetime, timezone
import pytz

MOSCOW_TZ = pytz.timezone("Europe/Moscow")
TIME_FORMAT = "%H:%M (%d.%m.%Y)"
DATE_FORMAT = "%d.%m.%Y"

def format_moscow_time(dt: datetime | None = None) -> str:
    """Return formatted time in Moscow timezone."""
    if dt is None:
        dt = datetime.now(timezone.utc)
    return dt.astimezone(MOSCOW_TZ).strftime(TIME_FORMAT)

def format_moscow_date(dt: datetime | None = None) -> str:
    """Return formatted date in Moscow timezone."""
    if dt is None:
        dt = datetime.now(timezone.utc)
    return dt.astimezone(MOSCOW_TZ).strftime(DATE_FORMAT)
