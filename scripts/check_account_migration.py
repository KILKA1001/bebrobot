"""
Назначение: модуль "check account migration" реализует продуктовый контур в зоне общая логика.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: общая логика.
"""

import os
from supabase import create_client

TABLES = [
    "scores",
    "actions",
    "ticket_actions",
    "bank_history",
    "fines",
    "fine_payments",
]


def main() -> int:
    url = (os.getenv("SUPABASE_URL") or "").strip()
    key = (
        os.getenv("SUPABASE_KEY")
        or os.getenv("SUPABASE_SECRET_KEY")
        or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or ""
    ).strip()
    if not url or not key:
        print("WARN: SUPABASE credentials are not configured")
        return 0

    sb = create_client(url, key)

    print("== account_id NULL audit ==")
    totals = 0
    for table in TABLES:

        try:
            resp = sb.table(table).select("id", count="exact").is_("account_id", "null").limit(1).execute()
            missing = resp.count or 0
            totals += missing
            print(f"{table}: missing_account_id={missing}")
        except Exception as e:
            print(f"{table}: account_id column missing or inaccessible ({e})")

        resp = sb.table(table).select("id", count="exact").is_("account_id", "null").limit(1).execute()
        missing = resp.count or 0
        totals += missing
        print(f"{table}: missing_account_id={missing}")


    print("\n== identity resolve health ==")
    no_identity = 0
    sample = sb.table("account_identities").select("provider, provider_user_id, account_id", count="exact").execute()
    total_identities = sample.count or 0
    print(f"account_identities rows={total_identities}")

    unresolved_scores = (
        sb.table("scores")
        .select("user_id", count="exact")
        .is_("account_id", "null")
        .execute()
    )
    unresolved_count = unresolved_scores.count or 0
    no_identity += unresolved_count
    print(f"legacy-path candidates (scores without account_id)={unresolved_count}")

    print("\n== summary ==")
    print(f"missing_account_id_total={totals}")
    print(f"operations_without_account_id_fallback_baseline={no_identity}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
