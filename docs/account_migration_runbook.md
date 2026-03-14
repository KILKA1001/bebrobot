# Account-first Migration Runbook (P3)

## 1) Preconditions
- Runtime is account-aware for reads/writes (P1/P2 complete).
- `account_identities` enforces uniqueness on `(provider, provider_user_id)`.
- Maintenance window is planned for strict-mode toggles.

## 2) Hardening rollout
1. Run `sql/p3_account_hardening.sql`.
2. Run readiness checks for NULL `account_id` in hot tables.
3. If all checks are zero, enable strict mode (`SET NOT NULL`) table-by-table.
4. Verify writes continue successfully and monitor fallback/identity metrics.

## 3) Legacy cleanup policy
Remove legacy `user_id` fallback paths only when:
- all critical operations run stably via `account_id`,
- monitoring shows no unresolved identity spikes,
- operational rollback is tested.


### Drop legacy players tables (safe order)
If you are decommissioning manual player registry, drop dependent FKs first, then legacy tables:

```sql
BEGIN;

-- 0) Optional guardrails
-- SELECT COUNT(*) FROM tournament_participants WHERE player_id IS NOT NULL;
-- SELECT COUNT(*) FROM tournament_players WHERE player_id IS NOT NULL;

-- 1) Remove FK dependencies on players(id)
ALTER TABLE IF EXISTS tournament_players
  DROP CONSTRAINT IF EXISTS tournament_players_player_id_fkey;

ALTER TABLE IF EXISTS tournament_participants
  DROP CONSTRAINT IF EXISTS tournament_participants_player_id_fkey;

-- 2) Remove obsolete legacy columns
ALTER TABLE IF EXISTS tournament_players
  DROP COLUMN IF EXISTS player_id;

ALTER TABLE IF EXISTS tournament_participants
  DROP COLUMN IF EXISTS player_id;

-- 3) Remove legacy tables
DROP TABLE IF EXISTS player_logs;
DROP TABLE IF EXISTS players;

COMMIT;
```

If historical compatibility is still required, skip step (2) and keep legacy columns until all code paths are migrated to `discord_user_id`/`account_id`.

## 4) Operations
### Merge duplicate accounts
1. Select canonical `account_id` (target) and deprecated `account_id` (source).
2. Repoint identities:
   - `UPDATE account_identities SET account_id = :target WHERE account_id = :source;`
3. Repoint hot tables:
   - `scores/actions/ticket_actions/bank_history/fines/fine_payments` set `account_id=:target` where `:source`.
4. Consolidate duplicated `scores` rows (sum points/tickets).
5. Validate and remove obsolete source rows.

### Unlink / relink
- Unlink: remove row in `account_identities` by `(provider, provider_user_id)`.
- Relink: Discord `/link_telegram` -> Telegram `/link <code>`.

## 5) Monitoring
Track:
- link success/fail rate (`link_consume_success`, `link_consume_fail`),
- operations without resolved `account_id` (`operations_without_account_id`),
- identity resolution errors (`identity_resolve_errors`),
- unlink success/fail (`unlink_success`, `unlink_fail`).

## 5.1) Optional parity projection table (P7)
When support requests mention "Telegram linked but Discord not linked" (or vice versa),
create `sql/p7_account_links_registry.sql` and query one projection row per account:

```sql
SELECT
  account_id,
  telegram_user_id,
  discord_user_id,
  (telegram_user_id IS NOT NULL) AS telegram_linked,
  (discord_user_id IS NOT NULL) AS discord_linked,
  (telegram_user_id IS NOT NULL AND discord_user_id IS NOT NULL) AS fully_linked,
  updated_at
FROM public.account_links_registry
ORDER BY updated_at DESC;
```

The migration also installs a trigger on `account_identities`, so new links/unlinks are
automatically mirrored in the projection table.

Additionally, linking flow writes `last_link_code_used` and `last_link_code_used_at` into
`account_links_registry` after successful code consumption for easier diagnostics.

## 6) Rollback
1. Roll back code if runtime behavior degrades.
2. Run `sql/p3_account_hardening_rollback.sql` (or affected statements only).
3. Continue in fallback mode while investigating.
4. Re-run readiness checks before re-enabling strict mode.
