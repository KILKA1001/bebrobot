# Account-first Migration Runbook (P3)

## 1) Preconditions
- Deploy code with account-aware reads/writes (P1/P2 done).
- Ensure `account_identities` has unique `(provider, provider_user_id)`.
- Announce maintenance window for strict-mode ALTERs.

## 2) Hardening rollout
1. Execute `sql/p3_account_hardening.sql`.
2. Run readiness checks (NULL `account_id` counters by table).
3. If all zeros, enable strict mode (`SET NOT NULL`) per table.
4. Re-run checks and verify no write failures.

## 3) Operations
### Merge duplicate accounts
1. Choose target `account_id` (canonical) and source `account_id` (to merge).
2. Repoint identities:
   - `UPDATE account_identities SET account_id = :target WHERE account_id = :source;`
3. Repoint hot tables:
   - `scores/actions/ticket_actions/bank_history/fines/fine_payments` set `account_id=:target` where `:source`.
4. Aggregate score/ticket values in `scores` to a single target row.
5. Remove obsolete source rows after validation.

### Unlink / relink provider
- Unlink: delete a specific row from `account_identities` by `(provider, provider_user_id)`.
- Relink: use Discord `/link_telegram` + Telegram `/link <code>` flow.

## 4) Monitoring
Track:
- Link success rate = `link_consume_success / (link_consume_success + link_consume_fail)`
- Identity resolve errors = `identity_resolve_errors`
- Fallback share = `operations_without_account_id / (operations_with_account_id + operations_without_account_id)`

Source of counters: in-process metrics from data/service layer logs.

## 5) Rollback
1. Keep NOT NULL changes for tables already clean; rollback code first if runtime issues.
2. If required, temporarily remove strict constraints for affected table:
   - `ALTER TABLE <table> ALTER COLUMN account_id DROP NOT NULL;`
3. Continue writes in fallback mode (`user_id`) while investigating.
4. Re-run migration checks before re-enabling strict mode.
