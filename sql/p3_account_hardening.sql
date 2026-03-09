-- P3: account-first schema hardening (run in maintenance window)
-- Backward-compatible rollout: validate -> backfill -> constraints/indexes.

BEGIN;

-- 1) Ensure identity uniqueness for all providers (discord/telegram/...).
CREATE UNIQUE INDEX IF NOT EXISTS ux_account_identities_provider_user
  ON account_identities(provider, provider_user_id);

-- 2) Hot-path indexes by account_id.
CREATE INDEX IF NOT EXISTS ix_scores_account_id ON scores(account_id);
CREATE INDEX IF NOT EXISTS ix_actions_account_id ON actions(account_id);
CREATE INDEX IF NOT EXISTS ix_ticket_actions_account_id ON ticket_actions(account_id);
CREATE INDEX IF NOT EXISTS ix_bank_history_account_id ON bank_history(account_id);
CREATE INDEX IF NOT EXISTS ix_fines_account_id ON fines(account_id);
CREATE INDEX IF NOT EXISTS ix_fine_payments_account_id ON fine_payments(account_id);

-- 3) Backfill account_id from account_identities for discord-linked rows.
-- scores
UPDATE scores s
SET account_id = ai.account_id
FROM account_identities ai
WHERE s.account_id IS NULL
  AND ai.provider = 'discord'
  AND ai.provider_user_id = s.user_id::text;

-- actions
UPDATE actions a
SET account_id = ai.account_id
FROM account_identities ai
WHERE a.account_id IS NULL
  AND ai.provider = 'discord'
  AND ai.provider_user_id = a.user_id::text;

-- ticket_actions
UPDATE ticket_actions ta
SET account_id = ai.account_id
FROM account_identities ai
WHERE ta.account_id IS NULL
  AND ai.provider = 'discord'
  AND ai.provider_user_id = ta.user_id::text;

-- bank_history
UPDATE bank_history bh
SET account_id = ai.account_id
FROM account_identities ai
WHERE bh.account_id IS NULL
  AND ai.provider = 'discord'
  AND ai.provider_user_id = bh.user_id::text;

-- fines
UPDATE fines f
SET account_id = ai.account_id
FROM account_identities ai
WHERE f.account_id IS NULL
  AND ai.provider = 'discord'
  AND ai.provider_user_id = f.user_id::text;

-- fine_payments
UPDATE fine_payments fp
SET account_id = ai.account_id
FROM account_identities ai
WHERE fp.account_id IS NULL
  AND ai.provider = 'discord'
  AND ai.provider_user_id = fp.user_id::text;

COMMIT;

-- 4) Readiness checks (execute and verify all counters are zero before NOT NULL):
-- SELECT 'scores' AS table_name, COUNT(*) AS missing FROM scores WHERE account_id IS NULL;
-- SELECT 'actions' AS table_name, COUNT(*) AS missing FROM actions WHERE account_id IS NULL;
-- SELECT 'ticket_actions' AS table_name, COUNT(*) AS missing FROM ticket_actions WHERE account_id IS NULL;
-- SELECT 'bank_history' AS table_name, COUNT(*) AS missing FROM bank_history WHERE account_id IS NULL;
-- SELECT 'fines' AS table_name, COUNT(*) AS missing FROM fines WHERE account_id IS NULL;
-- SELECT 'fine_payments' AS table_name, COUNT(*) AS missing FROM fine_payments WHERE account_id IS NULL;

-- 5) Strict mode (run only after checks == 0):
-- ALTER TABLE scores ALTER COLUMN account_id SET NOT NULL;
-- ALTER TABLE actions ALTER COLUMN account_id SET NOT NULL;
-- ALTER TABLE ticket_actions ALTER COLUMN account_id SET NOT NULL;
-- ALTER TABLE bank_history ALTER COLUMN account_id SET NOT NULL;
-- ALTER TABLE fines ALTER COLUMN account_id SET NOT NULL;
-- ALTER TABLE fine_payments ALTER COLUMN account_id SET NOT NULL;
