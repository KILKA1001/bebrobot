-- P3: account-first schema hardening (phase rollout, no UX changes)
-- Run in maintenance window.

BEGIN;

-- 1) Identity uniqueness
CREATE UNIQUE INDEX IF NOT EXISTS ux_account_identities_provider_user
  ON account_identities(provider, provider_user_id);

-- 2) Ensure account_id exists in hot tables
ALTER TABLE IF EXISTS scores ADD COLUMN IF NOT EXISTS account_id uuid;
ALTER TABLE IF EXISTS actions ADD COLUMN IF NOT EXISTS account_id uuid;
ALTER TABLE IF EXISTS ticket_actions ADD COLUMN IF NOT EXISTS account_id uuid;
ALTER TABLE IF EXISTS bank_history ADD COLUMN IF NOT EXISTS account_id uuid;
ALTER TABLE IF EXISTS fines ADD COLUMN IF NOT EXISTS account_id uuid;
ALTER TABLE IF EXISTS fine_payments ADD COLUMN IF NOT EXISTS account_id uuid;

-- 3) Hot-path indexes
CREATE INDEX IF NOT EXISTS ix_scores_account_id ON scores(account_id);
CREATE INDEX IF NOT EXISTS ix_actions_account_id ON actions(account_id);
CREATE INDEX IF NOT EXISTS ix_ticket_actions_account_id ON ticket_actions(account_id);
CREATE INDEX IF NOT EXISTS ix_bank_history_account_id ON bank_history(account_id);
CREATE INDEX IF NOT EXISTS ix_fines_account_id ON fines(account_id);
CREATE INDEX IF NOT EXISTS ix_fine_payments_account_id ON fine_payments(account_id);

-- 4) Backfill account_id from discord identities
UPDATE scores s
SET account_id = ai.account_id
FROM account_identities ai
WHERE s.account_id IS NULL
  AND ai.provider = 'discord'
  AND ai.provider_user_id = s.user_id::text;

UPDATE actions a
SET account_id = ai.account_id
FROM account_identities ai
WHERE a.account_id IS NULL
  AND ai.provider = 'discord'
  AND ai.provider_user_id = a.user_id::text;

UPDATE ticket_actions ta
SET account_id = ai.account_id
FROM account_identities ai
WHERE ta.account_id IS NULL
  AND ai.provider = 'discord'
  AND ai.provider_user_id = ta.user_id::text;

UPDATE bank_history bh
SET account_id = ai.account_id
FROM account_identities ai
WHERE bh.account_id IS NULL
  AND ai.provider = 'discord'
  AND ai.provider_user_id = bh.user_id::text;

UPDATE fines f
SET account_id = ai.account_id
FROM account_identities ai
WHERE f.account_id IS NULL
  AND ai.provider = 'discord'
  AND ai.provider_user_id = f.user_id::text;

UPDATE fine_payments fp
SET account_id = ai.account_id
FROM account_identities ai
WHERE fp.account_id IS NULL
  AND ai.provider = 'discord'
  AND ai.provider_user_id = fp.user_id::text;

COMMIT;

-- 5) Readiness checks (must be 0 before strict-mode)
-- SELECT 'scores' AS table_name, COUNT(*) AS missing FROM scores WHERE account_id IS NULL;
-- SELECT 'actions' AS table_name, COUNT(*) AS missing FROM actions WHERE account_id IS NULL;
-- SELECT 'ticket_actions' AS table_name, COUNT(*) AS missing FROM ticket_actions WHERE account_id IS NULL;
-- SELECT 'bank_history' AS table_name, COUNT(*) AS missing FROM bank_history WHERE account_id IS NULL;
-- SELECT 'fines' AS table_name, COUNT(*) AS missing FROM fines WHERE account_id IS NULL;
-- SELECT 'fine_payments' AS table_name, COUNT(*) AS missing FROM fine_payments WHERE account_id IS NULL;

-- 6) Strict-mode (enable table-by-table only after readiness checks)
-- ALTER TABLE scores ALTER COLUMN account_id SET NOT NULL;
-- ALTER TABLE actions ALTER COLUMN account_id SET NOT NULL;
-- ALTER TABLE ticket_actions ALTER COLUMN account_id SET NOT NULL;
-- ALTER TABLE bank_history ALTER COLUMN account_id SET NOT NULL;
-- ALTER TABLE fines ALTER COLUMN account_id SET NOT NULL;
-- ALTER TABLE fine_payments ALTER COLUMN account_id SET NOT NULL;
