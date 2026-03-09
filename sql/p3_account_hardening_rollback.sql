-- P3 rollback: relax strict mode if runtime issues are detected.
-- Use only for affected tables.

BEGIN;

ALTER TABLE IF EXISTS scores ALTER COLUMN account_id DROP NOT NULL;
ALTER TABLE IF EXISTS actions ALTER COLUMN account_id DROP NOT NULL;
ALTER TABLE IF EXISTS ticket_actions ALTER COLUMN account_id DROP NOT NULL;
ALTER TABLE IF EXISTS bank_history ALTER COLUMN account_id DROP NOT NULL;
ALTER TABLE IF EXISTS fines ALTER COLUMN account_id DROP NOT NULL;
ALTER TABLE IF EXISTS fine_payments ALTER COLUMN account_id DROP NOT NULL;

COMMIT;
