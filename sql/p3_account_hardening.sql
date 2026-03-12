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

-- 7) Reliability hardening for points/tickets (idempotent + row-level lock)
-- Requires pgcrypto for gen_random_uuid().
CREATE EXTENSION IF NOT EXISTS pgcrypto;

ALTER TABLE IF EXISTS actions
  ADD COLUMN IF NOT EXISTS op_key uuid;

CREATE UNIQUE INDEX IF NOT EXISTS ux_actions_op_key
  ON actions(op_key)
  WHERE op_key IS NOT NULL;

CREATE OR REPLACE FUNCTION public.apply_points_action(
  p_account_id uuid,
  p_user_id bigint,
  p_delta numeric,
  p_reason text,
  p_author_id bigint,
  p_op_key uuid DEFAULT gen_random_uuid()
)
RETURNS TABLE(applied boolean, new_points numeric)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_points numeric;
BEGIN
  IF p_account_id IS NULL AND p_user_id IS NULL THEN
    RAISE EXCEPTION 'Either p_account_id or p_user_id must be provided';
  END IF;

  -- Idempotency: do nothing if operation with same key already exists.
  IF p_op_key IS NOT NULL AND EXISTS (
    SELECT 1 FROM actions a WHERE a.op_key = p_op_key
  ) THEN
    RETURN QUERY
    SELECT false,
           COALESCE(
             (SELECT s.points FROM scores s WHERE s.account_id = p_account_id LIMIT 1),
             (SELECT s.points FROM scores s WHERE s.user_id = p_user_id LIMIT 1),
             0
           );
    RETURN;
  END IF;

  -- Ensure score row exists.
  INSERT INTO scores (account_id, user_id, points, tickets_normal, tickets_gold)
  VALUES (p_account_id, p_user_id, 0, 0, 0)
  ON CONFLICT (account_id) DO NOTHING;

  INSERT INTO scores (user_id, points, tickets_normal, tickets_gold)
  SELECT p_user_id, 0, 0, 0
  WHERE p_account_id IS NULL
  ON CONFLICT (user_id) DO NOTHING;

  -- Lock the row and apply delta without going below zero.
  SELECT s.points
    INTO v_points
  FROM scores s
  WHERE (p_account_id IS NOT NULL AND s.account_id = p_account_id)
     OR (p_account_id IS NULL AND s.user_id = p_user_id)
  FOR UPDATE;

  v_points := GREATEST(COALESCE(v_points, 0) + p_delta, 0);

  UPDATE scores s
     SET points = v_points,
         user_id = COALESCE(p_user_id, s.user_id)
   WHERE (p_account_id IS NOT NULL AND s.account_id = p_account_id)
      OR (p_account_id IS NULL AND s.user_id = p_user_id);

  INSERT INTO actions (
    account_id,
    user_id,
    points,
    reason,
    author_id,
    action_type,
    op_key,
    timestamp
  )
  VALUES (
    p_account_id,
    p_user_id,
    p_delta,
    p_reason,
    p_author_id,
    CASE WHEN p_delta < 0 THEN 'remove' ELSE 'add' END,
    p_op_key,
    NOW()
  );

  RETURN QUERY SELECT true, v_points;
END;
$$;

-- 8) Full reset template (points/tickets/fines/bank/tops).
-- Run manually in maintenance mode when you need a total wipe.
-- BEGIN;
-- UPDATE scores
--   SET points = 0,
--       tickets_normal = 0,
--       tickets_gold = 0;
--
-- DELETE FROM ticket_actions;
-- DELETE FROM actions;
-- DELETE FROM fine_payments;
-- DELETE FROM fines;
-- DELETE FROM bank_history;
-- DELETE FROM monthly_top_log;
-- DELETE FROM monthly_fine_hst;
--
-- INSERT INTO bank(id, total)
-- VALUES (1, 0)
-- ON CONFLICT (id)
-- DO UPDATE SET total = EXCLUDED.total;
-- COMMIT;
