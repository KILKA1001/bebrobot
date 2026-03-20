-- P9: expand role_change_audit for cross-platform admin diagnostics and richer incident review.

BEGIN;

ALTER TABLE IF EXISTS role_change_audit
    ALTER COLUMN actor_user_id DROP NOT NULL,
    ALTER COLUMN target_user_id DROP NOT NULL;

ALTER TABLE IF EXISTS role_change_audit
    ADD COLUMN IF NOT EXISTS actor_account_id text,
    ADD COLUMN IF NOT EXISTS actor_provider text,
    ADD COLUMN IF NOT EXISTS actor_provider_user_id text,
    ADD COLUMN IF NOT EXISTS target_account_id text,
    ADD COLUMN IF NOT EXISTS target_provider text,
    ADD COLUMN IF NOT EXISTS target_provider_user_id text,
    ADD COLUMN IF NOT EXISTS role_name text,
    ADD COLUMN IF NOT EXISTS before_value jsonb,
    ADD COLUMN IF NOT EXISTS after_value jsonb,
    ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'success',
    ADD COLUMN IF NOT EXISTS error_code text,
    ADD COLUMN IF NOT EXISTS error_message text;

UPDATE role_change_audit
SET role_name = COALESCE(NULLIF(role_name, ''), NULLIF(role_id, ''))
WHERE role_name IS NULL OR role_name = '';

UPDATE role_change_audit
SET actor_provider_user_id = COALESCE(NULLIF(actor_provider_user_id, ''), NULLIF(actor_user_id, ''))
WHERE actor_provider_user_id IS NULL OR actor_provider_user_id = '';

UPDATE role_change_audit
SET target_provider_user_id = COALESCE(NULLIF(target_provider_user_id, ''), NULLIF(target_user_id, ''))
WHERE target_provider_user_id IS NULL OR target_provider_user_id = '';

UPDATE role_change_audit
SET before_value = COALESCE(before_value, '{}'::jsonb),
    after_value = COALESCE(after_value, '{}'::jsonb)
WHERE before_value IS NULL OR after_value IS NULL;

CREATE INDEX IF NOT EXISTS idx_role_change_audit_status_created_at
    ON role_change_audit (status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_role_change_audit_role_name_created_at
    ON role_change_audit (role_name, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_role_change_audit_target_account_created_at
    ON role_change_audit (target_account_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_role_change_audit_actor_account_created_at
    ON role_change_audit (actor_account_id, created_at DESC);

COMMIT;
