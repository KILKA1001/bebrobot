-- P8: role change audit log for admin role management operations

BEGIN;

CREATE TABLE IF NOT EXISTS role_change_audit (
    id bigserial PRIMARY KEY,
    actor_user_id text NOT NULL,
    target_user_id text NOT NULL,
    action text NOT NULL,
    role_id text NOT NULL,
    source text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    reason text
);

CREATE INDEX IF NOT EXISTS idx_role_change_audit_target_created_at
    ON role_change_audit (target_user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_role_change_audit_actor_created_at
    ON role_change_audit (actor_user_id, created_at DESC);

COMMIT;
