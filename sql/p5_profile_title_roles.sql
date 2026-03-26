-- P5: explicit Discord role -> profile title mapping table

CREATE TABLE IF NOT EXISTS profile_title_roles (
    id bigserial PRIMARY KEY,
    discord_role_id bigint NOT NULL UNIQUE,
    title_name text NOT NULL,
    is_active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT timezone('utc', now()),
    updated_at timestamptz NOT NULL DEFAULT timezone('utc', now())
);

CREATE INDEX IF NOT EXISTS idx_profile_title_roles_is_active
    ON profile_title_roles (is_active)
    WHERE is_active = true;

-- Fill mapping with your real Discord role IDs.
-- IMPORTANT: placeholders like <ROLE_ID> are not valid SQL syntax.
-- Use numeric IDs directly, or keep NULL::bigint and replace later.
--
-- Executable template:
-- WITH seed(discord_role_id, title_name, is_active) AS (
--   VALUES
--     (NULL::bigint, 'Глава клуба', true),
--     (NULL::bigint, 'Главный вице', true),
--     (NULL::bigint, 'Оператор', true),
--     (NULL::bigint, 'Вице города', true),
--     (NULL::bigint, 'Админ', true),
--     (NULL::bigint, 'Младший админ', true),
--     (NULL::bigint, 'Ветеран города', true),
--     (NULL::bigint, 'Участник клубов', true)
-- )
-- INSERT INTO profile_title_roles (discord_role_id, title_name, is_active)
-- SELECT discord_role_id, title_name, is_active
-- FROM seed
-- WHERE discord_role_id IS NOT NULL
-- ON CONFLICT (discord_role_id) DO UPDATE
-- SET
--   title_name = EXCLUDED.title_name,
--   is_active = EXCLUDED.is_active,
--   updated_at = timezone('utc', now());
