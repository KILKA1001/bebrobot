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
-- Example template for your titles:
-- INSERT INTO profile_title_roles (discord_role_id, title_name)
-- VALUES
--   (<ID_РОЛИ_ГЛАВА_КЛУБА>, 'Глава клуба'),
--   (<ID_РОЛИ_ГЛАВНЫЙ_ВИЦЕ>, 'Главный вице'),
--   (<ID_РОЛИ_ВИЦЕ_ГОРОДА>, 'Вице города'),
--   (<ID_РОЛИ_ВЕТЕРАН_ГОРОДА>, 'Ветеран города'),
--   (<ID_РОЛИ_УЧАСТНИК_КЛУБОВ>, 'Участник клубов')
-- ON CONFLICT (discord_role_id) DO UPDATE
-- SET title_name = EXCLUDED.title_name,
--     is_active = true,
--     updated_at = timezone('utc', now());
