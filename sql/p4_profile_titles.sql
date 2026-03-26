-- P4: profile titles storage in unified account
BEGIN;

ALTER TABLE IF EXISTS accounts ADD COLUMN IF NOT EXISTS titles text[] DEFAULT '{}'::text[];
ALTER TABLE IF EXISTS accounts ADD COLUMN IF NOT EXISTS titles_updated_at timestamptz;
ALTER TABLE IF EXISTS accounts ADD COLUMN IF NOT EXISTS titles_source text;

COMMIT;

-- NOTE:
-- Authority/modeartion checks read titles directly from accounts.titles.
-- Discord role mapping via profile_title_roles is optional and needed only
-- when you want automatic sync from guild roles.
--
-- If you need точечно назначить звания без привязки к Discord role ID,
-- use direct account updates, for example:
--
-- 1) Add one title to a specific account (keeps existing titles):
-- UPDATE accounts
-- SET
--   titles = (
--     SELECT ARRAY(
--       SELECT DISTINCT t
--       FROM unnest(coalesce(accounts.titles, '{}'::text[]) || ARRAY['Админ']) AS t
--       WHERE nullif(trim(t), '') IS NOT NULL
--     )
--   ),
--   titles_source = 'manual_sql',
--   titles_updated_at = timezone('utc', now())
-- WHERE id = '00000000-0000-0000-0000-000000000000';
--
-- 2) Add several titles to a specific account:
-- UPDATE accounts
-- SET
--   titles = (
--     SELECT ARRAY(
--       SELECT DISTINCT t
--       FROM unnest(coalesce(accounts.titles, '{}'::text[]) || ARRAY['Оператор', 'Главный вице']) AS t
--       WHERE nullif(trim(t), '') IS NOT NULL
--     )
--   ),
--   titles_source = 'manual_sql',
--   titles_updated_at = timezone('utc', now())
-- WHERE id = '00000000-0000-0000-0000-000000000000';
