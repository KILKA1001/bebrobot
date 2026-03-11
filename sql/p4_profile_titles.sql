-- P4: profile titles storage in unified account
BEGIN;

ALTER TABLE IF EXISTS accounts ADD COLUMN IF NOT EXISTS titles text[] DEFAULT '{}'::text[];
ALTER TABLE IF EXISTS accounts ADD COLUMN IF NOT EXISTS titles_updated_at timestamptz;
ALTER TABLE IF EXISTS accounts ADD COLUMN IF NOT EXISTS titles_source text;

COMMIT;
