-- P6: custom editable profile fields
BEGIN;

ALTER TABLE IF EXISTS accounts ADD COLUMN IF NOT EXISTS custom_nick text;
ALTER TABLE IF EXISTS accounts ADD COLUMN IF NOT EXISTS profile_description text;
ALTER TABLE IF EXISTS accounts ADD COLUMN IF NOT EXISTS nulls_brawl_id text;
ALTER TABLE IF EXISTS accounts ADD COLUMN IF NOT EXISTS profile_updated_at timestamptz;

COMMIT;
