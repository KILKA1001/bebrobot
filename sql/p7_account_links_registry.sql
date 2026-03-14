-- P7: account link registry for fast Discord/Telegram parity checks.
-- Creates a single-row projection per account and keeps it synced from account_identities.

BEGIN;

CREATE TABLE IF NOT EXISTS public.account_links_registry (
  account_id uuid PRIMARY KEY REFERENCES public.accounts(id) ON DELETE CASCADE,
  telegram_user_id text UNIQUE,
  discord_user_id text UNIQUE,
  telegram_linked_at timestamptz,
  discord_linked_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT timezone('utc', now()),
  updated_at timestamptz NOT NULL DEFAULT timezone('utc', now()),
  last_link_code_used text,
  last_link_code_used_at timestamptz
);

CREATE INDEX IF NOT EXISTS ix_account_links_registry_telegram_user_id
  ON public.account_links_registry(telegram_user_id);

CREATE INDEX IF NOT EXISTS ix_account_links_registry_discord_user_id
  ON public.account_links_registry(discord_user_id);

ALTER TABLE IF EXISTS public.account_links_registry
  ADD COLUMN IF NOT EXISTS last_link_code_used text,
  ADD COLUMN IF NOT EXISTS last_link_code_used_at timestamptz;

CREATE OR REPLACE FUNCTION public.rebuild_account_links_registry_row(p_account_id uuid)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  v_telegram_user_id text;
  v_discord_user_id text;
  v_telegram_linked_at timestamptz;
  v_discord_linked_at timestamptz;
BEGIN
  IF p_account_id IS NULL THEN
    RETURN;
  END IF;

  SELECT
    MAX(provider_user_id) FILTER (WHERE provider = 'telegram'),
    MAX(provider_user_id) FILTER (WHERE provider = 'discord'),
    MAX(created_at) FILTER (WHERE provider = 'telegram'),
    MAX(created_at) FILTER (WHERE provider = 'discord')
  INTO
    v_telegram_user_id,
    v_discord_user_id,
    v_telegram_linked_at,
    v_discord_linked_at
  FROM public.account_identities
  WHERE account_id = p_account_id;

  -- Keep no empty rows: remove projection if account has no supported identities.
  IF v_telegram_user_id IS NULL AND v_discord_user_id IS NULL THEN
    DELETE FROM public.account_links_registry WHERE account_id = p_account_id;
    RETURN;
  END IF;

  INSERT INTO public.account_links_registry (
    account_id,
    telegram_user_id,
    discord_user_id,
    telegram_linked_at,
    discord_linked_at,
    updated_at
  )
  VALUES (
    p_account_id,
    v_telegram_user_id,
    v_discord_user_id,
    v_telegram_linked_at,
    v_discord_linked_at,
    timezone('utc', now())
  )
  ON CONFLICT (account_id)
  DO UPDATE SET
    telegram_user_id = EXCLUDED.telegram_user_id,
    discord_user_id = EXCLUDED.discord_user_id,
    telegram_linked_at = EXCLUDED.telegram_linked_at,
    discord_linked_at = EXCLUDED.discord_linked_at,
    updated_at = timezone('utc', now());
END;
$$;

CREATE OR REPLACE FUNCTION public.sync_account_links_registry_from_identity()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    PERFORM public.rebuild_account_links_registry_row(OLD.account_id);
    RETURN OLD;
  END IF;

  PERFORM public.rebuild_account_links_registry_row(NEW.account_id);

  IF TG_OP = 'UPDATE' AND OLD.account_id IS DISTINCT FROM NEW.account_id THEN
    PERFORM public.rebuild_account_links_registry_row(OLD.account_id);
  END IF;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS tr_sync_account_links_registry_from_identity ON public.account_identities;

CREATE TRIGGER tr_sync_account_links_registry_from_identity
AFTER INSERT OR UPDATE OR DELETE ON public.account_identities
FOR EACH ROW
EXECUTE FUNCTION public.sync_account_links_registry_from_identity();

-- Initial backfill for already linked users.
INSERT INTO public.account_links_registry (
  account_id,
  telegram_user_id,
  discord_user_id,
  telegram_linked_at,
  discord_linked_at,
  updated_at
)
SELECT
  ai.account_id,
  MAX(ai.provider_user_id) FILTER (WHERE ai.provider = 'telegram') AS telegram_user_id,
  MAX(ai.provider_user_id) FILTER (WHERE ai.provider = 'discord') AS discord_user_id,
  MAX(ai.created_at) FILTER (WHERE ai.provider = 'telegram') AS telegram_linked_at,
  MAX(ai.created_at) FILTER (WHERE ai.provider = 'discord') AS discord_linked_at,
  timezone('utc', now()) AS updated_at
FROM public.account_identities ai
WHERE ai.provider IN ('telegram', 'discord')
GROUP BY ai.account_id
HAVING MAX(ai.provider_user_id) FILTER (WHERE ai.provider = 'telegram') IS NOT NULL
    OR MAX(ai.provider_user_id) FILTER (WHERE ai.provider = 'discord') IS NOT NULL
ON CONFLICT (account_id)
DO UPDATE SET
  telegram_user_id = EXCLUDED.telegram_user_id,
  discord_user_id = EXCLUDED.discord_user_id,
  telegram_linked_at = EXCLUDED.telegram_linked_at,
  discord_linked_at = EXCLUDED.discord_linked_at,
  updated_at = timezone('utc', now());

COMMIT;
