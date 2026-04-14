-- P14: расширение диагностики каналов системных событий и нормализация audit details.

ALTER TABLE council_system_event_channels
    ADD COLUMN IF NOT EXISTS destination_title TEXT,
    ADD COLUMN IF NOT EXISTS destination_kind TEXT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'chk_council_system_event_channels_destination_kind'
    ) THEN
        ALTER TABLE council_system_event_channels
            ADD CONSTRAINT chk_council_system_event_channels_destination_kind
            CHECK (
                destination_kind IS NULL
                OR destination_kind IN ('chat', 'group', 'channel', 'thread')
            );
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_council_system_event_channels_provider_updated_at
    ON council_system_event_channels(provider, updated_at DESC);

-- Для system_event_channel всегда добавляем action в details,
-- чтобы анализировать аудит без чтения колонок верхнего уровня.
UPDATE council_audit_log
SET details = jsonb_set(COALESCE(details, '{}'::jsonb), '{action}', to_jsonb(action), true)
WHERE entity_type = 'system_event_channel'
  AND (details IS NULL OR NOT (details ? 'action'));
