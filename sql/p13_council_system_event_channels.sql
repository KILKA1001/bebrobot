-- P13: канал системных событий Совета по платформам.
-- Позволяет суперадмину задать отдельный канал для автопубликаций:
-- старт выборов, промежуточные изменения, итоги, старт обсуждения, старт голосования.

CREATE TABLE IF NOT EXISTS council_system_event_channels (
    provider TEXT PRIMARY KEY CHECK (provider IN ('telegram', 'discord')),
    destination_id TEXT NOT NULL,
    updated_by_user_id TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_council_system_event_channels_updated_at
    ON council_system_event_channels (updated_at DESC);
