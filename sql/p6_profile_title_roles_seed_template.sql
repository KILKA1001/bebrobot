-- P6: quick seed template for profile_title_roles
--
-- Как использовать:
-- 1) Замените NULL на реальные числовые ID ролей из Discord.
-- 2) Выполните скрипт в SQL-редакторе Supabase.
--
-- ВАЖНО: бот сам НЕ создаёт справочник званий. Таблицу нужно заполнить вручную
-- (или через этот скрипт), после чего фоновая синхронизация начнёт переносить
-- звания в общий аккаунт пользователей.
--
-- Скрипт валиден даже без подстановки ID: строки с NULL будут пропущены.

WITH seed(discord_role_id, title_name, is_active) AS (
    VALUES
        (NULL::bigint, 'Глава клуба', true),
        (NULL::bigint, 'Главный вице', true),
        (NULL::bigint, 'Вице города', true),
        (NULL::bigint, 'Ветеран города', true),
        (NULL::bigint, 'Участник клубов', true)
)
INSERT INTO profile_title_roles (discord_role_id, title_name, is_active)
SELECT discord_role_id, title_name, is_active
FROM seed
WHERE discord_role_id IS NOT NULL
ON CONFLICT (discord_role_id) DO UPDATE
SET
    title_name = EXCLUDED.title_name,
    is_active = EXCLUDED.is_active,
    updated_at = timezone('utc', now());
