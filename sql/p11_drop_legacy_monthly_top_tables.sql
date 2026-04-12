-- Удаляем устаревшие таблицы месячных топов: данные больше не используются, хранение ведётся в актуальных таблицах.
DROP TABLE IF EXISTS monthly_top_log;
DROP TABLE IF EXISTS monthly_fine_hst;
