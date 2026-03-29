-- p10_shop_items.sql
-- Витрина магазина ролей + аудит административных изменений.

CREATE TABLE IF NOT EXISTS public.shop_items (
  id BIGSERIAL PRIMARY KEY,
  category_code TEXT NOT NULL,
  role_name TEXT NOT NULL,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  base_price_points INTEGER NOT NULL DEFAULT 0,
  display_position INTEGER NOT NULL DEFAULT 0,
  sale_price_points INTEGER,
  sale_starts_at TIMESTAMPTZ,
  sale_ends_at TIMESTAMPTZ,
  updated_by TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT shop_items_unique_category_role UNIQUE (category_code, role_name),
  CONSTRAINT shop_items_price_non_negative CHECK (base_price_points >= 0),
  CONSTRAINT shop_items_sale_price_non_negative CHECK (sale_price_points IS NULL OR sale_price_points >= 0),
  CONSTRAINT shop_items_sale_period_valid CHECK (
    (sale_starts_at IS NULL AND sale_ends_at IS NULL)
    OR (sale_starts_at IS NOT NULL AND sale_ends_at IS NOT NULL AND sale_starts_at <= sale_ends_at)
  )
);

CREATE INDEX IF NOT EXISTS idx_shop_items_category_active_position
  ON public.shop_items (category_code, is_active, display_position, id);

COMMENT ON TABLE public.shop_items IS 'Витрина магазина: роли и цены/акции для Telegram+Discord.';
COMMENT ON COLUMN public.shop_items.category_code IS 'Код категории витрины, сейчас используется roles.';
COMMENT ON COLUMN public.shop_items.role_name IS 'Каноническое имя роли из таблицы roles.';
