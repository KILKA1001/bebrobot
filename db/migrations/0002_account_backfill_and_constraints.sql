-- Phase 2: backfill account_id for legacy Discord data and validate constraints.
-- Safe to run after creating accounts/account_identities/link_tokens and adding nullable account_id columns.

create extension if not exists pgcrypto;

-- 1) Build deterministic discord_id -> account_id map for missing identities.
create temporary table tmp_discord_account_map (
    discord_id text primary key,
    account_id uuid not null
) on commit drop;

insert into tmp_discord_account_map(discord_id, account_id)
select legacy.discord_id, gen_random_uuid()
from (
    select distinct user_id::text as discord_id
    from public.scores
    where user_id is not null
    union
    select distinct user_id::text as discord_id
    from public.actions
    where user_id is not null
    union
    select distinct discord_user_id::text as discord_id
    from public.tournament_participants
    where discord_user_id is not null
) legacy
where not exists (
    select 1
    from public.account_identities ai
    where ai.provider = 'discord'
      and ai.provider_user_id = legacy.discord_id
);

insert into public.accounts(id, status)
select account_id, 'active'
from tmp_discord_account_map;

insert into public.account_identities(account_id, provider, provider_user_id, is_verified, verified_at)
select account_id, 'discord', discord_id, true, now()
from tmp_discord_account_map;

-- 2) Backfill account_id in hot tables.
update public.scores s
set account_id = ai.account_id
from public.account_identities ai
where ai.provider = 'discord'
  and ai.provider_user_id = s.user_id::text
  and s.account_id is null;

update public.actions a
set account_id = ai.account_id
from public.account_identities ai
where ai.provider = 'discord'
  and ai.provider_user_id = a.user_id::text
  and a.account_id is null;

-- Adjust mapping below if fines/fine_payments use another legacy id column.
-- Example assumes fines.user_id and fine_payments.user_id are legacy Discord ids.
update public.fines f
set account_id = ai.account_id
from public.account_identities ai
where ai.provider = 'discord'
  and ai.provider_user_id = f.user_id::text
  and f.account_id is null;

update public.fine_payments fp
set account_id = ai.account_id
from public.account_identities ai
where ai.provider = 'discord'
  and ai.provider_user_id = fp.user_id::text
  and fp.account_id is null;

update public.tournament_participants tp
set account_id = ai.account_id
from public.account_identities ai
where ai.provider = 'discord'
  and ai.provider_user_id = tp.discord_user_id::text
  and tp.account_id is null;

-- 3) Post-backfill checks (should be 0 before strict mode).
-- select count(*) from public.scores where account_id is null;
-- select count(*) from public.actions where account_id is null;
-- select count(*) from public.fines where account_id is null;
-- select count(*) from public.fine_payments where account_id is null;
-- select count(*) from public.tournament_participants where account_id is null;

-- 4) Validate FKs once backfill is complete.
alter table public.scores validate constraint fk_scores_account;
alter table public.actions validate constraint fk_actions_account;
alter table public.fines validate constraint fk_fines_account;
alter table public.fine_payments validate constraint fk_fine_payments_account;
alter table public.tournament_participants validate constraint fk_tournament_participants_account;

-- 5) Optional strict mode (enable later, not in initial rollout):
-- alter table public.scores alter column account_id set not null;
-- alter table public.actions alter column account_id set not null;
-- alter table public.fines alter column account_id set not null;
-- alter table public.fine_payments alter column account_id set not null;
