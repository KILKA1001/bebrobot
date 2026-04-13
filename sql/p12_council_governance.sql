-- P12: council governance core schema.
-- Цели:
-- 1) Единая модель для созывов, выборов и вопросов.
-- 2) Единые статусы lifecycle (term/election/question).
-- 3) Индексы для частых выборок.
-- 4) Ограничение длины текста вопроса/предложения <= 1000 на уровне БД.

-- ===== Созывы =====
CREATE TABLE IF NOT EXISTS council_terms (
    id BIGSERIAL PRIMARY KEY,
    term_number INTEGER NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'draft'
        CHECK (status IN ('draft', 'pending_launch_confirmation', 'active', 'archived', 'cancelled')),
    starts_at TIMESTAMPTZ,
    ends_at TIMESTAMPTZ,
    launched_by_profile_id UUID,
    created_by_profile_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Частая выборка активного созыва (в идеале 0..1 строка).
CREATE UNIQUE INDEX IF NOT EXISTS uq_council_terms_one_active
    ON council_terms ((status))
    WHERE status = 'active';

CREATE INDEX IF NOT EXISTS idx_council_terms_status_starts_at
    ON council_terms (status, starts_at DESC);

-- ===== Подтверждения запуска созыва =====
CREATE TABLE IF NOT EXISTS council_term_launch_confirmations (
    id BIGSERIAL PRIMARY KEY,
    term_id BIGINT NOT NULL REFERENCES council_terms(id) ON DELETE CASCADE,
    profile_id UUID NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'confirmed', 'rejected', 'cancelled')),
    confirmed_at TIMESTAMPTZ,
    comment TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (term_id, profile_id)
);

CREATE INDEX IF NOT EXISTS idx_term_launch_confirmations_term_status
    ON council_term_launch_confirmations (term_id, status);

-- ===== Участники созыва =====
CREATE TABLE IF NOT EXISTS council_term_members (
    id BIGSERIAL PRIMARY KEY,
    term_id BIGINT NOT NULL REFERENCES council_terms(id) ON DELETE CASCADE,
    profile_id UUID NOT NULL,
    role_code TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    joined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    left_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (term_id, profile_id)
);

-- История по profile_id.
CREATE INDEX IF NOT EXISTS idx_council_term_members_profile_joined_at
    ON council_term_members (profile_id, joined_at DESC);

CREATE INDEX IF NOT EXISTS idx_council_term_members_term_active
    ON council_term_members (term_id, is_active);

-- ===== Выборы =====
CREATE TABLE IF NOT EXISTS council_elections (
    id BIGSERIAL PRIMARY KEY,
    term_id BIGINT NOT NULL REFERENCES council_terms(id) ON DELETE CASCADE,
    role_code TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft'
        CHECK (status IN ('draft', 'nomination', 'voting', 'completed', 'cancelled')),
    nomination_starts_at TIMESTAMPTZ,
    nomination_ends_at TIMESTAMPTZ,
    voting_starts_at TIMESTAMPTZ,
    voting_ends_at TIMESTAMPTZ,
    created_by_profile_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Частая выборка: активные выборы по роли.
CREATE INDEX IF NOT EXISTS idx_council_elections_role_active
    ON council_elections (role_code, voting_ends_at)
    WHERE status IN ('nomination', 'voting');

CREATE INDEX IF NOT EXISTS idx_council_elections_term_status
    ON council_elections (term_id, status);

-- ===== Кандидаты =====
CREATE TABLE IF NOT EXISTS council_election_candidates (
    id BIGSERIAL PRIMARY KEY,
    election_id BIGINT NOT NULL REFERENCES council_elections(id) ON DELETE CASCADE,
    profile_id UUID NOT NULL,
    nomination_text TEXT,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'confirmed', 'rejected', 'withdrawn', 'expired', 'elected', 'not_elected')),
    invite_expires_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() + INTERVAL '24 hours'),
    confirmed_at TIMESTAMPTZ,
    reviewed_by_profile_id UUID,
    reviewed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (election_id, profile_id)
);

CREATE INDEX IF NOT EXISTS idx_council_election_candidates_profile_created_at
    ON council_election_candidates (profile_id, created_at DESC);

-- ===== Голоса выборов =====
CREATE TABLE IF NOT EXISTS council_election_votes (
    id BIGSERIAL PRIMARY KEY,
    election_id BIGINT NOT NULL REFERENCES council_elections(id) ON DELETE CASCADE,
    candidate_id BIGINT NOT NULL REFERENCES council_election_candidates(id) ON DELETE CASCADE,
    voter_profile_id UUID NOT NULL,
    vote_weight NUMERIC(10, 2) NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (election_id, voter_profile_id)
);

CREATE INDEX IF NOT EXISTS idx_council_election_votes_candidate_created_at
    ON council_election_votes (candidate_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_council_election_votes_voter_history
    ON council_election_votes (voter_profile_id, created_at DESC);

-- ===== Вопросы =====
CREATE TABLE IF NOT EXISTS council_questions (
    id BIGSERIAL PRIMARY KEY,
    term_id BIGINT NOT NULL REFERENCES council_terms(id) ON DELETE CASCADE,
    author_profile_id UUID NOT NULL,
    title TEXT NOT NULL,
    question_text TEXT NOT NULL,
    proposal_text TEXT,
    status TEXT NOT NULL DEFAULT 'draft'
        CHECK (status IN ('draft', 'discussion', 'voting', 'decided', 'archived')),
    discussion_ends_at TIMESTAMPTZ,
    voting_ends_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_council_questions_question_text_len CHECK (char_length(question_text) <= 1000),
    CONSTRAINT chk_council_questions_proposal_text_len CHECK (proposal_text IS NULL OR char_length(proposal_text) <= 1000)
);

CREATE INDEX IF NOT EXISTS idx_council_questions_term_status_created_at
    ON council_questions (term_id, status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_council_questions_author_history
    ON council_questions (author_profile_id, created_at DESC);

-- ===== Голоса Совета по вопросам =====
CREATE TABLE IF NOT EXISTS council_question_votes (
    id BIGSERIAL PRIMARY KEY,
    question_id BIGINT NOT NULL REFERENCES council_questions(id) ON DELETE CASCADE,
    voter_profile_id UUID NOT NULL,
    vote_value TEXT NOT NULL CHECK (vote_value IN ('yes', 'no', 'abstain')),
    comment TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (question_id, voter_profile_id)
);

CREATE INDEX IF NOT EXISTS idx_council_question_votes_question_value
    ON council_question_votes (question_id, vote_value);

CREATE INDEX IF NOT EXISTS idx_council_question_votes_voter_history
    ON council_question_votes (voter_profile_id, created_at DESC);

-- ===== Решения =====
CREATE TABLE IF NOT EXISTS council_decisions (
    id BIGSERIAL PRIMARY KEY,
    term_id BIGINT NOT NULL REFERENCES council_terms(id) ON DELETE CASCADE,
    question_id BIGINT REFERENCES council_questions(id) ON DELETE SET NULL,
    decision_code TEXT NOT NULL,
    decision_text TEXT NOT NULL,
    decided_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by_profile_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Частая выборка: архив решений по дате.
CREATE INDEX IF NOT EXISTS idx_council_decisions_archive_date
    ON council_decisions (decided_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_council_decisions_term_date
    ON council_decisions (term_id, decided_at DESC);

-- ===== Аудит-лог =====
CREATE TABLE IF NOT EXISTS council_audit_log (
    id BIGSERIAL PRIMARY KEY,
    term_id BIGINT REFERENCES council_terms(id) ON DELETE SET NULL,
    entity_type TEXT NOT NULL,
    entity_id BIGINT,
    action TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'success' CHECK (status IN ('success', 'failed', 'denied')),
    actor_profile_id UUID,
    source_platform TEXT NOT NULL DEFAULT 'unknown' CHECK (source_platform IN ('telegram', 'discord', 'system', 'unknown')),
    details JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_council_audit_log_entity_created_at
    ON council_audit_log (entity_type, entity_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_council_audit_log_actor_created_at
    ON council_audit_log (actor_profile_id, created_at DESC);
