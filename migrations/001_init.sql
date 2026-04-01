-- 001_init.sql — Core schema for Employer Compliance API
-- All tables, indexes, and views required for Phase 1 launch.

------------------------------------------------------------
-- Extensions (must be created by superuser or granted)
------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

------------------------------------------------------------
-- Pipeline monitoring
------------------------------------------------------------
CREATE TABLE pipeline_runs (
    run_id          UUID PRIMARY KEY,
    source          TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'running',
    rows_fetched    INTEGER DEFAULT 0,
    rows_loaded     INTEGER DEFAULT 0,
    error_message   TEXT,
    started_at      TIMESTAMP NOT NULL,
    finished_at     TIMESTAMP
);

CREATE TABLE pipeline_errors (
    id              BIGSERIAL PRIMARY KEY,
    run_id          UUID NOT NULL,
    source          TEXT NOT NULL,
    error_message   TEXT NOT NULL,
    created_at      TIMESTAMP DEFAULT NOW()
);

------------------------------------------------------------
-- Employer data
------------------------------------------------------------
CREATE TABLE employer_profile (
    employer_id             UUID NOT NULL,
    snapshot_date           DATE NOT NULL,
    pipeline_run_id         UUID NOT NULL,

    -- Identity
    employer_name           TEXT NOT NULL,
    ein                     TEXT,
    address                 TEXT,
    city                    TEXT,
    state                   TEXT,
    zip                     TEXT,
    naics_code              TEXT,
    naics_description       TEXT,
    naics_sector            TEXT,

    -- OSHA
    osha_inspections_5yr    INTEGER DEFAULT 0,
    osha_violations_5yr     INTEGER DEFAULT 0,
    osha_serious_willful    INTEGER DEFAULT 0,
    osha_total_penalties    NUMERIC(12,2) DEFAULT 0,
    osha_open_date_latest   DATE,
    osha_avg_gravity        NUMERIC(4,2),

    -- WHD
    whd_cases_5yr           INTEGER DEFAULT 0,
    whd_backwages_total     NUMERIC(12,2) DEFAULT 0,
    whd_ee_violated_total   INTEGER DEFAULT 0,

    -- MSHA (Phase 2)
    msha_violations_5yr     INTEGER DEFAULT 0,
    msha_assessed_penalties NUMERIC(12,2) DEFAULT 0,
    msha_mine_status        TEXT,

    -- EPA ECHO (Phase 2)
    epa_qtrs_noncompliance  INTEGER DEFAULT 0,
    epa_compliance_status   TEXT,
    epa_permits             TEXT[],

    -- FMCSA (Phase 2)
    fmcsa_dot_number        TEXT,
    fmcsa_basics            JSONB,

    -- OFCCP (Phase 3)
    ofccp_evaluations       INTEGER DEFAULT 0,
    ofccp_violations_found  BOOLEAN DEFAULT FALSE,

    -- NLRB (Phase 3)
    nlrb_cases_5yr          INTEGER DEFAULT 0,
    nlrb_case_types         TEXT[],

    -- OFLC (Phase 3)
    oflc_lca_count          INTEGER DEFAULT 0,
    oflc_pw_wage_levels     TEXT[],

    -- Composite risk
    risk_tier               TEXT NOT NULL CHECK (risk_tier IN ('LOW', 'MEDIUM', 'ELEVATED', 'HIGH')),
    risk_score              NUMERIC(5,2),
    risk_flags              TEXT[],
    confidence_tier         TEXT,
    trend_signal            TEXT,

    -- Timestamps
    created_at              TIMESTAMP DEFAULT NOW(),
    updated_at              TIMESTAMP DEFAULT NOW(),

    PRIMARY KEY (employer_id, snapshot_date)
);

-- Indexes for API query patterns
CREATE INDEX idx_ep_employer_name_trgm ON employer_profile USING gin (employer_name gin_trgm_ops);
CREATE INDEX idx_ep_employer_name ON employer_profile (employer_name);
CREATE INDEX idx_ep_ein ON employer_profile (ein);
CREATE INDEX idx_ep_naics ON employer_profile (naics_code);
CREATE INDEX idx_ep_risk_tier ON employer_profile (risk_tier);
CREATE INDEX idx_ep_snapshot ON employer_profile (snapshot_date DESC);
CREATE INDEX idx_ep_employer_snapshot ON employer_profile (employer_id, snapshot_date DESC);

-- Latest-snapshot view — the API queries this by default
CREATE VIEW employer_profile_latest AS
SELECT DISTINCT ON (employer_id) *
FROM employer_profile
ORDER BY employer_id, snapshot_date DESC;

------------------------------------------------------------
-- Entity resolution
------------------------------------------------------------
CREATE TABLE cluster_id_mapping (
    employer_id     UUID NOT NULL,
    cluster_id      TEXT NOT NULL,
    pipeline_run_id UUID NOT NULL,
    first_seen_at   TIMESTAMP DEFAULT NOW(),
    superseded_by   UUID,
    PRIMARY KEY (employer_id, cluster_id)
);
CREATE INDEX idx_cluster_mapping_active ON cluster_id_mapping (employer_id) WHERE superseded_by IS NULL;

------------------------------------------------------------
-- Inspection history
------------------------------------------------------------
CREATE TABLE inspection_history (
    id               BIGSERIAL PRIMARY KEY,
    employer_id      UUID NOT NULL,
    activity_nr      TEXT NOT NULL,
    agency           TEXT NOT NULL DEFAULT 'OSHA',
    inspection_date  DATE,
    insp_type_label  TEXT,
    violations       JSONB,
    snapshot_date    DATE NOT NULL
);
CREATE INDEX idx_insp_employer ON inspection_history (employer_id);

------------------------------------------------------------
-- Auth & billing
------------------------------------------------------------
CREATE TABLE customers (
    id              SERIAL PRIMARY KEY,
    company_name    TEXT NOT NULL,
    email           TEXT NOT NULL UNIQUE,
    password_hash   TEXT NOT NULL,
    role            TEXT DEFAULT 'viewer' CHECK (role IN ('viewer', 'analyst', 'admin')),
    stripe_customer_id TEXT UNIQUE,
    plan            TEXT DEFAULT 'free',
    monthly_limit   INTEGER DEFAULT 5,
    current_usage   INTEGER DEFAULT 0,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE api_keys (
    id              SERIAL PRIMARY KEY,
    key_id          UUID DEFAULT gen_random_uuid() UNIQUE,
    customer_id     INTEGER REFERENCES customers(id) ON DELETE CASCADE,
    key_hash        TEXT NOT NULL,
    key_prefix      TEXT NOT NULL,
    label           TEXT,
    scopes          TEXT[] DEFAULT '{employer:read}',
    monthly_limit   INTEGER NOT NULL DEFAULT 0,
    current_usage   INTEGER DEFAULT 0,
    expires_at      TIMESTAMP,
    status          TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'rotating_out', 'revoked')),
    rotation_expires_at TIMESTAMP,
    last_used_at    TIMESTAMP,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE api_key_audit_log (
    id              BIGSERIAL PRIMARY KEY,
    key_id          UUID NOT NULL REFERENCES api_keys(key_id),
    customer_id     INTEGER REFERENCES customers(id),
    action          TEXT NOT NULL,
    performed_by    TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE api_usage (
    id              BIGSERIAL PRIMARY KEY,
    key_hash        TEXT NOT NULL,
    customer_id     INTEGER REFERENCES customers(id),
    endpoint        TEXT,
    lookup_count    INTEGER DEFAULT 1,
    queried_at      TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_api_usage_key_month ON api_usage (key_hash, queried_at);
CREATE INDEX idx_api_usage_customer ON api_usage (customer_id, queried_at);

------------------------------------------------------------
-- Email & password tokens
------------------------------------------------------------
CREATE TABLE email_verifications (
    id              SERIAL PRIMARY KEY,
    customer_id     INTEGER REFERENCES customers(id) ON DELETE CASCADE,
    token_hash      TEXT NOT NULL,
    expires_at      TIMESTAMP NOT NULL,
    used            BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE password_reset_tokens (
    id              SERIAL PRIMARY KEY,
    customer_id     INTEGER REFERENCES customers(id) ON DELETE CASCADE,
    token_hash      TEXT NOT NULL,
    expires_at      TIMESTAMP NOT NULL,
    used            BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMP DEFAULT NOW()
);

------------------------------------------------------------
-- Stripe
------------------------------------------------------------
CREATE TABLE stripe_webhook_events (
    event_id        TEXT PRIMARY KEY,
    event_type      TEXT NOT NULL,
    processed_at    TIMESTAMP DEFAULT NOW()
);

------------------------------------------------------------
-- Webhook subscriptions
------------------------------------------------------------
CREATE TABLE subscriptions (
    id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    customer_id     INTEGER REFERENCES customers(id),
    employer_ids    UUID[] NOT NULL,
    callback_url    TEXT NOT NULL,
    signing_secret  TEXT NOT NULL,
    events          TEXT[] DEFAULT '{risk_tier_change}',
    status          TEXT DEFAULT 'active' CHECK (status IN ('active', 'paused', 'disabled')),
    created_at      TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_subscriptions_customer ON subscriptions (customer_id);
CREATE INDEX idx_subscriptions_employer_ids ON subscriptions USING GIN (employer_ids);
CREATE INDEX idx_subscriptions_status ON subscriptions (status) WHERE status = 'active';

------------------------------------------------------------
-- Risk snapshots (for /employers/{id}/risk-history)
------------------------------------------------------------
CREATE TABLE risk_snapshots (
    id              BIGSERIAL PRIMARY KEY,
    employer_id     UUID NOT NULL,
    snapshot_date   DATE NOT NULL,
    risk_tier       TEXT NOT NULL,
    risk_score      NUMERIC(5,2),
    confidence_tier TEXT,
    trend_signal    TEXT,
    pipeline_run_id UUID,
    created_at      TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_risk_snap_employer ON risk_snapshots (employer_id, snapshot_date DESC);

------------------------------------------------------------
-- Batch jobs (async processing)
------------------------------------------------------------
CREATE TABLE batch_jobs (
    job_id          UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    customer_id     INTEGER REFERENCES customers(id),
    status          TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
    item_count      INTEGER NOT NULL,
    result_url      TEXT,
    expires_at      TIMESTAMP,
    created_at      TIMESTAMP DEFAULT NOW(),
    completed_at    TIMESTAMP
);

------------------------------------------------------------
-- Feedback
------------------------------------------------------------
CREATE TABLE feedback (
    id              BIGSERIAL PRIMARY KEY,
    employer_id     UUID NOT NULL,
    customer_id     INTEGER REFERENCES customers(id),
    type            TEXT NOT NULL CHECK (type IN ('incorrect_match', 'missing_data', 'wrong_employer', 'other')),
    description     TEXT,
    contact_email   TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);

------------------------------------------------------------
-- Entity resolution review queue
------------------------------------------------------------
CREATE TABLE review_queue (
    id              BIGSERIAL PRIMARY KEY,
    record_id_left  TEXT NOT NULL,
    record_id_right TEXT NOT NULL,
    match_probability NUMERIC(5,4),
    decision        TEXT CHECK (decision IN ('match', 'non_match')),
    reviewed_by     TEXT,
    reviewed_at     TIMESTAMP,
    pipeline_run_id UUID,
    created_at      TIMESTAMP DEFAULT NOW()
);

------------------------------------------------------------
-- Test fixtures
------------------------------------------------------------
CREATE TABLE test_fixtures (
    employer_id     UUID PRIMARY KEY,
    employer_name   TEXT NOT NULL,
    ein             TEXT,
    address         TEXT,
    city            TEXT,
    state           TEXT,
    zip             TEXT,
    naics_code      TEXT,
    naics_description TEXT,
    risk_tier       TEXT NOT NULL CHECK (risk_tier IN ('LOW', 'MEDIUM', 'ELEVATED', 'HIGH')),
    risk_score      NUMERIC(5,2) DEFAULT 0,
    confidence_tier TEXT DEFAULT 'HIGH',
    trend_signal    TEXT DEFAULT 'STABLE',
    osha_inspections_5yr INTEGER DEFAULT 0,
    osha_violations_5yr  INTEGER DEFAULT 0,
    osha_total_penalties NUMERIC(12,2) DEFAULT 0,
    whd_violations_5yr   INTEGER DEFAULT 0,
    response_json   JSONB NOT NULL
);
