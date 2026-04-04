-- 005_inspection_detail.sql — Tables for inspection + violation detail views

-- Per-inspection summary (one row per OSHA inspection, linked to employer)
CREATE TABLE IF NOT EXISTS inspection_detail (
    activity_nr         TEXT PRIMARY KEY,
    employer_id         VARCHAR NOT NULL,
    employer_name       TEXT,
    site_address        TEXT,
    site_city           TEXT,
    site_state          TEXT,
    zip5                TEXT,
    open_date           DATE,
    close_case_date     DATE,
    insp_type           TEXT,
    violation_count     INTEGER DEFAULT 0,
    serious_count       INTEGER DEFAULT 0,
    willful_count       INTEGER DEFAULT 0,
    repeat_count        INTEGER DEFAULT 0,
    other_count         INTEGER DEFAULT 0,
    total_penalties     NUMERIC(12,2) DEFAULT 0,
    avg_gravity         NUMERIC(4,2)
);

CREATE INDEX IF NOT EXISTS idx_insp_detail_employer ON inspection_detail (employer_id, open_date DESC);
CREATE INDEX IF NOT EXISTS idx_insp_detail_date ON inspection_detail (open_date DESC);

-- Per-violation detail (one row per citation)
CREATE TABLE IF NOT EXISTS violation_detail (
    id                  BIGSERIAL PRIMARY KEY,
    activity_nr         TEXT NOT NULL,
    citation_id         TEXT,
    viol_type           TEXT,
    gravity             INTEGER,
    nr_instances        INTEGER,
    initial_penalty     NUMERIC(12,2),
    current_penalty     NUMERIC(12,2),
    abate_date          DATE,
    issuance_date       DATE
);

CREATE INDEX IF NOT EXISTS idx_viol_detail_activity ON violation_detail (activity_nr);
